import streamlit as st
import sqlite3
import pandas as pd
import pdfplumber
import os
from io import StringIO
from pathlib import Path
import requests 

# --- CAMADA DE SEGURANÇA: INSTALAÇÃO FORÇADA ---
# Isso contorna problemas de cache ou leitura incorreta do requirements.txt no Streamlit Cloud.
try:
    import google.generativeai as genai
except ImportError:
    st.info("Biblioteca 'google-genai' não encontrada. Tentando instalação forçada...")
    import subprocess
    import sys
    try:
        # Usa 'google-genai', que é a versão preferida
        subprocess.check_call([sys.executable, "-m", "pip", "install", "google-genai"])
        st.success("Instalação concluída. Re-executando o aplicativo.")
        import google.generativeai as genai # Tenta importar novamente
        st.experimental_rerun() # Força a re-execução para carregar a biblioteca
    except Exception as e:
        st.error(f"Falha na instalação forçada: {e}")
        st.stop()
# --- FIM CAMADA DE SEGURANÇA ---


# --- CONFIGURAÇÃO E VARIÁVEIS ---
DB_FILENAME = "almg_local.db"
DOWNLOAD_DB_URL = "https://huggingface.co/datasets/TiagoPianezzola/BI/resolve/main/almg_local.db"

# --- Configuração de Secrets ---
HF_TOKEN = st.secrets.get("HF_TOKEN", "")
GEMINI_API_KEY = st.secrets.get("GEMINI_API_KEY", "")

if not GEMINI_API_KEY:
    st.error("ERRO: Configure **GEMINI_API_KEY** nos secrets do Streamlit Cloud.")
    st.stop()

# Configurar Gemini
try:
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-2.5-flash') # Alterado para 2.5-flash, modelo mais recente
except Exception as e:
    st.error(f"ERRO ao configurar o Gemini: {e}")
    st.stop()


# --- Gerenciamento de Estado para o Chat (MANTIDO) ---

if 'messages' not in st.session_state:
    st.session_state.messages = []


# --- FUNÇÃO CRÍTICA: DOWNLOAD ROBUSTO VIA REQUESTS (Usando cache) ---
@st.cache_resource(ttl=None)
def download_db_file(url, filename, token_value):
    db_path = Path("/tmp") / filename
    
    if db_path.exists():
        return str(db_path)

    with st.status("🔴 **Baixando 1.32 GB** (Isto pode levar **vários minutos** na primeira vez)...", expanded=True) as status:
        status.update(label="Iniciando download robusto do banco de dados do Hugging Face...", state="running")
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            if token_value:
                headers['Authorization'] = f'Bearer {token_value}'

            response = requests.get(url.strip(), stream=True, headers=headers, timeout=3600)
            response.raise_for_status()

            with open(db_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024*1024):
                    if chunk:
                        f.write(chunk)

            status.update(label=f"Banco de Dados ALMG (1.32 GB) concluído e salvo em {db_path}.", state="complete")
            return str(db_path)
        except Exception as e:
            if db_path.exists():
                os.remove(db_path)
            status.update(label=f"ERRO CRÍTICO no download do DB: {e}", state="error")
            raise Exception(f"Erro no download do DB: {e}")


# --- Funções de Carregamento de Recursos de Contexto ---

@st.cache_data(show_spinner="Carregando Schema (TXT)")
def load_schema_txt():
    file_name = "armazem_estruturado.txt" 
    if not Path(file_name).exists():
        st.warning(f"Arquivo de schema '{file_name}' não encontrado.")
        return ""
    with open(file_name, "r", encoding="utf-8") as f:
        return f.read()

@st.cache_data(show_spinner="Extraindo Contexto do PDF")
def load_pdf_text():
    file_name = "armazem.pdf" 
    if not Path(file_name).exists():
        st.warning(f"Arquivo de contexto '{file_name}' não encontrado.")
        return ""
    pdf_text = ""
    try:
        with pdfplumber.open(file_name) as pdf:
            for page in pdf.pages:
                pdf_text += page.extract_text() or ""
        return pdf_text
    except Exception as e:
        st.warning(f"ERRO ao processar o PDF: {e}. Prosseguindo sem o contexto do PDF.")
        return ""

# --- INICIALIZAÇÃO CRÍTICA ---
# Força o download do DB e o carregamento do schema e PDF na inicialização.

schema_txt = load_schema_txt()
pdf_text = load_pdf_text()

try:
    # A chamada forçada inicia o processo cacheado.
    db_path = download_db_file(DOWNLOAD_DB_URL, DB_FILENAME, HF_TOKEN)
except Exception as e:
    st.error(f"Falha Crítica na Inicialização do DB. O aplicativo não pode continuar: {e}")
    st.stop() 


# --- Funções de Lógica do Chatbot ---

def generate_sql(question, schema_txt, pdf_text):
    prompt = f"""
    Você é um especialista em SQL para o dataset ALMG. Gere UMA query SQL válida e simples para responder à pergunta em linguagem natural.
    
    Schema das tabelas e colunas (use EXATAMENTE essas):
    {schema_txt}
    
    Contexto detalhado das tabelas/colunas:
    {pdf_text}
    
    Pergunta do usuário: {question}
    
    Regras:
    - Use apenas SELECT.
    - Limite a 100 resultados: adicione LIMIT 100.
    - Retorne APENAS a query SQL, sem explicações.
    """
    response = model.generate_content(prompt)
    sql = response.text.strip().strip("```sql").strip("```").strip()
    
    forbidden = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER"]
    if any(word in sql.upper() for word in forbidden):
        raise ValueError("Query SQL inválida ou insegura detectada.")
    return sql

def execute_and_format(sql, db_path, question):
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(sql, conn)
        
        if df.empty:
            return "Nenhum resultado encontrado para essa pergunta."
        
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        
        prompt_format = f"""
        Formate esta resposta SQL de forma natural e útil em português, como um relatório curto.
        Pergunta original: {question}
        Query executada: {sql}
        Resultados (CSV): {csv_data}
        
        Responda em PT-BR, com:
        - Resumo chave (1-2 frases).
        - Tabela formatada.
        Mantenha conciso.
        """
        response = model.generate_content(prompt_format)
        formatted = response.text
        
        st.dataframe(df, use_container_width=True)
        return formatted
    finally:
        conn.close()


# --- Interface Streamlit Principal ---

st.title("🤖 Assistente BI ALMG - Pergunte em Linguagem Natural")

with st.sidebar:
    st.header("Exemplos de Perguntas")
    examples = [
        "Quantos deputados há na ALMG?",
        "Qual o gasto total em 2023?",
        "Liste as comissões ativas."
    ]
    for ex in examples:
        if st.button(ex, key=ex):
            st.session_state.messages.append({"role": "user", "content": ex})
            st.rerun()

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

if prompt := st.chat_input("Digite sua pergunta sobre os dados ALMG:"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)
    
    with st.chat_message("assistant"):
        with st.spinner("Gerando query SQL..."):
            try:
                sql = generate_sql(prompt, schema_txt, pdf_text) 
                st.info(f"**Query SQL gerada:**\n```{sql}```")
                
                with st.spinner("Executando e formatando..."):
                    response = execute_and_format(sql, db_path, prompt)
                    st.markdown(response)
                
                st.session_state.messages.append({"role": "assistant", "content": response})
            except Exception as e:
                error_msg = f"Erro: {str(e)}. Verifique a pergunta ou logs."
                st.error(error_msg)
                st.session_state.messages.append({"role": "assistant", "content": f"Desculpe, ocorreu um erro: {error_msg}"})


# Footer
st.markdown("---")
st.caption("App construído com Streamlit + Gemini + HuggingFace.")
