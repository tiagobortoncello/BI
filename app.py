import streamlit as st
import sqlite3
import pandas as pd
import pdfplumber
import os
from io import StringIO
from pathlib import Path
import requests 
import json 

# --- CONFIGURAÇÃO E VARIÁVEIS ---
DB_FILENAME = "almg_local.db"
DOWNLOAD_DB_URL = "https://huggingface.co/datasets/TiagoPianezzola/BI/resolve/main/almg_local.db"

# Endpoints da API do Gemini
GEMINI_MODEL = 'gemini-2.5-flash'
GEMINI_ENDPOINT_URL = f'https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent'

# --- Configuração de Secrets ---
HF_TOKEN = st.secrets.get("HF_TOKEN", "")
# ⚠️ AJUSTE CRÍTICO: Usando .strip() para garantir que não haja espaços invisíveis
GEMINI_API_KEY = st.secrets.get("GEMINI_API_KEY", "").strip() 

if not GEMINI_API_KEY:
    st.error("ERRO CRÍTICO: GEMINI_API_KEY está vazia. Verifique se o nome está correto em seu arquivo .streamlit/secrets.toml e se o valor está sem aspas.")
    st.stop()


# --- Gerenciamento de Estado para o Chat (MANTIDO) ---

if 'messages' not in st.session_state:
    st.session_state.messages = []


# --- FUNÇÃO CRÍTICA: DOWNLOAD ROBUSTO VIA REQUESTS (Usando cache) ---
@st.cache_resource(ttl=None)
def download_db_file(url, filename, token_value):
    """ Baixa o arquivo DB de 1.32 GB usando o cache de recurso do Streamlit. """
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


# --- Funções de Carregamento de Recursos de Contexto (MANTIDAS) ---

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

# --- INICIALIZAÇÃO CRÍTICA (DEVE OCORRER NO TOPO DO SCRIPT) ---

schema_txt = load_schema_txt()
pdf_text = load_pdf_text()

try:
    db_path = download_db_file(DOWNLOAD_DB_URL, DB_FILENAME, HF_TOKEN)
except Exception as e:
    st.error(f"Falha Crítica na Inicialização do DB. O aplicativo não pode continuar: {e}")
    st.stop() 


# --- FUNÇÃO PRINCIPAL: Geração da Query SQL (VIA HTTP - CÓPIA DO MÉTODO DE CHAMADA) ---
def generate_sql(question, schema_txt, pdf_text, api_key):
    full_prompt = f"""
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

    url_with_key = f"{GEMINI_ENDPOINT_URL}?key={api_key}"
    
    payload = {
        "contents": [{"parts": [{"text": full_prompt}]}],
        "config": {"temperature": 0.1} 
    }
    
    try:
        # Usamos json=payload (requests cuida da conversão para JSON)
        response = requests.post(url_with_key, json=payload) 
        response.raise_for_status() # Lança erro para 4xx/5xx status codes
        
        result = response.json()
        
        # Extração da resposta mais robusta
        sql = result.get("candidates", [])[0].get("content", {}).get("parts", [])[0].get("text", "")

        if not sql:
             error_msg = result.get('error', {}).get('message', 'A resposta da API está vazia ou bloqueada.')
             raise Exception(f"API Error: {error_msg}")
        
    except requests.exceptions.HTTPError as http_err:
        # Erro específico de autenticação (400) ou servidor (500)
        raise Exception(f"Erro HTTP {http_err.response.status_code}: Verifique a API Key e as permissões.")
    except Exception as e:
        # Outros erros de parsing/conexão
        raise Exception(f"Falha na Comunicação com Gemini: {e}")
        
    # Limpeza da Query
    sql = sql.strip().strip("```sql").strip("```").strip()
    
    forbidden = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER"]
    if any(word in sql.upper() for word in forbidden):
        raise ValueError("Query SQL inválida ou insegura detectada.")
    return sql

# --- Funções de Execução e Formatação (VIA HTTP - CÓPIA DO MÉTODO DE CHAMADA) ---
def execute_and_format(sql, db_path, question, api_key):
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(sql, conn)
        
        if df.empty:
            return "Nenhum resultado encontrado para essa pergunta."
        
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        
        # Chamada para a formatação (também via HTTP direto)
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
        
        url_with_key = f"{GEMINI_ENDPOINT_URL}?key={api_key}"
        payload = {"contents": [{"parts": [{"text": prompt_format}]}]}
        
        response = requests.post(url_with_key, json=payload)
        response.raise_for_status()
        result = response.json()

        # Extração da resposta mais robusta
        formatted = result.get("candidates", [])[0].get("content", {}).get("parts", [])[0].get("text", "Não foi possível formatar a resposta.")

        st.dataframe(df, use_container_width=True)
        return formatted
    except requests.exceptions.HTTPError as http_err:
        st.error(f"Erro HTTP na formatação: {http_err.response.status_code}. Verifique a API Key e as permissões.")
        return "Falha na formatação da resposta devido a um erro da API."
    except Exception as e:
        st.error(f"Erro de Execução/Formatação: {e}")
        return "Falha na execução ou formatação da resposta."
    finally:
        conn.close()


# --- Interface Streamlit Principal (MANTIDA) ---

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
                sql = generate_sql(prompt, schema_txt, pdf_text, GEMINI_API_KEY) 
                st.info(f"**Query SQL gerada:**\n```{sql}```")
                
                with st.spinner("Executando e formatando..."):
                    response = execute_and_format(sql, db_path, prompt, GEMINI_API_KEY)
                    st.markdown(response)
                
                st.session_state.messages.append({"role": "assistant", "content": response})
            except Exception as e:
                error_msg = f"Erro: {str(e)}"
                st.error(error_msg)
                st.session_state.messages.append({"role": "assistant", "content": f"Desculpe, ocorreu um erro: {error_msg}"})


# Footer
st.markdown("---")
st.caption("App construído com Streamlit + Gemini + HuggingFace.")
