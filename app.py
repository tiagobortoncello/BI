import streamlit as st
import sqlite3
import pandas as pd
import pdfplumber
# Removendo a importação direta, pois o download será feito via requests
# from huggingface_hub import hf_hub_download 
import google.generativeai as genai
import os
from io import StringIO
from pathlib import Path
import requests # NOVO: Para download de arquivos grandes

# --- CONFIGURAÇÃO E VARIÁVEIS ---
# O arquivo será salvo e acessado aqui
DB_FILENAME = "almg_local.db"
# URL para download direto no Hugging Face
DOWNLOAD_DB_URL = "https://huggingface.co/datasets/TiagoPianezzola/BI/resolve/main/almg_local.db"

# --- Configuração de Secrets ---

HF_TOKEN = st.secrets.get("HF_TOKEN", "")
GEMINI_API_KEY = st.secrets.get("GEMINI_API_KEY", "")

if not HF_TOKEN or not GEMINI_API_KEY:
    st.error("ERRO: Configure **HF_TOKEN** e **GEMINI_API_KEY** nos secrets do Streamlit Cloud.")
    st.stop()

# Configurar Gemini
try:
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-2.0-flash')
except Exception as e:
    st.error(f"ERRO ao configurar o Gemini: {e}")
    st.stop()


# --- Gerenciamento de Estado para o Carregamento do DB ---

if 'db_loaded' not in st.session_state:
    st.session_state.db_loaded = False
if 'db_path' not in st.session_state:
    st.session_state.db_path = None
if 'schema_txt' not in st.session_state:
    st.session_state.schema_txt = None
if 'pdf_text' not in st.session_state:
    st.session_state.pdf_text = None


# --- FUNÇÃO CRÍTICA: DOWNLOAD ROBUSTO VIA REQUESTS ---
@st.cache_resource(ttl=None)
def download_db_file(url, filename, token_value):
    """
    Baixa o arquivo DB de 1.32 GB do Hugging Face usando requests (streamed) 
    para maior robustez contra timeouts e estouro de memória, e salva em /tmp.
    """
    db_path = Path("/tmp") / filename
    
    if db_path.exists():
        # Retorna o caminho se já estiver no cache do recurso
        return str(db_path)

    st.info(f"Iniciando download do banco de dados ({filename})...")
    
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        # Adiciona o token de HF para acesso privado, se necessário (embora datasets sejam geralmente públicos)
        if token_value:
             headers['Authorization'] = f'Bearer {token_value}'

        # Usa stream=True para download chunked (proteção contra OOM)
        response = requests.get(url.strip(), stream=True, headers=headers, timeout=3600) # Timeout de 1h
        response.raise_for_status()

        # O Streamlit Cloud já tem um progress bar em st.status, mas vamos garantir o chunking
        with open(db_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024*1024): # 1 MB chunks
                if chunk:
                    f.write(chunk)

        st.success(f"Download do DB concluído e salvo em {db_path}.")
        return str(db_path)
    except Exception as e:
        # Se falhar, limpa o cache para tentar novamente
        if db_path.exists():
             os.remove(db_path)
        raise Exception(f"Erro no download do DB: {e}")


# --- Funções de Carregamento de Recursos (Sem o DB de 1.32 GB) ---

@st.cache_data(show_spinner="Carregando Schema (TXT)")
def load_schema_txt():
    file_name = "armazem_estruturado.txt"
    if not Path(file_name).exists():
        raise FileNotFoundError(f"ERRO: Arquivo **{file_name}** não encontrado no repositório.")
    with open(file_name, "r", encoding="utf-8") as f:
        return f.read()

@st.cache_data(show_spinner="Extraindo Contexto do PDF")
def load_pdf_text():
    file_name = "armazem.pdf"
    if not Path(file_name).exists():
        raise FileNotFoundError(f"ERRO: Arquivo **{file_name}** não encontrado no repositório.")
    pdf_text = ""
    try:
        with pdfplumber.open(file_name) as pdf:
            for page in pdf.pages:
                pdf_text += page.extract_text() or ""
        return pdf_text
    except Exception as e:
        raise Exception(f"ERRO ao processar o PDF. Detalhes: {e}")

# Função para iniciar o download do DB quando o usuário clicar no botão
def start_db_loading():
    try:
        # Carrega arquivos menores primeiro (devem ser rápidos)
        st.session_state.schema_txt = load_schema_txt()
        st.session_state.pdf_text = load_pdf_text()

        # Inicia o download demorado DENTRO da sessão do usuário
        with st.status("Preparando Base de Dados: **Baixando 1.32 GB** (Isto pode levar **vários minutos** na primeira vez, seja paciente)...", expanded=True) as status:
            status.update(label="Iniciando download robusto do banco de dados do Hugging Face...", state="running")
            
            # Chama a função cachêada de download segmentado
            db_path = download_db_file(DOWNLOAD_DB_URL, DB_FILENAME, HF_TOKEN)
            st.session_state.db_path = db_path
            
            status.update(label="Banco de Dados ALMG (1.32 GB) carregado com sucesso!", state="complete")
            st.session_state.db_loaded = True
            st.success("Base de dados pronta. Você já pode fazer suas perguntas!")
            st.rerun() 
            
    except FileNotFoundError as e:
        st.error(f"ERRO no carregamento de arquivos locais: {str(e)}. Verifique se os arquivos estão no seu repositório.")
    except Exception as e:
        st.error(f"ERRO CRÍTICO no download do DB. Detalhes: {e}")
        st.stop()


# --- Funções de Lógica do Chatbot ---

def generate_sql(question, schema_txt, pdf_text):
    prompt = f"""
    Você é um especialista em SQL para o dataset ALMG. Gere UMA query SQL válida e simples para responder à pergunta em linguagem natural.
    
    Schema das tabelas e colunas (use EXATAMENTE essas):
    {schema_txt}
    
    Contexto detalhado das tabelas/colunas (use para escolher o que representa melhor o conceito na pergunta):
    {pdf_text}
    
    Pergunta do usuário: {question}
    
    Regras:
    - Use apenas SELECT (nada de INSERT/UPDATE/DELETE/DROP para segurança).
    - Limite a 100 resultados: adicione LIMIT 100.
    - Seja preciso: associe conceitos da pergunta às colunas certas baseando no contexto.
    - Retorne APENAS a query SQL, sem explicações.
    """
    response = model.generate_content(prompt)
    sql = response.text.strip().strip("```sql").strip("```").strip()
    
    forbidden = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER"]
    if any(word in sql.upper() for word in forbidden):
        raise ValueError("Query SQL inválida ou insegura detectada.")
    return sql

def execute_and_format(sql, db_path, question):
    # Conexão SQLite (agora muito mais estável, pois o download foi robusto)
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
        - Tabela formatada (descreva se necessário).
        - Insights breves se aplicável.
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

# Sidebar com exemplos
with st.sidebar:
    st.header("Exemplos de Perguntas")
    examples = [
        "Quantos deputados há na ALMG?",
        "Qual o gasto total em 2023?",
        "Liste as comissões ativas."
    ]
    for ex in examples:
        if st.button(ex, key=ex) and not st.session_state.db_loaded:
             st.warning("Clique no botão de 'Carregar DB' na área principal para iniciar o download dos dados.")
        elif st.button(ex, key=ex) and st.session_state.db_loaded:
            st.session_state.messages.append({"role": "user", "content": ex})
            st.rerun()


# --- Lógica de Carregamento de Estado (A CORREÇÃO CRÍTICA) ---

if not st.session_state.db_loaded:
    st.warning("O banco de dados de 1.32 GB deve ser baixado antes de usar o chat.")
    st.markdown("⚠️ **Atenção:** O Streamlit Cloud tem um limite de tempo para inicialização. Este arquivo grande pode levar vários minutos para ser baixado. Clique no botão abaixo para iniciar o processo.")
    
    if st.button("🔴 Iniciar Download e Carregamento do DB (1.32 GB)", type="primary"):
        start_db_loading()
else:
    # App fully loaded: Exibir histórico e chat
    
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
                    sql = generate_sql(prompt, st.session_state.schema_txt, st.session_state.pdf_text)
                    st.info(f"**Query SQL gerada:**\n```{sql}```")
                    
                    with st.spinner("Executando e formatando..."):
                        response = execute_and_format(sql, st.session_state.db_path, prompt)
                        st.markdown(response)
                    
                    st.session_state.messages.append({"role": "assistant", "content": response})
                except Exception as e:
                    st.error(f"Erro: {str(e)}. Verifique a pergunta ou logs.")

# Footer
st.markdown("---")
st.caption("App construído com Streamlit + Gemini + HuggingFace. Dataset privado ALMG.")
