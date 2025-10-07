import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, inspect
import os
import requests
import re
import google.generativeai as genai
from mindsql import MindSQL  # NL2SQL gratuito

# --- CONFIGURAÇÃO ---
DB_FILE = 'almg_local.db'
RELATIONS_FILE = "relacoes.txt"
DB_SQLITE = f'sqlite:///{DB_FILE}'

DOWNLOAD_DB_URL = "https://huggingface.co/datasets/TiagoPianezzola/BI/resolve/main/almg_local.db"
DOWNLOAD_RELATIONS_URL = "https://huggingface.co/datasets/TiagoPianezzola/BI/resolve/main/relacoes.txt"

def get_secrets():
    return {
        "gemini_key": st.secrets.get("GOOGLE_API_KEY", ""),
        "hf_token": st.secrets.get("HF_TOKEN", "")
    }

def download_file(url, dest_path, description):
    if os.path.exists(dest_path):
        return True
    secrets = get_secrets()
    hf_token = secrets["hf_token"]
    st.info(f"Iniciando download do {description}...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        if hf_token:
            headers["Authorization"] = f"Bearer {hf_token}"
        response = requests.get(url.strip(), stream=True, headers=headers)
        response.raise_for_status()
        with open(dest_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024*1024): 
                if chunk:
                    f.write(chunk)
        st.success(f"Download do {description} concluído.")
        return True
    except Exception as e:
        st.error(f"Erro no download do {description}: {e}")
        return False

@st.cache_data
def download_database_and_relations():
    db_ok = download_file(DOWNLOAD_DB_URL, DB_FILE, "banco de dados")
    rel_ok = download_file(DOWNLOAD_RELATIONS_URL, RELATIONS_FILE, "arquivo de relações")
    return db_ok and rel_ok

@st.cache_resource
def get_database_engine():
    if not download_database_and_relations():
        return None, "Falha na inicialização.", None
    try:
        engine = create_engine(DB_SQLITE)
        inspector = inspect(engine)
        tabelas = inspector.get_table_names()
        esquema = ""
        for tabela in tabelas:
            if tabela.startswith('sqlite_'):
                continue
            df_cols = pd.read_sql(f"PRAGMA table_info({tabela})", engine)
            colunas_com_tipo = [f"{row['name']} ({row['type']})" for _, row in df_cols.iterrows()]
            esquema += f"Tabela {tabela}:\n- " + "\n- ".join(colunas_com_tipo) + "\n\n"
        return engine, esquema, None 
    except Exception as e:
        return None, f"Erro ao conectar: {e}", None

# --- FUNÇÃO PRINCIPAL ---
def executar_plano_de_analise(engine, esquema, prompt_usuario):
    API_KEY = get_secrets()["gemini_key"]
    if not API_KEY:
        return "Erro: GOOGLE_API_KEY não configurada.", None

    try:
        # --- 1️⃣ Gerar SQL com MindSQL ---
        mindsql = MindSQL(database_type="sqlite", schema_description=esquema)
        query_sql = mindsql.translate(prompt_usuario)
        st.subheader("Query SQL Gerada (MindSQL):")
        st.code(query_sql, language='sql')

        # --- 2️⃣ Executar SQL no SQLite ---
        df_resultado = pd.read_sql(query_sql, engine)

        # --- 3️⃣ Formatação e interpretação com Gemini ---
        genai.configure(api_key=API_KEY)
        model = genai.GenerativeModel('gemini-2.5-flash')

        # Gerar resposta formatada
        instrucao = f"""
Você é um especialista em BI. Recebeu o seguinte resultado do banco de dados:

{df_resultado.head(20).to_string(index=False)}

Explique resumidamente os dados e formate em HTML ou Markdown se possível.
"""
        response = model.generate_content(instrucao)
        resposta_formatada = response.text.strip()

        return "Query executada com sucesso!", resposta_formatada

    except Exception as e:
        return f"Erro ao executar a análise: {e}\n\nQuery: {query_sql}", None

# --- STREAMLIT UI ---
st.title("🤖 Assistente BI da ALMG")

engine, esquema_db, _ = get_database_engine() 
if engine is None:
    st.error(esquema_db)
else:
    prompt_usuario = st.text_area("Faça uma pergunta...", height=100)
    if st.button("Executar Análise"):
        if prompt_usuario:
            with st.spinner("Processando..."):
                mensagem, resultado = executar_plano_de_analise(engine, esquema_db, prompt_usuario) 
                if resultado is not None:
                    st.subheader("Resultado")
                    st.write(resultado, unsafe_allow_html=True)
                st.info(f"Status: {mensagem}")
        else:
            st.warning("Digite uma pergunta.")
