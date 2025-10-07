# --- INSTALAÇÃO AUTOMÁTICA DE PACOTES ---
import subprocess
import sys

packages = [
    "streamlit>=1.25.0",
    "pandas>=2.0.3",
    "sqlalchemy>=2.0.20",
    "requests>=2.31.0",
    "google-generativeai>=0.2.0",
    "torch>=2.2.0",
    "transformers>=4.33.0",
    "sentencepiece>=0.1.99",
    "git+https://github.com/mindsql-ai/mindsql.git"
]

for package in packages:
    try:
        __import__(package.split('>=')[0])
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])

# --- IMPORTS ---
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, inspect
import os
import requests
import re
import google.generativeai as genai
from mindsql import MindSQL

# --- CONFIGURAÇÃO ---
DB_FILE = 'almg_local.db'
DB_SQLITE = f'sqlite:///{DB_FILE}'
DOWNLOAD_DB_URL = "https://huggingface.co/datasets/TiagoPianezzola/BI/resolve/main/almg_local.db"

# --- Função para baixar banco ---
def download_file(url, dest_path, description):
    if os.path.exists(dest_path):
        return True
    st.info(f"Baixando {description}...")
    try:
        response = requests.get(url.strip(), stream=True)
        response.raise_for_status()
        with open(dest_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024*1024):
                if chunk:
                    f.write(chunk)
        st.success(f"{description} baixado.")
        return True
    except Exception as e:
        st.error(f"Erro no download do {description}: {e}")
        return False

# --- Conectar ao SQLite ---
@st.cache_resource
def get_database_engine():
    if not download_file(DOWNLOAD_DB_URL, DB_FILE, "banco de dados"):
        return None, "Falha ao baixar banco."
    try:
        engine = create_engine(DB_SQLITE)
        inspector = inspect(engine)
        tabelas = inspector.get_table_names()
        esquema = ""
        colunas_disponiveis = {}
        for tabela in tabelas:
            if tabela.startswith('sqlite_'):
                continue
            df_cols = pd.read_sql(f"PRAGMA table_info({tabela})", engine)
            colunas = [row['name'] for _, row in df_cols.iterrows()]
            colunas_disponiveis[tabela] = colunas
            esquema += f"Tabela {tabela}:\n- " + "\n- ".join(colunas) + "\n\n"
        return engine, esquema, colunas_disponiveis
    except Exception as e:
        return None, f"Erro ao conectar: {e}", None

# --- Validação de colunas ---
def validar_colunas(query_sql, colunas_disponiveis):
    palavras = re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', query_sql)
    erros = []
    todas_colunas = [c for cols in colunas_disponiveis.values() for c in cols]
    for p in palavras:
        if p not in todas_colunas and p.upper() not in ['SELECT','DISTINCT','FROM','JOIN','ON','WHERE','GROUP','BY','ORDER','LIMIT','AS','AND','OR','COUNT']:
            erros.append(p)
    return list(set(erros))

# --- Função principal ---
def executar_analise(engine, esquema, colunas_disponiveis, prompt_usuario):
    API_KEY = st.secrets.get("GOOGLE_API_KEY", "")
    if not API_KEY:
        return "Erro: GOOGLE_API_KEY não configurada.", None

    try:
        # --- NL2SQL com MindSQL ---
        mindsql = MindSQL(
            database_type="sqlite",
            schema_description=esquema,
            prompt_prefix="""
Use apenas as colunas listadas no esquema; não invente colunas.
Se precisar do nome do autor, use dim_autor_proposicao.
Para ligações de autoria, use fat_autoria_proposicao.
Faça joins corretos entre tabelas.
"""
        )
        query_sql = mindsql.translate(prompt_usuario)
        st.subheader("Query SQL Gerada (MindSQL):")
        st.code(query_sql, language='sql')

        # --- Validação ---
        erros = validar_colunas(query_sql, colunas_disponiveis)
        if erros:
            return f"Erro: colunas inexistentes detectadas -> {erros}", None

        # --- Executar SQL ---
        df_resultado = pd.read_sql(query_sql, engine)

        # --- Formatação com Gemini ---
        genai.configure(api_key=API_KEY)
        model = genai.GenerativeModel('gemini-2.5-flash')
        instrucao = f"""
Você é um especialista em BI. Recebeu o seguinte resultado do banco de dados:

{df_resultado.head(20).to_string(index=False)}

Explique resumidamente os dados e formate em HTML ou Markdown se possível.
"""
        response = model.generate_content(instrucao)
        resposta_formatada = response.text.strip()

        return "Query executada com sucesso!", resposta_formatada

    except Exception as e:
        return f"Erro ao executar a análise: {e}", None

# --- Streamlit UI ---
st.title("🤖 Assistente BI da ALMG")

engine, esquema_db, colunas_disponiveis = get_database_engine()
if engine is None:
    st.error(esquema_db)
else:
    prompt_usuario = st.text_area("Faça uma pergunta...", height=100)
    if st.button("Executar Análise") and prompt_usuario:
        with st.spinner("Processando..."):
            mensagem, resultado = executar_analise(engine, esquema_db, colunas_disponiveis, prompt_usuario)
            if resultado:
                st.subheader("Resultado")
                st.write(resultado, unsafe_allow_html=True)
            st.info(f"Status: {mensagem}")
