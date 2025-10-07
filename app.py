import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, inspect
import os
import requests
import re
import google.generativeai as genai
# Certifique-se de que 'mindsql' está instalado (pip install mindsql)
from mindsql import MindSQL

# --- CONFIGURAÇÃO ---
DB_FILE = 'almg_local.db'
DB_SQLITE = f'sqlite:///{DB_FILE}'
DOWNLOAD_DB_URL = "https://huggingface.co/datasets/TiagoPianezzola/BI/resolve/main/almg_local.db"

# Inicialização do Session State
if 'db_ready' not in st.session_state:
    st.session_state['db_ready'] = os.path.exists(DB_FILE)

# --- 1. DOWNLOAD (SÓ LÓGICA DE I/O, SEM ST.X) ---
def download_file(url, dest_path):
    """Baixa o arquivo se não existir. Retorna (True, None) ou (False, erro)."""
    if os.path.exists(dest_path):
        return True, None

    try:
        response = requests.get(url.strip(), stream=True)
        response.raise_for_status()
        with open(dest_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024*1024):
                if chunk:
                    f.write(chunk)
        return True, None
    except Exception as e:
        return False, str(e)

# --- 2. CONFIGURAÇÃO DO ENGINE (AGORA SÓ LÓGICA DO BANCO E EM CACHE) ---
@st.cache_resource
def get_database_engine(db_file):
    """Cria e cacheia o engine e o esquema do banco."""
    try:
        engine = create_engine(f"sqlite:///{db_file}")
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

# --- VALIDAÇÃO DE COLUNAS (MANTIDA) ---
def validar_colunas(query_sql, colunas_disponiveis):
    palavras = re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', query_sql)
    erros = []
    todas_colunas = [c for cols in colunas_disponiveis.values() for c in cols]
    SQL_KEYWORDS = ['SELECT','DISTINCT','FROM','JOIN','ON','WHERE','GROUP','BY','ORDER','LIMIT','AS','AND','OR','COUNT','SUM','AVG','MAX','MIN','NULL','IS','NOT']
    for p in palavras:
        if p not in todas_colunas and p.upper() not in SQL_KEYWORDS:
            erros.append(p)
    return list(set(erros))

# --- FUNÇÃO PRINCIPAL (MANTIDA) ---
def executar_analise(engine, esquema, colunas_disponiveis, prompt_usuario):
    API_KEY = st.secrets.get("GOOGLE_API_KEY", "")
    if not API_KEY:
        st.error("Erro: GOOGLE_API_KEY não configurada em st.secrets.")
        return "Falha na configuração da API.", None

    try:
        # --- NL2SQL com MindSQL ---
        mindsql = MindSQL(
            database_type="sqlite",
            schema_description=esquema,
            prompt_prefix="Use apenas as colunas listadas no esquema; não invente colunas."
        )
        query_sql = mindsql.translate(prompt_usuario)
        st.subheader("Query SQL Gerada (MindSQL):")
        st.code(query_sql, language='sql')

        # --- Validação ---
        erros = validar_colunas(query_sql, colunas_disponiveis)
        if erros:
            return f"Erro: colunas inexistentes detectadas -> {', '.join(erros)}", None

        # --- Executar query ---
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

# --- STREAMLIT UI (FLUXO PRINCIPAL REVISADO) ---
st.title("🤖 Assistente BI da ALMG")

# --- LÓGICA DE DOWNLOAD CONDICIONAL ---
if not st.session_state['db_ready']:
    st.info("Iniciando verificação/download do banco de dados (aprox. 12MB)...")
    with st.spinner("Baixando arquivo... aguarde."):
        sucesso, erro = download_file(DOWNLOAD_DB_URL, DB_FILE)

    if sucesso:
        st.session_state['db_ready'] = True
        st.rerun() # Dispara um rerun para sair do bloco de download e ir para a UI principal
    else:
        st.error(f"Falha ao baixar banco de dados: {erro}")
        st.stop() # Parar a execução se o download falhar

# --- CARREGAR ENGINE QUANDO TUDO ESTIVER PRONTO ---
if st.session_state['db_ready']:
    engine, esquema_db, colunas_disponiveis = get_database_engine(DB_FILE)

    if engine is None:
        st.error(f"Erro de Conexão no Cache: {esquema_db}")
    else:
        # UI de Interação
        prompt_usuario = st.text_area("Faça uma pergunta sobre a ALMG (ex: 'quais são os 5 partidos com mais deputados?')...", height=100)
        if st.button("Executar Análise") and prompt_usuario:
            with st.spinner("Processando..."):
                mensagem, resultado = executar_analise(engine, esquema_db, colunas_disponiveis, prompt_usuario)
                if resultado:
                    st.subheader("Resultado")
                    # Usando st.markdown para renderizar Markdown/HTML
                    st.markdown(resultado, unsafe_allow_html=True)
                st.info(f"Status: {mensagem}")
