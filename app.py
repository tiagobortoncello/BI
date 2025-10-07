import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, inspect
import os
import requests
import re
import google.generativeai as genai

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

TABLE_ID_TO_NAME = {
    78: "dim_norma_juridica",
    111: "dim_proposicao",
    27: "dim_autor_proposicao",
    612: "fat_publicacao_norma_juridica",
}

def load_relationships_from_file(relations_file=RELATIONS_FILE):
    if not os.path.exists(relations_file):
        return {}
    try:
        df_rel = pd.read_csv(relations_file, sep='\t')
        df_rel = df_rel[df_rel['IsActive'] == True]
        rel_map = {}
        for _, row in df_rel.iterrows():
            from_table = row['FromTableID']
            to_table = row['ToTableID']
            if from_table not in rel_map:
                rel_map[from_table] = set()
            rel_map[from_table].add(to_table)
        return rel_map
    except Exception as e:
        st.error(f"Erro ao carregar {relations_file}: {e}")
        return {}

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
        genai.configure(api_key=API_KEY)
        model = genai.GenerativeModel('gemini-2.5-flash')
        
        # --- PROMPT SIMPLES E DIRETO ---
        instrucao = f"""
Você é um especialista em SQL para o Armazém de Dados da ALMG.
Gere uma consulta SQL no dialeto SQLite para a pergunta do usuário.

REGRAS:
1. USE APENAS as tabelas e colunas listadas abaixo.
2. NÃO INVENTE nomes de colunas.
3. USE SEMPRE `SELECT DISTINCT`.
4. Use os aliases: dp (dim_proposicao), dnj (dim_norma_juridica), dap (dim_autor_proposicao), fap (fat_autoria_proposicao), fpnj (fat_publicacao_norma_juridica).

Esquema do banco de dados:
{esquema}

Pergunta do usuário: {prompt_usuario}

Gere APENAS o código SQL, começando com SELECT.
"""

        response = model.generate_content(instrucao)
        query_sql = response.text.strip()
        query_sql = re.sub(r'^[^`]*```sql\s*', '', query_sql, flags=re.DOTALL)
        query_sql = re.sub(r'```.*$', '', query_sql, flags=re.DOTALL).strip()
        match = re.search(r'(SELECT.*)', query_sql, flags=re.IGNORECASE | re.DOTALL)
        if match:
            query_sql = match.group(1).strip()

        st.subheader("Query SQL Gerada:")
        st.code(query_sql, language='sql')

        df_resultado = pd.read_sql(query_sql, engine)
        # ... (restante da lógica de formatação permanece igual)

        # --- LÓGICA DE FORMATAÇÃO (mantida) ---
        total_encontrado = len(df_resultado)
        if 'url' in df_resultado.columns:
            df_resultado['Link'] = df_resultado['url'].apply(
                lambda x: f'<a href="{x}" target="_blank">🔗</a>' if pd.notna(x) else ""
            )
            df_resultado = df_resultado.drop(columns=['url'])
        
        styler = df_resultado.style.set_properties(**{'text-align': 'center'})
        table_html = styler.to_html(escape=False, index=False)
        table_html = table_html.replace('<thead>\n<tr><th></th>', '<thead>\n<tr>')
        table_html = re.sub(r'<tr>\s*<td>\s*\d+\s*</td>', '<tr>', table_html, flags=re.DOTALL)
        html_output = table_html.replace('\n', '')
        
        return "Query executada com sucesso!", html_output

    except Exception as e:
        return f"Erro ao executar a query: {e}\n\nQuery: {query_sql}", None

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
