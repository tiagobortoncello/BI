import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, inspect
import os 
import requests 
import json
from google import genai
import re 

# --- CONFIGURAÇÃO DO ARQUIVO DE DADOS (Hugging Face) ---
DB_FILE = 'almg_local.db'
DB_SQLITE = f'sqlite:///{DB_FILE}'

# LINK DE DOWNLOAD DIRETO DO SEU DATASET 
DOWNLOAD_URL = "https://huggingface.co/datasets/TiagoPianezzola/BI/resolve/main/almg_local.db" 
# -----------------------------------------------------------


# --- FUNÇÕES DE INFRAESTRUTURA (Não alteradas) ---
def get_api_key():
    return st.secrets.get("GOOGLE_API_KEY", "") 

def download_database(url, dest_path):
    if os.path.exists(dest_path):
        return True
    # (Resto da função de download que já está funcionando...)
    st.info("Iniciando download do Hugging Face Hub...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, stream=True, headers=headers)
        response.raise_for_status()
        st.info("Download em andamento...")
        with open(dest_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024*1024): 
                if chunk:
                    f.write(chunk)
        st.success("Download concluído com sucesso. Conectando ao banco de dados.")
        return True
    except Exception as e:
        st.error(f"Erro no download do banco de dados: {e}")
        st.warning("Verifique se o link do Hugging Face Hub está correto.")
        return False


# --- FUNÇÃO DE CONEXÃO E METADADOS DO BANCO (JOINs Adicionados) ---
def get_database_engine():
    """Tenta baixar o banco de dados e retorna o objeto engine e o esquema."""
    
    if not download_database(DOWNLOAD_URL, DB_FILE):
        return None, "Download do banco de dados falhou."
    
    try:
        engine = create_engine(DB_SQLITE)
        inspector = inspect(engine)
        tabelas = inspector.get_table_names()
        
        esquema = ""
        for tabela in tabelas:
            df_cols = pd.read_sql(f"PRAGMA table_info({tabela})", engine)
            colunas = [f"{row['name']} ({row['type']})" for index, row in df_cols.iterrows()]
            esquema += f"Tabela {tabela} (Colunas: {', '.join(colunas)})\n"
        
        # ⚠️ CORREÇÃO DE ESQUEMA: ADICIONANDO AS RELAÇÕES (JOINs)
        esquema += "\nRELAÇÕES DE CHAVE (JOINs):\n"
        # Estas são CHAVES ASSUMIDAS, ajuste se necessário!
        esquema += "Tabela dim_deputado_estadual está relacionada com dim_legislatura pela chave 'sk_legislatura'.\n"
        esquema += "Tabela dim_deputado_estadual está relacionada com dim_partido pela chave 'sk_partido'.\n"
        # Adicione aqui outras relações importantes (Fato com Dimensão)
        
        return engine, esquema

    except Exception as e:
        return None, f"Erro ao conectar ao SQLite: {e}"


# --- FUNÇÃO PRINCIPAL DO ASSISTENTE (RAG - Prompt Aprimorado) ---
def executar_plano_de_analise(engine, esquema, prompt_usuario):
    """Gera o SQL com Gemini e executa no banco de dados."""
    
    API_KEY = get_api_key()
    if not API_KEY:
        return "Erro: A chave de API do Gemini não foi configurada no `.streamlit/secrets.toml`.", None
    
    try:
        client = genai.Client(api_key=API_KEY)
        
        # 1. Prompt de Instrução para o Gemini
        instrucao = (
            f"Você é um assistente de análise de dados da Assembleia Legislativa de Minas Gerais (ALMG). "
            f"Sua tarefa é converter a pergunta do usuário em uma única consulta SQL no dialeto SQLite. "
            f"**INCLUA JOINs** (LEFT JOIN ou INNER JOIN) para combinar tabelas (especialmente Fatos/Dimensões) "
            f"sempre que uma tabela de Dimensão for usada para filtro ou seleção. Limite a consulta a 10 resultados. "
            f"As colunas e RELAÇÕES DE CHAVE estão disponíveis no esquema:\n{esquema}\n\n"
            f"Pergunta do usuário: {prompt_usuario}"
        )

        # 2. Geração da Query SQL
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=instrucao
        )
        
        query_sql = response.text.strip().replace("```sql", "").replace("```", "").strip()

        # Limpeza extra para remover lixo como 'ite' ou 'sqlite' no início.
        query_sql = re.sub(r'^\s*(sqlite|sql|ite|)\s*', '', query_sql, flags=re.IGNORECASE).strip() 
        
        st.subheader("Query SQL Gerada:")
        st.code(query_sql, language='sql')

        # 3. Execução da Query
        df_resultado = pd.read_sql(query_sql, engine)
        
        return "Query executada com sucesso!", df_resultado

    except Exception as e:
        return f"Erro ao executar a query no banco de dados: {e}. Query SQL gerada: {query_sql}", None


# --- STREAMLIT UI PRINCIPAL ---

st.title("🤖 Assistente BI da ALMG (SQLite Local)")

engine, esquema_db = get_database_engine()

if engine is None:
    st.error(esquema_db)
else:
    with st.sidebar.expander("Esquema do Banco de Dados"):
        st.code(esquema_db)

    prompt_usuario = st.text_area(
        "Faça uma pergunta sobre os dados da ALMG (Ex: 'Quais são os 5 deputados mais votados do PT?')", 
        height=100
    )

    if st.button("Executar Análise"):
        if prompt_usuario:
            with st.spinner("Processando... Gerando e executando a consulta SQL."):
            
                mensagem, resultado = executar_plano_de_analise(engine, esquema_db, prompt_usuario)
                
                if resultado is not None:
                    st.subheader("Resultado da Análise")
                    st.dataframe(resultado)
                
                st.info(f"Status: {mensagem}")
