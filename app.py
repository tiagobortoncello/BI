import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os 
import requests 
import json
from google import genai

# --- CONFIGURAÇÃO DO ARQUIVO DE DADOS (SQLite no Drive) ---
DB_FILE = 'almg_local.db'
DB_SQLITE = f'sqlite:///{DB_FILE}'

# ⚠️ SUBSTITUIR: ID do arquivo almg_local.db no Google Drive (APENAS A ID)
DRIVE_FILE_ID = "INSIRA_AQUI_A_SUA_FICHA_ID_LONGA" 
# URL de download direto (não precisa mexer nesta linha)
DRIVE_DOWNLOAD_URL = f"https://drive.google.com/uc?export=download&id={DRIVE_FILE_ID}"
# -----------------------------------------------------------


# --- CONFIGURAÇÃO DA API KEY ---
def get_api_key():
    """Tenta obter a chave de API dos secrets ou usa um placeholder."""
    # ⚠️ Mantenha sua API Key no .streamlit/secrets.toml
    return st.secrets.get("GOOGLE_API_KEY") or "SUA_CHAVE_COMPLETA_DA_API_AQUI" 
# -------------------------------


# --- FUNÇÃO DE DOWNLOAD DO DRIVE ---
@st.cache_resource
def download_database_from_drive(url, dest_path):
    """Baixa o arquivo .db do Google Drive se ele não existir."""
    if os.path.exists(dest_path):
        st.success("Arquivo de banco de dados já presente.")
        return True

    st.info(f"Arquivo de banco de dados não encontrado. Iniciando download de 1.32 GB...")
    
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Lógica para lidar com o aviso de 'arquivo muito grande' do Drive (> 1GB)
        if 'Content-Disposition' not in response.headers and 'confirm' in response.text:
            st.warning("Detectado aviso de arquivo grande. Bypassando...")
            id_drive = url.split("id=")[-1]
            url_confirm = f"https://drive.google.com/uc?export=download&id={id_drive}&confirm=t" # Token simulado
            response = requests.get(url_confirm, stream=True)
            response.raise_for_status()
            
        total_size = int(response.headers.get('content-length', 0))
        
        with open(dest_path, 'wb') as f:
            dl = 0
            chunk_size = 1024*1024 # 1MB chunks
            with st.progress(0, text=f"Baixando {total_size/1024**3:.2f} GB...") as progress_bar:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        dl += len(chunk)
                        progress_bar.progress(dl / total_size, text=f"Baixado: {dl/1024**3:.2f} GB / {total_size/1024**3:.2f} GB")
                        
        st.success("Download concluído.")
        return True

    except Exception as e:
        st.error(f"Erro no download do banco de dados: {e}")
        st.warning("Verifique se o Link do Drive está correto e com 'Qualquer pessoa com o link'.")
        return False


# --- FUNÇÃO DE CONEXÃO E METADADOS DO BANCO ---
@st.cache_resource
def get_database_engine():
    """Tenta baixar o banco de dados e retorna o objeto engine."""
    
    if not download_database_from_drive(DRIVE_DOWNLOAD_URL, DB_FILE):
        return None, "Download do banco de dados falhou."
    
    try:
        engine = create_engine(DB_SQLITE)
        
        # Obtém o esquema das tabelas para o prompt do Gemini
        inspector = pd.io.sql.SQLAlchemy().connect(engine)
        tabelas = inspector.get_table_names()
        
        esquema = ""
        for tabela in tabelas:
            df = pd.read_sql(f"SELECT * FROM {tabela} LIMIT 0", engine)
            esquema += f"Tabela {tabela} (Colunas: {', '.join(df.columns)})\n"
            
        st.sidebar.success("Conexão e Metadados OK.")
        return engine, esquema

    except Exception as e:
        return None, f"Erro ao conectar ao SQLite: {e}"


# --- FUNÇÃO PRINCIPAL DO ASSISTENTE (RAG) ---
def executar_plano_de_analise(engine, esquema, prompt_usuario):
    """Gera o SQL com Gemini e executa no banco de dados."""
    
    API_KEY = get_api_key()
    if not API_KEY or API_KEY == "SUA_CHAVE_COMPLETA_DA_API_AQUI":
        return "Erro: A chave de API do Gemini não foi configurada nos segredos.", None
    
    try:
        client = genai.Client(api_key=API_KEY)
        
        # 1. Prompt de Instrução para o Gemini
        instrucao = (
            f"Você é um assistente de análise de dados da Assembleia Legislativa de Minas Gerais (ALMG). "
            f"Sua tarefa é converter a pergunta do usuário em uma única consulta SQL no dialeto SQLite, "
            f"usando as tabelas e colunas fornecidas. Limite a consulta a 10 resultados. "
            f"As colunas estão disponíveis no esquema:\n{esquema}\n\n"
            f"Pergunta do usuário: {prompt_usuario}"
        )

        # 2. Geração da Query SQL
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=instrucao
        )
        
        # O Gemini gera a query no formato de código Markdown SQL.
        query_sql = response.text.strip().replace("```sql", "").replace("```", "").strip()

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
    # Mostra o esquema no sidebar para referência (opcional)
    with st.sidebar.expander("Esquema do Banco de Dados"):
        st.code(esquema_db)

    prompt_usuario = st.text_area(
        "Faça uma pergunta sobre os dados da ALMG (Ex: 'Quais são os 5 deputados mais votados do PT?')", 
        height=100
    )

    if st.button("Executar Análise"):
        if prompt_usuario:
            st.spinner("Processando... Gerando e executando a consulta SQL.")
            
            mensagem, resultado = executar_plano_de_analise(engine, esquema_db, prompt_usuario)
            
            if resultado is not None:
                st.subheader("Resultado da Análise")
                st.dataframe(resultado)
            
            st.info(f"Status: {mensagem}")
