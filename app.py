import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, inspect
import os 
import requests 
import json
from google import genai

# --- CONFIGURAÇÃO DO ARQUIVO DE DADOS (Hugging Face) ---
DB_FILE = 'almg_local.db'
DB_SQLITE = f'sqlite:///{DB_FILE}'

# ⚠️ LINK DE DOWNLOAD DIRETO DO SEU DATASET INSERIDO AQUI!
# Formato: https://huggingface.co/datasets/USUARIO/DATASET/resolve/main/ARQUIVO.db
DOWNLOAD_URL = "https://huggingface.co/datasets/TiagoPianezzola/BI/resolve/main/almg_local.db" 
# -----------------------------------------------------------


# --- CONFIGURAÇÃO DA API KEY ---
def get_api_key():
    """Obtém a chave de API dos secrets do Streamlit."""
    return st.secrets.get("GOOGLE_API_KEY", "") 
# -------------------------------


# --- FUNÇÃO DE DOWNLOAD ROBUSTO (Para Hugging Face) ---
def download_database(url, dest_path):
    """Baixa o arquivo .db de qualquer URL de download direto."""
    # Garante que não baixa o arquivo em cada refresh, apenas na primeira sessão
    if os.path.exists(dest_path):
        return True

    st.info("Iniciando download do Hugging Face Hub. Esta é a última etapa de infraestrutura!")
    
    try:
        # Usa headers para simular um navegador e evitar bloqueios.
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, stream=True, headers=headers)
        response.raise_for_status() # Verifica se há erros HTTP (4xx ou 5xx)
            
        st.info("Download em andamento...")

        with open(dest_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024*1024): 
                if chunk:
                    f.write(chunk)

        st.success("Download concluído com sucesso. Conectando ao banco de dados.")
        return True

    except Exception as e:
        st.error(f"Erro no download do banco de dados: {e}")
        st.warning("Verifique se o link do Hugging Face Hub está correto e se o dataset é público.")
        return False


# --- FUNÇÃO DE CONEXÃO E METADADOS DO BANCO ---
def get_database_engine():
    """Tenta baixar o banco de dados e retorna o objeto engine e o esquema."""
    
    if not download_database(DOWNLOAD_URL, DB_FILE):
        return None, "Download do banco de dados falhou."
    
    try:
        engine = create_engine(DB_SQLITE)
        
        # Usando 'inspect' direto do SQLAlchemy (correção de versão)
        inspector = inspect(engine)
        tabelas = inspector.get_table_names()
        
        esquema = ""
        for tabela in tabelas:
            df_cols = pd.read_sql(f"PRAGMA table_info({tabela})", engine)
            colunas = [f"{row['name']} ({row['type']})" for index, row in df_cols.iterrows()]
            esquema += f"Tabela {tabela} (Colunas: {', '.join(colunas)})\n"
            
        return engine, esquema

    except Exception as e:
        # Se falhar agora, significa que o arquivo foi baixado, mas ainda está corrompido.
        return None, f"Erro ao conectar ao SQLite: {e}"


# --- FUNÇÃO PRINCIPAL DO ASSISTENTE (RAG) ---
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
    # Mostra o esquema no sidebar para referência
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
