import streamlit as st
import sqlite3
import pandas as pd
import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold
from huggingface_hub import hf_hub_download
import os
import google.api_core.exceptions as google_exceptions

# Configurar a chave da API do Gemini a partir dos secrets
try:
    genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
    st.success("API do Gemini configurada com sucesso.")
except Exception as e:
    st.error(f"Erro na configuração da API do Gemini: {str(e)}. Verifique a chave nos secrets.")
    st.stop()

# Configurações do Hugging Face e do dataset
try:
    HF_TOKEN = st.secrets["HF_TOKEN"]
    st.success("Token do Hugging Face carregado.")
except Exception as e:
    st.error(f"Erro no token do Hugging Face: {str(e)}. Verifique os secrets.")
    st.stop()

REPO_ID = "TiagoPianezzola/BI"  # ID do repositório no Hugging Face
DB_FILENAME = "almg_local.db"  # Nome do arquivo .db no dataset

@st.cache_resource
def load_database():
    """Baixa e carrega o banco de dados SQLite do Hugging Face."""
    try:
        db_path = hf_hub_download(
            repo_id=REPO_ID,
            filename=DB_FILENAME,
            token=HF_TOKEN,
            repo_type="dataset"
        )
        st.success(f"Banco de dados baixado: {db_path}")
        return db_path
    except Exception as e:
        st.error(f"Erro ao baixar o banco de dados: {str(e)}. Verifique o dataset e o token.")
        return None

@st.cache_data
def get_schema(db_path):
    """Extrai o esquema do banco de dados (tabelas e colunas)."""
    if not db_path:
        return "Erro: Banco de dados não carregado."
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Obter lista de tabelas
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    schema = []
    for table in tables:
        table_name = table[0]
        # Obter colunas da tabela
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        col_info = [f"{col[1]} {col[2]}" for col in columns]  # nome tipo
        schema.append(f"Tabela: {table_name}\nColunas: {', '.join(col_info)}")
    
    conn.close()
    full_schema = "\n\n".join(schema)
    # Truncar para evitar prompts muito longos (limite ~8000 tokens)
    if len(full_schema) > 2000:
        full_schema = full_schema[:2000] + "\n... (esquema truncado para evitar erros de tamanho)"
    return full_schema

def initialize_model():
    """Inicializa o modelo Gemini com fallback e configurações de segurança."""
    model_name = "gemini-1.5-flash-exp"  # Versão experimental para prompts longos
    safety_settings = [
        {
            "category": HarmCategory.HARM_CATEGORY_HARASSMENT,
            "threshold": HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
        },
        {
            "category": HarmCategory.HARM_CATEGORY_HATE_SPEECH,
            "threshold": HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
        },
        # Adicione mais categorias se necessário
    ]
    
    try:
        model = genai.GenerativeModel(
            model_name,
            generation_config={"temperature": 0.1, "top_p": 0.8, "max_output_tokens": 500},
            safety_settings=safety_settings
        )
        # Testar a API com um prompt mínimo
        test_response = model.generate_content("Teste: retorne apenas 'OK'.")
        if "OK" in test_response.text:
            st.success(f"Modelo {model_name} inicializado e testado com sucesso.")
            return model
        else:
            st.error("Teste de API falhou: resposta inesperada.")
            return None
    except google_exceptions.InvalidArgument as e:
        st.error(f"Erro InvalidArgument com {model_name}: {str(e)}. Tentando fallback para gemini-pro...")
        try:
            model = genai.GenerativeModel('gemini-pro')
            test_response = model.generate_content("Teste: retorne apenas 'OK'.")
            if "OK" in test_response.text:
                st.warning("Usando modelo fallback: gemini-pro.")
                return model
            else:
                return None
        except Exception as e2:
            st.error(f"Fallback falhou: {str(e2)}.")
            return None
    except Exception as e:
        st.error(f"Erro inesperado ao inicializar {model_name}: {str(e)}.")
        return None

def generate_sql(question, schema, model):
    """Usa Gemini para gerar uma query SQL a partir da pergunta em linguagem natural."""
    if not model:
        return "Erro: Modelo não disponível."
    
    # Prompt otimizado e mais curto
    prompt = f"""Você é um especialista em SQL. Baseado EXCLUSIVAMENTE no esquema abaixo, gere UMA query SQL válida (apenas SELECT) para a pergunta.

Esquema (truncado se necessário):
{schema}

Pergunta: {question}

Responda APENAS com a query SQL, sem texto extra. Use LIMIT 10 se aplicável. Use " para nomes com espaços."""
    
    try:
        response = model.generate_content(prompt)
        sql_query = response.text.strip()
        # Limpar markdown
        if sql_query.startswith("```sql:disable-run
            sql_query = sql_query[6:]
        if sql_query.endswith("```"):
            sql_query = sql_query[:-3]
        sql_query = sql_query.strip()
        return sql_query if sql_query else "Query gerada vazia - tente reformular a pergunta."
    except google_exceptions.InvalidArgument as e:
        return f"Erro InvalidArgument na geração: {str(e)}. Verifique o prompt ou modelo."
    except Exception as e:
        return f"Erro na geração da query: {str(e)}"

def execute_query(db_path, sql_query):
    """Executa a query SQL no banco de dados e retorna os resultados como DataFrame."""
    if not db_path:
        st.error("Banco de dados não disponível.")
        return None
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(sql_query, conn)
        return df
    except Exception as e:
        st.error(f"Erro ao executar a query: {str(e)}")
        return None
    finally:
        conn.close()

# Interface do Streamlit
st.title("Assistente de Consulta SQL com IA - ALMG")
st.write("Digite uma pergunta em linguagem natural sobre os dados da ALMG.")

# Carregar DB e esquema
with st.spinner("Carregando banco de dados..."):
    db_path = load_database()
    if db_path:
        schema = get_schema(db_path)
    else:
        schema = None
        st.stop()

# Inicializar o modelo Gemini
if 'model' not in st.session_state:
    st.session_state.model = None

if st.button("Recarregar Modelo Gemini") or st.session_state.model is None:
    st.session_state.model = initialize_model()

model = st.session_state.model
if model is None:
    st.error("Falha ao inicializar o modelo. Verifique a chave da API e clique em 'Recarregar'.")
    st.stop()

# Input do usuário
question = st.text_input("Sua pergunta:", placeholder="Ex: Quais deputados do PT mais apresentaram projetos em 2023?")

if question:
    with st.spinner("Gerando query SQL..."):
        sql_query = generate_sql(question, schema, model)
        st.subheader("Query SQL Gerada:")
        st.code(sql_query, language="sql")
    
    if st.button("Executar Query"):
        with st.spinner("Executando query..."):
            results = execute_query(db_path, sql_query)
            if results is not None and not results.empty:
                st.subheader("Resultados:")
                st.dataframe(results)
            elif results is not None:
                st.info("Query executada, mas sem resultados.")
            else:
                st.warning("Falha na execução. Verifique a query.")

# Sidebar
with st.sidebar:
    st.header("Debug e Instruções")
    st.write("""
    - Use uma chave nova do Google AI Studio.
    - Modelo: gemini-1.5-flash-exp (fallback: gemini-pro).
    - Exemplo: "Top 5 deputados por projetos em 2023".
    """)
    if st.checkbox("Mostrar Esquema (Debug)"):
        st.text_area("Esquema do DB:", schema, height=200)
    st.info("Logs detalhados no Streamlit Cloud > Manage app > Logs.")

### Próximos Passos
- **Reimplante o app** com o `app.py` acima e o `requirements.txt` atualizado.
- **Teste com a nova chave**: Após atualizar, teste uma pergunta simples como "Liste as tabelas principais".
- **Se o erro persistir**: Compartilhe os logs completos do Streamlit Cloud (Manage app > Logs) ou o output do teste local. Pode ser necessário usar `gemini-pro` permanentemente se o flash tiver restrições no nível gratuito.

Isso deve resolver o `InvalidArgument` – a maioria dos casos é resolvida com chave nova + prompt otimizado. Me avise o resultado!
