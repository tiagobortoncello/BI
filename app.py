import streamlit as st
import sqlite3
import pandas as pd
import pdfplumber
from huggingface_hub import hf_hub_download
import google.generativeai as genai
import os
from io import StringIO
from pathlib import Path
# Importando SQLAlchemy, conforme solicitado, para manter a arquitetura original
from sqlalchemy import create_engine 

# --- Configuração Inicial e Verificação de Secrets ---

HF_TOKEN = st.secrets.get("HF_TOKEN", "")
GEMINI_API_KEY = st.secrets.get("GEMINI_API_KEY", "")

# Adicione a checagem de um caminho para o DB se necessário, ou confie no HF
# NÃO precisamos de DATABASE_URL aqui, pois estamos usando o arquivo SQLite local.

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

# Adiciona o estado inicial do DB (Assume-se que o DB não está pronto no startup)
if 'db_loaded' not in st.session_state:
    st.session_state.db_loaded = False
if 'db_path' not in st.session_state:
    st.session_state.db_path = None
if 'schema_txt' not in st.session_state:
    st.session_state.schema_txt = None
if 'pdf_text' not in st.session_state:
    st.session_state.pdf_text = None

# --- Funções de Carregamento de Recursos (Otimizadas com Cache) ---

# Baixar DB do HuggingFace (cache em /tmp). Esta função é responsável pelo download pesado.
@st.cache_resource(ttl=None) # TTL=None significa cache eterno (ideal para DBs grandes)
def load_db_cached(hf_token_value):
    # NOTA: O download de 1.32 GB ocorre aqui.
    db_path = hf_hub_download(
        repo_id="TiagoPianezzola/BI",
        filename="almg_local.db",
        token=hf_token_value,
        local_dir="/tmp"
    )
    return db_path

# Carregamento de arquivos locais (TXT e PDF)
# NOTA: O uso do Path.exists() ajuda a diagnosticar erros de arquivos ausentes no deploy.
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
        with st.status("Preparando Base de Dados: **Baixando 1.32 GB** (Isto pode levar **vários minutos** na primeira vez)...", expanded=True) as status:
            status.update(label="Iniciando download do banco de dados do Hugging Face...", state="running")
            # Chama a função cachêada para fazer o download
            db_path = load_db_cached(HF_TOKEN)
            st.session_state.db_path = db_path
            
            # NOTA: Se você estivesse usando um engine SQLAlchemy aqui:
            # st.session_state.db_engine = create_engine(f"sqlite:///{db_path}")

            status.update(label="Banco de Dados ALMG (1.32 GB) carregado com sucesso!", state="complete")
            st.session_state.db_loaded = True
            st.success("Base de dados pronta. Você já pode fazer suas perguntas!")
            st.rerun() # Reinicia para carregar a interface de chat
            
    except FileNotFoundError as e:
        st.error(f"ERRO no carregamento de arquivos locais: {str(e)}. Verifique se os arquivos estão no seu repositório.")
    except Exception as e:
        st.error(f"ERRO CRÍTICO no download do DB. Verifique seu **HF_TOKEN** ou a conexão. Detalhes: {e}")

# --- Funções de Lógica do Chatbot ---

# Gerar SQL via Gemini
def generate_sql(question, schema_txt, pdf_text):
    prompt = f"""
    Você é um especialista em SQL para o dataset ALMG. Gere UMA query SQL válida e simples para responder à pergunta em linguagem natural.
    
    Schema das tabelas e colunas (use EXATAMENTE essas):
    {schema_txt}
    
    Contexto detalhado das tabelas/colunas (use para escolher o que representa melhor o conceito na pergunta):
    {pdf_text}
    
    Pergun ta do usuário: {question}
    
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

# Executar SQL e formatar resposta (usando o DB Path local)
def execute_and_format(sql, db_path, question):
    # Conecta o SQLite no arquivo local baixado
    conn = sqlite3.connect(db_path) 
    try:
        # Pandas usa a conexão SQLite diretamente, mesmo com SQLAlchemy no ambiente
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

# Chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Sidebar com exemplos
with st.sidebar:
    st.header("Exemplos de Perguntas")
    examples = [
        "Quantos deputados há na ALMG?",
        "Qual o gasto total em 2023?",
        "Liste as comissões ativas."
    ]
    for ex in examples:
        # Se o DB não está carregado, mostre um aviso em vez de adicionar a mensagem
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
    
    # Exibir histórico
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Input do usuário
    if prompt := st.chat_input("Digite sua pergunta sobre os dados ALMG:"):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        
        with st.chat_message("assistant"):
            with st.spinner("Gerando query SQL..."):
                try:
                    # Usa as variáveis de estado
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
