import streamlit as st
import sqlite3
import pandas as pd
import pdfplumber
from huggingface_hub import hf_hub_download
import google.generativeai as genai
import os
from io import StringIO

# Configuração de secrets (Streamlit Cloud)
HF_TOKEN = st.secrets.get("HF_TOKEN", "")
GEMINI_API_KEY = st.secrets.get("GEMINI_API_KEY", "")

if not HF_TOKEN or not GEMINI_API_KEY:
    st.error("Configure HF_TOKEN e GEMINI_API_KEY nos secrets do Streamlit Cloud.")
    st.stop()

# Configurar Gemini
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-2.0-flash')

# Baixar DB do HuggingFace (cache em /tmp)
@st.cache_resource
def load_db():
    db_path = hf_hub_download(
        repo_id="TiagoPianezzola/BI",
        filename="almg_local.db",
        token=HF_TOKEN,
        local_dir="/tmp"
    )
    return db_path

# Ler schema do TXT
@st.cache_data
def load_schema_txt():
    with open("armazem_estruturado.txt", "r", encoding="utf-8") as f:
        return f.read()

# Extrair texto do PDF
@st.cache_data
def load_pdf_text():
    pdf_text = ""
    with pdfplumber.open("armazem.pdf") as pdf:
        for page in pdf.pages:
            pdf_text += page.extract_text() or ""
    return pdf_text

# Gerar SQL via Gemini
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
    # Validação básica de segurança
    forbidden = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER"]
    if any(word in sql.upper() for word in forbidden):
        raise ValueError("Query SQL inválida detectada.")
    return sql

# Executar SQL e formatar resposta via Gemini
def execute_and_format(sql, db_path, question):
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(sql, conn)
        if df.empty:
            return "Nenhum resultado encontrado para essa pergunta."
        
        # Formatar resultados como CSV string para Gemini
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
        
        # Mostrar tabela raw também
        st.dataframe(df, use_container_width=True)
        return formatted
    finally:
        conn.close()

# Interface Streamlit
st.title("🤖 Assistente BI ALMG - Pergunte em Linguagem Natural")

# Carregar recursos
db_path = load_db()
schema_txt = load_schema_txt()
pdf_text = load_pdf_text()

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
        if st.button(ex, key=ex):
            st.session_state.messages.append({"role": "user", "content": ex})
            st.rerun()

# Exibir histórico
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Input do usuário
if prompt := st.chat_input("Digite sua pergunta sobre os dados ALMG:"):
    # Adicionar à history
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)
    
    with st.chat_message("assistant"):
        with st.spinner("Gerando query SQL..."):
            try:
                sql = generate_sql(prompt, schema_txt, pdf_text)
                st.info(f"**Query SQL gerada:**\n```{sql}```")
                
                with st.spinner("Executando e formatando..."):
                    response = execute_and_format(sql, db_path, prompt)
                    st.markdown(response)
                
                # Salvar na history
                st.session_state.messages.append({"role": "assistant", "content": response})
            except Exception as e:
                st.error(f"Erro: {str(e)}. Verifique a pergunta ou logs.")

# Footer
st.markdown("---")
st.caption("App construído com Streamlit + Gemini + HuggingFace. Dataset privado ALMG.")
