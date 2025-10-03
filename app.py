import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import json 
import sys
import requests # NOVO: Usaremos requests para chamar a API
import os # Necessário para API Key

# A biblioteca psycopg2 é necessária para a conexão PostgreSQL
try:
    import psycopg2
except ImportError:
    st.error("O driver psycopg2 não foi encontrado. Instale-o com 'pip install psycopg2-binary'")
    sys.exit()

# --- CONFIGURAÇÕES GERAIS E API KEY ---

st.set_page_config(layout="wide")
DB_CONFIGURED = False
API_KEY_CONFIGURED = False
engine = None

# Função para obter a API Key (Adaptada do seu código)
def get_api_key():
    """Tenta obter a chave de API das variáveis de ambiente ou secrets do Streamlit."""
    # Usando GOOGLE_API_KEY para consistência com o seu código RAG
    api_key = os.environ.get("GOOGLE_API_KEY") or st.secrets.get("GOOGLE_API_KEY")
    if not api_key:
        st.sidebar.error("Erro: A chave de API ('GOOGLE_API_KEY') não foi configurada nos segredos.")
        return None
    return api_key

# --- 1. CONFIGURAÇÃO REAL DO BANCO DE DADOS ---

try:
    if "postgres" in st.secrets:
        # Lendo credenciais do arquivo .streamlit/secrets.toml
        DB_USER = st.secrets["postgres"]["user"]
        DB_PASSWORD = st.secrets["postgres"]["password"]
        DB_HOST = st.secrets["postgres"]["host"]
        DB_PORT = st.secrets["postgres"]["port"]
        DB_NAME = st.secrets["postgres"]["database"]
        
        # Cria a string de conexão
        DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        
        engine = create_engine(DATABASE_URL)
        DB_CONFIGURED = True
        st.sidebar.success("Conexão PostgreSQL configurada com sucesso!")

except KeyError:
    st.sidebar.error("Verifique se as 5 chaves (user, password, host, database, port) estão em .streamlit/secrets.toml.")
except Exception as e:
    DB_CONFIGURED = False
    st.sidebar.error(f"Erro ao conectar o PostgreSQL. Verifique host/porta/firewall: {e}")

# Contexto do Modelo de Dados (Ajuste as colunas para refletir seu DB)
DATA_MODEL_CONTEXT = """
Tabela 'proposicoes': Colunas disponíveis: siglaTipo, ano, ementa, situacao.
Tabela 'deputados': Colunas disponíveis: nome, partido, cargo.
"""

# --- 2. FUNÇÃO: Geração do Plano de Análise (Usando requests) ---

def gerar_plano_de_analise(pergunta_usuario, model_context, api_key):
    """Usa o Gemini (via requests) para converter a pergunta em um plano de análise JSON."""
    
    if not api_key:
        return None
        
    prompt = f"""
    Sua tarefa é analisar a pergunta do usuário e gerar um **Plano de Análise** em formato JSON, baseado no modelo de dados fornecido. Este plano será traduzido em SQL e executado no banco de dados.

    Modelo de Dados Disponível:
    {model_context}
    
    O JSON deve ter a seguinte estrutura:
    {{
      "tabela": "Nome da Tabela (Ex: proposicoes ou deputados)",
      "operacao": "Tipo de operação (Ex: 'contar', 'listar', 'agrupar')",
      "filtros": [
        {{"coluna": "nome da coluna", "valor": "valor do filtro"}}, 
        ...
      ],
      "coluna_alvo": "Coluna para aplicar a operação (Ex: 'id' para contagem, 'partido' para agrupar)"
    }}
    
    Instruções:
    1. Responda APENAS com o objeto JSON do plano.
    2. Coloque aspas duplas (") nos nomes das colunas e tabelas no JSON para evitar conflitos de sintaxe.
    3. Se a pergunta não puder ser respondida, retorne: {{"tabela": "invalida", "operacao": "erro", "filtros": [], "coluna_alvo": ""}}

    Pergunta do Usuário: "{pergunta_usuario}"
    """
    
    # URL do endpoint (usando gemini-2.5-flash para melhor desempenho)
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key}"

    payload = {
        "contents": [{"parts": [{"text": prompt}]}]
    }

    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        result = response.json()
        
        # Extração da resposta do Gemini
        raw_text = result.get("candidates", [])[0].get("content", {}).get("parts", [])[0].get("text", "")
        raw_text = raw_text.strip()
        
        # Extração robusta do JSON
        clean_json_string = ""
        if '{' in raw_text and '}' in raw_text:
            start_index = raw_text.find('{')
            end_index = raw_text.rfind('}')
            clean_json_string = raw_text[start_index:end_index + 1]
        
        return json.loads(clean_json_string)
        
    except requests.exceptions.HTTPError as http_err:
        st.error(f"Erro na comunicação com a API: {http_err}")
        return None
    except Exception as e:
        st.error(f"Erro ao gerar plano de análise: {e}. Resposta bruta: {raw_text}")
        return None


# --- 3. FUNÇÃO: Execução e Formatação da Resposta (SQL Real) ---
# (Esta função permanece inalterada em relação à execução do SQL)

def executar_plano_de_analise(plano):
    """Constrói o SQL, executa no DB e formata a resposta."""
    
    if not DB_CONFIGURED:
        return "Erro: A conexão com o banco de dados não está ativa."
        
    tabela = plano.get('tabela')
    operacao = plano.get('operacao')
    filtros = plano.get('filtros', [])
    coluna_alvo = plano.get('coluna_alvo')

    if tabela == "invalida":
        return "Desculpe, não consigo responder a essa pergunta com os dados disponíveis."

    where_clauses = []
    for filtro in filtros:
        col = filtro['coluna']
        val = filtro['valor']
        # Usando ILIKE para pesquisa case-insensitive (PostgreSQL)
        where_clauses.append(f"\"{col}\" ILIKE '%{val}%'") 

    where_sql = " AND ".join(where_clauses)
    if where_sql:
        where_sql = " WHERE " + where_sql

    if operacao == 'contar':
        sql_query = f"SELECT COUNT(*) FROM \"{tabela}\"{where_sql};"
    elif operacao == 'agrupar':
        sql_query = f"SELECT \"{coluna_alvo}\", COUNT(*) AS total FROM \"{tabela}\"{where_sql} GROUP BY 1 ORDER BY total DESC LIMIT 3;"
    elif operacao == 'listar':
        sql_query = f"SELECT * FROM \"{tabela}\"{where_sql} LIMIT 5;"
    else:
        return f"Operação '{operacao}' não suportada."
    
    
    # EXECUÇÃO DA QUERY NO BANCO DE DADOS
    try:
        df_resultado = pd.read_sql(sql_query, engine)

        # TRATAMENTO E FORMATAÇÃO DO RESULTADO FINAL
        if operacao == 'contar':
            total = df_resultado.iloc[0, 0]
            return f"O total de registros na tabela '{tabela}' é de **{total}**."
        
        elif operacao == 'agrupar':
            if df_resultado.empty: return "Nenhum dado encontrado para agrupar."
            
            df_resultado.columns = ['Grupo', 'Total']
            resultado_principal = f"O grupo mais comum de '{coluna_alvo}' é **{df_resultado.iloc[0]['Grupo']}** com {df_resultado.iloc[0]['Total']} ocorrências."
            
            top_grupos = df_resultado.head(3).to_dict('records')
            detalhe = [f"{item['Grupo']}: {item['Total']}" for item in top_grupos]
            return f"{resultado_principal}. Detalhes (Top 3): {'; '.join(detalhe)}."
        
        elif operacao == 'listar':
             if df_resultado.empty:
                 return "Nenhum registro encontrado com os filtros."
             primeiro_resultado = df_resultado.iloc[0].to_dict()
             return f"Encontrados {len(df_resultado)} registros. Exemplo da primeira linha: {primeiro_resultado}"

        return "Análise executada, mas o resultado não pôde ser formatado."

    except Exception as e:
        return f"Erro ao executar a query no banco de dados: {e}. Query SQL gerada: **{sql_query}**"


# --- LÓGICA PRINCIPAL DA APLICAÇÃO ---

st.title("Assistente de Q&A sobre o Armazém de Dados da ALMG")

user_query = st.text_input("Sua pergunta para o armazém de dados:", placeholder="Ex: Quantos deputados há no partido PT?")

if user_query:
    st.markdown("---")
    
    api_key = get_api_key()
    
    if not api_key: 
        st.error("A chave da API do Google não foi configurada.")
    elif not DB_CONFIGURED:
        st.error("A conexão com o PostgreSQL falhou. Corrija o arquivo `.streamlit/secrets.toml`.")
    
    else:
        # 1. GERA O PLANO DE AÇÃO
        with st.spinner("Analisando sua pergunta e gerando o plano de análise SQL..."):
            plano_analise = gerar_plano_de_analise(user_query, DATA_MODEL_CONTEXT, api_key)
        
        if plano_analise:
            # 2. EXECUTA O PLANO CONTRA O BANCO DE DADOS REAL
            with st.spinner(f"Executando a query SQL no PostgreSQL..."):
                resposta_final = executar_plano_de_analise(plano_analise) 
            
            # 3. APRESENTA A RESPOSTA FINAL (LIMPA)
            st.success("✅ Resposta Final:")
            st.markdown(f"**{resposta_final}**")
        else:
            st.warning("Não foi possível gerar um plano de análise válido para sua pergunta.")
