import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import google.generativeai as genai
import json 
import sys
# A biblioteca psycopg2 é necessária para a conexão PostgreSQL
try:
    import psycopg2
except ImportError:
    st.error("O driver psycopg2 não foi encontrado. Instale-o com 'pip install psycopg2-binary'")
    sys.exit()

# --- CONFIGURAÇÕES E CONSTANTES ---

st.set_page_config(layout="wide")
CHAVE_GEMINI_CONFIGURADA = False 
DB_CONFIGURED = False
engine = None

if "GEMINI_API_KEY" in st.secrets:
    try:
        genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
        CHAVE_GEMINI_CONFIGURADA = True
    except Exception as e:
        st.error(f"Erro ao configurar a API do Gemini: {e}")
else:
    st.warning("Chave da API do Gemini não encontrada nos segredos do Streamlit.")

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
        
        # Cria o engine de conexão que será usado para executar o SQL
        engine = create_engine(DATABASE_URL)
        DB_CONFIGURED = True
        st.sidebar.success("Conexão PostgreSQL configurada com sucesso!")

except KeyError:
    st.sidebar.error("Verifique se as 5 chaves (user, password, host, database, port) estão em .streamlit/secrets.toml na seção [postgres].")
except Exception as e:
    DB_CONFIGURED = False
    st.sidebar.error(f"Erro ao conectar o PostgreSQL. Verifique host/porta/firewall: {e}")

# Contexto do Modelo de Dados (Ajuste as colunas para refletir seu DB)
# O Gemini usa isso para saber quais tabelas e colunas pode usar no SQL
DATA_MODEL_CONTEXT = """
Tabela 'proposicoes': Colunas disponíveis: siglaTipo, ano, ementa, situacao.
Tabela 'deputados': Colunas disponíveis: nome, partido, cargo.
"""

# --- 2. FUNÇÃO: Geração do Plano de Análise (Gemini) ---

def gerar_plano_de_analise(pergunta_usuario, model_context):
    """Usa o Gemini para converter a pergunta em um plano de análise JSON."""
    
    if not CHAVE_GEMINI_CONFIGURADA: return None
        
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
    
    try:
        model = genai.GenerativeModel('gemini-2.5-flash')
        response = model.generate_content(prompt, stream=False)
        raw_text = response.text.strip()
        
        # Extração robusta do JSON
        clean_json_string = ""
        if '{' in raw_text and '}' in raw_text:
            start_index = raw_text.find('{')
            end_index = raw_text.rfind('}')
            clean_json_string = raw_text[start_index:end_index + 1]
        
        return json.loads(clean_json_string)
        
    except Exception as e:
        st.error(f"Erro ao gerar plano de análise: {e}. Resposta bruta: {raw_text}")
        return None

# --- 3. FUNÇÃO: Execução e Formatação da Resposta (SQL Real) ---

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

    # Mapeamento do filtro para a cláusula WHERE do SQL
    where_clauses = []
    for filtro in filtros:
        col = filtro['coluna']
        val = filtro['valor']
        # Usando ILIKE para pesquisa case-insensitive (PostgreSQL)
        where_clauses.append(f"\"{col}\" ILIKE '%{val}%'") 

    where_sql = " AND ".join(where_clauses)
    if where_sql:
        where_sql = " WHERE " + where_sql

    # Construção do SQL para a operação
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
        # Executa o SQL real no seu PostgreSQL
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
st.subheader("Tradução de Linguagem Natural para SQL (Conexão PostgreSQL Real)")

st.markdown("---")

with st.expander("❓ Dicas de Perguntas"):
    st.markdown("""
    Faça perguntas sobre **contagem**, **listagem** ou **agrupamento** nas tabelas `proposicoes` e `deputados`.

    > **"Conte quantas proposições do tipo PL estão em 'Tramitação'."**
    > **"Liste os nomes dos deputados do partido 'PT'."**
    > **"Agrupe as proposições por 'siglaTipo' para ver qual é o mais comum."**
    """)

user_query = st.text_input("Sua pergunta para o armazém de dados:", placeholder="Ex: Quantas proposições do tipo PL estão em 'Tramitação'?")

if user_query:
    st.markdown("---")
    
    if not CHAVE_GEMINI_CONFIGURADA: 
        st.error("A chave do Gemini não está configurada.")
    elif not DB_CONFIGURED:
        st.error("A conexão com o PostgreSQL falhou. Verifique seu `.streamlit/secrets.toml`.")
    
    else:
        # 1. GERA O PLANO DE AÇÃO
        with st.spinner("Analisando sua pergunta e gerando o plano de análise SQL..."):
            plano_analise = gerar_plano_de_analise(user_query, DATA_MODEL_CONTEXT)
        
        if plano_analise:
            # 2. EXECUTA O PLANO CONTRA O BANCO DE DADOS REAL
            with st.spinner(f"Executando a query SQL no PostgreSQL..."):
                resposta_final = executar_plano_de_analise(plano_analise) 
            
            # 3. APRESENTA A RESPOSTA FINAL
            st.success("✅ Resposta Final:")
            st.markdown(f"**{resposta_final}**")
        else:
            st.warning("Não foi possível gerar um plano de análise válido para sua pergunta.")
