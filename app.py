import streamlit as st
import sqlite3
import requests
import os
import json
import time

# --- Configurações Iniciais ---
st.set_page_config(
    page_title="Conversational BI (ALMG)",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Constantes do Banco de Dados
# O banco de dados (almg_local.db) está hospedado publicamente no Hugging Face
DB_URL_RAW = "https://huggingface.co/datasets/TiagoPianezzola/BI/resolve/main/almg_local.db"
DB_FILE = "almg_local.db"

# URL da API Gemini e nome do modelo
MODEL_NAME = "gemini-2.0-flash" 
API_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models/"

# Conteúdo do 'database_schema_oficial.md' para INSTRUÇÃO DE SISTEMA.
# ESTE CONTEXTO DEFINE AS REGRAS DE BI E NOMENCLATURA PARA O LLM.
BI_SCHEMA_CONTEXT = """
# Esquema e Contexto do Armazém de Dados ALMG (EDITÁVEL)

Este documento define as regras de Business Intelligence (BI) para o assistente virtual.
O LLM deve usar as chaves de surrogate (`sk_`) para realizar as junções (JOINs) entre as entidades.

## Estrutura das Tabelas (Dimensões)

### Dimensão: `dim_autor_emenda_proposicao` (Autores - Deputados, Partidos, Comissões)

| Coluna | Tipo | Descrição | 
 | ----- | ----- | ----- | 
| `sk_autor_emenda_proposicao` | INTEGER | **Chave Surrogate (SK)**. ID único do Autor. **Chave de JOIN.** | 
| `nome_autor` | TEXT | Nome completo do Autor/Parlamentar. | 
| `dep_partido_atual` | TEXT | Sigla do Partido Atual do Deputado. | 
| `dep_situacao_mandato` | TEXT | Situação do Mandato (Ex: 'Em Exercício', 'Licenciado'). | 
| `genero` | TEXT | Gênero do Autor (se for pessoa física). | 

### Dimensão: `dim_proposicao` (Detalhes dos Projetos de Lei e Propostas)

| Coluna | Tipo | Descrição | 
 | ----- | ----- | ----- | 
| `sk_proposicao` | INTEGER | **Chave Surrogate (SK)**. ID único da Proposição/Projeto de Lei. **Chave de JOIN.** | 
| `proposicao_tipo` | TEXT | Tipo da Proposição (Ex: 'Projeto de Lei', 'PEC'). | 
| `ementa` | TEXT | Resumo do tema ou propósito da Proposição. | 
| `numero_proposicao` | TEXT | Número de identificação da proposição. | 
| `data_apresentacao` | DATE | Data em que a proposição foi apresentada. | 

### Dimensão: `dim_tipo_despesa` (Tipos de Despesa de Gabinete)

| Coluna | Tipo | Descrição | 
 | ----- | ----- | ----- | 
| `sk_tipo_despesa` | INTEGER | **Chave Surrogate (SK)** do tipo de despesa. **Chave de JOIN.** | 
| `tipo_despesa` | TEXT | Categoria do Gasto (Ex: 'Telefonia', 'Combustível', 'Passagens Aéreas'). | 
| `natureza_despesa` | TEXT | Classificação da natureza da despesa. | 

## Estrutura das Tabelas (Fatos)

### Fato: `fat_despesa_gabinete` (Registro de Gastos)

| Coluna | Tipo | Descrição | Valores Possíveis / Detalhes | 
 | ----- | ----- | ----- | ----- | 
| `sk_autor_emenda_proposicao` | INTEGER | **Chave de JOIN** para `dim_autor_emenda_proposicao`. |  | 
| `sk_tipo_despesa` | INTEGER | **Chave de JOIN** para `dim_tipo_despesa`. |  | 
| `valor_despesa` | REAL | O valor exato da despesa em **Reais (R\$)**. |  | 
| `data_referencia` | DATE | Mês e ano da despesa (Formato: AAAA-MM-DD). |  | 

### Fato: `fat_voto_proposicao` (Registro de Votos)

| Coluna | Tipo | Descrição | Valores Possíveis / Detalhes | 
 | ----- | ----- | ----- | ----- | 
| `sk_autor_emenda_proposicao` | INTEGER | **Chave de JOIN** para a dimensão do autor (Deputado). | Para saber quem votou. | 
| `sk_proposicao` | INTEGER | **Chave de JOIN** para a dimensão da proposição. | Permite ligar o voto ao projeto específico. | 
| `opcao` | TEXT | Opção do voto. | **'Sim', 'Não', 'Branco'** | 
| `turno` | TEXT | Turno da votação. | **'1º Turno'; '2º Turno'; 'Turno Único'; 'Redação Final'** | 
| `data_votacao` | DATE | Data exata em que o voto foi registrado. |  | 

## Regras de BI Cruciais para o LLM

1. **Regra de JOIN Central:** O LLM **deve** usar as colunas `sk_autor_emenda_proposicao`, `sk_proposicao` e `sk_tipo_despesa` como chaves para fazer JOINs entre as tabelas de Fato e Dimensão.

2. **Regra de Moeda:** A coluna `valor_despesa` está em **Reais (R\$)**.

3. **Regra de Datas:** Use a função `STRFTIME` para manipular campos do tipo `DATE` (data_referencia, data_apresentacao, data_votacao, etc.) no SQLite.

4. **Regra de Nomes:** Use os nomes de colunas e tabelas exatamente como definidos neste esquema.

5. **Regra de Valores:** Use os valores válidos (ex: `'Sim'`, `'Não'`, `'2º Turno'`) na cláusula `WHERE` para filtragem.
"""


# --- Funções de Utilitário ---

@st.cache_resource
def load_db():
    """Baixa o arquivo SQLite do Hugging Face (se não existir) e retorna a conexão."""
    try:
        if not os.path.exists(DB_FILE):
            st.info(f"Baixando banco de dados '{DB_FILE}' de Hugging Face. Isso pode levar alguns segundos...")
            # Verifica se o token do HF está presente nos secrets para repositórios privados
            if "HF_TOKEN" in st.secrets:
                headers = {"Authorization": f"Bearer {st.secrets['HF_TOKEN']}"}
            else:
                headers = {}

            response = requests.get(DB_URL_RAW, stream=True, headers=headers)
            response.raise_for_status()
            
            with open(DB_FILE, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            st.success(f"Banco de dados '{DB_FILE}' baixado com sucesso!")
        
        conn = sqlite3.connect(DB_FILE)
        return conn
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401 or e.response.status_code == 403:
            st.error("Erro 401/403: Acesso não autorizado ao DB. Verifique se o repositório é privado e se a chave 'HF_TOKEN' está correta nos Secrets.")
        else:
            st.error(f"Erro HTTP ao carregar o banco de dados: {e}")
        st.stop()
    except Exception as e:
        st.error(f"Erro ao carregar ou conectar ao banco de dados: {e}")
        st.stop()

@st.cache_data
def get_db_schema(conn):
    """Extrai o esquema de todas as tabelas do SQLite e formata para o LLM."""
    # Esta função extrai a estrutura real do DB (DDL) para complementar o BI_SCHEMA_CONTEXT
    try:
        cursor = conn.cursor()
        schema = "--- ESQUEMA DO BANCO DE DADOS SQLite (DDL) ---\n"
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [t[0] for t in cursor.fetchall()]
        
        # Filtra apenas tabelas relevantes - AGORA ALINHADAS PERFEITAMENTE COM O BI_SCHEMA_CONTEXT
        relevant_tables = [t for t in tables if t in [
            'dim_autor_emenda_proposicao', 
            'dim_tipo_despesa', 
            'dim_proposicao', 
            # 'fat_autoria_proposicao' foi removida para simplificar
            'fat_despesa_gabinete', 
            'fat_voto_proposicao'
        ]]
        
        for table in relevant_tables:
            schema += f"\n-- Tabela: {table} --\n"
            cursor.execute(f"PRAGMA table_info({table});")
            columns = cursor.fetchall()
            
            col_definitions = [f"{col[1]} {col[2]}" for col in columns]
            schema += f"CREATE TABLE {table} ({', '.join(col_definitions)});\n"
            
        schema += "\n--- FIM DO ESQUEMA DDL ---\n"
        return schema
    except Exception as e:
        st.error(f"Erro ao extrair o esquema do banco de dados: {e}")
        return None

def execute_sql(conn, sql_query):
    """Executa a query SQL no banco de dados e retorna os resultados ou erro."""
    try:
        cursor = conn.cursor()
        cursor.execute(sql_query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        
        if data:
            results = [dict(zip(columns, row)) for row in data]
            return json.dumps(results, ensure_ascii=False)
        else:
            return "Nenhum resultado encontrado."
    except sqlite3.Error as e:
        return f"Erro de SQL: {e}"
    except Exception as e:
        return f"Erro inesperado ao executar SQL: {e}"

def call_gemini_api(system_instruction, user_prompt, max_retries=5):
    """
    Chama a API Gemini com tratamento de erro e backoff exponencial.
    Retorna o texto gerado.
    """
    # A API key é carregada automaticamente pelo ambiente Streamlit
    api_key = st.secrets.get("GEMINI_API_KEY", "") 
    api_url = f"{API_BASE_URL}{MODEL_NAME}:generateContent?key={api_key}"
    
    payload = {
        "contents": [{"parts": [{"text": user_prompt}]}],
        "systemInstruction": {"parts": [{"text": system_instruction}]},
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.post(
                api_url, 
                headers={'Content-Type': 'application/json'},
                data=json.dumps(payload)
            )
            response.raise_for_status() 
            
            result = response.json()
            # Extrai o texto da resposta
            text = result['candidates'][0]['content']['parts'][0]['text']
            return text
            
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429 and attempt < max_retries - 1:
                # Backoff exponencial
                wait_time = 2 ** attempt
                time.sleep(wait_time)
                continue
            st.error(f"Erro na API Gemini ({response.status_code}): {response.text}")
            return None
        except Exception as e:
            st.error(f"Erro inesperado na comunicação com a API Gemini: {e}")
            return None
    
    st.error("Falha ao chamar a API Gemini após várias tentativas (Timeout ou Rate Limit).")
    return None

# --- Pipeline Principal de BI Conversacional ---

def nl_to_sql_to_nl(user_question, conn, schema_ddl):
    """
    Passo 1: Linguagem Natural -> SQL Query 
    Passo 2: Executar SQL Query
    Passo 3: Resultado da Query -> Linguagem Natural
    """
    global BI_SCHEMA_CONTEXT
    
    # --- Passo 1: NL -> SQL ---
    
    # Instrução do sistema: Torna explícito que o CONTEXTO DE BI é a única fonte de regras.
    sql_system_instruction = (
        "Você é um especialista em SQL, focado em gerar consultas SQLite válidas a partir de perguntas em linguagem natural. "
        "Sua ÚNICA fonte de verdade para nomes de tabelas, chaves de junção (sk_) e regras de BI é o 'BI_SCHEMA_CONTEXT'. "
        "O Esquema DDL é apenas para referência de tipo. Use estritamente as regras de JOIN e os nomes definidos no CONTEXTO. "
        "Sua saída DEVE conter APENAS a instrução SQL. Não inclua explicações, código Python, nem formatação de markdown (como ```sql)."
    )
    
    sql_prompt = (
        f"--- CONTEXTO DE BI E REGRAS ---\n{BI_SCHEMA_CONTEXT}\n\n"
        f"--- ESQUEMA DDL REAL ---\n{schema_ddl}\n\n"
        "A partir da seguinte pergunta em português, gere a consulta SQL SQLite 3 correspondente. "
        "Pergunta: '{user_question}'"
    )
    
    with st.spinner("🧠 Processando pergunta e gerando SQL..."):
        sql_query = call_gemini_api(sql_system_instruction, sql_prompt.format(user_question=user_question))
        
    if not sql_query or "SELECT" not in sql_query.upper():
        st.warning("Não foi possível gerar uma consulta SQL válida. Tente ser mais específico sobre os dados que deseja.")
        return "Desculpe, não consegui converter sua pergunta em uma consulta SQL válida."

    sql_query = sql_query.strip().replace(";", "")
    st.session_state.chat_history.append({"role": "assistant_sql", "content": sql_query})
    
    # --- Passo 2: Executar SQL ---
    with st.spinner(f"🗄️ Executando consulta SQL: `{sql_query}`..."):
        sql_result = execute_sql(conn, sql_query)
        
    # --- Passo 3: Resultado -> NL ---
    
    nl_system_instruction = (
        "Você é um assistente de Business Intelligence. Sua tarefa é analisar o resultado de uma consulta SQL e responder à pergunta do usuário de forma amigável, concisa e informativa, em português. "
        "Use as regras de BI (R\$ e contexto ALMG) fornecidas no BI_SCHEMA_CONTEXT para enriquecer a resposta. "
        "Se o resultado for um erro de SQL, apenas relate o erro."
    )
    
    nl_prompt = (
        f"Pergunta Original: '{user_question}'\n"
        f"Consulta SQL Gerada: {sql_query}\n"
        f"Resultado da Consulta: {sql_result}"
    )
    
    with st.spinner("✍️ Gerando resposta em linguagem natural..."):
        final_response = call_gemini_api(nl_system_instruction, nl_prompt)
        
    return final_response

# --- Interface Streamlit ---

def main():
    st.title("🇧🇷 BI Conversacional (ALMG) com Gemini")
    
    st.sidebar.header("Configuração")
    st.sidebar.markdown(
        "Este aplicativo utiliza a API do Gemini para converter perguntas em Linguagem Natural "
        "em consultas SQL, executá-las em um banco de dados SQLite da ALMG e gerar a resposta final."
    )
    
    # Verifica chaves de API
    if "GEMINI_API_KEY" not in st.secrets:
        st.sidebar.error("Chave 'GEMINI_API_KEY' não encontrada nos secrets do Streamlit. Por favor, adicione-a.")
        return
        
    # Carrega DB e extrai o esquema
    conn = load_db()
    db_schema_ddl = get_db_schema(conn)

    if not db_schema_ddl:
        return

    st.sidebar.subheader("Esquema do DB (DDL extraído)")
    with st.sidebar.expander("Ver Esquema DDL Completo"):
        st.code(db_schema_ddl, language='sql')
    
    # Inicializa o histórico do chat
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []
        st.session_state.chat_history.append({
            "role": "assistant",
            "content": (
                "Assistente de BI ativado. O esquema que define as regras do banco de dados (chaves sk_, nomes de tabelas, R$) "
                "está no arquivo **`database_schema_oficial.md`** na barra lateral. Minha única função é gerar SQL com base nele. "
                "Qual análise de BI você precisa?"
            )
        })

    # Exibe o histórico do chat
    for message in st.session_state.chat_history:
        if message["role"] == "assistant_sql":
            # Exibe o SQL gerado para fins de transparência
            st.code(f"SQL Gerado: {message['content']}", language='sql')
        else:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])

    # Captura a entrada do usuário
    if prompt := st.chat_input("Faça sua pergunta de BI aqui..."):
        
        st.session_state.chat_history.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # Processa a pergunta e gera a resposta final
        with st.chat_message("assistant"):
            final_response = nl_to_sql_to_nl(prompt, conn, db_schema_ddl)
            if final_response:
                st.markdown(final_response)
                st.session_state.chat_history.append({"role": "assistant", "content": final_response})

if __name__ == "__main__":
    main()
