import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, inspect
import os
import requests
import re
import google.generativeai as genai

# --- CONFIGURAÇÃO E DEFINIÇÕES ---
DB_FILE = 'almg_local.db'
DB_SQLITE = f'sqlite:///{DB_FILE}'
# URLS
DOWNLOAD_DB_URL = "https://huggingface.co/datasets/TiagoPianezzola/BI/resolve/main/almg_local.db"
DOWNLOAD_RELATIONS_URL = "https://huggingface.co/datasets/TiagoPianezzola/BI/resolve/main/relacoes.txt"
RELATIONS_FILE = "relacoes.txt"

# 1. LISTAS DE COLUNAS FIXAS POR INTENÇÃO (MANTIDAS)
PROPOSICAO_COLS = [
    "dp.tipo_descricao", "dp.numero", "dp.ano", "dp.ementa", "dp.url"
]
NORMA_COLS = [
    "dnj.tipo_descricao AS tipo_norma", "dnj.numeracao AS numero_norma", 
    "dnj.ano AS ano_norma", "dnj.ementa AS ementa_norma", 
    "dp.url"
]

# 2. DEFINIÇÕES DE ROTAS (MANTIDAS)
NORMA_KEYWORDS = ['norma', 'lei', 'ato', 'legislação', 'decreto', 'resolução', 'publicada']
NORMA_KEYWORDS_STR = ", ".join([f"'{k}'" for k in NORMA_KEYWORDS])

# Instruções de JOIN (mantidas)
NORMA_JOIN_INSTRUCTION = (
    "Para consultar Normas, você DEVE usar o caminho de Proposição para Norma: "
    "FROM dim_proposicao AS dp "
    "INNER JOIN fat_proposicao_proposicao_lei_norma_juridica AS fplnj ON dp.sk_proposicao = fplnj.sk_proposicao "
    "INNER JOIN dim_norma_juridica AS dnj ON fplnj.sk_norma_juridica = dnj.sk_norma_juridica. "
    "**PARA FILTRAR POR DATA (OBRIGATÓRIO PARA 'publicada')**: Use a fat_publicacao_norma_juridica (alias fpnj) e a dim_data (alias dd). **O JOIN DE DATA DEVE SER SEMPRE FEITO PELA CHAVE: ON fpnj.data_publicacao = dd.sk_data**. Use **`fpnj.data_publicacao`** como a chave estrangeira de data."
    "Quando usar dnj, **NUNCA filtre por dp.tipo_descricao**."
)

PROPOSICAO_JOIN_INSTRUCTION = (
    "Para consultar Proposições (Projetos, Requerimentos, etc.), use: "
    "FROM dim_proposicao AS dp. "
    "Use JOINs com outras dimensões (como dim_autor_proposicao (dap), dim_data (dd) via dp.sk_data_protocolo = dd.sk_data, etc.) conforme necessário."
)

# **ADICIONADO:** Instrução de Robustez para os filtros
ROBUSTEZ_INSTRUCAO = (
    "**ROBUSTEZ DE FILTROS:**\n"
    "1. **Nomes de Autores (dap.nome):** SEMPRE use `LOWER(dap.nome) LIKE LOWER('%nome do autor%')` para evitar erros de maiúsculas/minúsculas ou sobrenomes/títulos incompletos.\n"
    "2. **Ano:** Se o usuário perguntar por um ano futuro (ex: 2025, sendo que a base só tem 2024), **substitua o ano futuro pelo ano de 2024** para demonstrar a funcionalidade, a menos que o ano seja claramente um filtro histórico (ex: 2010)."
)


ROTEAMENTO_INSTRUCAO = f"""
**ANÁLISE DE INTENÇÃO (ROTEAMENTO OBRIGATÓRIO):**
1. Se a pergunta do usuário contiver as palavras-chave de NORMA ({NORMA_KEYWORDS_STR}), use a instrução de JOIN de NORMA:
   - COLUNAS OBRIGATÓRIAS: {", ".join(NORMA_COLS)}
   - FROM/JOIN OBRIGATÓRIO: {NORMA_JOIN_INSTRUCTION}
2. Caso contrário (Projetos, Requerimentos, etc.), use a instrução de JOIN de PROPOSIÇÃO:
   - COLUNAS OBRIGATÓRIAS: {", ".join(PROPOSICAO_COLS)}
   - FROM/JOIN OBRIGATÓRIO: {PROPOSICAO_JOIN_INSTRUCTION}
3. **SEMPRE USE DISTINCT.**
{ROBUSTEZ_INSTRUCAO}
"""

# --- FUNÇÕES DE INFRAESTRUTURA (MANTIDAS) ---
def get_api_key():
    return st.secrets.get("GOOGLE_API_KEY", "") 

def download_file(url, dest_path, description):
    if os.path.exists(dest_path):
        return True
    
    st.info(f"Iniciando download do {description}...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url.strip(), stream=True, headers=headers)
        response.raise_status()
        
        with open(dest_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024*1024): 
                if chunk:
                    f.write(chunk)
        st.success(f"Download do {description} concluído com sucesso.")
        return True
    except Exception as e:
        st.error(f"Erro no download do {description} de {url}: {e}")
        return False

@st.cache_data
def download_database_and_relations():
    db_ok = download_file(DOWNLOAD_DB_URL, DB_FILE, "banco de dados (almg_local.db)")
    rel_ok = download_file(DOWNLOAD_RELATIONS_URL, RELATIONS_FILE, "arquivo de relações (relacoes.txt)")
    return db_ok and rel_ok

def load_relationships_from_file(relations_file=RELATIONS_FILE):
    if not os.path.exists(relations_file):
        st.warning(f"Arquivo de relações '{relations_file}' não encontrado. O sistema de JOIN pode falhar.")
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
        st.error(f"Erro ao carregar {relations_file}. Verifique o formato do arquivo: {e}")
        return {}

TABLE_ID_TO_NAME = {
    12: "fat_proposicao", 18: "dim_tipo_proposicao", 21: "dim_situacao", 24: "dim_ementa", 78: "dim_norma_juridica",
    111: "dim_proposicao", 414: "fat_proposicao_proposicao_lei_norma_juridica", 
    27: "dim_autor_proposicao", 30: "dim_comissao", 33: "dim_comissao_acao_reuniao", 42: "dim_data", 54: "dim_deputado_estadual", 69: "dim_partido", 
    87: "dim_data_publicacao_proposicao", 198: "dim_deputado_estadual", 201: "dim_data", 228: "dim_proposicao",
    42: "dim_data", 84: "dim_data", 534: "fat_proposicao_tramitacao", 540: "dim_proposicao", 
    606: "fat_rqc", 609: "fat_proposicao", 612: "fat_publicacao_norma_juridica",
}

@st.cache_resource
def get_database_engine():
    if not download_database_and_relations():
        return None, "Download do banco de dados ou relações falhou. Não é possível conectar.", None

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
            esquema += f"Tabela {tabela} (Colunas: {', '.join(colunas_com_tipo)})\n"

        rel_map = load_relationships_from_file(RELATIONS_FILE) 
        esquema += "\nRELAÇÕES PRINCIPAIS (JOINs sugeridos):\n"
        for from_id, to_ids in rel_map.items():
            from_name = TABLE_ID_TO_NAME.get(from_id, f"tabela_{from_id}")
            to_names = [TABLE_ID_TO_NAME.get(tid, f"tabela_{tid}") for tid in to_ids]
            esquema += f"- {from_name} se relaciona com: {', '.join(to_names)}\n"

        esquema += "\nDICA: Use INNER JOIN entre tabelas relacionadas. As chaves geralmente seguem o padrão 'sk_<nome>'.\n"
        
        return engine, esquema, None 

    except Exception as e:
        return None, f"Erro ao conectar ao SQLite: {e}", None

# --- FUNÇÃO PRINCIPAL DO ASSISTENTE (MANTIDA) ---
def executar_plano_de_analise(engine, esquema, prompt_usuario):
    API_KEY = get_api_key()
    if not API_KEY:
        return "Erro: A chave de API do Gemini não foi configurada no `.streamlit/secrets.toml`.", None

    query_sql = ""
    try:
        genai.configure(api_key=API_KEY)
        model = genai.GenerativeModel('gemini-2.5-flash')
        
        instrucao = (
            f"Você é um assistente de análise de dados da Assembleia Legislativa de Minas Gerais (ALMG). "
            f"Sua tarefa é converter a pergunta do usuário em uma única consulta SQL no dialeto SQLite. "
            f"SEMPRE use INNER JOIN para combinar tabelas, seguindo as RELAÇÕES PRINCIPAIS. "
            f"Se a pergunta envolver data, ano, legislatura ou período, FAÇA JOIN com dim_data. "
            f"**ATENÇÃO:** Use 'dp' como alias para 'dim_proposicao', 'dnj' para 'dim_norma_juridica' e 'dd' para 'dim_data'."
            f"{ROTEAMENTO_INSTRUCAO}" 
            f"Esquema e relações:\n{esquema}\n\n"
            f"Pergunta do usuário: {prompt_usuario}"
        )

        response = model.generate_content(instrucao)
        query_sql = response.text.strip()
        
        # --- Limpeza da Query ---
        query_sql = re.sub(r'^[^`]*```sql\s*', '', query_sql, flags=re.DOTALL)
        query_sql = re.sub(r'```.*$', '', query_sql, flags=re.DOTALL).strip()
        
        match = re.search(r'(SELECT.*)', query_sql, flags=re.IGNORECASE | re.DOTALL)
        if match:
            query_sql = match.group(1).strip()
        # ------------------------

        st.subheader("Query SQL Gerada:")
        st.code(query_sql, language='sql')

        df_resultado = pd.read_sql(query_sql, engine)

        # --- FORMATAÇÃO DO URL (MANTIDA) ---
        if 'url' in df_resultado.columns:
            df_resultado['Link'] = df_resultado['url'].apply(
                lambda x: f'<a href="{x}" target="_blank">🔗</a>' if pd.notna(x) else ""
            )
            df_resultado = df_resultado.drop(columns=['url'])
            
            if 'numero_norma' in df_resultado.columns and 'Link' in df_resultado.columns:
                cols = df_resultado.columns.tolist()
                idx_numero = cols.index('numero_norma')
                cols.remove('Link')
                cols.insert(idx_numero + 1, 'Link')
                df_resultado = df_resultado[cols]
            elif 'numero' in df_resultado.columns and 'Link' in df_resultado.columns:
                 cols = df_resultado.columns.tolist()
                 idx_numero = cols.index('numero')
                 cols.remove('Link')
                 cols.insert(idx_numero + 1, 'Link')
                 df_resultado = df_resultado[cols]
            
            df_styler = df_resultado.style.format({'Link': lambda x: x}, escape="html")
            return "Query executada com sucesso!", df_styler

        return "Query executada com sucesso!", df_resultado

    except Exception as e:
        error_msg = f"Erro ao executar a query: {e}"
        if query_sql:
            error_msg += f"\n\nQuery gerada (pós-limpeza): {query_sql}"
        return error_msg, None
    
# --- STREAMLIT UI PRINCIPAL (MANTIDA) ---
st.title("🤖 Assistente BI da ALMG (SQLite Local)")

engine, esquema_db, _ = get_database_engine() 

if engine is None:
    st.error(esquema_db)
else:
    with st.sidebar:
        st.subheader("Regras de Roteamento")
        st.markdown(ROTEAMENTO_INSTRUCAO)
        st.markdown("---")
        with st.expander("Esquema Detalhado (Para fins de debug)"):
            st.code(esquema_db)

    prompt_usuario = st.text_area(
        "Faça uma pergunta sobre os dados da ALMG (Ex: 'Quais leis foram publicadas em setembro de 2024?' ou 'Quantos projetos de lei de 2023 estão parados na comissão X?')",
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
        else:
            st.warning("Por favor, digite uma pergunta para iniciar a análise.")
