import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, inspect
import os
import requests
import re
import google.generativeai as genai

# --- CONFIGURAÇÃO E DEFINIÇÕES ---
DB_FILE = 'almg_local.db'
DOC_FILE = 'armazem.pdf' # Novo arquivo de documentação
RELATIONS_FILE = "relacoes.txt"
DB_SQLITE = f'sqlite:///{DB_FILE}'

# URLS (ADICIONADO DOWNLOAD_DOC_URL - AJUSTE ESTE URL PARA ONDE O PDF ESTIVER)
DOWNLOAD_DB_URL = "https://huggingface.co/datasets/TiagoPianezzola/BI/resolve/main/almg_local.db"
DOWNLOAD_RELATIONS_URL = "https://huggingface.co/datasets/TiagoPianezzola/BI/resolve/main/relacoes.txt"
# Assumindo que o PDF será colocado no mesmo repositório
DOWNLOAD_DOC_URL = "https://huggingface.co/datasets/TiagoPianezzola/BI/resolve/main/armazem.pdf"


# 1. LISTAS DE COLUNAS FIXAS POR INTENÇÃO
PROPOSICAO_COLS = [
    "dp.tipo_descricao", "dp.numero", "dp.ano", "dp.ementa", "dp.url"
]
# CORREÇÃO: URL AGORA VEM DE dnj.url
NORMA_COLS = [
    "dnj.tipo_descricao AS tipo_norma", "dnj.numeracao AS numero_norma",
    "dnj.ano AS ano_norma", "dnj.ementa AS ementa_norma",
    "dnj.url AS url"  # <--- CORREÇÃO APLICADA
]

# 2. DEFINIÇÕES DE ROTAS (MANTIDAS)
NORMA_KEYWORDS = ['norma', 'lei', 'ato', 'legislação', 'decreto', 'resolução', 'publicada']
NORMA_KEYWORDS_STR = ", ".join([f"'{k}'" for k in NORMA_KEYWORDS])

# **INSTRUÇÃO AJUSTADA (NORMA):** Foco explícito em dnj e remoção do JOIN redundante com dim_data
NORMA_JOIN_INSTRUCTION = (
    "Para consultar **Normas publicadas** (Leis, Decretos, Resoluções), **você DEVE** usar o caminho completo: "
    "FROM dim_proposicao AS dp "
    "INNER JOIN fat_proposicao_proposicao_lei_norma_juridica AS fplnj ON dp.sk_proposicao = fplnj.sk_proposicao "
    "INNER JOIN dim_norma_juridica AS dnj ON fplnj.sk_norma_juridica = dnj.sk_norma_juridica "
    "INNER JOIN fat_publicacao_norma_juridica AS fpnj ON dnj.sk_norma_juridica = fpnj.sk_norma_juridica. "
    "**NÃO FAÇA JOIN com dim_data (dd)** para filtros simples de ano. "
    "**FILTRO DE DATA (OBRIGATÓRIO)**: Use o campo de texto `fpnj.DATA` com `STRFTIME('%Y', fpnj.DATA) = 'YYYY'` para filtrar o ano da publicação."
    "**Foco:** Os dados da Norma (incluindo o **URL**, que está em `dnj.url`) e os filtros devem vir de `dnj` e `fpnj`."
)

# **INSTRUÇÃO AJUSTADA (PROPOSIÇÃO):** Prioridade para dp.tipo_sigla e ATENÇÃO À PONTUAÇÃO (RQC e PL.)
PROPOSICAO_JOIN_INSTRUCTION = (
    "Para consultar Proposições (Projetos, Requerimentos, etc.), use: "
    "FROM dim_proposicao AS dp. "
    "**PREFERÊNCIA DE FILTRO DE TIPO**: SEMPRE use `dp.tipo_sigla` para filtrar o tipo de proposição. "
    "**SIGLAS ESPECÍFICAS (Obrigatório)**: "
    "- 'Projeto de Lei' usa **`'PL.'`** (com ponto final). "
    "- 'Requerimento de Comissão' usa **`'RQC'`** (sem ponto final). "
    "- Outras siglas (como PEC, REQ, etc.) devem seguir a formatação exata da base de dados (com ou sem ponto)."
    "Use JOINs com outras dimensões (como dim_autor_proposicao (dap), dim_data (dd) via dp.sk_data_protocolo = dd.sk_data, etc.) conforme necessário."
)

# **INSTRUÇÃO AJUSTADA (ROBUSTEZ):** REGRA DE ANO FUTURO REMOVIDA
ROBUSTEZ_INSTRUCAO = (
    "**ROBUSTEZ DE FILTROS:**\n"
    "1. **Nomes de Autores (dap.nome):** SEMPRE use `LOWER(dap.nome) LIKE LOWER('%nome do autor%')` para evitar erros de maiúsculas/minúsculas ou sobrenomes/títulos incompletos.\n"
    "2. **Filtro de Ano:** Use o ano exato fornecido pelo usuário. Não substitua anos futuros, mesmo que possam retornar resultados vazios."
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

# Função para buscar todos os segredos
def get_secrets():
    return {
        "gemini_key": st.secrets.get("GOOGLE_API_KEY", ""),
        "hf_token": st.secrets.get("HF_TOKEN", "") 
    }

# Função auxiliar para manter a compatibilidade
def get_api_key():
    return get_secrets()["gemini_key"]

# Função download_file AGORA LÊ O HF_TOKEN E O INCLUI NO HEADER
def download_file(url, dest_path, description):
    if os.path.exists(dest_path):
        return True
    
    secrets = get_secrets()
    hf_token = secrets["hf_token"]
    
    st.info(f"Iniciando download do {description}...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        
        # Adiciona o token de autorização se estiver disponível
        if hf_token:
            headers["Authorization"] = f"Bearer {hf_token}"
            
        response = requests.get(url.strip(), stream=True, headers=headers)
        response.raise_for_status() # Verifica erros HTTP (incluindo 401/403)
        
        with open(dest_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024*1024): 
                if chunk:
                    f.write(chunk)
        st.success(f"Download do {description} concluído com sucesso.")
        return True
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code
        if status_code in [401, 403]:
            st.error(f"Erro {status_code}: O {description} está privado ou o token expirou. Verifique se o **HF_TOKEN** no `.streamlit/secrets.toml` é válido e tem permissão de leitura.")
        else:
            st.error(f"Erro no download do {description} de {url} (HTTP {status_code}): {e}")
        return False
    except Exception as e:
        st.error(f"Erro no download do {description} de {url}: {e}")
        return False

@st.cache_data
def load_documentation_content(doc_path):
    """Lê o conteúdo do PDF como um texto para injeção no prompt (simulando a extração)"""
    if not os.path.exists(doc_path):
        return "ATENÇÃO: Arquivo de documentação semântica armazem.pdf não encontrado. Contexto de filtros e valores pode estar incompleto."
    
    try:
        with open(doc_path, 'r', encoding='utf-8') as f:
            content = f.read()
            return f"--- INÍCIO DA DOCUMENTAÇÃO SEMÂNTICA ARMÁZEM ALMG ---\n{content}\n--- FIM DA DOCUMENTAÇÃO SEMÂNTICA ---"
    except UnicodeDecodeError:
        return "ATENÇÃO: Documento armazem.pdf não pôde ser lido como texto simples para injeção no prompt. Use uma biblioteca de extração de PDF."
    except Exception as e:
        return f"ATENÇÃO: Erro ao ler armazem.pdf: {e}"

@st.cache_data
def download_database_and_relations():
    db_ok = download_file(DOWNLOAD_DB_URL, DB_FILE, "banco de dados (almg_local.db)")
    rel_ok = download_file(DOWNLOAD_RELATIONS_URL, RELATIONS_FILE, "arquivo de relações (relacoes.txt)")
    doc_ok = download_file(DOWNLOAD_DOC_URL, DOC_FILE, "documentação semântica (armazem.pdf)")
    
    return db_ok and rel_ok and doc_ok

# O resto do código permanece o mesmo, exceto pela injeção no prompt

TABLE_ID_TO_NAME = {
    12: "fat_proposicao", 18: "dim_tipo_proposicao", 21: "dim_situacao", 24: "dim_ementa", 78: "dim_norma_juridica",
    111: "dim_proposicao", 414: "fat_proposicao_proposicao_lei_norma_juridica", 
    27: "dim_autor_proposicao", 30: "dim_comissao", 33: "dim_comissao_acao_reuniao", 42: "dim_data", 54: "dim_deputado_estadual", 69: "dim_partido", 
    87: "dim_data_publicacao_proposicao", 198: "dim_deputado_estadual", 201: "dim_data", 228: "dim_proposicao",
    42: "dim_data", 84: "dim_data", 534: "fat_proposicao_tramitacao", 540: "dim_proposicao", 
    606: "fat_rqc", 609: "fat_proposicao", 612: "fat_publicacao_norma_juridica",
}

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

@st.cache_resource
def get_database_engine():
    if not download_database_and_relations():
        return None, "Falha na inicialização: Não foi possível baixar todos os arquivos necessários.", None

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

# --- FUNÇÃO PRINCIPAL DO ASSISTENTE ---
def executar_plano_de_analise(engine, esquema, prompt_usuario):
    API_KEY = get_api_key()
    if not API_KEY:
        return "Erro: A chave de API do Gemini (GOOGLE_API_KEY) não foi configurada.", None

    query_sql = ""
    try:
        genai.configure(api_key=API_KEY)
        model = genai.GenerativeModel('gemini-2.5-flash')
        
        # 1. CARREGA O CONTEÚDO DA DOCUMENTAÇÃO
        documentacao_semantica = load_documentation_content(DOC_FILE)
        
        instrucao = (
            f"Você é um assistente de análise de dados da Assembleia Legislativa de Minas Gerais (ALMG). "
            f"Sua tarefa é converter a pergunta do usuário em uma única consulta SQL no dialeto SQLite. "
            f"SEMPRE use INNER JOIN para combinar tabelas, seguindo as RELAÇÕES PRINCIPAIS. "
            f"Se a pergunta envolver data, ano, legislatura ou período, FAÇA JOIN com dim_data. "
            f"**ATENÇÃO:** Use 'dp' como alias para 'dim_proposicao', 'dnj' para 'dim_norma_juridica' e 'dd' para 'dim_data'."
            
            # 2. INJETA O CONTEÚDO DA DOCUMENTAÇÃO NO PROMPT
            f"*** REGRAS SEMÂNTICAS (CONSULTE ESTA SEÇÃO ANTES DE GERAR O SQL) ***:\n{documentacao_semantica}\n\n"
            
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

        # --- REORDENAÇÃO E CRIAÇÃO DO LINK HTML ---
        if 'url' in df_resultado.columns:
            # 1. Cria a coluna Link com HTML (o ícone 🔗)
            df_resultado['Link'] = df_resultado['url'].apply(
                lambda x: f'<a href="{x}" target="_blank">🔗</a>' if pd.notna(x) else ""
            )
            
            # 2. Define a ordem final das colunas
            # Ordem desejada: tipo_descricao, numero, ano, ementa, Link
            expected_order = ['tipo_descricao', 'numero', 'ano', 'ementa', 'Link']
            
            # Se for Norma, a ordem é ajustada para os nomes das colunas de Norma
            if 'tipo_norma' in df_resultado.columns:
                 expected_order = ['tipo_norma', 'numero_norma', 'ano_norma', 'ementa_norma', 'Link']

            
            # 3. Constroi a nova ordem baseada nas colunas existentes
            new_order = [col for col in expected_order if col in df_resultado.columns]
            
            # 4. Adiciona outras colunas que vieram na query (para robustez)
            for col in df_resultado.columns:
                if col not in new_order and col != 'url':
                    new_order.append(col)
                    
            # 5. Reordena o DataFrame e remove a coluna 'url'
            df_resultado = df_resultado.drop(columns=['url'])
            df_resultado = df_resultado[new_order]
            
            return "Query executada com sucesso!", df_resultado

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
                    # escape=False garante que o HTML (ícone 🔗) seja renderizado
                    st.write(resultado.to_html(escape=False), unsafe_allow_html=True)
                st.info(f"Status: {mensagem}")
        else:
            st.warning("Por favor, digite uma pergunta para iniciar a análise.")
