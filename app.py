import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, inspect
import os
import requests
import re
import google.generativeai as genai

# --- CONFIGURAÇÃO E DEFINIÇÕES ---
DB_FILE = 'almg_local.db'
DOC_FILE = 'armazem_estruturado.txt'  # Arquivo local no repositório
RELATIONS_FILE = "relacoes.txt"
DB_SQLITE = f'sqlite:///{DB_FILE}'

# URLs (apenas para o banco de dados e relacoes.txt, que estão no Hugging Face)
DOWNLOAD_DB_URL = "https://huggingface.co/datasets/TiagoPianezzola/BI/resolve/main/almg_local.db"
DOWNLOAD_RELATIONS_URL = "https://huggingface.co/datasets/TiagoPianezzola/BI/resolve/main/relacoes.txt"

# --- FUNÇÕES DE INFRAESTRUTURA ---

def get_secrets():
    return {
        "gemini_key": st.secrets.get("GOOGLE_API_KEY", ""),
        "hf_token": st.secrets.get("HF_TOKEN", "") 
    }

def get_api_key():
    return get_secrets()["gemini_key"]

def download_file(url, dest_path, description):
    if os.path.exists(dest_path):
        return True
    
    secrets = get_secrets()
    hf_token = secrets["hf_token"]
    
    st.info(f"Iniciando download do {description}...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        if hf_token:
            headers["Authorization"] = f"Bearer {hf_token}"
            
        response = requests.get(url.strip(), stream=True, headers=headers)
        response.raise_for_status()
        
        with open(dest_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024*1024): 
                if chunk:
                    f.write(chunk)
        st.success(f"Download do {description} concluído com sucesso.")
        return True
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code
        if status_code in [401, 403]:
            st.error(f"Erro {status_code}: O {description} está privado ou o token expirou. Verifique o HF_TOKEN.")
        else:
            st.error(f"Erro no download do {description} de {url} (HTTP {status_code}): {e}")
        return False
    except Exception as e:
        st.error(f"Erro no download do {description} de {url}: {e}")
        return False

@st.cache_data
def load_documentation_content(doc_path):
    """Carrega o conteúdo do arquivo armazem_estruturado.txt (local)."""
    if not os.path.exists(doc_path):
        return "ERRO: Arquivo de documentação semântica armazem_estruturado.txt não encontrado no diretório do projeto."
    try:
        with open(doc_path, 'r', encoding='utf-8') as f:
            content = f.read()
            return f"--- DOCUMENTAÇÃO COMPLETA DO ESQUEMA DO ARMAZÉM ALMG ---\n{content}\n--- FIM DA DOCUMENTAÇÃO ---"
    except Exception as e:
        return f"ERRO ao ler armazem_estruturado.txt: {e}"

@st.cache_data
def download_database_and_relations():
    # O arquivo de documentação NÃO é baixado; ele já está no repo.
    db_ok = download_file(DOWNLOAD_DB_URL, DB_FILE, "banco de dados (almg_local.db)")
    rel_ok = download_file(DOWNLOAD_RELATIONS_URL, RELATIONS_FILE, "arquivo de relações (relacoes.txt)")
    return db_ok and rel_ok

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
        st.warning(f"Arquivo de relações '{relations_file}' não encontrado.")
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
        
        # Carrega a documentação completa (do arquivo local)
        documentacao_semantica = load_documentation_content(DOC_FILE)
        
        # --- PROMPT PRINCIPAL COM A DOCUMENTAÇÃO COMPLETA ---
        instrucao = f"""
Você é um especialista em SQL para o Armazém de Dados da Assembleia Legislativa de Minas Gerais (ALMG).
Sua única tarefa é gerar uma consulta SQL válida no dialeto SQLite com base na pergunta do usuário.

### REGRAS ABSOLUTAS
1. **USE SOMENTE** as tabelas e colunas listadas na seção "DOCUMENTAÇÃO COMPLETA DO ESQUEMA".
2. **NÃO INVENTE** nomes de colunas, tabelas, aliases ou valores que não estejam na documentação.
3. **SEMPRE** use `SELECT DISTINCT`.
4. **SEMPRE** use os aliases de tabela padronizados:
   - `dp` para `dim_proposicao`
   - `dnj` para `dim_norma_juridica`
   - `dap` para `dim_autor_proposicao`
   - `dd` para `dim_data`
   - `fpnj` para `fat_publicacao_norma_juridica`
   - `fap` para `fat_autoria_proposicao`
5. Para normas publicadas, **USE O JOIN** com `fat_publicacao_norma_juridica`.
6. Para proposições prontas para ordem do dia, **USE** `pronta_para_ordem_do_dia = 'Sim'`.

### DOCUMENTAÇÃO COMPLETA DO ESQUEMA DO ARMAZÉM ALMG
{documentacao_semantica}

### PERGUNTA DO USUÁRIO
{prompt_usuario}

### SUA TAREFA
Gere APENAS o código SQL, sem explicações, comentários ou markdown. Comece com "SELECT".
"""

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

        # Renomeação fallback
        if 'partido_atual' in df_resultado.columns:
            df_resultado.rename(columns={'partido_atual': 'Partido'}, inplace=True)
        if 'dep_partido_atual' in df_resultado.columns:
            df_resultado.rename(columns={'dep_partido_atual': 'Partido'}, inplace=True)

        # --- LÓGICA DE CONTADOR E CONCORDÂNCIA GRAMATICAL ---
        total_encontrado = len(df_resultado)
        if 'Tipo' in df_resultado.columns:
            item_type_plural = 'itens'
            if 'projetos de lei' in prompt_usuario.lower():
                item_type_plural = 'Projetos de Lei'
            elif 'leis' in prompt_usuario.lower():
                 item_type_plural = 'Leis'
            elif 'normas' in prompt_usuario.lower():
                 item_type_plural = 'Normas'
            
            if total_encontrado == 1:
                if item_type_plural == 'Projetos de Lei':
                    item_type = 'Projeto de Lei'
                elif item_type_plural == 'Leis':
                    item_type = 'Lei'
                elif item_type_plural == 'Normas':
                    item_type = 'Norma'
                else:
                    item_type = 'item'
            else:
                item_type = item_type_plural

            status_desc = ''
            if 'prontos para ordem do dia' in prompt_usuario.lower() or 'pronto para ordem do dia' in prompt_usuario.lower():
                if 'Lei' in item_type:
                    status_adj = ' pronta' if total_encontrado == 1 else ' prontas'
                else:
                    status_adj = ' pronto' if total_encontrado == 1 else ' prontos'
                status_desc = f"{status_adj} para Ordem do Dia em Plenário."
            elif 'publicados' in prompt_usuario.lower() or 'publicadas' in prompt_usuario.lower():
                 if 'Lei' in item_type:
                     status_adj = ' publicada' if total_encontrado == 1 else ' publicadas'
                 else:
                     status_adj = ' publicado' if total_encontrado == 1 else ' publicados'
                 status_desc = f"{status_adj}."
            else:
                 status_desc = '.'
                 
            frase_total = f"Há **{total_encontrado}** {item_type}{status_desc} Confira a seguir:"
            st.markdown(frase_total)

        elif 'Quantidade de Projetos' in df_resultado.columns:
            item_type_plural = 'Projetos de Lei' if 'projetos de lei' in prompt_usuario.lower() else 'Proposições'
            item_type = item_type_plural[:-1] if total_encontrado == 1 and item_type_plural.endswith('s') else item_type_plural
            frase_total = f"Há **{total_encontrado}** {item_type} encontrados(as) entre 2023 e 2025. Confira os top 10:"
            st.markdown(frase_total)

        # --- Criação do Link e Reordenação ---
        if 'url' in df_resultado.columns:
            df_resultado['Link'] = df_resultado['url'].apply(
                lambda x: f'<a href="{x}" target="_blank">🔗</a>' if pd.notna(x) else ""
            )
            expected_order = ['Tipo', 'Número', 'Ano', 'Ementa', 'Partido', 'Link']
            new_order = [col for col in expected_order if col in df_resultado.columns]
            for col in df_resultado.columns:
                if col not in new_order and col != 'url':
                    new_order.append(col)
            df_resultado = df_resultado.drop(columns=['url'], errors='ignore')
            df_resultado = df_resultado[new_order]

        # --- APLICAÇÃO DE ESTILO ---
        styler = df_resultado.style.set_properties(**{'text-align': 'center'})
        if 'Quantidade de Projetos' in df_resultado.columns:
            styler = styler.format({"Quantidade de Projetos": "{:.0f}"})
        styler = styler.set_table_styles([{'selector': 'th', 'props': [('text-align', 'center')]}])
        table_html = styler.to_html(escape=False, index=False)
        table_html = table_html.replace('<thead>\n<tr><th></th>', '<thead>\n<tr>')
        table_html = re.sub(r'<tr>\s*<td>\s*\d+\s*</td>', '<tr>', table_html, flags=re.DOTALL)
        html_output = table_html.replace('\n', '')
        
        return "Query executada com sucesso!", html_output

    except Exception as e:
        error_msg = f"Erro ao executar a query: {e}"
        if query_sql:
            error_msg += f"\n\nQuery gerada (pós-limpeza): {query_sql}"
        return error_msg, None

# --- STREAMLIT UI PRINCIPAL ---
st.title("🤖 Assistente BI da ALMG (SQLite Local)")

engine, esquema_db, _ = get_database_engine() 

if engine is None:
    st.error(esquema_db)
else:
    with st.sidebar:
        st.subheader("Esquema Detalhado")
        with st.expander("Ver"):
            st.code(esquema_db)

    prompt_usuario = st.text_area(
        "Faça uma pergunta sobre os dados da ALMG (Ex: 'Quais leis foram publicadas em setembro de 2024?' ou 'Quantos projetos de lei de 2023 estão prontos para ordem do dia?')",
        height=100
    )

    if st.button("Executar Análise"):
        if prompt_usuario:
            with st.spinner("Processando... Gerando e executando a consulta SQL."):
                mensagem, resultado = executar_plano_de_analise(engine, esquema_db, prompt_usuario) 
                if resultado is not None:
                    st.subheader("Resultado da Análise")
                    st.write(resultado, unsafe_allow_html=True)
                st.info(f"Status: {mensagem}")
        else:
            st.warning("Por favor, digite uma pergunta para iniciar a análise.")
