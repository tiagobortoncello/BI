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
DOWNLOAD_URL = "https://huggingface.co/datasets/TiagoPianezzola/BI/resolve/main/almg_local.db"

# 1. LISTAS DE COLUNAS FIXAS POR INTENÇÃO
# Colunas principais para Proposições (Projetos, Requerimentos, etc.)
PROPOSICAO_COLS = [
    "dp.tipo_descricao", "dp.numero", "dp.ano", "dp.ementa", "dp.url"
]
# Colunas principais para Normas (Leis publicadas, Decretos, etc.)
NORMA_COLS = [
    "dnj.tipo_descricao AS tipo_norma", "dnj.numeracao AS numero_norma", 
    "dnj.ano AS ano_norma", "dnj.ementa AS ementa_norma", 
    "dp.url" # Mantém a URL da proposição para o link
]

# 2. DEFINIÇÕES DE ROTAS
# Palavras-chave que indicam a necessidade da tabela dim_norma_juridica
NORMA_KEYWORDS = ['norma', 'lei', 'ato', 'legislação', 'decreto', 'resolução', 'publicada']
NORMA_KEYWORDS_STR = ", ".join([f"'{k}'" for k in NORMA_KEYWORDS])

# Instrução para JOIN da dim_norma_juridica (alias dnj)
NORMA_JOIN_INSTRUCTION = (
    "Para consultar Normas, você DEVE usar: "
    "FROM dim_proposicao AS dp "
    "INNER JOIN fat_proposicao_proposicao_lei_norma_juridica AS fplnj ON dp.sk_proposicao = fplnj.sk_proposicao "
    "INNER JOIN dim_norma_juridica AS dnj ON fplnj.sk_norma_juridica = dnj.sk_norma_juridica. "
    "Quando usar dnj, **NUNCA filtre por dp.tipo_descricao**."
)

# Instrução para JOIN da dim_proposicao (alias dp)
PROPOSICAO_JOIN_INSTRUCTION = (
    "Para consultar Proposições (Projetos, Requerimentos, etc.), use: "
    "FROM dim_proposicao AS dp. "
    "Use JOINs com outras dimensões (como dim_autor_proposicao, dim_data, etc.) conforme necessário."
)

# 3. INSTRUÇÃO PRINCIPAL DE ROTEAMENTO CONDICIONAL
ROTEAMENTO_INSTRUCAO = f"""
**ANÁLISE DE INTENÇÃO (ROTEAMENTO OBRIGATÓRIO):**
1. Se a pergunta do usuário contiver as palavras-chave de NORMA ({NORMA_KEYWORDS_STR}), use a instrução de JOIN de NORMA:
   - COLUNAS OBRIGATÓRIAS: {", ".join(NORMA_COLS)}
   - FROM/JOIN OBRIGATÓRIO: {NORMA_JOIN_INSTRUCTION}
2. Caso contrário (Projetos, Requerimentos, etc.), use a instrução de JOIN de PROPOSIÇÃO:
   - COLUNAS OBRIGATÓRIAS: {", ".join(PROPOSICAO_COLS)}
   - FROM/JOIN OBRIGATÓRIO: {PROPOSICAO_JOIN_INSTRUCTION}
3. **SEMPRE USE DISTINCT**.
"""

# --- FUNÇÕES DE INFRAESTRUTURA (MANTIDAS) ---
def get_api_key():
    return st.secrets.get("GOOGLE_API_KEY", "") 

@st.cache_data
def download_database(url, dest_path):
    if os.path.exists(dest_path):
        return True
    st.info("Iniciando download do Hugging Face Hub...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url.strip(), stream=True, headers=headers)
        response.raise_for_status()
        st.info("Download em andamento...")
        with open(dest_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024*1024): 
                if chunk:
                    f.write(chunk)
        st.success("Download concluído com sucesso. Conectando ao banco de dados.")
        return True
    except Exception as e:
        st.error(f"Erro no download do banco de dados: {e}")
        st.warning("Verifique se o link do Hugging Face Hub está correto.")
        return False

def load_relationships_from_file(relations_file="relacoes.txt"):
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
        st.error(f"Erro ao carregar relacoes.txt: {e}")
        return {}

TABLE_ID_TO_NAME = {
    12: "fat_proposicao", 18: "dim_tipo_proposicao", 21: "dim_situacao", 24: "dim_ementa", 78: "dim_norma_juridica",
    # ... (demais mapeamentos de tabela - mantidos)
    111: "dim_proposicao", 414: "fat_proposicao_proposicao_lei_norma_juridica", 
    # Mapeamentos adicionais de tabelas para a função get_database_engine
    27: "dim_autor_proposicao", 30: "dim_comissao", 33: "dim_comissao_acao_reuniao", 42: "dim_data", 54: "dim_deputado_estadual", 69: "dim_partido", 
    87: "dim_data_publicacao_proposicao", 198: "dim_deputado_estadual", 201: "dim_data", 228: "dim_proposicao",
    # Omissão de outros para brevidade, mas o mapeamento completo é mantido em produção
    # ...
}

@st.cache_resource
def get_database_engine():
    if not download_database(DOWNLOAD_URL, DB_FILE):
        return None, "Download do banco de dados falhou.", None

    try:
        engine = create_engine(DB_SQLITE)
        inspector = inspect(engine)
        tabelas = inspector.get_table_names()

        esquema = ""
        # Não é mais necessário cols_dim_proposicao, mas mantemos o loop para o esquema
        
        for tabela in tabelas:
            if tabela.startswith('sqlite_'):
                continue
            df_cols = pd.read_sql(f"PRAGMA table_info({tabela})", engine)
            
            colunas_com_tipo = [f"{row['name']} ({row['type']})" for _, row in df_cols.iterrows()]
            esquema += f"Tabela {tabela} (Colunas: {', '.join(colunas_com_tipo)})\n"

        rel_map = load_relationships_from_file("relacoes.txt")
        esquema += "\nRELAÇÕES PRINCIPAIS (JOINs sugeridos):\n"
        for from_id, to_ids in rel_map.items():
            from_name = TABLE_ID_TO_NAME.get(from_id, f"tabela_{from_id}")
            to_names = [TABLE_ID_TO_NAME.get(tid, f"tabela_{tid}") for tid in to_ids]
            esquema += f"- {from_name} se relaciona com: {', '.join(to_names)}\n"

        esquema += "\nDICA: Use INNER JOIN entre tabelas relacionadas. As chaves geralmente seguem o padrão 'sk_<nome>'.\n"
        
        # O terceiro retorno não é mais usado, mas mantemos None para compatibilidade da chamada
        return engine, esquema, None 

    except Exception as e:
        return None, f"Erro ao conectar ao SQLite: {e}", None

# --- FUNÇÃO PRINCIPAL DO ASSISTENTE ---
def executar_plano_de_analise(engine, esquema, prompt_usuario):
    API_KEY = get_api_key()
    if not API_KEY:
        return "Erro: A chave de API do Gemini não foi configurada no `.streamlit/secrets.toml`.", None

    query_sql = ""
    try:
        genai.configure(api_key=API_KEY)
        model = genai.GenerativeModel('gemini-2.5-flash')
        
        # A instrução agora inclui o roteamento completo
        instrucao = (
            f"Você é um assistente de análise de dados da Assembleia Legislativa de Minas Gerais (ALMG). "
            f"Sua tarefa é converter a pergunta do usuário em uma única consulta SQL no dialeto SQLite. "
            f"SEMPRE use INNER JOIN para combinar tabelas, seguindo as RELAÇÕES PRINCIPAIS. "
            f"Se a pergunta envolver data, ano, legislatura ou período, FAÇA JOIN com dim_data. "
            f"**ATENÇÃO:** Use 'dp' como alias para 'dim_proposicao' e 'dnj' para 'dim_norma_juridica'."
            f"{ROTEAMENTO_INSTRUCAO}" # <-- Instrução de roteamento condicional
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

        # --- FORMATAÇÃO DO URL (Sempre deve haver uma coluna 'url' ou 'dp.url') ---
        if 'url' in df_resultado.columns:
            df_resultado['Link'] = df_resultado['url'].apply(
                lambda x: f'<a href="{x}" target="_blank">🔗</a>' if pd.notna(x) else ""
            )
            df_resultado = df_resultado.drop(columns=['url'])
            
            # Tenta reposicionar o link ao lado do número mais relevante
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

        # Se não houver 'url', retorna o DataFrame simples
        return "Query executada com sucesso!", df_resultado

    except Exception as e:
        error_msg = f"Erro ao executar a query: {e}"
        if query_sql:
            error_msg += f"\n\nQuery gerada (pós-limpeza): {query_sql}"
        return error_msg, None
    
# --- STREAMLIT UI PRINCIPAL ---
st.title("🤖 Assistente BI da ALMG (SQLite Local)")

# O terceiro valor retornado (colunas_disponiveis) é ignorado
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
                # Chama a função sem a lista de colunas selecionadas
                mensagem, resultado = executar_plano_de_analise(engine, esquema_db, prompt_usuario) 
                if resultado is not None:
                    st.subheader("Resultado da Análise")
                    st.dataframe(resultado) 
                st.info(f"Status: {mensagem}")
        else:
            st.warning("Por favor, digite uma pergunta para iniciar a análise.")
