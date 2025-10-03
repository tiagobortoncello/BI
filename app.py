import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, inspect
import os
import requests
import re
import google.generativeai as genai

# --- CONFIGURAÇÃO DO ARQUIVO DE DADOS (Hugging Face) ---
DB_FILE = 'almg_local.db'
DB_SQLITE = f'sqlite:///{DB_FILE}'

# LINK DE DOWNLOAD DIRETO DO SEU DATASET 
DOWNLOAD_URL = "https://huggingface.co/datasets/TiagoPianezzola/BI/resolve/main/almg_local.db"

# -----------------------------------------------------------

# --- FUNÇÕES DE INFRAESTRUTURA ---
def get_api_key():
    # Pega a chave da seção 'secrets' do Streamlit
    return st.secrets.get("GOOGLE_API_KEY", "") 

@st.cache_data
def download_database(url, dest_path):
    """Baixa o arquivo .db de qualquer URL de download direto."""
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
    """Carrega relações ativas do arquivo relacoes.txt."""
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

# --- MAPEAMENTO CONFIAVEL TableID → TableName (MANTIDO) ---
TABLE_ID_TO_NAME = {
    12: "fat_proposicao", 18: "dim_tipo_proposicao", 21: "dim_situacao", 24: "dim_ementa",
    27: "dim_autor_proposicao", 30: "dim_comissao", 33: "dim_comissao_acao_reuniao",
    36: "dim_comissao_distribuicao", 39: "dim_conteudo_documental", 42: "dim_data",
    45: "dim_data_acao_reuniao_comissao", 48: "dim_data_protocolo_emenda_proposicao",
    51: "dim_data_protocolo_proposicao", 54: "dim_deputado_estadual", 57: "dim_destinatario_diligencia",
    60: "dim_destinatario_requerimento", 63: "dim_emenda_proposicao", 66: "dim_evento_institucional",
    69: "dim_partido", 72: "dim_evento_legislativo", 75: "dim_instituicao", 78: "dim_norma_juridica",
    81: "dim_primeiro_autor_proposicao", 84: "dim_data", 87: "dim_data_publicacao_proposicao",
    90: "dim_sth_municipio", 93: "dim_data_recebimento_proposicao", 96: "dim_sth_thesaurus_tema_municipio",
    99: "dim_sth_thesaurus_tema", 102: "dim_sth_thesaurus_tema", 105: "dim_sth_completo",
    108: "dim_instituicao", 111: "dim_proposicao", 114: "dim_proposicao_lei",
    117: "dim_proposicao_distribuicao_comissao", 120: "dim_reuniao_comissao", 123: "dim_reuniao_plenario",
    126: "fat_destinatario_diligencia", 129: "fat_proposicao", 132: "dim_sth_assembleia_fiscaliza",
    135: "dim_sth_comissoes_requerimentos", 138: "dim_sth_politicas_publicas", 141: "fat_destinatario_requerimento",
    144: "fat_proposicao", 147: "dim_sth_thesaurus_destinatarios", 150: "dim_sth_thesaurus_tema",
    153: "dim_sth_thesaurus_tema_municipio", 156: "dim_sth_municipio", 159: "fat_rqc", 162: "fat_proposicao",
    165: "dim_norma_juridica", 168: "fat_proposicao_sth_completo", 171: "fat_proposicao",
    174: "dim_sth_completo", 177: "dim_sth_thesaurus_tema", 180: "fat_proposicao_sth_thesaurus_tema",
    183: "fat_proposicao", 186: "dim_sth_thesaurus_tema", 189: "dim_sth_thesaurus_tema_municipio",
    192: "fat_vinculacao_deputado_proposicao", 195: "fat_proposicao", 198: "dim_deputado_estadual",
    201: "dim_data", 204: "dim_sth_municipio", 207: "dim_sth_thesaurus_tema_municipio",
    210: "dim_sth_thesaurus_tema", 222: "fat_mate_anexada", 225: "fat_proposicao", 228: "dim_proposicao",
    231: "dim_sth_completo", 234: "fat_mate_origem", 237: "fat_proposicao", 240: "dim_proposicao",
    243: "fat_mate_vide", 246: "fat_proposicao", 249: "dim_proposicao", 252: "fat_mate_anexada_a",
    255: "fat_proposicao", 258: "dim_proposicao", 261: "fat_proposicao_tramitacao", 264: "fat_proposicao",
    267: "dim_proposicao", 273: "fat_composicao_comissao", 276: "dim_comissao", 279: "fat_presenca_deputado_reuniao_comissao",
    282: "dim_reuniao_comissao", 294: "fat_proposicao_acao_reuniao_comissao", 297: "fat_proposicao",
    300: "dim_comissao", 303: "dim_data", 306: "dim_reuniao_comissao", 309: "fat_proposicao_acao_reuniao_plenario",
    312: "fat_proposicao", 315: "dim_reuniao_plenario", 318: "dim_data", 324: "fat_proposicao_agendamento_reuniao_comissao",
    327: "fat_proposicao", 330: "dim_reuniao_comissao", 333: "dim_comissao", 336: "dim_data",
    339: "dim_deputado_estadual", 342: "dim_sth_municipio", 369: "fat_proposicao_relatoria_comissao",
    372: "fat_proposicao", 375: "fat_proposicao_agendamento_reuniao_plenario", 378: "fat_proposicao",
    381: "dim_reuniao_plenario", 384: "dim_data", 387: "dim_deputado_estadual", 393: "fat_proposicao_conteudo_documental",
    396: "fat_proposicao", 399: "fat_proposicao_distribuicao_comissao", 402: "fat_proposicao_lei",
    405: "fat_proposicao", 408: "dim_proposicao_lei", 414: "fat_proposicao_proposicao_lei_norma_juridica",
    417: "fat_proposicao", 420: "dim_proposicao_lei", 423: "dim_norma_juridica", 441: "fat_proposicao_sth_assembleia_fiscaliza",
    444: "fat_proposicao", 447: "dim_sth_assembleia_fiscaliza", 450: "dim_sth_completo",
    453: "fat_proposicao_sth_comissoes_requerimentos", 456: "fat_proposicao", 459: "dim_sth_comissoes_requerimentos",
    462: "dim_sth_completo", 465: "dim_sth_thesaurus_tema", 468: "dim_sth_thesaurus_tema_municipio",
    474: "fat_proposicao_sth_politicas_publicas", 477: "fat_proposicao", 480: "dim_sth_politicas_publicas",
    483: "fat_proposicao_sth_thesaurus_destinatarios", 486: "fat_proposicao", 489: "dim_sth_thesaurus_destinatarios",
    492: "dim_sth_completo", 495: "dim_sth_thesaurus_tema", 501: "fat_proposicao_sth_thesaurus_tema_municipio",
    504: "fat_proposicao", 507: "dim_sth_thesaurus_tema_municipio", 510: "dim_sth_municipio",
    513: "dim_sth_completo", 516: "dim_sth_thesaurus_tema", 519: "fat_vinculacao_deputado_proposicao",
    522: "fat_proposicao", 525: "dim_deputado_estadual", 528: "dim_data", 531: "dim_sth_municipio",
    534: "fat_proposicao_tramitacao", 537: "fat_proposicao", 540: "dim_proposicao", 543: "fat_composicao_comissao",
    546: "fat_proposicao", 549: "dim_comissao", 552: "dim_deputado_estadual", 555: "dim_data",
    558: "dim_sth_completo", 561: "fat_presenca_deputado_reuniao_comissao", 564: "fat_proposicao",
    567: "dim_reuniao_comissao", 570: "dim_deputado_estadual", 573: "dim_data", 576: "dim_sth_municipio",
    579: "fat_proposicao_sth_completo", 582: "fat_proposicao", 585: "dim_sth_completo",
    588: "fat_proposicao_sth_thesaurus_tema", 591: "fat_proposicao", 594: "dim_sth_thesaurus_tema",
    597: "fat_proposicao_sth_thesaurus_tema_municipio", 600: "fat_proposicao", 603: "dim_sth_thesaurus_tema_municipio",
    606: "fat_rqc", 609: "fat_proposicao", 621: "fat_proposicao_relatoria_comissao", 624: "fat_proposicao",
    627: "dim_comissao_distribuicao", 639: "fat_proposicao_agendamento_reuniao_comissao", 642: "fat_proposicao",
    648: "fat_proposicao_agendamento_reuniao_plenario", 651: "fat_proposicao", 654: "dim_reuniao_plenario",
    657: "dim_data", 660: "fat_proposicao_conteudo_documental", 663: "fat_proposicao",
    666: "dim_conteudo_documental", 669: "dim_sth_completo", 672: "dim_sth_thesaurus_tema",
    675: "dim_sth_thesaurus_tema_municipio", 678: "fat_proposicao_distribuicao_comissao", 681: "fat_proposicao",
    684: "dim_proposicao_distribuicao_comissao", 687: "fat_proposicao_lei", 690: "fat_proposicao",
    693: "dim_proposicao_lei", 696: "fat_proposicao_proposicao_lei_norma_juridica", 699: "fat_proposicao",
    702: "dim_proposicao_lei", 705: "fat_proposicao_sth_assembleia_fiscaliza", 708: "fat_proposicao",
    711: "dim_sth_assembleia_fiscaliza", 714: "fat_proposicao_sth_comissoes_requerimentos", 717: "fat_proposicao",
    720: "dim_sth_comissoes_requerimentos", 723: "fat_proposicao_sth_politicas_publicas", 726: "fat_proposicao",
    729: "dim_sth_politicas_publicas", 732: "fat_proposicao_sth_thesaurus_destinatarios", 735: "fat_proposicao",
    738: "dim_sth_thesaurus_destinatarios", 741: "fat_proposicao_sth_thesaurus_tema", 744: "fat_proposicao",
    747: "dim_sth_thesaurus_tema", 750: "fat_proposicao_sth_thesaurus_tema_municipio", 753: "fat_proposicao",
    756: "dim_sth_thesaurus_tema_municipio", 759: "dim_sth_municipio",
}

# --- FUNÇÃO DE CONEXÃO E METADADOS DO BANCO ---
@st.cache_resource
def get_database_engine():
    if not download_database(DOWNLOAD_URL, DB_FILE):
        return None, "Download do banco de dados falhou.", None

    try:
        engine = create_engine(DB_SQLITE)
        inspector = inspect(engine)
        tabelas = inspector.get_table_names()

        esquema = ""
        cols_dim_proposicao = [] 
        
        for tabela in tabelas:
            if tabela.startswith('sqlite_'):
                continue
            df_cols = pd.read_sql(f"PRAGMA table_info({tabela})", engine)
            
            colunas_com_tipo = [f"{row['name']} ({row['type']})" for _, row in df_cols.iterrows()]
            esquema += f"Tabela {tabela} (Colunas: {', '.join(colunas_com_tipo)})\n"
            
            if tabela == "dim_proposicao":
                 # Armazena as colunas para o seletor do Streamlit
                 cols_dim_proposicao = [row['name'] for _, row in df_cols.iterrows()]

        # Carregar relações do arquivo
        rel_map = load_relationships_from_file("relacoes.txt")

        # Gerar descrição textual das relações com nomes reais
        esquema += "\nRELAÇÕES PRINCIPAIS (JOINs sugeridos):\n"
        for from_id, to_ids in rel_map.items():
            from_name = TABLE_ID_TO_NAME.get(from_id, f"tabela_{from_id}")
            to_names = [TABLE_ID_TO_NAME.get(tid, f"tabela_{tid}") for tid in to_ids]
            esquema += f"- {from_name} se relaciona com: {', '.join(to_names)}\n"

        esquema += "\nDICA: Use INNER JOIN entre tabelas relacionadas. As chaves geralmente seguem o padrão 'sk_<nome>'.\n"
        
        return engine, esquema, cols_dim_proposicao

    except Exception as e:
        return None, f"Erro ao conectar ao SQLite: {e}", None

# --- FUNÇÃO PRINCIPAL DO ASSISTENTE ---
def executar_plano_de_analise(engine, esquema, prompt_usuario, colunas_selecionadas):
    API_KEY = get_api_key()
    if not API_KEY:
        return "Erro: A chave de API do Gemini não foi configurada no `.streamlit/secrets.toml`.", None

    query_sql = ""
    try:
        genai.configure(api_key=API_KEY)
        model = genai.GenerativeModel('gemini-2.5-flash')
        
        # Monta a instrução para o Gemini
        if colunas_selecionadas:
            colunas_str = ', '.join([f"dp.{col}" for col in colunas_selecionadas])
            instrucao_colunas = (
                f"INCLUA OBRIGATORIAMENTE os seguintes campos da tabela 'dim_proposicao' (alias 'dp') na sua cláusula SELECT: {colunas_str}. "
                f"Não os inclua se a tabela 'dim_proposicao' não for necessária."
            )
        else:
            instrucao_colunas = "Selecione as colunas mais relevantes da tabela principal para a cláusula SELECT."

        instrucao = (
            f"Você é um assistente de análise de dados da Assembleia Legislativa de Minas Gerais (ALMG). "
            f"Sua tarefa é converter a pergunta do usuário em uma única consulta SQL no dialeto SQLite. "
            f"SEMPRE use INNER JOIN para combinar tabelas, seguindo as RELAÇÕES PRINCIPAIS listadas abaixo. "
            f"Se a pergunta envolver data, ano, legislatura ou período, FAÇA JOIN com dim_data. "
            f"**ATENÇÃO:** A tabela 'dim_proposicao' DEVE ter o alias 'dp'. "
            f"{instrucao_colunas} " # <-- INSTRUÇÃO REFORÇADA AQUI
            f"Esquema e relações:\n{esquema}\n\n"
            f"Pergunta do usuário: {prompt_usuario}"
        )

        response = model.generate_content(instrucao)
        query_sql = response.text.strip()
        
        # --- Limpeza Aprimorada para remover lixo antes do SELECT ---
        query_sql = re.sub(r'^[^`]*```sql\s*', '', query_sql, flags=re.DOTALL)
        query_sql = re.sub(r'```.*$', '', query_sql, flags=re.DOTALL).strip()
        
        match = re.search(r'(SELECT.*)', query_sql, flags=re.IGNORECASE | re.DOTALL)
        if match:
            query_sql = match.group(1).strip()
        # ------------------------------------------------------------


        st.subheader("Query SQL Gerada:")
        st.code(query_sql, language='sql')

        df_resultado = pd.read_sql(query_sql, engine)
        return "Query executada com sucesso!", df_resultado

    except Exception as e:
        error_msg = f"Erro ao executar a query: {e}"
        if query_sql:
            error_msg += f"\n\nQuery gerada (pós-limpeza): {query_sql}"
        return error_msg, None
    
# --- STREAMLIT UI PRINCIPAL ---
st.title("🤖 Assistente BI da ALMG (SQLite Local)")

# Adiciona @st.cache_resource na função get_database_engine para melhor desempenho
engine, esquema_db, colunas_disponiveis = get_database_engine()

if engine is None:
    st.error(esquema_db)
else:
    with st.sidebar:
        st.subheader("Configuração da Query")
        
        # Cria a lista de colunas iniciais (as mais relevantes)
        colunas_padrao = [col for col in colunas_disponiveis if col in ['tipo_descricao', 'numero', 'ano', 'ementa', 'url']]
        
        # Componente de seleção
        colunas_selecionadas = st.multiselect(
            "Selecione as colunas **OBRIGATÓRIAS** (dim_proposicao):",
            options=colunas_disponiveis,
            default=colunas_padrao,
            help="Estas colunas serão forçadas na cláusula SELECT. Se a pergunta exigir outras colunas (ex: nome do deputado), o assistente adicionará as necessárias."
        )

        st.markdown("---")
        with st.expander("Esquema Detalhado (Leia para entender as colunas)"):
            st.code(esquema_db)

    prompt_usuario = st.text_area(
        "Faça uma pergunta sobre os dados da ALMG (Ex: 'Quais proposições foram recebidas em 2023?')",
        height=100
    )

    if st.button("Executar Análise"):
        if prompt_usuario:
            if not colunas_selecionadas:
                # Se o usuário não selecionar colunas, usamos um conjunto básico para evitar erro
                colunas_selecionadas = ['tipo_descricao', 'numero', 'ano']
                st.warning("Nenhuma coluna selecionada. Usando colunas básicas: tipo_descricao, numero, ano.")

            with st.spinner("Processando... Gerando e executando a consulta SQL."):
                mensagem, resultado = executar_plano_de_analise(engine, esquema_db, prompt_usuario, colunas_selecionadas)
                if resultado is not None:
                    st.subheader("Resultado da Análise")
                    st.dataframe(resultado)
                st.info(f"Status: {mensagem}")
