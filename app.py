import streamlit as st
import pandas as pd
from datasets import load_dataset, DatasetDict # Importado DatasetDict para lógica de erro
import os
import json
from google import genai
from google.genai import types

# --- Configurações Iniciais ---
st.set_page_config(
    page_title="Hugging Face + Gemini Data Formatter",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Constantes para os segredos
API_KEY_SECRET = "GEMINI_API_KEY"
DATASET_NAME_SECRET = "HF_DATASET_NAME" # Mantido para referência, mas ignorado abaixo
HF_TOKEN_SECRET = "HF_TOKEN" # Segredo para tokens de acesso
# Usaremos o seu dataset TiagoPianezzola/BI. ESTE NOME É O VERDADEIRO.
DEFAULT_DATASET = "TiagoPianezzola/BI" 
# O split padrão é 'train', mas pode ser 'default', 'test', etc.
DEFAULT_DATASET_SPLIT = "train" 
# Nome do arquivo ou caminho dentro do repositório HF (agora com o valor fornecido)
DEFAULT_DATA_FILES = "almg_local.db" 

# --- Funções de Inicialização e Carregamento de Dados ---

@st.cache_resource(hash_funcs={str: hash}) # Adicionado hash_funcs para forçar recarregamento se o token mudar
def load_hf_dataset(dataset_path, split_name, hf_token, data_files):
    """Carrega o dataset do Hugging Face e retorna um Pandas DataFrame."""
    
    # FORÇANDO O USO DO NOME CORRETO (IGNORANDO O POSSÍVEL ERRO NO SECRET HF_DATASET_NAME)
    dataset_name = dataset_path 
    
    load_message = f"Carregando dataset: **{dataset_name}** (Split: {split_name})."
    if hf_token:
         load_message += " Usando HF_TOKEN para autenticação."
    if data_files:
         load_message += f" Buscando arquivo: **{data_files}**."
    load_message += " Isso pode demorar um pouco..."
    st.info(load_message)
    
    # Prepara argumentos para load_dataset
    load_args = {
        "path": dataset_name,
        "split": split_name,
        "token": hf_token
    }
    if data_files:
        # Se data_files for fornecido, ele é passado para a função load_dataset
        # Se for um único arquivo, o split pode ser ignorado ou ser 'train'
        load_args["data_files"] = data_files 
        
    # Remove o split se data_files for fornecido e o split for o padrão 'train' (para evitar conflitos)
    # No caso de um único arquivo (como um .db), muitas vezes não há um split formal
    if data_files and load_args.get("split") == "train":
         load_args.pop("split")

    try:
        # Tenta carregar o split e data_files especificados
        data = load_dataset(**load_args)
        
        # O resultado de load_dataset para um único arquivo pode ser um Dataset ou um DatasetDict.
        if isinstance(data, DatasetDict):
            # Se for um DatasetDict, pegamos o primeiro split encontrado (geralmente 'train' ou 'default')
            first_split = list(data.keys())[0]
            st.warning(f"O DatasetDict foi carregado. Usando o primeiro split disponível: '{first_split}'.")
            data = data[first_split]

        # Converte para Pandas DataFrame
        df = data.to_pandas()
        st.success(f"Dataset carregado com sucesso! Linhas: {len(df)}")
        return df
        
    except Exception as e:
        # Se falhar, tenta carregar o DatasetDict inteiro (sem split e data_files) para verificar os splits
        st.warning(f"Falha ao carregar o arquivo '{data_files}'. Tentando carregar a estrutura do DatasetDict...")
        
        # Remove split e data_files para tentar carregar apenas a estrutura
        load_args_no_split = load_args.copy()
        if "split" in load_args_no_split:
            load_args_no_split.pop("split")
        if "data_files" in load_args_no_split:
            load_args_no_split.pop("data_files")
            
        try:
            # Tenta carregar sem split e data_files para ver a estrutura de splits
            dict_data = load_dataset(dataset_name, token=hf_token)
            
            if isinstance(dict_data, DatasetDict):
                available_splits = list(dict_data.keys())
                st.error(
                    f"Erro de carregamento inicial. O nome do arquivo ou o token HF está incorreto."
                    f" Splits disponíveis neste dataset: **{', '.join(available_splits)}**."
                )
            # Se não for um DatasetDict, a falha é no arquivo ou token
            st.error(f"Erro fatal ao carregar o dataset '{dataset_name}' com o arquivo '{data_files}'. Causa: O formato '.db' não é suportado nativamente pelo `datasets`, ou o Token não tem permissão. Erro detalhado: {e}")
            
        except Exception as inner_e:
             st.error(f"Erro fatal ao carregar o dataset '{dataset_name}'. Causa: O formato '.db' não é suportado nativamente pelo `datasets`, ou o Token não tem permissão. Erro detalhado: {inner_e}")
            
        return pd.DataFrame()

@st.cache_resource
def initialize_gemini():
    """Inicializa o cliente Gemini."""
    try:
        # Obtém a chave da API dos segredos do Streamlit
        api_key = st.secrets[API_KEY_SECRET]
        client = genai.Client(api_key=api_key)
        return client
    except KeyError:
        st.error(f"Erro: A chave da API do Gemini ({API_KEY_SECRET}) não foi encontrada nos segredos do Streamlit. Por favor, adicione-a.")
        st.stop()
    except Exception as e:
        st.error(f"Erro ao inicializar o cliente Gemini: {e}")
        st.stop()

# --- Componentes Principais do App ---

def gemini_query_formatter(client, dataframe, user_prompt):
    """
    Envia a query do usuário e uma amostra dos dados ao modelo Gemini
    para formatação da resposta.
    """
    if dataframe.empty:
        return "Não há dados para processar."

    # 1. Preparar a amostra de dados
    # Pegamos as 5 primeiras linhas e as formatamos como uma string JSON para o LLM.
    sample_data = dataframe.head(5).to_json(orient='records', indent=2)

    # 2. Criar o prompt com contexto
    system_prompt = (
        "Você é um assistente de análise de dados. Sua tarefa é usar o 'JSON de Amostra de Dados' "
        "e as 'Colunas Disponíveis' para responder e formatar a 'Consulta do Usuário'. "
        "Sua resposta deve ser concisa, informativa e usar uma linguagem clara em Português. "
        "Não mencione que você está usando uma amostra; aja como se estivesse analisando o conjunto completo."
    )
    
    full_prompt = (
        f"JSON de Amostra de Dados:\n```json\n{sample_data}\n```\n\n"
        f"Colunas Disponíveis: {list(dataframe.columns)}\n\n"
        f"Consulta do Usuário: {user_prompt}"
    )

    try:
        with st.spinner("🧠 Gemini está processando e formatando a resposta..."):
            
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=full_prompt,
                config=types.GenerateContentConfig(
                    system_instruction=system_prompt,
                    # Adicionando grounding tool para respostas baseadas em conhecimento geral, se necessário
                    tools=[{"google_search": {}}] 
                )
            )
            
            return response.text

    except Exception as e:
        return f"Ocorreu um erro ao chamar a API do Gemini: {e}"


# --- Layout do Streamlit ---

def main():
    st.title("📊 Hugging Face + Gemini Data Formatter")
    st.caption("Um aplicativo Streamlit com dados carregados do Hugging Face e saídas formatadas pelo Gemini.")
    
    # 1. Inicializar serviços
    client = initialize_gemini()
    
    # 2. Sidebar para Configurações e Informações
    with st.sidebar:
        st.header("Configurações e Status")
        st.write(f"Modelo Gemini: `gemini-2.5-flash`")
        
        # Campo para configurar o nome do split (permite ao usuário corrigir o erro de 'train')
        split_name_input = st.text_input(
            "Nome do Split do Dataset (ex: 'train', 'default'):",
            value=DEFAULT_DATASET_SPLIT,
            key="split_input"
        )
        
        # NOVO CAMPO: Nome do arquivo ou caminho (com valor padrão do arquivo .db)
        data_files_input = st.text_input(
            "Nome do Arquivo/Caminho (ex: 'data.csv' ou 'pasta/meu_arquivo.db'):",
            value=DEFAULT_DATA_FILES if DEFAULT_DATA_FILES is not None else "",
            key="data_files_input"
        )
        # Se o usuário deixar vazio, deve ser None
        data_files_value = data_files_input if data_files_input.strip() else None
        
        dataset_name = DEFAULT_DATASET
        st.markdown(f"**Dataset (HF):** `{dataset_name}`")
        
        hf_token = st.secrets.get(HF_TOKEN_SECRET, None)
        hf_token_status = "Configurado (Ótimo!)" if hf_token else "Ausente (Verificar se é privado)"
        st.markdown(f"**Token HF (`HF_TOKEN`):** {hf_token_status}")
        
        st.divider()
        
    # 3. Carregar o DataFrame após as configurações da sidebar
    # Passamos data_files_value
    data_frame = load_hf_dataset(DEFAULT_DATASET, split_name_input, hf_token, data_files_value)
    
    # 4. Continuação da Sidebar
    with st.sidebar:
        if not data_frame.empty:
            st.subheader("Visualização dos Dados (Amostra)")
            st.dataframe(data_frame.head(3), use_container_width=True)
            
            st.subheader("Colunas")
            st.code("\n".join(data_frame.columns))
            
            st.info("O Gemini usará as 5 primeiras linhas como contexto para formatar a resposta.")
        
        st.divider()
        st.markdown("---")
        st.markdown("Desenvolvido para ajustes iterativos conforme solicitado.")


    # 5. Área Principal: Interação
    if not data_frame.empty:
        st.subheader("Faça sua Pergunta sobre os Dados")
        
        user_prompt = st.text_area(
            "Digite sua consulta. Ex: 'Quais são as colunas e qual é a avaliação média na amostra?'",
            key="user_prompt_input",
            height=150
        )
        
        if st.button("Obter Resposta Formatada (Gemini)", use_container_width=True, type="primary"):
            if user_prompt:
                # Chama a função de formatação
                formatted_response = gemini_query_formatter(client, data_frame, user_prompt)
                
                st.markdown("---")
                st.subheader("Resposta do Gemini")
                st.markdown(formatted_response)
            else:
                st.warning("Por favor, digite uma consulta para o Gemini.")

# Executa a função principal
if __name__ == "__main__":
    main()
