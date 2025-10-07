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
DATASET_NAME_SECRET = "HF_DATASET_NAME"
HF_TOKEN_SECRET = "HF_TOKEN" # Novo segredo para tokens de acesso
# Usaremos o seu dataset TiagoPianezzola/BI. 
DEFAULT_DATASET = "TiagoPianezzola/BI" 
# O split padrão é 'train', mas pode ser 'default', 'test', etc.
DEFAULT_DATASET_SPLIT = "train" 

# --- Funções de Inicialização e Carregamento de Dados ---

@st.cache_resource
def load_hf_dataset(dataset_path, split_name, hf_token):
    """Carrega o dataset do Hugging Face e retorna um Pandas DataFrame."""
    
    # Tenta carregar o nome do dataset dos segredos, se disponível
    dataset_name = st.secrets.get(DATASET_NAME_SECRET, dataset_path)
    
    load_message = f"Carregando dataset: **{dataset_name}** (Split: {split_name})."
    if hf_token:
         load_message += " Usando HF_TOKEN para autenticação."
    load_message += " Isso pode demorar um pouco..."
    st.info(load_message)
    
    try:
        # Tenta carregar o split especificado
        # O token é passado aqui para autenticação
        data = load_dataset(dataset_name, split=split_name, token=hf_token)
        
        # Converte para Pandas DataFrame
        df = data.to_pandas()
        st.success(f"Dataset carregado com sucesso! Linhas: {len(df)}")
        return df
        
    except Exception as e:
        # Se falhar, tenta carregar o DatasetDict inteiro para verificar os splits
        st.warning(f"Falha ao carregar com split='{split_name}'. Tentando carregar o DatasetDict completo...")
        try:
            dict_data = load_dataset(dataset_name, token=hf_token)
            
            if isinstance(dict_data, DatasetDict):
                available_splits = list(dict_data.keys())
                st.error(
                    f"Erro de carregamento inicial. O nome do split '{split_name}' pode estar incorreto."
                    f" Splits disponíveis neste dataset: **{', '.join(available_splits)}**."
                    " Por favor, ajuste o 'Nome do Split' na barra lateral e recarregue o app."
                )
            else:
                # Se for um único Dataset (sem DatasetDict), usa-o diretamente.
                 st.success(f"Dataset carregado sem split. Linhas: {len(dict_data)}")
                 return dict_data.to_pandas()

        except Exception as inner_e:
            # Captura a falha final (geralmente problema de nome ou token)
            st.error(f"Erro fatal ao carregar o dataset '{dataset_name}'. Verifique o nome (letras maiúsculas/minúsculas, sem underscore) e se o HF_TOKEN está correto. Erro: {inner_e}")
            
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
        
        dataset_name = st.secrets.get(DATASET_NAME_SECRET, DEFAULT_DATASET)
        st.markdown(f"**Dataset (HF):** `{dataset_name}`")
        
        # Obter o token para passar para a função de carregamento
        hf_token = st.secrets.get(HF_TOKEN_SECRET, None)
        hf_token_status = "Configurado (Ótimo!)" if hf_token else "Ausente (Verificar se é privado)"
        st.markdown(f"**Token HF (`HF_TOKEN`):** {hf_token_status}")
        
        st.divider()
        
    # 3. Carregar o DataFrame após as configurações da sidebar
    # Passamos o split_name_input e o hf_token para a função
    data_frame = load_hf_dataset(DEFAULT_DATASET, split_name_input, hf_token)
    
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
