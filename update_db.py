import pandas as pd
import os
from sqlalchemy import create_engine
import requests
import io
import msal

# Tenta importar as configurações do arquivo local 'config.py' (para desenvolvimento)
try:
    from config import TENANT_ID, CLIENT_ID, CLIENT_SECRET
except ImportError:
    TENANT_ID = None
    CLIENT_ID = None
    CLIENT_SECRET = None

# --- CONFIGURAÇÕES DE PRODUÇÃO E FALLBACK LOCAL ---
# Pega as credenciais das variáveis de ambiente (para o Render) ou usa as do config.py
TENANT_ID = os.getenv('TENANT_ID', TENANT_ID)
CLIENT_ID = os.getenv('CLIENT_ID', CLIENT_ID)
CLIENT_SECRET = os.getenv('CLIENT_SECRET', CLIENT_SECRET)
# Pega a URL do banco de dados do Render ou usa o arquivo local como padrão
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///vendas.db')

# Informações do arquivo
USER_EMAIL = 'suporte@gestaoparceiros.onmicrosoft.com' 
FILE_PATH = 'Oi Corp/Record/BASE_ESTEIRA_VENDAS/ESTEIRA_RECORD_AGF.xlsx' 
NOME_TABELA = 'vendas'

def obter_token_acesso():
    """
    Usa as credenciais do Azure para se autenticar e obter um token de acesso
    para a API Microsoft Graph.
    """
    if not all([TENANT_ID, CLIENT_ID, CLIENT_SECRET]):
        print("ERRO: Credenciais (TENANT_ID, CLIENT_ID, CLIENT_SECRET) não configuradas.")
        return None
        
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    scope = ["https://graph.microsoft.com/.default"]
    app = msal.ConfidentialClientApplication(
        CLIENT_ID, authority=authority,
        client_credential=CLIENT_SECRET,
    )
    result = app.acquire_token_silent(scope, account=None)
    if not result:
        print("Nenhum token em cache, solicitando um novo.")
        result = app.acquire_token_for_client(scopes=scope)
    if "access_token" in result:
        print("Token de acesso obtido com sucesso.")
        return result['access_token']
    else:
        print("Erro ao obter token de acesso:")
        print(result.get("error"))
        print(result.get("error_description"))
        return None

def baixar_arquivo_do_sharepoint(access_token):
    """
    Usa o token de acesso para chamar a API Microsoft Graph e baixar o arquivo.
    """
    if not access_token:
        return None
    graph_url = f"https://graph.microsoft.com/v1.0/users/{USER_EMAIL}/drive/root:/{FILE_PATH}:/content"
    headers = {'Authorization': 'Bearer ' + access_token}
    print(f"Baixando arquivo de: {FILE_PATH}")
    response = requests.get(graph_url, headers=headers)
    if response.status_code == 200:
        print("Arquivo baixado com sucesso.")
        return response.content
    else:
        print(f"Erro ao baixar o arquivo: {response.status_code}")
        print(response.json())
        return None

def atualizar_banco_de_dados():
    """
    Orquestra o processo: obtém token, baixa o arquivo e atualiza o banco de dados.
    """
    print("Iniciando a atualização do banco de dados...")
    
    engine = create_engine(DATABASE_URL)
    access_token = obter_token_acesso()
    conteudo_arquivo = baixar_arquivo_do_sharepoint(access_token)
    
    if not conteudo_arquivo:
        print("Falha na atualização, pois o download do arquivo não foi concluído.")
        return

    try:
        arquivo_excel = io.BytesIO(conteudo_arquivo)
        df = pd.read_excel(arquivo_excel, engine='openpyxl', sheet_name='ESTEIRA_RECORD')
        
        # Adiciona a nova coluna 'MÉTODO DE PAGAMENTO'
        df = df.rename(columns={
            'ÍNDICE': 'indice', 'NOME': 'cliente', 'VENDEDOR': 'vendedor',
            'CPF': 'cpf', 'DT PEDIDO': 'dt_pedido', 'DT INST': 'dt_inst',
            'PERÍODO': 'periodo', 'SITUAÇÃO': 'situacao', 'OS': 'os',
            'MÉTODO DE PAGAMENTO': 'metodo_pagamento' 
        })
        
        # Adiciona 'metodo_pagamento' à lista de colunas desejadas
        colunas_desejadas = ['indice', 'cliente', 'vendedor', 'cpf', 'dt_pedido', 'dt_inst', 'periodo', 'situacao', 'os', 'metodo_pagamento']
        
        colunas_existentes = [col for col in colunas_desejadas if col in df.columns]
        df_selecionado = df[colunas_existentes]
        
        df_selecionado.to_sql(NOME_TABELA, engine, if_exists='replace', index=False)
        
        print(f"{len(df_selecionado)} linhas salvas no banco de dados com sucesso!")

    except Exception as e:
        print(f"Ocorreu um erro durante o processamento: {e}")

if __name__ == '__main__':
    atualizar_banco_de_dados()