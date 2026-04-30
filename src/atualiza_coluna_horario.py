import sys
import pickle
import yaml
import pandas as pd
import googleapiclient.discovery
from datetime import datetime
from snowflake.snowpark.session import Session

# ==============================================================================
# 1. FUNÇÕES DE EXTRAÇÃO
# ==============================================================================
def get_values_as_dataframe(sheet_link, token_file, aba):
    try:
        spreadsheet_id = sheet_link.split('/')[-1]
        service = googleapiclient.discovery.build('sheets', 'v4', credentials=token_file)
        
        request = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=aba)
        response = request.execute()
        
        values = response.get('values', [])
        if not values:
            return pd.DataFrame()
            
        df = pd.DataFrame(values[1:], columns=values[0])
        return df
    except Exception as e:
        print(f"❌ Erro ao ler Google Sheets: {e}")
        raise

# ==============================================================================
# 2. CONEXÃO SNOWFLAKE
# ==============================================================================
def conectar_snowflake():
    print("🔌 Carregando credenciais do arquivo YAML...")
    try:
        with open("<ARQUIVO_PROFILES>.yml", "r") as file:
            profile_data = yaml.safe_load(file)

        snowflake_params = profile_data["credentials"]["outputs"]["prod"]

        snowflake_config = {
            "account": snowflake_params.get("account", ""),
            "user": snowflake_params.get("user", ""),
            "password": snowflake_params.get("password", ""),
            "warehouse": snowflake_params.get("warehouse", "<WAREHOUSE_NAME>"),
            "database": snowflake_params.get("database", "<DATABASE>"),
            "schema": "<SCHEMA_BRONZE>",
            "role": snowflake_params.get("role", "<ROLE_NAME>"), 
            "client_session_keep_alive": True,
            "network_timeout": 300,
            "retry_attempts": 10
        }
        
        session = Session.builder.configs(snowflake_config).create()
        print("✅ Conexão Snowflake estabelecida com sucesso!")
        return session
    except Exception as e:
        print(f"❌ Erro ao conectar no Snowflake: {e}")
        raise

# ==============================================================================
# 3. MAIN
# ==============================================================================
def main():
    print("🚀 Iniciando extração de dados da planilha...")
    session = None
    
    try:
        # --- CARREGAMENTO DO TOKEN ---
        print("🔑 Carregando token do Google Sheets...")
        with open("<ARQUIVO_TOKEN>.json", 'rb') as token:
            token_file = pickle.load(token)

        # --- EXTRAÇÃO DO GOOGLE SHEETS ---
        print("📊 Lendo dados da planilha...")
        sheet_link = 'https://docs.google.com/spreadsheets/d/<SPREADSHEET_ID>'
        range_name = '<NOME_ABA>!A1:ZZ'
        
        df_sheet = get_values_as_dataframe(sheet_link, token_file, range_name)

        if df_sheet.empty:
            print("⚠️ A planilha retornou vazia. Nenhum dado para enviar.")
            return "Planilha vazia."

        # ======================================================================
        # --- LIMPEZA DE DADOS --
        # ======================================================================
        # 1. Remove colunas vazias
        df_sheet = df_sheet.loc[:, df_sheet.columns.notna() & (df_sheet.columns != '')]
        
        # 2. Garante que os nomes das colunas estão em maiúsculas
        df_sheet.columns = [str(col).upper().strip() for col in df_sheet.columns]

        # 3. Filtra apenas as linhas onde a coluna chave está preenchida
        if 'ID' in df_sheet.columns:
            df_sheet = df_sheet[df_sheet['ID'].astype(str).str.strip() != ""]
            df_sheet = df_sheet.dropna(subset=['ID'])
        else:
            df_sheet = df_sheet.dropna(how='all')

        print(f"✅ Dados REAIS extraídos após limpeza: {len(df_sheet)} linhas.")
        # ======================================================================

        # --- TRANSFORMAÇÃO ---
        df_sheet['DT_INSERCAO_BD'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # --- CARGA NO SNOWFLAKE ---
        session = conectar_snowflake()

        print("☁️ Enviando Tabela (<TABLE_TARGET>)...")
        snowpark_df = session.write_pandas(
            df_sheet, 
            "<TABLE_TARGET>", 
            database="<DATABASE>",
            schema="<SCHEMA_BRONZE>",
            warehouse="<WAREHOUSE_NAME>",
            role="<ROLE_NAME>",
            auto_create_table=True, 
            overwrite=True
        )
        
        nrows = len(df_sheet)
        
        mensagem_final = f"Sucesso! {nrows} linhas inseridas no banco."
        print(f"🏁 {mensagem_final}")
        return mensagem_final

    except Exception as e:
        print(f"❌ Falha na execução do processo: {e}")
        raise
        
    finally:
        if session is not None:
            session.close()
            print("🔌 Sessão Snowflake encerrada.")

# ==============================================================================
# PONTO DE ENTRADA (EXECUÇÃO)
# ==============================================================================
if __name__ == "__main__":
    main()