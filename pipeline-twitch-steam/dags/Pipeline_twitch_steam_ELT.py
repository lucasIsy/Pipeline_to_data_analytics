# =============================================================================
# --- IMPORTS AIRFLOW ---
# =============================================================================
from airflow.sdk import dag, task, Variable
from datetime import datetime, timedelta
import json
import io
import os

# =============================================================================
# --- FUNÇÕES (EXTRACT) ---
# =============================================================================

def get_twitch_auth_token(client_id, client_secret):
    """Obtém um token de autenticação do Twitch."""
    import requests # <--- Lazy Import

    url = "https://id.twitch.tv/oauth2/token"
    params = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials',
    }
    response = requests.post(url, params=params)
    response.raise_for_status()
    return response.json()['access_token']

def get_twitch_streams(client_id, auth_token, game_ids_list):
    """Busca as streams ao vivo para uma lista de IDs de jogos."""
    import requests # <--- Lazy Import

    url = "https://api.twitch.tv/helix/streams"
    headers = {'Client-ID': client_id, 'Authorization': f'Bearer {auth_token}'}
    params = {'game_id': game_ids_list, 'first': 100}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()

def get_steam_player_count(api_key, app_id):
    """Busca a contagem de jogadores para um app_id (retorna None em falha)."""
    import requests # <--- Lazy Import

    base_url = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
    params = {'key': api_key, 'appid': app_id, 'format': 'json'}
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        if data.get('response', {}).get('result') == 1:
            return data['response']['player_count']
        else:
            return None
    except Exception:
        return None

# =============================================================================
# --- FUNÇÃO (LOAD) ---
# =============================================================================

def data_to_stream_format(python_dict_data: dict) -> io.BytesIO:
    """
    Converte um dicionário Python em uma string JSON, depois em um objeto io.BytesIO para upload.
    """
    # Transforma o dict em json, e aplica otimizações no arquivo
    # Menos payload, upload mais rápido e menor curso de armazenamento
    return io.BytesIO(json.dumps(python_dict_data, separators=(',', ':')).encode())

def upload_to_dtbck(data: dict, path: str, ts_nodash, prefix):
    """Realiza o upload de um stream de dados em memória para um Volume do Databricks."""
    from databricks.sdk import WorkspaceClient # <--- Lazy Import
    w = WorkspaceClient(
        host=Variable.get("DATABRICKS_HOST"),
        token=Variable.get("DATABRICKS_TOKEN")
    )
    # Define o nome e local de salvamento do arquivo no databricks
    file_name = f"{prefix}_{ts_nodash}.json"
    full_path = os.path.join(path, file_name)
    
    # Transforma o json para byte
    stream = data_to_stream_format(data)

    # Faz o upload
    try:
        print(f"Iniciando upload de stream de dados para '{path}'...")
        
        w.files.upload(
            file_path=full_path,
            contents=stream,
            overwrite=True 
        )
        print("Upload de stream concluído com sucesso!")
    except Exception as e:
        print(f"Ocorreu um erro durante o upload do stream: {e}")


# =============================================================================
# --- ARGUMENTOS PADRÃO DO DAG ---
# =============================================================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# =============================================================================
# --- DEFINIÇÃO DO DAG ---
# =============================================================================

@dag(
    dag_id='Pipeline_twitch_steam',
    default_args=default_args,
    description='Extrai (Twitch/Steam) e Carrega (Databricks)',
    schedule='*/5 * * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['databricks', 'twitch', 'steam', 'elt_project'],
)
def twitch_steam_pipeline():
    """
    Este DAG define o pipeline completo:
    1. Ingestão dos dados da Twitch e Steam via API.
    2. Converte os dados para formato stream e faz o carregamento para o Databricks.
    """
    # --- Task 1: Extrai os dados da twitch e faz o upload para o databricks ---
    @task
    def get_twitch_data(ts_nodash)->json:
        TWITCH_CLIENT_ID = Variable.get("TWITCH_CLIENT_ID")
        TWITCH_CLIENT_SECRET = Variable.get("TWITCH_CLIENT_SECRET")
        JOGOS_PARA_MONITORAR_TWITCH = Variable.get("JOGOS_PARA_MONITORAR_TWITCH", deserialize_json=True)

        # Extração dos dados da Twitch
        twitch_ids_lista = [info for info in JOGOS_PARA_MONITORAR_TWITCH.values() if info != "N/A"]
        auth_token = get_twitch_auth_token(TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET)
        
        # Dict com o timestamp e os dados extraídos da twitch
        dados_twitch = {
            # Timestamp
            "ingestion_timestamp_utc": datetime.now().isoformat(), 
            # Extração dos dados
            "twitch_data": get_twitch_streams(TWITCH_CLIENT_ID, auth_token, twitch_ids_lista)
        }
        
        path_data_twitch = "/Volumes/workspace/default/my_volume/raw/twitch"
        
        # Envia os dados para o databricks
        upload_to_dtbck(dados_twitch, path_data_twitch, ts_nodash, "twitch")

    # --- Task 2: Extrai os dados da steam e faz o upload para o databricks ---  
    @task
    def get_steam_data(ts_nodash)->json:

        # Puxa a key do airflow variables
        STEAM_API_KEY = Variable.get("STEAM_API_KEY")
        JOGOS_PARA_MONITORAR_STEAM = Variable.get("JOGOS_PARA_MONITORAR_STEAM", deserialize_json=True)
        
        # Extrai os dados da steam e adiciona o timestamp, id, nome do jogo e a qtd dos jogadores
        dados_steam = []
        for nome_jogo, id_game in JOGOS_PARA_MONITORAR_STEAM.items():
            player_count = get_steam_player_count(STEAM_API_KEY, id_game) # A api só retorna a qtd de jogadores
            dados_steam.append({
                "ingestion_timestamp_utc": datetime.now().isoformat(), 
                "id": id_game,
                "jogo": nome_jogo,
                "qtd_jogadores": player_count
            })

        # Coloca o horario da extração no nome dos arquivos e define o local de salavamento no databricks
        path_data_steam = "/Volumes/workspace/default/my_volume/raw/steam"
    
        # Envia os dados para o databricks
        upload_to_dtbck(dados_steam, path_data_steam, ts_nodash, "steam")

    # Chama as tasks
    get_twitch_data(ts_nodash="{{ ts_nodash }}")
    get_steam_data(ts_nodash="{{ ts_nodash }}")

# --- CHAMADA DO DAG ---
twitch_steam_pipeline()