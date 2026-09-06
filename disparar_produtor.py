import os
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv


# Carrega do arquivo .env as configurações de acesso ao Airflow
load_dotenv()

# Usa valores padrão para o endereço e o usuário quando não forem definidos
url = os.getenv("AIRFLOW_URL", "http://localhost:8080")
usuario = os.getenv("AIRFLOW_USERNAME", "airflow")
senha = os.getenv("AIRFLOW_PASSWORD")
dag_id = "dag_produtora_externa"

# Interrompe o script se a senha não estiver configurada
if not senha:
    raise ValueError("AIRFLOW_PASSWORD não definida no arquivo .env.")

# Obtém o token de autenticação
resposta = requests.post(
    f"{url}/auth/token",
    json={
        "username": usuario,
        "password": senha,
    },
    timeout=15,
)
# Gera uma exceção se a API não aceitar o disparo da DAG
resposta.raise_for_status()

token = resposta.json()["access_token"]

# Dispara a DAG produtora
resposta = requests.post(
    f"{url}/api/v2/dags/{dag_id}/dagRuns",
    headers={
        "Authorization": f"Bearer {token}",
    },
    json={
        "logical_date": datetime.now(timezone.utc).isoformat(),
        "conf": {
            "origem": "script_externo",
        },
    },
    timeout=15,
)
resposta.raise_for_status()

resultado = resposta.json()

# Exibe o identificador e o estado inicial da execução criada
print("DAG disparada com sucesso!")
print("Execução:", resultado["dag_run_id"])
print("Estado:", resultado["state"])
