# Pipeline produtor-consumidor com Apache Airflow

## Descrição do projeto e objetivos

Este projeto implementa um pipeline de imagens com Apache Airflow, dividido em duas DAGs:

- a DAG produtora baixa cinco imagens de uma API pública, salva os arquivos em um volume compartilhado e envia seus caminhos por XCom;
- a DAG consumidora recebe os caminhos, verifica e lê as imagens, simula três épocas de treinamento e valida as métricas produzidas.

A integração utiliza duas funcionalidades do Airflow: o `TriggerDagRunOperator` dispara a DAG consumidora ao final da produtora, enquanto o `ExternalTaskSensor` aguarda a conclusão da execução correspondente da produtora.

O projeto também possui um script Python externo que autentica na API REST do Airflow e dispara a DAG produtora.

Objetivos:

- Demonstrar a configuração local do Airflow com Docker Compose.
- Implementar a comunicação entre DAGs.
- Utilizar XCom para compartilhar metadados.
- Disparar pipelines pela API REST do Airflow.
- Simular o treinamento, sem gerar um modelo real.

## Tecnologias utilizadas

- Apache Airflow 3.1.7;
- Docker e Docker Compose;
- Python 3;
- PostgreSQL 16;
- Redis 7.2;
- VsCode;
- Bibliotecas Python `requests` e `python-dotenv`;
- API pública Random Duck.

## Passo a passo para executar o projeto


### 1. Subir o ambiente

Antes de começar, instale o Docker com o Docker Compose. No WSL, siga a [documentação oficial de instalação](https://docs.docker.com/compose/install/linux/). A configuração do Airflow utilizada neste projeto é baseada no [guia oficial do Airflow com Docker Compose](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).

#### 1.1. Atualização do Sistema e Instalação do Docker Compose

Atualize os repositórios de pacotes e instale o plugin do Docker Compose.

```bash
sudo apt-get update

sudo apt-get install docker-compose-plugin

```

---

#### 1.2. Download do arquivo `docker-compose.yaml` do Apache Airflow

Crie o diretório  `~/airflow-docker` e baixe o arquivo oficial de configuração do Airflow (versão 3.1.7).

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.1.7/docker-compose.yaml'
```

---

#### 1.3. Preparação da Estrutura de Diretórios e Variáveis de Ambiente

Crie as pastas necessárias que serão montadas como volumes pelos containers e configure a variável `AIRFLOW_UID` no arquivo `.env`.

```bash
mkdir -p ./dags ./logs ./plugins ./config

echo -e "AIRFLOW_UID=$(id -u)" > .env
```

---

#### 1.4. Inicialização da Infraestrutura e Download das Imagens

Execute o serviço de inicialização do Airflow (`airflow-init`), que baixa as imagens Docker necessárias (`postgres`, `redis`, `apache/airflow`) e prepara o banco de dados.

```bash
sudo docker compose up airflow-init
```

---

#### 1.5. Validação da Execução do `airflow-init`

Após a conclusão das baixas e inicialização do banco, o container de inicialização conclui com sucesso com o código de saída 0 (`exited with code 0`).

```text
airflow-init-1 exited with code 0
```

---

#### 1.6. Verificação dos Containers do Banco de Dados e Cache

Verifique se os serviços de suporte (PostgreSQL e Redis) já estavam em execução.

```bash
sudo docker ps
```

---

#### 1.7. Verificação das Imagens Docker Baixadas

Confirme que todas as imagens Docker do ecossistema Airflow foram baixadas com sucesso localmente.

```bash
sudo docker images
```

**Imagens baixadas:**
- `postgres:16`
- `redis:7.2-bookworm`
- `apache/airflow:3.1.7`

---

#### 1.8. Subindo todos os Serviços do Apache Airflow

Por fim, subir o ambiente completo do Apache Airflow.

```bash
sudo docker compose up
```

**Serviços iniciados:**
- `airflow-docker-postgres-1`
- `airflow-docker-redis-1`
- `airflow-docker-airflow-init-1`
- `airflow-docker-airflow-apiserver-1`
- `airflow-docker-airflow-dag-processor-1`
- `airflow-docker-airflow-scheduler-1`
- `airflow-docker-airflow-triggerer-1`
- `airflow-docker-airflow-worker-1`

---

#### 1.9. Acessar a Web UI

> **Acesso ao Web UI:**  
> Com o ambiente rodando, a interface web do Airflow pode ser acessada pelo navegador no endereço: `http://localhost:8080` (o login e senha vêm por padrão: `airflow` para amabas )



### 2. Ativar e executar as DAGs

Após baixar instalar o Airflow e baixar o projeto do repositório, na interface do Airflow:

1. Ative `dag_produtora_externa`.
2. Ative `dag_consumidora_externa`.
3. Abra as duas DAGs

As imagens serão armazenadas em `./minhas_imagens` na máquina local (criada na raiz, ou seja, em airflow-docker). Essa pasta é mapeada nos containers como `/tmp/images`.

### 3. Disparar a DAG produtora pela API

O script `disparar_produtor.py` deve ser executado na máquina local, com o Airflow em funcionamento.

Crie e ative um ambiente virtual e instale as dependências:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install requests python-dotenv
```

Execute o script na raiz (no caso em airflow-docker):

```bash
python3 disparar_produtor.py
```

Uma resposta esperada é:

```text
DAG disparada com sucesso!
Execução: manual__...
Estado: queued
```

O estado `queued` indica que o Airflow aceitou o disparo e colocou a execução na fila. O resultado final deve ser acompanhado na interface ou nos logs do Airflow das duas DAGs (produtora e consumidora).

### 5. Encerrar o ambiente

Para interromper os containers sem apagar o banco de dados:

```bash
docker compose down
```

## Estrutura dos diretórios e arquivos

```text
airflow-docker/
├── dags/
│   ├── dag_requisicao_produtor.py     # Define a DAG produtora
│   ├── dag_requisicao_consumidor.py   # Define a DAG consumidora
│   └── include/
│       ├── __init__.py                 # Identifica o pacote Python
│       ├── common.py                   # Configurações comuns das DAGs
│       ├── produtor.py                 # Função de download das imagens
│       └── consumidor.py               # Processamento, treino e validação
├── config/                             # Configuração local do Airflow
├── disparar_produtor.py                # Disparo externo pela API REST
├── docker-compose.yaml                 # Serviços Docker do Airflow
├── Dockerfile                          # Imagem personalizada do Airflow
└── README.md                           # Instruções do projeto
```
