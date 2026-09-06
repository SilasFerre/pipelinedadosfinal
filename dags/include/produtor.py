import pathlib
import logging
logger = logging.getLogger(__name__)

# Quantidade de imagens que deve ser obtida em cada execução da produtora
QUANTIDADE_IMAGENS = 5

def requisicao_foto(ti):
    import requests

    # O diretório é um volume compartilhado entre os serviços do Airflow
    output_path = pathlib.Path("/tmp/images")
    output_path.mkdir(parents=True, exist_ok=True)

    url = "https://random-d.uk/api/random"

    imagens_baixadas = []

    # Consulta a API e baixa cada imagem para o volume compartilhado
    for i in range(QUANTIDADE_IMAGENS):
        
        try:
            
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            dados_da_api = response.json()
            link_foto = dados_da_api["url"]

            # A primeira resposta contém apenas a URL; esta requisição baixa o arquivo
            response_foto = requests.get(link_foto, timeout=10)
            response_foto.raise_for_status()

            nome_arquivo = output_path / f"imagem_{i+1}.jpg"

            
            with open(nome_arquivo, "wb") as f:
                f.write(response_foto.content)

            imagens_baixadas.append(nome_arquivo)

            logger.info("Imagem salva com sucesso em: %s",nome_arquivo,)
            
        except requests.RequestException as erro:
            logger.warning(
                "Falha ao baixar a imagem na tentativa %s: %s",
                i + 1,
                erro)

    # Impede que a task termine com sucesso quando algum download falhar
    if len(imagens_baixadas) != QUANTIDADE_IMAGENS:
        raise RuntimeError(
            f"Esperadas {QUANTIDADE_IMAGENS} imagens, mas somente "
            f"{len(imagens_baixadas)} foram baixadas."
        )    

    # Envia somente os caminhos das imagens, evitando armazenar arquivos no XCom
    ti.xcom_push(
        key="path_files",
        value=[str(imagem) for imagem in imagens_baixadas],
    )
