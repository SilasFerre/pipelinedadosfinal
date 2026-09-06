import pathlib
import logging
logger = logging.getLogger(__name__)

def processar_dados(ti):

    # Recebe da DAG produtora os caminhos, e não o conteúdo, das imagens
    # A busca fica restrita à execução lógica correspondente entre as DAGs
    caminhos_imagens = ti.xcom_pull(key='path_files',
                            dag_id="dag_produtora_externa",
                            task_ids='requisicao_fotos',
                            include_prior_dates=False)

    if caminhos_imagens:        

        # Remove possíveis caminhos duplicados preservando a ordem original
        caminhos_unicos = list(
            dict.fromkeys(caminhos_imagens)
        )

        imagens_paths = [
            pathlib.Path(caminho)
            for caminho in caminhos_unicos
        ]
        logger.info("Imagens prontas para processamento: %s",imagens_paths,)

        # Confirma que cada imagem existe e pode ser lida no volume compartilhado
        for f in imagens_paths:
            if not f.exists():
                raise FileNotFoundError(
                    f"Imagem não encontrada: {f}"
                )

            dados_imagem = f.read_bytes()

            logger.info("%s bytes carregados da imagem %s.",len(dados_imagem),
                            f.name,)

            logger.info("Imagem %s processada com sucesso.",
                        f.name,)

        # O retorno vira automaticamente um XCom com a chave "return_value"
        return [str(f) for f in imagens_paths] ##

    else:
        raise ValueError(
            "Nenhum caminho de imagem foi recebido pelo XCom."
        )

def simular_treinamento(ti):
    # Recebe o XCom criado pelo retorno da task "processar_imagens" desta DAG
    caminhos_imagens = ti.xcom_pull(
        task_ids="processar_imagens",
        key="return_value",
    )


    total_epocas = 3

    logger.info("Iniciando treinamento simulado com %s imagens.",
                len(caminhos_imagens),)

    for epoca in range(1, total_epocas + 1):
        logger.info("Iniciando época %s/%s.",epoca,total_epocas,)

        for caminho in caminhos_imagens:
            imagem = pathlib.Path(caminho)

            if not imagem.exists():
                raise FileNotFoundError(
                    f"Imagem não encontrada durante o treinamento: {imagem}"
                )

            logger.info("Época %s/%s: treinando com %s.",
                        epoca,total_epocas,imagem.name,)

    # Métricas fixas usadas apenas para demonstrar a etapa de validação
    metricas = {
        "status": "concluido",
        "total_imagens": len(caminhos_imagens),
        "total_epocas": total_epocas,
        "acuracia_simulada": 0.92,
    }

    logger.info("Treinamento concluído. Métricas: %s",
                metricas,)

    # As métricas retornadas ficam disponíveis à task de validação via XCom
    return metricas

def validar_treinamento(ti):
    # Recupera o XCom criado pelo retorno da task "treinar_modelo"
    metricas = ti.xcom_pull(
        task_ids="treinar_modelo",
        key="return_value",
    )

    if not metricas:
        raise ValueError(
            "Nenhuma métrica foi recebida do treinamento."
        )

    if metricas["status"] != "concluido":
        raise ValueError(
            "O treinamento não foi concluído."
        )

    if metricas["total_imagens"] <= 0:
        raise ValueError(
            "O treinamento não utilizou nenhuma imagem."
        )

    acuracia_minima = 0.80

    if metricas["acuracia_simulada"] < acuracia_minima:
        raise ValueError(
            "A acurácia simulada ficou abaixo do mínimo."
        )

    logger.info(
    "Validação concluída com sucesso: "
    "%s imagens, %s épocas e acurácia simulada de %.2f%%.",
    metricas["total_imagens"],
    metricas["total_epocas"],
    metricas["acuracia_simulada"] * 100,
    )
