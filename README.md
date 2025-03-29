# CASE de ETL BEES

Este projeto demonstra um pipeline de ETL usando Airflow, PySpark e Google Cloud Storage (GCS) para processar dados da API Open Brewery DB.

## Pré-requisitos

* Conta Google Cloud Platform (GCP) com Cloud Composer e GCS habilitados.
* Python 3.x

## Configuração

1.  **Cloud Composer:**
    * Criar um ambiente Cloud Composer.
2.  **Google Cloud Storage (GCS):**
    * Criar um bucket GCS para armazenar os dados das camadas bronze, silver e gold.
    * Ajustar os caminhos no script `transform_data.py` para o bucket.
3.  **Airflow:**
    * Copiar o arquivo `brewery_pipeline.py` para a pasta `dags` do ambiente Cloud Composer.
    * Acessar a interface do Airflow para visualizar e executar a DAG.

## Execução

1.  Na interface do Airflow, localizar a DAG `brewery_pipeline`.
2.  Clicar no botão "Trigger DAG" para executar o pipeline manualmente ou deixar ele rodar conforme agendamento diário, às 7h da manhã.

## Monitoramento e Alerta

* É possível consultar logs e métricas do Cloud Composer no Cloud Monitoring.
* Em caso de falhas, o Airflow tentará executar até 3 vezes a DAG, com intervalo de 5 minutos, além de disparar alerta para o e-mail configurado (monitoramento@bees.com).

## Testes

Este projeto inclui testes unitários para verificar a funcionalidade dos scripts PySpark.

* Os testes estão localizados no diretório `tests/`.
* Para executar os testes, use o seguinte comando:
    ```bash
    python -m unittest tests/test_transform_data.py
    ```

Os testes verificam se os DataFrames são criados corretamente, se as colunas esperadas estão presentes e se as transformações e agregações são feitas conforme esperado.
