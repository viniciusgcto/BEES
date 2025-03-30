# CASE de ETL BEES

Este projeto demonstra um pipeline de ETL usando Airflow, PySpark e Google Cloud Storage (GCS) para processar dados da API Open Brewery DB.

## Pré-requisitos

* Conta Google Cloud Platform (GCP) com Cloud Composer e GCS habilitados.
* Python 3.x

## Configuração

1.  **Google Cloud Storage (GCS):**
    * Criar um bucket GCS para armazenar os dados das camadas bronze, silver e gold.
    * Ajustar os caminhos no script `transform_data.py` para o bucket (neste caso: bees_case).
2.  **Cloud Composer:**
    * Criar o ambiente Cloud Composer.
    * Configurar permissões adequadas para gravar dados no bucket GCS (bees_case).
3.  **Airflow:**
    * Copiar o arquivo `brewery_pipeline.py` para a pasta `dags` do ambiente Cloud Composer.
    * Acessar a interface do Airflow para visualizar e executar a DAG.

## Execução

1.  Na interface do Airflow, localizar a DAG `brewery_pipeline`.
2.  Clicar em "Trigger DAG" para executar o pipeline manualmente ou deixar ele rodar conforme agendamento (diário, às 7h da manhã).
3.  Ao iniciar a execução, irá trazer os dados brutos da API para camada bronze no bucket do GCS.
4.  Na sequência, irá selecionar as colunas que serão mantidas, substituir registros nulos por 'Not Available' e então escrever na camada silver, particionando pelo campo 'state'.
5.  Por fim, na camada gold, agrega-se os dados de cervejarias por estado e tipo, contando o número de cervejarias em cada grupo, e ordena os resultados por essa contagem, mas em ordem decrescente.

## Monitoramento e Alerta

* Em caso de falhas, o Airflow tentará executar até 3 vezes a DAG, com intervalo de 5 minutos, além de disparar alerta para o e-mail configurado (monitoramento@bees.com).
* O Cloud Composer é integrado ao Cloud Monitoring, permitindo o monitoramento de logs e métricas para seus ambientes e DAGs do Airflow. Os logs e métricas podem ser acessados através do console do Google Cloud, da API do Cloud Monitoring ou da ferramenta de linha de comando 'gcloud'.

### Métricas Monitoradas

O Cloud Monitoring coleta várias métricas do Cloud Composer, incluindo:

* Métricas de ambiente: Uso de CPU, uso de memória, uso de disco, etc.
* Métricas de DAG: Duração da execução do DAG, status da tarefa, etc.

## Testes

Foram criados testes unitários para verificar a funcionalidade dos scripts PySpark.
Os testes verificam se os DataFrames são criados corretamente, se as colunas esperadas estão presentes e se as transformações e agregações são feitas conforme esperado.

* Os testes estão localizados no diretório `tests/`.
* Para executar os testes, usar o comando:
    ```bash
    python -m unittest tests/test_transform_data.py
    ```
