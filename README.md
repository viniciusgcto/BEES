# CASE de ETL BEES

Este projeto implementa um pipeline de dados que consome a Open Brewery DB API, processa os dados com PySpark e persiste em um data lake no Google Cloud Storage (GCS) seguindo a arquitetura medallion (bronze, silver, gold). O Apache Airflow, gerenciado pelo Cloud Composer, é usado para orquestração.

## Objetivo

Camada Bronze: Dados brutos da API em JSON no GCS.
Camada Silver: Dados transformados em Parquet, particionados por "state", manter só as colunas desejadas, alterando nulos para "Not Available".
Camada Gold: Visão agregada dos dados de cervejarias por estado e tipo, contando o número de cervejarias em cada grupo, e ordenando os resultados por essa contagem, em ordem decrescente.

## Pré-requisitos

Conta na Google Cloud Platform (GCP) com um projeto ativo.
Ferramentas instaladas localmente:
   Google Cloud SDK (gcloud) para interagir com a GCP.
   Python 3.8+ (versão compatível com Composer).
   Bibliotecas Python: _requests, pyspark, google-cloud-storage_.
Dependências do Composer:
O ambiente do Cloud Composer já inclui o Airflow, mas é preciso especificar dependências adicionais no ambiente
Subir as dependências na aba "PyPI": _requests==2.28.1, pyspark==3.3.0, google-cloud-storage==2.5.0_

## Configuração GCP

1.  **Google Cloud Storage (GCS):**
    * No console GCP, em "Cloud Storage" > "Buckets" > "Create". Nomear o bucket (Ex: "bees_case"). Região: "us-central1" ou outra.
2.  **Cloud Composer:**
    * No console GCP, em "Composer" > "Create Environment". Nome: brewery-pipeline. Localização: mesma do bucket (ex.: "us-central1"). Versão do Airflow: 2.x. Python: 3.8 ou superior. Deixar as opções padrão para máquina (ex.: 3 nós pequenos).
3.  **Airflow:**
    * Após criação do ambiente, acessar UI do Airflow, em "Admin" > "Variables" e adicionar: gcs_bucket: "bees_case", gcs_prefix: data (prefixo base para camadas bronze, silver, gold).
    * O Composer precisa de uma conta de serviço para acessar o GCS. Para identificar a conta, acessar o console GCP, ir em "Composer" > "Environments" > clicar no ambiente criado > aba "Details". Anotar o "Service Account" listado.
    * Adicionar Permissões ao Bucket: Em "Cloud Storage" > clicar no bucket > aba "Permissions". Clicar em "Add" e inserir o e-mail da conta de serviço. Atribuir a role Storage Object Admin (permissão total para ler/escrever).
    * O DAG será sincronizado de um bucket GCS associado ao Composer (criado automaticamente). Encontre o bucket do Composer: No console, em "Composer" > "Environment Details" > "DAGs Folder". Fazer o upload do arquivo "brewery_pipeline.py" para a pasta de DAGs.

## Execução

1.  Na interface do Airflow, localizar a DAG "brewery_pipeline".
2.  Clicar em "Trigger DAG" para executar o pipeline manualmente ou deixar ele rodar conforme agendamento (diário, às 7h da manhã).

## Monitoramento e Alerta

* Em caso de falhas, o Airflow tentará executar até 3 vezes a DAG, com intervalo de 5 minutos, além de disparar alerta para o e-mail configurado (monitoramento@bees.com).
* O Cloud Composer é integrado ao Cloud Monitoring, permitindo o monitoramento de logs e métricas para seus ambientes e DAGs do Airflow. Os logs e métricas podem ser acessados através do console do Google Cloud, da API do Cloud Monitoring ou da ferramenta de linha de comando "gcloud".

## Métricas Monitoradas

O Cloud Monitoring coleta várias métricas do Cloud Composer, incluindo:

* Métricas de ambiente: Uso de CPU, uso de memória, uso de disco, etc.
* Métricas de DAG: Duração da execução do DAG, status da tarefa, etc.

## Testes

Foram criados testes unitários para verificar a funcionalidade dos scripts PySpark.
Os testes verificam se os DataFrames são criados corretamente, se as colunas esperadas estão presentes e se as transformações e agregações são feitas conforme esperado.

* Os testes estão localizados no diretório "tests/".
