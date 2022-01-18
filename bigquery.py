import logging
import credentials
from google.cloud import bigquery
from pandas import DataFrame

class Bigquery():
    # Clase genérica para execução no ambiente do bigquery
    def __init__(self) -> None:
        # Instância das credenciais via variavel de ambiente para acesso ao serviço do bigquery no GCP
        credentials.set_environment_keys()
        # Instância do cliente principal do bigquery
        self.client = bigquery.Client()

    def query_cost(self, query):
        # Método para rastreamento do volume das querys criadas para geração das tabelas no BigQuery
        job_config = bigquery.QueryJobConfig(
            dry_run=True, use_query_cache=False)
        query_job = self.client.query(query, job_config=job_config)
        bytes_billed = query_job.total_bytes_processed

        return bytes_billed

    def query_to_df(self, query: str) -> DataFrame:
        # Método para conversão de uma query SQL em um dataframe pandas
        bytes_billed = self.query_cost(query)
        logging.info(f'Serão gastos {bytes_billed} bytes na query')
        self.write_metadata({'processed_bytes': bytes_billed})
        dataframe = self.client.query(
            query, project=self.client.project).to_dataframe()

        return dataframe

    def create_dataset(self, zone:str, namespace:str) -> None:
        # Método genêrico para criar datasets no ambiente do bigquery
        dataset_id = "{}.{}_{}".format(self.client.project, namespace, zone)
        self.client.create_dataset(dataset_id, timeout=30)

        logging.info("Created dataset {}.{}".format(self.client.project, dataset_id))

        return None

    def list_datasets(self) -> list:
        datasets = [dataset for dataset in self.client.list_datasets()]

        return datasets

class Table():
    # Classe principal para uma tabela genérica do bigquery
    def __init__(self, 
                 dataset_name: str, 
                 table_name: str, 
                 location: str = 'US', 
                 **kwargs) -> None:

        self.bq = Bigquery()
        self.client = self.bq.client
        self.project = self.client.project
        self.dataset_name = dataset_name
        self.table_name = table_name
        self.location = location
        self.dataset_ref = bigquery.DatasetReference( project=self.project, dataset_id=dataset_name)
        self.dataset_table = f"{dataset_name}.{table_name}"
        self.full_name = f"{self.project}.{self.dataset_table}"
        self.kwargs = kwargs
        if self.exists():
            table_ref = self.dataset_ref.table(self.table_name)
            self.table_ref = self.client.get_table(table_ref)
            self.schema = self.table_ref.schema
            self.metadata = [x.to_api_repr() for x in self.schema]
        else:
            logging.warning(
                f'Tabela {self.full_name} instanciada ainda não existe')

    def set_date_partition(self, job_config):
        # Método para definir o tipo de particionamento de cada tabela a fim de reduzir os custos de processamento das tabelas
        field = self.kwargs['date_partition_column']['field']
        type = 'DAY'
        if 'type' in self.kwargs['date_partition_column'].keys():
            type = self.kwargs['date_partition_column']['type']
        job_config.time_partitioning = bigquery.TimePartitioning(
            field=field, type=type)

        return job_config

    def delete_matched(self, source_table, upsert_columns):
        # Método gênerico para deletar tabelas de zonas sandbox genéricas durante o processo de escrita/ingestão de novas tabelas no bigquery
        bq_client = bigquery.Client()
        upsert_expression = [
            f'a.{column} = b.{column}' for column in upsert_columns]
        upsert_expression = ' and '.join(upsert_expression)

        delete_stmt = f"""
            DELETE
            FROM {self.full_name} as a
            where exists
            (
                SELECT 1
                FROM {source_table} as b
                where {upsert_expression}
            );"""

        bytes_billed = self.bq.query_cost(delete_stmt)
        logging.info(
            f'Serão gastos {bytes_billed} bytes no processo de deleção de dados do processo de upsert')

        delete_job = bq_client.query(delete_stmt)
        delete_job.result()

    def exists(self) -> bool:
        # Método simples para definir se uma tabela existe ou não no ambiente do bigquery
        try:
            self.client.get_table(self.full_name)
            return True
        except:
            return False

    def export_to_gcs(self, 
                      zone, 
                      namespace, 
                      format='PARQUET', 
                      compression='GZIP', 
                      spread_files=False):
        
        # Método para exportar e escrever tabelas do bigquery em um bucket genêrico do data lake.
        if spread_files:
            file = f"{self.table_name}-*.{format.lower()}"
        else:
            file = f"{self.table_name}.{format.lower()}"
        
        bucket = f"path do bucket"
        table_path = f"{namespace}-{self.table_name}/{file}"
        target_path = f'gs://{bucket}/{namespace}/{table_path}'

        job_config_export = bigquery.ExtractJobConfig(destination_format=format, compression=compression)
        extract_job = self.client.extract_table(self.table_ref, target_path, location=self.location, job_config=job_config_export)
        response = extract_job.result()
        
        logging.info(f'Tabela {self.full_name} carregada para o path "{target_path}"')
        
        return response

    def delete(self) -> None:
        # Método gênerico geral para deletar tabelas do bigquery
        self.client.delete_table(self.full_name, not_found_ok=True)
        logging.info(f"Tabela '{self.full_name}' deletada.")
        
        return None

    def write_query_results(self, 
                            sql_query: str, 
                            upsert_columns: list = None, 
                            time_partition_on: str = None, 
                            if_exists: str = 'append'):
        
        # Método para escrever tabelas no bigquery via script SQL padrão e genêrico
        bytes_billed = self.bq.query_cost(sql_query)
        logging.info(f'Serão gastos {bytes_billed} bytes na execução da query')

        if not self.exists():
            job_config = bigquery.QueryJobConfig(destination=self.table_ref)
            if 'date_partition_column' in self.kwargs.keys():
                job_config = self.set_date_partition(job_config)
            Job = self.client.query(sql_query, job_config=job_config)
            Job.result()
            return 'Dados carregados com sucesso'

        if upsert_columns:
            temp_table = Table('data_sandbox', self.table_name)
            if temp_table.exists():
                temp_table.delete()
            job_config = bigquery.QueryJobConfig(
                destination=temp_table.full_name)
            Job = self.client.query(sql_query, job_config=job_config)
            Job.result()
            self.delete_matched(temp_table.full_name, upsert_columns)
            temp_table.delete()
            write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            schema_update_options = [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
            ]
        elif if_exists == 'append':
            write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            schema_update_options = [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
            ]
        elif if_exists == 'replace':
            write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            schema_update_options = None

        job_config = bigquery.QueryJobConfig(
            destination=self.table_ref,
            write_disposition=write_disposition,
            schema_update_options=schema_update_options
        )

        Job = self.client.query(sql_query, job_config=job_config)
        result = Job.result()

        metadata_payload = {'target_table': f'{self.dataset_table}',
                            'num_rows_inserted': result.total_rows, 'bytes_processed': bytes_billed}
        self.bq.write_metadata(metadata_payload)

        return 'Dados carregados com sucesso'

    def write_df_data(self, 
                      dataframe: DataFrame, 
                      upsert_columns: list = None, 
                      time_partition_on:str = None) -> None:

        # Método para escrever tabelas no bigquery via DataFrame padrão e genêrico
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
        if time_partition_on:
            job_config.time_partitioning = bigquery.table.TimePartitioning(
                field=time_partition_on)

        if not self.exists():
            Job = self.client.load_table_from_dataframe(
                dataframe, self.full_name, job_config=job_config)
            Job.result()
            return 'Dados carregados com sucesso'

        if upsert_columns:
            job_config.schema = self.client.get_table(self.full_name).schema
            temp_table = f"{self.project}.{'data_sandbox'}.{self.table_name}"
            
            if self.exists(temp_table):
                self.delete_table(dataset_name='data_sandbox',
                                  table_name=self.table_name)
            
            Job = self.load_table_from_dataframe(dataframe, temp_table)
            Job.result()
            
            self.delete_matched(self.full_name, temp_table, upsert_columns)
            
            self.delete_table(dataset_name='data_sandbox', table_name=self.table_name)
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            job_config.schema_update_options = [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION]
        else:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

        Job = self.client.load_table_from_dataframe(dataframe, 
                                                    self.full_name, 
                                                    job_config=job_config)
        
        Job.result()

        destination_table = self.client.get_table(self.full_name)

        return 'Dados carregados com sucesso'