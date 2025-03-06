import os
import sys
import dlt
import warnings

from pathlib import Path
from typing import Optional
from datetime import datetime
from dateutil.parser import parse as parse_date
# from dotenv import load_dotenv
# from decouple import config, Csv
# from prefect import flow, task, get_run_logger

# BASE_DIR = Path(__file__).resolve().parents[1]
# sys.path.append(str(BASE_DIR))
warnings.filterwarnings('ignore') 

# from utils.helpers import load_yaml
# from utils.prefect import generate_flow_run_name
from dlt_sources.databricks import databricks_source

# os.environ["EXECUTION_DATE"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# load_dotenv(BASE_DIR.joinpath(".env"), override=True)
# config_yaml = load_yaml(path=BASE_DIR.joinpath('config.yaml'))

# @flow(name=config_yaml['client'], flow_run_name=generate_flow_run_name)
def ingest_databricks(
    config_share_file: str,
    tables_and_fields: dict    
):
    
    # logger = get_run_logger()
    
    try:

        dlt_pipeline = dlt.pipeline(
            pipeline_name="brasilprev-databricks",
            destination="postgres",
            dataset_name="databricks",
            progress="log"
        )

        source = databricks_source(
            # config_share_file=config("CONFIG_SHARE_FILE"),
            config_share_file=config_share_file
            # logger=logger
        )

        # tables_and_fields = config("TABLES", default={}, cast=lambda value: { item.split("|")[0]: item.split("|")[1] for item in value.split(",")})
        
        if not tables or len(tables) == 0:
            tables = source.resources.keys()

        # logger.info(f"Tabelas selecionadas para ingestão: {list(tables_and_fields.keys())}")

        # TODO: implementar carregamento incremental
        # source = databricks_source(
        #     config_share_file=config("CONFIG_SHARE_FILE"),
        #     logger=logger
        # ).with_resources(*tables_and_fields.keys())

        # for table_name, res in source.resources.items():

        #     print("res", res, type(res))

        #     write_disposition = "replace"
        #     if table_name in tables_and_fields:
        #         write_disposition = "append"
        #         cursor_field = tables_and_fields[table_name]
        #         res.apply_hints(incremental=dlt.sources.incremental(cursor_field, initial_value=config("EXECUTION_DATE", cast=parse_date)))

        #     load_info = dlt_pipeline.run(res, table_name=table_name, write_disposition=write_disposition)
        #     logger.info(load_info)
        #     logger.info(dlt_pipeline.last_trace.last_normalize_info)

        for table, cursor_field in tables_and_fields.items():
            
            # logger.info(f"Executando ingestão de `{table}`")

            load_info = dlt_pipeline.run(
                source.with_resources(table),
                write_disposition="replace"
            )
            # logger.info(load_info)
            # logger.info(dlt_pipeline.last_trace.last_normalize_info)
            print(load_info)
           
    except Exception as e:
        print(f"Erro ao capturar dados de tabelas Databricks: {e}")
        raise e
    
if __name__ == "__main__":
    ingest_databricks()