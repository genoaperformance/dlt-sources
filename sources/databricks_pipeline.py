import dlt

from dlt_sources.databricks import databricks_source

def ingest_databricks(
    config_share_file: str,
    table: str
):
    
    try:

        dlt_pipeline = dlt.pipeline(
            pipeline_name="brasilprev-databricks",
            destination="postgres",
            dataset_name="databricks",
            progress="log"
        )

        source = databricks_source(
            config_share_file
        )
        
        if not tables or len(tables) == 0:
            tables = source.resources.keys()

        load_info = dlt_pipeline.run(
            source.with_resources(table),
            write_disposition="replace"
        )
        print(load_info)
        print(dlt_pipeline.last_trace.last_normalize_info)
           
    except Exception as e:
        print(f"Erro ao capturar dados de tabelas Databricks: {e}")
        raise e
    
if __name__ == "__main__":
    ingest_databricks()