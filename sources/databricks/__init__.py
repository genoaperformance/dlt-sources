import dlt

from decouple import config
from datetime import datetime
from dlt.extract.source import DltResource
from dlt.common.typing import TDataItem
from typing import Iterable, List
from delta_sharing import SharingClient, load_as_pandas


@dlt.source(name="databricks", max_table_nesting=2)
def databricks_source(
    config_share_file: str,
    date_column: str|None = None,
    execution_date: None = None,
    **kwargs
) -> Iterable[DltResource]:
    
    
    logger = kwargs.get("logger", None)
    
    if logger: logger.debug(f"Arquivo de conexão: {config_share_file}")

    delta_sharing_client = SharingClient(config_share_file)
    
    def get_available_tables():
        
        available_tables: List[TDataItem] = []
        
        for i, table in enumerate(delta_sharing_client.list_all_tables()):
            
            available_tables.append({
                "name": table.name,
                "share": table.share,
                "schema": table.schema,  
                "url": "{profile}#{share}.{schema}.{table}".format(
                    profile=config_share_file,
                    share=table.share,
                    schema=table.schema,
                    table=table.name
                ),
                "date": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
        
        return available_tables
    
    available_tables = get_available_tables()
        
    @dlt.resource(
        name="available_tables"
        # primary_key="url", 
        # write_disposition="replace",
        # parallelized=False
    )
    def available_tables_resource(
        # ingested_at_=dlt.sources.incremental("ingested_at_", initial_value="1970-01-01T00:00:00Z")
    ) -> Iterable[TDataItem]:
        if logger: logger.debug("Coletando tabelas dispnoníveis no Delta Lake")
        yield from available_tables
        
    yield available_tables_resource

    def get_table_data(
        table_url: str,
        # ingested_at_=dlt.sources.incremental("ingested_at_", initial_value="1970-01-01T00:00:00Z")
    ):
        
        if logger: logger.debug(f"Coletando registros da tabela `{table_url}`")

        df = load_as_pandas(
            table_url, 
            limit=config("LOAD_LIMIT", default=100000, cast=int)
        )
        if date_column:
            if logger: logger.debug(f"Filtrando registros: `{date_column}` >= {execution_date}")
            df = df[df[date_column]>=execution_date]
        
        for _, row in df.iterrows():
            yield row.to_dict()
            
    for table in available_tables:
        yield dlt.resource(
            get_table_data,
            name=table["name"],
            # primary_key="url", 
            # write_disposition="replace",
            # parallelized=False
        )(table["url"])