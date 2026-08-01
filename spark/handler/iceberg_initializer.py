
import functools
import inspect
from .spark_config import build_spark


def iceberg_initialisation(func=None, *, models=None):
    """
    Decorator to ensure Iceberg namespaces and tables are initialized and created 
    before the decorated Spark job/function starts executing.
    
    If 'models' is not specified, it dynamically discovers all classes in 
    `spark.model.models` that subclass `SparkHandler` and define a `TABLE_NAME`.
    
    Can be used:
        - Without arguments:
            @iceberg_initialisation
            def main():
                ...
        
        - With specific models:
            @iceberg_initialisation(models=[RawStockPrices])
            def main():
                ...
    """
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            # 1. Access/Create Spark Session — must go through build_spark() so
            # Iceberg's SQL extensions and the Nessie catalog config are
            # registered on the session BEFORE any CREATE NAMESPACE / CREATE
            # TABLE statements run against it. A bare SparkSession.builder
            # .getOrCreate() here would create (or attach to) a session with
            # no Iceberg catalog registered, and the DDL below would fail or
            # silently target the wrong catalog.
            spark = build_spark()
            
            # 2. Resolve target models
            target_models = models
            if target_models is None:
                # Deferred import to prevent circular import issues
                from spark.model import models as models_module
                from spark.handler import SparkHandler
                
                target_models = []
                for name, obj in inspect.getmembers(models_module, inspect.isclass):
                    # Check if the class inherits from SparkHandler and defines a TABLE_NAME
                    if issubclass(obj, SparkHandler) and obj is not SparkHandler and hasattr(obj, "TABLE_NAME"):
                        target_models.append(obj)
            
            # 3. Create tables if not exists
            for model_cls in target_models:
                table_name = getattr(model_cls, "TABLE_NAME", None)
                if not table_name:
                    continue
                
                # Fetch schema and metadata
                schema = model_cls.get_schema()
                partition_by = getattr(model_cls, "PARTITION_BY", None)
                properties = getattr(model_cls, "TABLE_PROPERTIES", {})
                
                # Ensure the namespace exists
                parts = table_name.split(".")
                if len(parts) >= 2:
                    namespace = ".".join(parts[:-1])
                    print(f"[IcebergInitializer] Ensuring namespace exists: {namespace}")
                    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
                
                # Construct columns definitions
                columns_def = []
                for field in schema.fields:
                    nullability = "NOT NULL" if not field.nullable else ""
                    # simpleString() gets PySpark datatypes representation which are standard SQL types
                    col_type = field.dataType.simpleString()
                    columns_def.append(f"`{field.name}` {col_type} {nullability}")
                
                columns_sql = ", ".join(columns_def)
                
                # Partitioning DDL segment
                partition_sql = ""
                if partition_by:
                    if isinstance(partition_by, list):
                        partition_sql = f"PARTITIONED BY ({', '.join(partition_by)})"
                    else:
                        partition_sql = f"PARTITIONED BY ({partition_by})"
                
                # Table properties DDL segment
                properties_sql = ""
                if properties:
                    props_list = [f"'{k}'='{v}'" for k, v in properties.items()]
                    properties_sql = f"TBLPROPERTIES ({', '.join(props_list)})"

                # Full DDL construct
                ddl = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {columns_sql}
                )
                USING iceberg
                {partition_sql}
                {properties_sql}
                """

                print(f"[IcebergInitializer] Creating table if not exists: {table_name}")
                # Execute DDL
                spark.sql(ddl)

                # Existing Iceberg tables are not changed by CREATE TABLE IF
                # NOT EXISTS. Apply additive partition evolution when a model
                # asks for it so older tables can be brought forward without
                # dropping data or snapshots.
                for partition_field in getattr(model_cls, "PARTITION_EVOLUTION", []):
                    try:
                        spark.sql(f"ALTER TABLE {table_name} ADD PARTITION FIELD {partition_field}")
                        print(
                            "[IcebergInitializer] Added partition field "
                            f"{partition_field} to {table_name}"
                        )
                    except Exception as exc:
                        message = str(exc).lower()
                        already_exists = (
                            "already exists" in message
                            or "duplicate partition field" in message
                            or "cannot add duplicate" in message
                        )
                        if already_exists:
                            print(
                                "[IcebergInitializer] Partition field already present "
                                f"on {table_name}: {partition_field}"
                            )
                        else:
                            raise
                
            # 4. Proceed to the main function execution
            return f(*args, **kwargs)
        return wrapper

    if func is None:
        return decorator
    else:
        return decorator(func)
