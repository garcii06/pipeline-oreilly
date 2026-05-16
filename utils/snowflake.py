import snowflake.connector
from utils.config import settings

def get_snowflake_connection():
    connection = snowflake.connector.connect(
        account=settings.snowflake_account,
        user=settings.snowflake_user,
        password=settings.snowflake_password,
        database=settings.snowflake_database,
        warehouse=settings.snowflake_warehouse,
        role=settings.snowflake_role
    )
    return connection