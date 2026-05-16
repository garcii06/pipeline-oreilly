from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    snowflake_account: str
    snowflake_user: str
    snowflake_password: str
    snowflake_database: str = 'OREILLY_DB'
    snowflake_warehouse: str = 'OREILLY_WH'
    snowflake_role: str = 'LOADER'

    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8')

settings = Settings()