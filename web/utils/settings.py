from pydantic import BaseSettings


class Settings(BaseSettings):
    secret: str

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
