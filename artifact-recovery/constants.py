ENV_FILE = ".env"
DATABASE_PATH = "./hypha-app-database.db"
LOGLEVEL = "DEBUG"
S3_BUCKET = "hypha-workspaces"
ADDITIONAL_ENV_FILES = ["bioimageio_env", "shareloc_env"]
ADDITIONAL_ENV_KEYS = [
    "SANDBOX_ZENODO_ACCESS_TOKEN",
    "ZENODO_ACCESS_TOKEN",
    "S3_ENDPOINT_URL",
    "S3_ACCESS_KEY_ID",
    "S3_SECRET_ACCESS_KEY",
    "S3_REGION_NAME",
    "S3_PREFIX",
    "S3_BUCKET",
]
DB_USER = "hypha-admin"
DATABASE_URI = f"sqlite+aiosqlite:///{DATABASE_PATH}"
DB_PASSWORD = ""  # TODO: add password from .env
DB_NAME = "hypha-db"
DB_HOST = ""  # TODO: add host from .env
DB_PORT = "5432"
DATABASE_URL = (
    f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)
