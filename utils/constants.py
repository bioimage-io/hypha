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
DATABASE_URI = f"sqlite+aiosqlite:///{DATABASE_PATH}"
DATABASE_URL = ""
DB_USER_DEFAULT = "hypha-admin"
DB_NAME_DEFAULT = "hypha-db"
DB_PORT_DEFAULT = "5432"
