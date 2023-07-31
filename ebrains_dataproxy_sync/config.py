import os

auth_token=os.getenv("AUTH_TOKEN")

kg_endpoint=os.getenv("KG_ENDPOINT", "https://core.kg.ebrains.eu")

stage=os.getenv("KG_STAGE", "RELEASED")