import os
from dotenv import load_dotenv

load_dotenv()

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
OMNICOMM_BASE_URL = os.getenv("OMNICOMM_BASE_URL")
OMNICOMM_USERNAME = os.getenv("OMNICOMM_USERNAME")
OMNICOMM_PASSWORD = os.getenv("OMNICOMM_PASSWORD")

REQUEST_TIMEOUT = 10
