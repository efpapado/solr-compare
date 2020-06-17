# settings.py
import os
from dotenv import load_dotenv

load_dotenv()

SOLR_BEFORE = os.getenv("SOLR_BEFORE")
SOLR_AFTER = os.getenv("SOLR_AFTER")
