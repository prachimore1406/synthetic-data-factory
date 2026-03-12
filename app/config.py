import os
from typing import Dict, Any

def load_env() -> bool:
    try:
        from dotenv import load_dotenv
    except Exception:
        return False
    load_dotenv()
    return True

def openai_config() -> Dict[str, Any]:
    return {
        "api_key": os.getenv("OPENAI_API_KEY", ""),
        "base_url": os.getenv("OPENAI_BASE_URL", ""),
        "organization": os.getenv("OPENAI_ORG_ID", ""),
        "model": os.getenv("OPENAI_MODEL", ""),
    }
