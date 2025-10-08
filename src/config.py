# Utility for loading environment variables
import os
from dotenv import load_dotenv

load_dotenv()

CONGRESS_API_KEY = os.getenv("CONGRESS_API_KEY")
FEC_API_KEY = os.getenv("FEC_API_KEY")
LOBBYING_API_KEY = os.getenv("LOBBYING_API_KEY")

if not (CONGRESS_API_KEY and FEC_API_KEY and LOBBYING_API_KEY):
    print("Warning: One or more API keys are missing. Please check your .env file.")
