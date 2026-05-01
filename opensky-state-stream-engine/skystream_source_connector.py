import os
import requests
from datetime import datetime, timedelta
import time

RATE_LIMIT_TOTAL_REQUESTS = 4000
RATE_LIMIT_RESET_INTERVAL = 24*60*60 #seconds

# make an API call after __ seconds
REQUEST_INTERVAL = RATE_LIMIT_RESET_INTERVAL/RATE_LIMIT_TOTAL_REQUESTS 

CLIENT_ID , CLIENT_SECRET = os.getenv('OPENSKY_CLIENT_ID') , os.getenv('OPENSKY_CLIENT_SECRET')

if not ClIENT_ID or not CLIENT_SECRET:
    raise ValueError("OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET environment variables must be set")

TOKEN_REFRESH_INTERVAL = 30 #minuites
TOKEN_REFRESH_MARGIN = 30 #seconds
TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

class TokenManager:
    def __init__(self, session: requests.Session ):
        self.session = session
        self.token = None
        self.expires_at = None

    def get_token(self):
        if self.token and self.expires_at and datetime.now() < self.expires_at:
            return self.token
        return self._refresh_token()
    
    def _refresh_token(self):
        r = self.session.post(
            TOKEN_URL,
            data = {
                "grant_type": "client_credentials",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET
            }
        )
        r.raise_for_status()

        data = r.json()
        self.token = data["access_token"]
        expires_in = data.get("expires_in", 1800)
        self.expires_at = datetime.now() + timedelta(seconds=expires_in - TOKEN_REFRESH_MARGIN)
        return self.token

    def headers(self):
        return {"Authorization": f"Bearer {self.get_token()}"}

DATA_URL = "https://opensky-network.org/api/states/all"
session = requests.Session()
tokens = TokenManager(session)
i=2
while(i--):
    response = session.get(
        DATA_URL,
        headers = tokens.headers()
    )
    print(f"Status Code : {response.status_code}")
    if response.status_code == 200:
        print(response.json())
    else:
        print("Error fetching data")
    
    time.sleep(REQUEST_INTERVAL)

session.close()