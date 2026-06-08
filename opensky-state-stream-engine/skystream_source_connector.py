import os
import json
import time
import requests
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from kafka import KafkaProducer
import argparse

class TokenManager:
    def __init__(self,
        session: requests.Session,
        token_url: str,
        client_id: str,
        client_secret: str,
        refresh_margin_secs: int = 30) :

        self.session = session
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_margin_secs = refresh_margin_secs

        self._token: Optional[str] = None
        self._expires_at : Optional[datetime] = None

    def get_token(self) -> str :
        if self._token and self._expires_at and datetime.now() < self.expires_at :
            return self._token
        return self.refresh_token()

    def refresh_token(self):
        response = self.session.post(
            self.token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret
            }
        )
        response.raise_for_status()
        data = response.json()

        self._token = data["access_token"]
        expires_in = data.get("expires_in", 1800)
        self._expires_at = datetime.now() + timedelta(seconds=expires_in - self.refresh_margin_secs)
        return self._token 

    def get_headers(self):
        return {"Authorization": f"Bearer {self.get_token()}"}

class OpenskyAPIClient:
    def __init__(self, client_id:str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret

        self.rate_limit_total_requests = 4000
        self.rate_limit_reset_interval_secs = 24 * 60 * 60 
        self.token_url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
        self.data_url = "https://opensky-network.org/api/states/all"
        
        # Exact interval pacing to prevent rate-limit exhaustion
        self.request_interval = self.rate_limit_reset_interval_secs / self.rate_limit_total_requests 

        if not self.client_id or not self.client_secret:
            raise ValueError("OpenSky credentials must be set explicitly.") 
            
        self.session: Optional[requests.Session] = None
        self.tokens: Optional[TokenManager] = None

    def open_session(self) -> None : 
        self.session = requests.Session()
        self.tokens = TokenManager( self.session, self.token_url, self.client_id, self.client_secret)

    def fetch_states(self) -> Optional[Dict[str,Any]]:
        if not self.session or not self.tokens:
            raise RuntimeError("Session not initialized. Call open_session() first.")
        
        try:
            response = self.session.get(self.data_url, headers=self.tokens.get_headers())
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error fetching data: HTTP {response.status_code}")
                return None
        except requests.RequestException as e:
            print(f"Network error during fetch: {e}")
            return None

    def close_session(self) -> None:
        if self.session:
            self.session.close()


class KafkaProducerApp:
    def __init__(self,bootstrap_servers: str):
        self.bootstrap_servers=bootstrap_servers
        self.producer = KafkaProducer(
            bootstrap_servers = self.bootstrap_servers,
            key_serializer = lambda k: k.encode('utf-8') if isinstance(k,str) else k,
            value_serializer = lambda v : json.dumps(v).encode('utf-8')
        )
    def send_message(self, topic: str, key: str, value: Any) -> None :
        try:
            self.producer.send(topic,key=key,value=value)
        except Exception as e:
            print(f"Failed to publish message to Kafka: {e}")
    def flush_and_close(self) -> None:
        self.producer.flush()
        self.producer.close()


if __name__ == "__main__":
    # api_client = OpenskyAPIClient(os.getenv("client_id"), os.getenv("client_secret"))
    parser = argparse.ArgumentParser()
    parser.add_argument("--client_id",type=str)
    parser.add_argument("--client_secret",type=str)
    args = parser.parse_args()


    api_client = OpenskyAPIClient(args.client_id, args.client_secret)
    producer = KafkaProducerApp(bootstrap_servers="localhost:19092,localhost:29092,localhost:39092")

    api_client.open_session()

    try:
        # while True:
        data = api_client.fetch_states()
        # print(data)
        # with open("data.txt","w") as file:
        #     # file.write(data)
        #     json.dump(data,file, indent=4)
        if data and data.get("states"):
            for state in data["states"]:
                icao24 = state[0]
                producer.send_message(
                    topic="opensky_raw_vectors",
                    key=icao24,
                    value=state
                )
        time.sleep(api_client.request_interval)
    except KeyboardInterrupt:
        print("Shutting down ingestion engine securely...")
    finally:
        api_client.close_session()
        producer.flush_and_close()

