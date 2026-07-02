import json
import os
import requests
import time
from confluent_kafka import Producer
import argparse
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

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
    
        self._token : Option[str] = None
        self._expires_at : Optional[datetime] = None

    def get_token(self) -> str : 
        if self._token and self._expires_at and datetime.now() < self._expires_at :
            return self._token
        return self.refresh_token()
    
    def refresh_token(self) :
        response = self.session.post(
            self.token_url,
            data={
                "grant_type" : "client_credentials",
                "client_id" : self.client_id,
                "client_secret" : self.client_secret
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


class OpenSkyFlightClient:
    def __init__(self, client_id:str, client_secret: str, begin_ts: int, end_ts: int):
        self.client_id = client_id
        self.client_secret = client_secret
        self.begin_ts = begin_ts
        self.end_ts = end_ts

        self.rate_limit_total_requests = 4000
        self.rate_limit_reset_interval_secs = 24 * 60 * 60 
        self.token_url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
        self.data_url = "https://opensky-network.org/api/flights/all"

        # Exact interval pacing to prevent rate-limit exhaustion
        self.request_interval = self.rate_limit_reset_interval_secs / self.rate_limit_total_requests 

        if not self.client_id or not self.client_secret:
            raise ValueError("OpenSky credentials must be set explicitly.") 
            
        self.session: Optional[requests.Session] = None
        self.token: Optional[TokenManager] = None
        self.params = {
            "begin": self.begin_ts,
            "end": self.end_ts
        }

    def open_session(self) -> None : 
        self.session = requests.Session()
        self.token = TokenManager( self.session, self.token_url, self.client_id, self.client_secret)

    def get_flights(self):
        if not self.session or not self.token:
            raise RuntimeError("Session not initialized. Call open_session() first.")

        try:
            response = self.session.get(self.data_url, headers=self.token.get_headers(), params=self.params)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error fetching data: HTTP {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None
    
    def close_session(self) -> None:
        if self.session:
            self.session.close()

class KafkaProducerApp:
    def __init__(self,brokers,topic):
        self.brokers = brokers
        self.topic = topic

        self.conf = {
            'bootstrap.servers': brokers,
            'client.id' : 'flights-bootstrapper'
        }

        self.producer = Producer(self.conf)
    
    def delivery_report(self,err,msg):
        if err is not None:
            print(f"Failed to deliver record: {err}")
    
    def produce_message(self,key:str,value:Any) :
        try:
            self.producer.produce(
                topic = self.topic,
                key = key,
                value = json.dumps(value).encode('utf-8'),
                callback=self.delivery_report
            )
        except Exception as e:
            print(f"Failed to publish message to Kafka: {e}")

    def flush_and_close(self) -> None:
        self.producer.flush()
        self.producer.close()
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--client_id",type=str)
    parser.add_argument("--client_secret",type=str)
    args = parser.parse_args()

    begin_ts = int(time.time()) - 3600
    end_ts = int(time.time())
    
    api_client = OpenSkyFlightClient(args.client_id, args.client_secret,begin_ts,end_ts)
    producer = KafkaProducerApp(brokers="localhost:19092,localhost:29092,localhost:39092",topic="raw_flights")

    api_client.open_session()

    try:
        # while True:
        data = api_client.get_flights()
        if data:
            for flight in data:
                icao24 = flight.get("icao24")
                if icao24:
                    producer.produce_message(
                        key=icao24,
                        value=flight
                    )
        
    except KeyboardInterrupt:
        print("Shutting down ingestion engine securely...")
    finally:
        api_client.close_session()
        producer.flush_and_close()
    


