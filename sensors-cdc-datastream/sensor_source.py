import mysql.connector
import random
import time
import uuid
from datetime import datetime


HOST = ""
USER = "sensor_source"
DB = "sensor_telemetry"
USER_PASS = ""

def get_connection():
    return mysql.connector.connect(
        host=HOST,
        user=USER,
        password=USER_PASS,
        database=DB
    )

def create_table():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS sensors(
                        sensor_id VARCHAR(36) PRIMARY KEY,
                        device_type VARCHAR(36),
                        temperature DECIMAL(5,2),
                        status VARCHAR(20),
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                   )
                   """)
    
    conn.commit()
    cursor.close()
    conn.close()

def simulate_telemetry():
    conn = get_connection()
    cursor = conn.cursor()
    active_sensors=[]
    device_types=['HVAC', 'Thermometer', 'Freezer_Unit']

    print("Simulating sensor readings, exit to stop simulation")
    try:
        while True:
            action = random.choices(["INSERT","UPDATE","DELETE"],
                                    weights=[50.0,40.0,10.0])[0]
            if action=="INSERT" or not active_sensors:
                sensor_id = str(uuid.uuid4())
                device_type = random.choice(device_types)
                temperature = round(random.uniform(10.0,80.0),2)

                cursor.execute("INSERT INTO sensors(sensor_id,device_type,temperature,status) VALUES (%s,%s,%s,%s)",
                                    (sensor_id,device_type,temperature,"ACTIVE"))
                active_sensors.append(sensor_id)
                print(f"INSERT: Sensor {sensor_id[:8]} provisioned.")
            
            elif action=="UPDATE" and active_sensors:
                sensor_id = random.choice(active_sensors)
                temp_fluctuation = round(random.uniform(-5.0, 5.0), 2)
                cursor.execute("UPDATE sensors SET temperature = temperature + %s WHERE sensor_id=%s",
                               (temp_fluctuation,sensor_id))
                print(f"UPDATE: Sensor {sensor_id[:8]} temperature fluctuated by {temp_fluctuation}.")
            
            elif action=="DELETE" and active_sensors:
                sensor_id = random.choice(active_sensors)
                cursor.execute("DELETE FROM sensors where sensor_id = %s",[sensor_id])
                active_sensors.remove(sensor_id)
                print(f"DELETE: Sensor {sensor_id[:8]} decommissioned.")
            
            conn.commit()
            time.sleep(10)

    except KeyboardInterrupt:
        print("\nSimulation stopped.")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    create_table()
    simulate_telemetry()
