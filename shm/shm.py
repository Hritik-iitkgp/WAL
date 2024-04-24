from flask import jsonify,request
from fastapi import FastAPI
import uvicorn
import os,subprocess,random, requests,time
import threading
import random
import sqlite3,os
import mysql.connector 
from typing import Dict

app = FastAPI() 


N=0
schema={}
shards=[]
servers={}
hashmaps={}
primary_servers={}
counts = {}
lock1 = threading.Lock()


# DB_FILE = "metadata.db"
# conn = sqlite3.connect(DB_FILE,timeout=60*60)

# while True:
#      try:
#         conn = mysql.connector.connect(
#             host="metadb",
#             user="root",
#             password="giri123456",
#             database="metadb"
#         )
#         print("connected")
#         break
#      except  Exception as e:
#          time.sleep(1)
# conn.close()
 
# if os.path.exists(DB_FILE):
#     os.remove(DB_FILE)


def is_dead(server_id)->bool:
    print(f"checking the heartbeat of {server_id}...",flush=True)
    for i in range(3):
        try:
            resp = requests.get(f"http://{server_id}:5002/heartbeat",timeout=15)
            if resp.ok:
                return False
        except requests.RequestException as e:
            time.sleep(0.01)
            print("Trying again....",flush=True)
    return True

def respawn(server_id):
    global servers
    dead_server_shards = servers.pop(server_id)
    while True:
        try:
            conn = mysql.connector.connect(
                host="metadb",
                user="root",
                password="giri123456",
                database="metadb"
            )
            print("connected")
            break
        except  Exception as e:
            time.sleep(1)
            continue

    cursor = conn.cursor()
    cursor.execute("DELETE FROM MapT WHERE Server_id=%s",(server_id,))
    new_server = "rspn"+server_id
    
    command =  f"docker run --name {new_server} --network net1 -d server"
    result = subprocess.run(command,shell=True,text=True)
            
    if result.returncode == 0:
        while True:
                    try:
                        print({
                            "schema":schema,
                            "shards":dead_server_shards
                        },flush=True)
                        response = requests.post(f"http://{new_server}:5002/config",json={
                            "schema":schema,
                            "shards":dead_server_shards
                        },timeout=2000)
                        print(response.json())
                        break
                    except Exception as e:
                        # print("retrying")
                        continue

        for sh in dead_server_shards:

            hashmaps[sh].add_server_instance(new_server)
            hashmaps[sh].remove_server_instance(server_id)

            cursor.execute("SELECT Server_id FROM MapT WHERE Shard_id=%s",(sh,))
            row = cursor.fetchone()
            sh_server = row[0]
            resp = requests.get(f"http://{sh_server}:5002/copy",json={
                "shards":[sh]
            },timeout=20)
            if resp.status_code == 200:
                data = resp.json()[sh]
                if len(data) == 0:
                    continue
                resp1 = requests.post(f"http://{new_server}:5002/write",json={
                    "shard":sh,
                    "curr_idx": 507,
                    "data": data
                })
                print(resp1.json())
                if not resp1.ok:
                    return "some error"
                cursor.execute("INSERT INTO MapT (Shard_id, Server_id,Prim) VALUES (%s, %s, %s)",(new_server,sh,0))
                print(f"Successfully copied {sh} from {sh_server} to {new_server}")
    servers[new_server] = dead_server_shards
    print(f"Successfully respawned {server_id}:{new_server}")
    conn.commit()
    conn.close()
    

def checking_health():
    while True:
        # get locks over shared resource 
        time.sleep(10)
        with lock1:
            print("aqcuired the lock| health check")
            __servers = list(servers)
            for server_id in __servers:
                print(f"checking the health of {server_id}...")
                if is_dead(server_id):
                    respawn(server_id)
            print("released the lock| health check")

def primary_elect(servers):
    global primary_servers

    primary_servers = {}
    while True:
        try:
            conn = mysql.connector.connect(
                host="metadb",
                user="root",
                password="giri123456",
                database="metadb"
            )
            print("connected")
            break
        except  Exception as e:
            time.sleep(1)
            continue

    cursor = conn.cursor()
    cursor.execute("UPDATE MapT SET Prim = 0 WHERE Prim = 1")


    for server, shards in servers.items():
        for shard in shards:
            if shard not in primary_servers:
                primary_servers[shard] = server
                response = requests.post(f"http://{server}:5002/get_commits", json={"shard": shard})
                response = response.json()
                counts[shard]  = response.get('commits', 0)
            else:
                # Shard already has a primary server, choose randomly
                temp = requests.post(f"http://{server}:5002/get_commits", json={"shard": shard})
                temp = temp.json()
                if temp.get('commits', 0)>counts[shard]:
                    counts[shard] = temp.get('commits', 0)
                    primary_servers[shard] = server

    for shard, server in primary_servers.items():
        cursor.execute("UPDATE MapT SET Prim = 1 WHERE Shard_id = %s AND Server_id = %s", (shard, server))

    print(primary_servers,flush=True)
    
    conn.commit()
    conn.close()

    
    return primary_servers


@app.post("/get_servers")
async def handle_servers(servers_list: Dict[str, list]):
    # Assuming servers is your dictionary containing server information
    # Do whatever you need to do with the servers dictionary
    global servers

    servers = servers_list
    print(servers)
    primary_servers = primary_elect(servers)
    
    return {"message": "Servers received successfully!"}







if __name__ == '__main__':

    t2 = threading.Thread(target=checking_health)
    t2.start()
    print("WORKING",flush=True)
    uvicorn.run(app,host='0.0.0.0', port=5001)

    t2.join()
