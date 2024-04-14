from flask import Flask, jsonify,request
import os,subprocess,random,requests,time
import threading
import random
import sqlite3,os
import mysql.connector 

app = Flask(__name__)


N=0
schema={}
shards=[]
servers={}
hashmaps={}
lock1 = threading.Lock()


DB_FILE = "metadata.db"
# conn = sqlite3.connect(DB_FILE,timeout=60*60)

# while True:
#      try:
#         _conn = mysql.connector.connect(
#             host="metadb",
#             user="root",
#             password="giri123456",
#             database="metadb"
#         )
#         print("connected")
#         break
#      except  Exception as e:
#          time.sleep(2)
# _conn.close()
 
# if os.path.exists(DB_FILE):
#     os.remove(DB_FILE)



def is_dead(server_id)->bool:
    print(f"checking the heartbeat of {server_id}...",flush=True)
    for i in range(3):
        try:
            resp = requests.get(f"http://{server_id}:5000/heartbeat",timeout=15)
            if resp.ok:
                return False
        except requests.RequestException as e:
            time.sleep(0.01)
            print("Trying again....",flush=True)
    return True

def respawn(server_id):
    global servers
    dead_server_shards = servers.pop(server_id)
    conn = sqlite3.connect(DB_FILE,timeout=60*60)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM MapT WHERE Server_id=?",(server_id,))
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
                        response = requests.post(f"http://{new_server}:5000/config",json={
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

            cursor.execute("SELECT Server_id FROM MapT WHERE Shard_id=?",(sh,))
            row = cursor.fetchone()
            sh_server = row[0]
            resp = requests.get(f"http://{sh_server}:5000/copy",json={
                "shards":[sh]
            },timeout=20)
            if resp.status_code == 200:
                data = resp.json()[sh]
                if len(data) == 0:
                    continue
                resp1 = requests.post(f"http://{new_server}:5000/write",json={
                    "shard":sh,
                    "curr_idx": 507,
                    "data": data
                })
                print(resp1.json())
                if not resp1.ok:
                    return "some error"
                cursor.execute("INSERT INTO MapT (Shard_id, Server_id) VALUES (?, ?)",(new_server,sh))
                print(f"Successfully copied {sh} from {sh_server} to {new_server}")
    servers[new_server] = dead_server_shards
    print(f"Successfully respawned {server_id}:{new_server}")
    

def checking_health():
    while True:
        # get locks over shared resource 
        time.sleep(30*2)
        with lock1:
            print("aqcuired the lock| health check")
            __servers = list(servers)
            for server_id in __servers:
                if is_dead(server_id):
                    respawn(server_id)
            print("released the lock| health check")

def select_primary_server(shard):
    replicas = [server for server, shards in servers.items() if shard in shards]
    for replica in replicas:
        if checking_health(replica):
            return replica
    return None

def handle_remove_server(num_instances, servers):
    # Logic to handle removing server instances
    pass

def primary_elect(shard, removed_servers=[]):
    # Logic to select a new primary server for the shard
    # If any servers were removed, they would be listed in removed_servers
    new_primary = select_primary_server(shard)
    if new_primary:
        return new_primary
    else:
        # Handle the case where no healthy servers are available for the shard
        return None





@app.route('/primary_elect', methods=['GET'])
def primary_elect():
    try:
        shard = request.args.get('shard')
        removed_servers = request.args.getlist('removed_servers')

        new_primary = primary_elect(shard, removed_servers)
        if new_primary:
            response = {
                "message": f"New primary server for shard {shard}: {new_primary}",
                "status": "success"
            }
        else:
            response = {
                "message": f"No healthy servers available for shard {shard}",
                "status": "error"
            }
        return jsonify(response), 200

    except Exception as e:
        response = {
            "message": str(e),
            "status": "error"
        }
        return jsonify(response), 500

if __name__ == '__main__':

    t2 = threading.Thread(target=checking_health)
    t2.start()

    app.run(host='0.0.0.0', port=5001,debug=True)

    t2.join()
