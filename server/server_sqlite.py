# from flask import Flask, jsonify, request
from fastapi import FastAPI,Request,HTTPException
import os
import sqlite3
import sys
from log import *
import time
import requests
import uvicorn
#app = Flask(__name__)

app = FastAPI() 
# # Function to create MySQL connection
# async def create_mysql_connection():
#     return mysql.connector.connect(
#         host=os.getenv("MYSQL_HOST"),
#         user=os.getenv("MYSQL_USER","hritik"),
#         password=os.getenv("MYSQL_PASSWORD","hritik@123"),
#         database=os.getenv("MYSQL_DATABSE","StudentDB")
#     )
# while True:
#     try:
#         global conn
#         conn = create_mysql_connection()
#         break
#     except Exception as e:
#         time.sleep(0.04)



conn = sqlite3.connect("student.db",check_same_thread=False)

logger = {}

commit_log = {} 

@app.post("/config")
async def config(request : Request):
    global logger
    global commit_log
    try:
        payload = await request.json()

        # Extract schema and shards information from the payload
        schema = payload['schema']
        shards = payload['shards']

        # Initialize shards
        # Create MySQL connection
        global conn
        cursor = conn.cursor()

        # Create tables for each shard
        for shard in shards:
            table_name = f"{shard}"
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
            for column, dtype in zip(schema['columns'], schema['dtypes']):
                create_table_query += f"{column} {'INT' if dtype=='Number' else 'VARCHAR(100)'}, "
            create_table_query = create_table_query.rstrip('') + f" PRIMARY KEY (Stud_id) )"
            cursor.execute(create_table_query)
            conn.commit()

        for shard in shards:
             logger[shard] = FileLogger("logs", str(shard) + ".log")
             commit_log[shard] = 0

        # Close cursor and connection
        cursor.close()
        response = {
            "message": f"Server:{', '.join(shards)} configured",
            "status": "success"
        }
        return response

    except sqlite3.Error as err:
        conn.rollback()
        raise HTTPException(status_code=500, detail = f"An error {err} occurred.")
    except Exception as e:
        print(e)
        raise HTTPException(status_code=400,detail = "Invalid")

@app.get('/heartbeat')
async def heartbeat():
    response={
        "message": ""
    }
    return response


@app.post('/get_commits')
async def getcommits(request: Request):
    payload = await request.json()

    global commit_log

    shard_id = payload['shard']

    response = {
        "commits": commit_log[shard_id]
    }

    return response


@app.get('/copy')
async def copy(request : Request):
    try:
        payload = await request.json()
        shards_to_copy = payload['shards']
        # 
        global conn
        cursor = conn.cursor()
        response = {
            # need to add ,
            "status": "success"
        }
        for shard in shards_to_copy:
            table_name = f"{shard}"
            query = f"SELECT * FROM {table_name}"
            cursor.execute(query)
            shard_data = cursor.fetchall()
            response[shard] = [{"Stud_id":stud[0],"Stud_name":stud[1],"Stud_marks":stud[2]} for stud in shard_data]

        # Close cursor
        cursor.close()

        # Add status to response data
        return response

    except sqlite3.Error as err:
        conn.rollback()
        raise HTTPException(status_code=500, detail = f"An error {err} occurred.")
    except:
        raise HTTPException(status_code=400,detail = "Invalid")
@app.post('/read')
async def read(request : Request):
    try:
        payload = await request.json()
        keys = list(payload.keys())
        shard_id = payload['shard']
        stud_id_range = payload[keys[1]]
        global conn
        # Extract low and high values from the range
        low = int(stud_id_range['low'])
        high = int(stud_id_range['high'])
        cursor = conn.cursor()
        # Retrieve the requested data 
        data=[]
        table_name = f"{shard_id}" 
        query = f"SELECT * FROM {table_name} WHERE {keys[1]} >= {low} AND {keys[1]} <= {high}"
        cursor.execute(query)
        data = cursor.fetchall()

        # Close cursor
        cursor.close()
        response = {
            "data": data,
            "status": "success"
        }
        return response

    except sqlite3.Error as err:
        conn.rollback()
        raise HTTPException(status_code=500, detail = f"An error {err} occurred.")
    except:
        raise HTTPException(status_code=400,detail = "Invalid")


@app.post('update_local_data')
async def update_local_data(request : Request):
    try:
        req = request.json()
        server = req["servers"]
        shard = req["shards"]
        list_to_update = req["data"]
        for row in list_to_update:
            if row["type"].lower() == "write":
                requests.post(
                    f"http://{server}:5002/write",
                    json={"shard": shard, "data": row["data"]["data"]},
                )
            elif row["type"].lower() == "delete":
                requests.delete(
                    f"http://{server}:5002/del",
                    json={"shard": shard, "Stud_id": row["data"]["Stud_id"]},
                )
            elif row["type"].lower() == "update":
                # "shard": shard, "Stud_id": stud_id, "data": data
                requests.put(
                    f"http://{server}:5002/update",
                    json={
                        "shard": shard,
                        "Stud_id": row["data"]["Stud_id"],
                        "data": row["data"]["data"],
                    },
                )
            else:
                print("Incorrect type ", row["type"])
    
    except:
        raise HTTPException(status_code=400,detail = "Invalid")

    return {"message": "Updated the local data successfully", "status": "success"}

@app.post('/write')
async def write(request : Request):
    global commit_log
    global logger
    try:
        payload = await request.json()
        print(payload)
        shard_id = payload['shard']
        data_entries = payload['data']

        cursor = conn.cursor()

        if payload["primary_server"] == 1:
            print("I am primary")
            secondary_servers = payload["secondary_servers"]
            votes = 0
            for row in data_entries:
                id = int(logger[shard_id].get_last_log_id()) + 1
                log = Log(id, LogType(0), row, datetime.now())
                logger[shard_id].add_log(log)
            for server in secondary_servers:
                while True:
                    try:
                        print("sending req to sec servers",flush=True)
                        response = requests.post(f"http://{server}:5002/write",json={
                            "shard":shard_id,
                            "data": data_entries,
                            "primary_server":0,
                            "commit" : commit_log[shard_id]
                        },timeout=200)
                        if response.status_code == 200:
                            response = response.json()
                            if response.get('messsage',0) == "Not Uptodate":
                                curr_commit_index = response.get('last_commit',0)
                                list_to_update = logger[shard_id].get_requests_from_given_index(shard_id, int(curr_commit_index) + 1)
                                print(list_to_update)
                                resp = requests.post(
                                    f"http://{server}:5002/update_local_data",
                                    json={"shards": shard_id,"data":list_to_update,"servers":server},
                                )
                                resp = resp.json()
                                print(resp)
                                break
                            
                            votes += 1
                            break
                    except Exception as e:
                        print("retrying...")
            if votes >= len(secondary_servers) // 2:
                commit_log[shard_id] = logger[shard_id].get_last_log_id()
                for row in data_entries:
                    cursor.execute(
                        f"INSERT INTO {shard_id} (Stud_id, Stud_name, Stud_marks) VALUES (?, ?, ?)",
                        (row["Stud_id"], row["Stud_name"], row["Stud_marks"]),
                    )
                conn.commit()
        else:
            commit_index = payload["commit"]
            curr_commit_index = logger[shard_id].get_last_log_id()
            if commit_index != curr_commit_index:
                return {"message": "Not Uptodate", "last_commit": curr_commit_index}
            for row in data_entries:
                id = int(logger[shard_id].get_last_log_id()) + 1
                log = Log(id, LogType(0), row, datetime.now())
                logger[shard_id].add_log(log)
            for row in data_entries:
                cursor.execute(
                    f"INSERT INTO {shard_id} (Stud_id, Stud_name, Stud_marks) VALUES (?, ?, ?)",
                    (row["Stud_id"], row["Stud_name"], row["Stud_marks"]),
                )
            commit_log[shard_id] = logger[shard_id].get_last_log_id()
            conn.commit()

        response = {"message": "Data entries added", "status": "success"}

        return response       #
        
    except sqlite3.Error as err:
        conn.rollback()
        raise HTTPException(status_code=500, detail = f"An error {err} occurred.")
    except Exception as e:
        print(e)
        raise HTTPException(status_code=400,detail = "Invalid")

@app.put('/update')
async def update(request : Request):
    try:
        global conn
        payload = await request.json()
        
        shard_id = payload['shard']
        stud_id = payload['Stud_id']
        
        updated_data = payload['data']
        stud_marks = updated_data['Stud_marks']
        stud_name = updated_data['Stud_name']

        # Create MySQL cursor
        cursor = conn.cursor()

        if payload["primary_server"] == 1:
            secondary_servers = payload["secondary_servers"]
            votes = 0
            
            id = int(logger[shard_id].get_last_log_id()) + 1
            log = Log(id, LogType(1), {"shard":shard_id,"Stud_id":stud_id,"data":updated_data}, datetime.now())
            logger[shard_id].add_log(log)
            for server in secondary_servers:
                while True:
                    try:
                        response = requests.put(f"http://{server}:5002/update",json={
                            "shard":shard_id,
                            "Stud_id":stud_id,
                            "data": updated_data,
                            "primary_server":0,
                            "commit" : commit_log[shard_id]
                        },timeout=200)
                        if response.status_code == 200:

                            response = response.json()
                            if response.get('messsage',0)== "Not Uptodate":
                                curr_commit_index = response.get('last_commit',0)
                                list_to_update = logger[shard_id].get_requests_from_given_index(shard_id, int(curr_commit_index) + 1)
                                print(list_to_update)
                                resp = requests.post(
                                    f"http://{server}:5002/update_local_data",
                                    json={"shards": shard_id,"data":list_to_update,"servers":server},
                                )
                                resp = resp.json()
                                print(resp)
                                break
                            
                            votes += 1
                            break
                    except Exception as e:
                        print("retrying...")
            if votes >= len(secondary_servers) // 2:
                commit_log[shard_id] = logger[shard_id].get_last_log_id()
                
                cursor.execute(
                    f"UPDATE {shard_id} SET Stud_name = ? , Stud_marks = ? WHERE Stud_id = ?",
                    (stud_name, stud_marks, stud_id),
                )
                conn.commit()
        else:
            commit_index = payload["commit"]
            curr_commit_index = logger[shard_id].get_last_log_id()
            if commit_index != curr_commit_index:
                return {"message": "Not Uptodate", "last_commit": curr_commit_index}
            
            id = int(logger[shard_id].get_last_log_id()) + 1
            log = Log(id, LogType(1), {"shard":shard_id,"Stud_id":stud_id,"data":updated_data}, datetime.now())
            logger[shard_id].add_log(log)
            
            cursor.execute(
                 f"UPDATE {shard_id} SET Stud_name = ? , Stud_marks = ? WHERE Stud_id = ?",
                    (stud_name, stud_marks, stud_id),
            )
            commit_log[shard_id] = logger[shard_id].get_last_log_id()
            conn.commit()

        # Close cursor
        cursor.close()
        response = {
            "message": f"Data entry for Stud_id:{stud_id} updated",
            "status": "success"        }
        return response

    except sqlite3.Error as err:
        conn.rollback()
        raise HTTPException(status_code=500, detail = f"An error {err} occurred.")
    except:
        raise HTTPException(status_code=400,detail = "Invalid")

@app.delete('/del')
async def delete(request : Request):
    try:
        payload = await request.json()
        shard_id = payload['shard']
        stud_id = payload['Stud_id']

        # Remove the data entry 
        # Create MySQL cursor
        global conn
        cursor = conn.cursor()

        if payload["primary_server"] == 1:
            print("I am primary")
            secondary_servers = payload["secondary_servers"]
            votes = 0
            
            id = int(logger[shard_id].get_last_log_id()) + 1
            log = Log(id, LogType(2), {"Stud_id":stud_id}, datetime.now())
            logger[shard_id].add_log(log)
            for server in secondary_servers:
                while True:
                    try:
                        print("Sending sec ser req",flush=True)
                        response = requests.delete(f"http://{server}:5002/del",json={
                            "shard":shard_id,
                            "Stud_id":stud_id,
                            "primary_server":0,
                            "commit" : commit_log[shard_id]
                        },timeout=200)
                        if response.status_code == 200:
                            response = response.json()
                            if response.get('messsage',0) == "Not Uptodate":
                                curr_commit_index = response.get('last_commit',0)
                                list_to_update = logger[shard_id].get_requests_from_given_index(shard_id, int(curr_commit_index) + 1)
                                print(list_to_update)
                                resp = requests.post(
                                    f"http://{server}:5002/update_local_data",
                                    json={"shards": shard_id,"data":list_to_update,"servers":server},
                                )
                                resp = resp.json()
                                print(resp)
                                break
                            
                            votes += 1
                            break
                    except Exception as e:
                        print("retrying...")
            if votes >= len(secondary_servers) // 2:
                commit_log[shard_id] = logger[shard_id].get_last_log_id()
                
                cursor.execute(
                    f"DELETE FROM {shard_id} WHERE Stud_id = ?",
                    ( stud_id,),
                )
                conn.commit()
        else:
            print("I am sec")
            commit_index = payload["commit"]
            curr_commit_index = logger[shard_id].get_last_log_id()
            if commit_index != curr_commit_index:
                return {"message": "Not Uptodate", "last_commit": curr_commit_index}
            
            id = int(logger[shard_id].get_last_log_id()) + 1
            log = Log(id, LogType(1), {"Stud_id":stud_id}, datetime.now())
            logger[shard_id].add_log(log)
            
            cursor.execute(
                 f"DELETE FROM {shard_id} WHERE Stud_id = ?",
                    ( stud_id,),
            )
            commit_log[shard_id] = logger[shard_id].get_last_log_id()
            conn.commit()

        # Close cursor
        cursor.close()
        response = {
            "message": f"Data entry with Stud_id:{stud_id} removed",
            "status": "success"
        }
        return response

    except sqlite3.Error as err:
        conn.rollback()
        raise HTTPException(status_code=500, detail = f"An error {err} occurred.")
    except Exception as e:
        print(e,flush= True)
        raise HTTPException(status_code=400,detail = "Invalid")
if __name__ == '__main__':
    print("started server")
    uvicorn.run(app,host='0.0.0.0', port=5002)
