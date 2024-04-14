from flask import Flask, jsonify, request
import os
import sqlite3
import time
app = Flask(__name__)

# # Function to create MySQL connection
# def create_mysql_connection():
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


@app.route('/config', methods=['POST'])
def config():
    try:
        payload = request.get_json()

        # Extract schema and shards information from the payload
        schema = payload['schema']
        shards = payload['shards']

        # Initialize shards
        # Create MySQL connection
        global conn
        cursor = conn.cursor()

        # Create tables for each shard
        for shard in shards:
            table_name = f"StudT_{shard}"
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
            for column, dtype in zip(schema['columns'], schema['dtypes']):
                create_table_query += f"{column} {'INT' if dtype=='Number' else 'VARCHAR(100)'}, "
            create_table_query = create_table_query.rstrip('') + f" PRIMARY KEY (Stud_id) )"
            cursor.execute(create_table_query)
            conn.commit()

        # Close cursor and connection
        cursor.close()
        response = {
            "message": f"Server:{', '.join(shards)} configured",
            "status": "success"
        }
        return jsonify(response), 200

    except Exception as e:
        response = {
            "message": str(e),
            "status": "error"
        }
        return jsonify(response), 500

@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    response={
        "message": ""
    }
    return jsonify(response), 200

@app.route('/copy', methods=['GET'])
def copy():
    try:
        payload = request.get_json()
        shards_to_copy = payload['shards']
        # 
        global conn
        cursor = conn.cursor()
        response = {
            # need to add ,
            "status": "success"
        }
        for shard in shards_to_copy:
            table_name = f"StudT_{shard}"
            query = f"SELECT * FROM {table_name}"
            cursor.execute(query)
            shard_data = cursor.fetchall()
            response[shard] = [{"Stud_id":stud[0],"Stud_name":stud[1],"Stud_marks":stud[2]} for stud in shard_data]

        # Close cursor
        cursor.close()

        # Add status to response data
        return jsonify(response), 200

    except Exception as e:
        response = {
            "message": str(e),
            "status": "error"
        }
        return jsonify(response), 500

@app.route('/read', methods=['POST'])
def read():
    try:
        payload = request.get_json()
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
        table_name = f"StudT_{shard_id}" 
        query = f"SELECT * FROM {table_name} WHERE {keys[1]} >= {low} AND {keys[1]} <= {high}"
        cursor.execute(query)
        data = cursor.fetchall()

        # Close cursor
        cursor.close()
        response = {
            "data": data,
            "status": "success"
        }
        return jsonify(response), 200

    except Exception as e:
        response = {
            "message": str(e),
            "status": "error"
        }
        return jsonify(response), 500

@app.route('/write', methods=['POST'])
def write():
    try:
        payload = request.get_json()
        print(payload)
        shard_id = payload['shard']
        curr_idx = payload['curr_idx']
        data_entries = payload['data']

        # wrtite 
        cursor = conn.cursor()
        columns = list(data_entries[0].keys())
        column_names = ", ".join(columns)
        placeholders = ", ".join(["?" for _ in columns])
        # Insert data into the shard table
        table_name = f"StudT_{shard_id}"
        insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
        insert_values = [[entry[column] for column in columns] for entry in data_entries]
        cursor.executemany(insert_query, insert_values)

        # Commit changes
        conn.commit()        
        new_idx = curr_idx + len(data_entries)

        response = {
            "message": "Data entries added",
            "current_idx": new_idx,
            "status": "success"
        }
        return jsonify(response), 200

    except Exception as e:
        response = {
            "message": str(e),
            "status": "error"
        }
        return jsonify(response), 500

@app.route('/update', methods=['PUT'])
def update():
    try:
        global conn
        payload = request.get_json()
        keys = list(payload.keys())
        shard_id = payload['shard']
        stud_id = payload[keys[1]]
        updated_data = payload['data']

        # Create MySQL cursor
        cursor = conn.cursor()

        # Update data entry in the shard table
        table_name = f"StudT_{shard_id}"
        update_query = f"UPDATE {table_name} SET "
        update_query += ", ".join([f"{column}=?" for column in updated_data.keys()])
        update_query += f" WHERE {keys[1]} = ?"
        update_values = list(updated_data.values()) + [stud_id]
        cursor.execute(update_query, update_values)

        # Commit changes
        conn.commit()

        # Close cursor
        cursor.close()
        response = {
            "message": f"Data entry for Stud_id:{stud_id} updated",
            "status": "success"        }
        return jsonify(response), 200

    except Exception as e:
        response = {
            "message": str(e),
            "status": "error"
        }
        return jsonify(response), 500

@app.route('/del', methods=['DELETE'])
def delete():
    try:
        payload = request.get_json()
        shard_id = payload['shard']
        stud_id = payload['Stud_id']

        # Remove the data entry 
        # Create MySQL cursor
        global conn
        cursor = conn.cursor()

        # Delete data entry from the shard table
        table_name = f"StudT_{shard_id}"
        delete_query = f"DELETE FROM {table_name} WHERE Stud_id = ?"
        cursor.execute(delete_query, (stud_id,))

        # Commit changes
        conn.commit()

        # Close cursor
        cursor.close()
        response = {
            "message": f"Data entry with Stud_id:{stud_id} removed",
            "status": "success"
        }
        return jsonify(response), 200

    except Exception as e:
        response = {
            "message": str(e),
            "status": "error"
        }
        return jsonify(response), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
