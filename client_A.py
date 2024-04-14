# import requests
# import time
# import random

# # Replace the following variables with your actual server information
# SERVER_URL = "http://localhost:5000"
# WRITE_DATA = [{"Stud_id": i, "Stud_name": f"Student_{i}", "Stud_marks": random.randint(0, 30)} for i in range(100)]


# print("OUTPUT FOR B-1:- ")
# print()

# # Function to perform 100 write requests
# def test_write_requests():
#     start_time = time.time()
    
#     for data in WRITE_DATA:
#         response = requests.post(f"{SERVER_URL}/write", json={"data": [data]})
#         if response.status_code != 200:
#             print(f"Error occurred while writing data: {response.json()}")
    
#     end_time = time.time()
#     print(f"Time taken for 100 write requests: {end_time - start_time} seconds")

# # Function to perform 100 read requests
# def test_read_requests():
#     start_time = time.time()
    
#     for i in range(100):
#         READ_STUD_ID_LOW = random.randint(0, 90)
#         READ_STUD_ID_HIGH = READ_STUD_ID_LOW + random.randint(0, 10)
        
#         response = requests.post(f"{SERVER_URL}/read", json={"Stud_id": {"low": READ_STUD_ID_LOW, "high": READ_STUD_ID_HIGH}})
#         if response.status_code != 200:
#             print(f"Error occurred while reading data: {response.json()}")
    
#     end_time = time.time()
#     print(f"Time taken for 100 read requests: {end_time - start_time} seconds")
    

# if __name__ == "__main__":
#     print("Testing write requests...")
#     test_write_requests()
#     print("\nTesting read requests...")
#     test_read_requests()

import aiohttp
import asyncio
import random
import time

# Replace the following variables with your actual server information
SERVER_URL = "http://localhost:5000"
WRITE_DATA = [{"Stud_id": i, "Stud_name": f"Student_{i}", "Stud_marks": random.randint(0, 30)} for i in range(10000)]

print("OUTPUT FOR A-3:- ")
print()

async def write_request(session, data):
    async with session.post(f"{SERVER_URL}/write", json={"data": [data]}) as response:
        if response.status != 200:
            print(f"Error occurred while writing data: {await response.json()}")


async def read_request(session):
    READ_STUD_ID_LOW = random.randint(0, 9000)
    READ_STUD_ID_HIGH = READ_STUD_ID_LOW + random.randint(0, 1000)

    async with session.post(f"{SERVER_URL}/read", json={"Stud_id": {"low": READ_STUD_ID_LOW, "high": READ_STUD_ID_HIGH}}) as response:
        if response.status != 200:
            print(f"Error occurred while reading data: {await response.json()}")

async def test_write_requests():
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60*60)) as session:
        await asyncio.wait_for(
            asyncio.gather(*[write_request(session, data) for data in WRITE_DATA]), 
            timeout=60*60
        )


async def test_read_requests():
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60*60)) as session:
        await asyncio.wait_for(
            asyncio.gather(*[read_request(session) for _ in range(10000)]), 
            timeout=60*60
        )


if __name__ == "__main__":
    print("Testing write requests...")
    start_time = time.time()
    asyncio.run(test_write_requests())
    end_time = time.time()
    print(f"Time taken for 10000 write requests: {end_time - start_time} seconds")

    print("\nTesting read requests...")
    start_time = time.time()
    asyncio.run(test_read_requests())
    end_time = time.time()
    print(f"Time taken for 10000 read requests: {end_time - start_time} seconds")
