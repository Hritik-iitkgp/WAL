# start:
# 	# sudo docker-compose up -d metadb
# 	sudo docker-compose up lb 

# build:
# 	sudo docker-compose build

# clean:
# 	make clean_servers
# 	-docker-compose down

# clean_servers:
# 	-docker rm -f $$(docker ps -aqf "ancestor=server")

# remove_images:
# 	-docker rmi -f server lb 
	

start:
	sudo docker-compose up -d metadb
	sudo docker-compose up lb shm  # Include shard_manager service

build:
	sudo docker-compose build

clean:
	make clean_servers
	-docker-compose down

clean_servers:
	-docker rm -f $$(docker ps -aqf "ancestor=server")

remove_images:
	-docker rmi -f server lb shm  # Include shard_manager image
