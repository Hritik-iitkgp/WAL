class ConsistentHashMap:
    def __init__(self, M, N, K):
        self.M = M  # Total number of slots
        self.N = N  # Number of server containers
        self.K = K  # Number of virtual servers for each server container
        self.server_containers = M*[None]  # Array to represent server containers
    
    def custom_hash(self,string):
        return hash(string)

    def hash_request(self, Rid):
        #return (Rid ** 2 + 2 * Rid + 17) % self.M
        return (Rid ** 2 + 3 * Rid + 64) % self.M
        

    def hash_server(self, Sid, j):
        #return (Sid ** 2 + j ** 2 + 2 * j + 25) % self.M
        return (Sid ** 2 + j ** 2 + 2 * j*Sid + 75) % self.M

    def add_server_instance(self,rep_name):
        self.N+=1
        Sid= self.custom_hash(rep_name)
        slot = self.hash_server(Sid,0)
        while self.server_containers[slot]!= None:
            slot+=1
            slot%=self.M
        self.server_containers[slot]=rep_name
        self.add_virtual_servers(Sid,rep_name)


    def remove_server_instance(self,rep_name):
        self.N=self.N-1
        for i in range(self.M):
            if self.server_containers[i]==rep_name:
                self.server_containers[i]=None
    
    def map_request_to_server(self, Rid):
        slot = self.hash_request(Rid)
        slot=slot+1
        while self.server_containers[slot]== None:
            slot=slot+1
            slot%=self.M
        
        container = self.server_containers[slot]
        
        return container

    def handle_server_failure(self, failed_server):
        # Shift requests to the next server in a clockwise direction
        failed_container_index = self.hash_server(failed_server) % self.N
        next_container_index = (failed_container_index + 1) % self.N
        self.server_containers[next_container_index].update(self.server_containers[failed_container_index])
        self.server_containers[failed_container_index] = {}

    def add_virtual_servers(self, Sid,rep_name):
        for j in range(self.K):
            #virtual_server_id = Sid * self.K + j
            slot = self.hash_server(Sid,j)
            while self.server_containers[slot]!= None:
                slot+=1
                slot%=self.M
            self.server_containers[slot]=rep_name

             
        


