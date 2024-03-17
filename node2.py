import threading
import time
import random
from concurrent import futures
import grpc
import raft_pb2
import raft_pb2_grpc
from temp import setup_directories
import text_readers
import os
from random import randint
import utils
# Global variable for stpring IP of all nodes.
nodes = ['localhost:50051', 'localhost:50052']


# Lock for state changes
state_lock = threading.Lock()

class RaftServicer(raft_pb2_grpc.RaftServicer):
    def __init__(self, nodeID):

        # Create a directory on initialisation
        self.nodeID = nodeID

        # recover values from metadata file after crash
        if os.path.exists('logs_node_{self.nodeID}'):
            values = text_readers.read_metadata_file(self.nodeID)
            self.current_term, self.voted_for , self.commit_length, self.current_leader = values[0], values[1], values[2], values[3]
            
            # these values are reset.
            self.current_role = 'FOLLOWER'
            self.current_leader = None
            self.votesReceived = set()
            self.sentLength  = []
            self.ackedLength = [] 
            self.election_timeout = random.uniform(5, 10)
            self.directory = "logs_node_{self.nodeID}"
            
            # this should also get reset right ?
            self.leader_lease_timeout = None

        
        else:
            # preserve these 4 for disk crashes.
            self.current_term = 0
            self.voted_for = None
            self.log = [] # Contains pairs of form <msg, term> 
            self.commit_length = 0
            self.current_role = "FOLLOWER"  # can be follower, candidate, or leader
            self.current_leader = None
            self.votesReceived = set()
            self.sentLength  = []
            self.ackedLength = [] 
            self.leader_lease_timeout = None
            self.election_timeout = -1
            self.directory = "logs_node_{self.nodeID}"
            setup_directories(self.nodeID)
            self.init()
        

    def init(self):
        self.set_election_timeout()
        utils.run_thread(fn=self.on_election_timeout, args=())
        

    def set_election_timeout(self, timeout=None):
        # Reset this whenever previous timeout expires and starts a new election
        if timeout:
            self.election_timeout = timeout
        else:
            self.election_timeout = time.time() + randint(5000,10000)/1000.0
        print("time out set to:" + str(self.election_timeout))

    def on_election_timeout(self):
        while True:
            # Check everytime that state is either FOLLOWER or CANDIDATE before sending
            # vote requests.

            # The possibilities in this path are:
            # 1. Requestor sends requests, receives replies and becomes leader
            # 2. Requestor sends requests, receives replies and becomes follower again, repeat on election timeout
            
            print(time.time())
            print(self.current_role)
            time.sleep(2)
            if time.time() > self.election_timeout and \
                    (self.current_role == 'FOLLOWER' or self.current_role == 'CANDIDATE'):

                print("Timeout....")
                self.set_election_timeout()
                self.start_election()  

    def start_election(self):
        print("Starting election...")

        # At the start of election, set state to CANDIDATE and increment term
        # also vote for self.
        self.current_role = 'CANDIDATE'
        self.voted_for = self.nodeID
        self.current_term += 1
        self.votesReceived.add(self.nodeID)
        self.last_term = 0

        if len(self.log) > 0:
            self.last_term = text_readers.last_term(self.nodeID)

        # request votes in parallell
        threads = []
        for i in range(len(nodes)):
            if i != self.nodeID:
                t = utils.run_thread(fn=self.send_vote_req, args = (nodes[i],))
                threads += [t]
        
        for t in threads:
            t.join()
        return True
    
    def send_vote_req(self, node_address):
        
        channel = grpc.insecure_channel(node_address)
        stub = raft_pb2_grpc.RaftStub(channel)

        try:
                response = stub.RequestVote(raft_pb2.RequestVoteRequest(
                term=self.current_term,
                candidateId=str(self.nodeID),
                lastLogIndex =len(self.log),
                lastLogTerm=text_readers.last_term(self.nodeID)
            ))
                
        except grpc.RpcError as e:
                print(f"Error occurred while sending RequestVote RPC to node {node_address}: {e}")

        print('vote responses were:' + str(response))
        

 
    def RequestVote(self, request, context):
        print(f"Received RequestVote RPC from candidate")
        # Perform voting logic here
        return raft_pb2.RequestVoteResponse(term=10, voteGranted=True, leaseDuration=5)     


    


    def AppendEntries(self, request, context):
        global current_term, voted_for, commit_length, log, state
        with state_lock:
            if request.term < current_term:
                return raft_pb2.AppendEntriesResponse(success=False, term=current_term)
            
            # Reset election timeout
            reset_election_timeout()

            # More logic here for appending entries and updating state
            # ...

            return raft_pb2.AppendEntriesResponse(success=True, term=current_term)
    

    

    def ServeClient(self, request, context):
        try:
            global state, log, commit_length
            
            # Correctly access the 'request' field from ServeClientArgs message
            command = request.request.split()  # Notice the lowercase 'r' in 'request.request'
            
            if state != "leader":
                return raft_pb2.ServeClientReply(data="", leaderId="leader_id_if_known", success=False)

            if command[0] == "SET":
                _, key, value = command
                log.append({"term": current_term, "command": f"SET {key} {value}"})
                commit_length += 1  # This is simplified; real commit logic is needed
                return raft_pb2.ServeClientReply(data="", leaderId="", success=True)
            elif command[0] == "GET":
                key = command[1]
                value = next((entry["command"].split()[2] for entry in log if entry["command"].startswith(f"SET {key}")), "")
                return raft_pb2.ServeClientReply(data=value, leaderId="", success=True)
            else:
                return raft_pb2.ServeClientReply(data="Invalid command", leaderId="", success=False)
        except Exception as e:
            print(f"Exception in ServeClient: {e}")
            return raft_pb2.ServeClientReply(data="An error occurred", leaderId="", success=False)


def reset_election_timeout():
    global election_timeout
    election_timeout = random.uniform(5, 10)
    # Logic to reset election timeout

def is_candidate_log_up_to_date(candidate_last_log_index, candidate_last_log_term):
    # Logic to determine if the candidate's log is at least as up-to-date as the receiver's log
    return True # Placeholder, replace with actual logic

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_RaftServicer_to_server(RaftServicer(1), server)
    server.add_insecure_port('[::]:50052')
    server.start()
    print("Server started at [::]:50052")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()