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
nodes = ["localhost:50050" , "localhost:50051", "localhost:50052", "localhost:50053"] 


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
            self.sentLength  = dict()
            self.ackedLength = dict()
            self.election_timeout = random.uniform(5, 10)
            self.directory = "logs_node_{self.nodeID}"
            
            # this should also get reset right ?
            self.leader_lease_timeout = None

        
        else:
            # preserve these 4 for disk crashes.
            self.current_term = 0
            self.voted_for = None
            self.log = [] # Contains objects of type raftpb2.logentry
            self.commit_length = 0
            self.current_role = "FOLLOWER"  # can be follower, candidate, or leader
            self.current_leader = None
            self.votesReceived = set()
            self.sentLength  = dict()
            self.ackedLength = dict()
            self.leader_lease_timeout = None
            self.election_timeout = -1
            self.directory = "logs_node_{self.nodeID}"
            setup_directories(self.nodeID)
    
        self.init()
    def init(self):
        self.set_election_timeout()
        utils.run_thread(fn=self.on_election_timeout, args=())
        utils.run_thread(fn=self.leader_append_entries, args=())
        

    def set_election_timeout(self, timeout=None):
        # Reset this whenever previous timeout expires and starts a new election
        if timeout:
            self.election_timeout = timeout
        else:
            self.election_timeout = time.time() + randint(0,10)
        print("time out set to:" + str(self.election_timeout))

    # 1
    def on_election_timeout(self):
        while True:            
            # print(time.time())
            # print(self.current_role)
            time.sleep(2)
            if time.time() > self.election_timeout and \
                    (self.current_role == 'FOLLOWER' or self.current_role == 'CANDIDATE'):

                # print("Timeout....")
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
        if len(nodes) == 1:
            self.current_role = 'LEADER'
            self.current_leader = self.nodeID
            print(f"Node {self.nodeID} is now the leader in a single-node cluster.")
            return
        print("num of nodes : " + str(len(nodes)))
        for i in range(len(nodes)):
            if i != self.nodeID:
                t = utils.run_thread(fn=self.send_vote_req, args = (nodes[i], i))
                threads += [t]
        
        # wait for all threads to complete, i.e. get vote response from all nodes.
        for t in threads:
            t.join()

        return True
    
    def send_vote_req(self, node_address, nodeID):
        try:
            channel = grpc.insecure_channel(node_address)
            stub = raft_pb2_grpc.RaftStub(channel)
            response = stub.RequestVote(raft_pb2.RequestVoteRequest(
                term=self.current_term,
                candidateId=str(self.nodeID),
                lastLogIndex=0,  # Your current logic
                lastLogTerm=0  # Your current logic
            ), timeout=1)
            print('Vote response was:', response.voteGranted)
            self.collecting_votes(response, nodeID)
        except grpc.RpcError as e:
            # Logging the error with more details
            print(f"Unable to send RequestVote to node {node_address}. Error: {e.details()}")




    
    # 2
    def RequestVote(self, request, context):
        print(f"Received RequestVote RPC from candidate" + str(request.candidateId))
        # Perform voting logic here
        print(self.current_term)
        print(request.term)
        if request.term > self.current_term:
            print('Request from a candidate, set self state to follower')
            self.current_term = request.term
            self.current_role = 'FOLLOWER'
            self.voted_for = None
            self.set_election_timeout(time.time() + 10)
        
        lastTerm = 0
        if len(self.log) > 0:
            lastTerm = text_readers.last_term(self.nodeID)
        
        logOk = (request.lastLogTerm > lastTerm) or (request.lastLogTerm == lastTerm and request.lastLogIndex >= len(self.log))
        # change here lease duration.
        print(request.term == self.current_term)
        print(logOk)
        print(self.voted_for)
        if request.term == self.current_term and logOk and self.voted_for in (request.candidateId, None):
            print('vote Granted')
            self.voted_for = request.candidateId
            return raft_pb2.RequestVoteResponse(term=self.current_term, voteGranted=True, leaseDuration=5)
        else:
            print('vote not granted')
            return raft_pb2.RequestVoteResponse(term=self.current_term, voteGranted=False, leaseDuration=5)

    # 3
    def collecting_votes(self, response, node_address):
        # Collecting votes
        if self.current_role == 'CANDIDATE' and response.term == self.current_term and response.voteGranted:
            print("collecting votes")
            self.votesReceived.add(node_address)
            if len(self.votesReceived) >= ((len(nodes)//2) + 1):
                self.current_role = 'LEADER'
                # or we can make it nodes[self.nodeID] if we want IP
                self.current_leader = self.nodeID
                self.log.append('NO-OP')
                self.set_election_timeout(time.time() + 10)

        
        elif self.current_term < response.term:
            print('stepping down')
            self.current_term = response.term
            self.current_role = 'FOLLOWER'
            self.voted_for = None
            self.set_election_timeout(time.time() + 10)

    # function to send append_enteries and heartbeats 
    def leader_append_entries(self):
        while True:
            time.sleep(1)
            if self.current_role == 'LEADER':
                print("sending entries from leader")
                threads = []
                for i in range(len(nodes)):
                    if i != self.nodeID:
                        # check these
                        self.sentLength[i] = len(self.log)
                        self.ackedLength[i] = 0
                        t = utils.run_thread(fn=self.send_entry_req, args = (nodes[i], i))
                        threads += [t]

                for t in threads:
                    t.join()
                self.CommitLogEntries()

    #4 --> not written yet
    #5 REPLICATE LOG FOR EACH NODE
    def send_entry_req(self, node_address, follower_id):
        prefixLen = self.sentLength[follower_id]  # Ensure this accesses the correct value
        suffix = self.log[prefixLen:]  # More Pythonic way to get the suffix

        prefixTerm = 0
        if prefixLen > 0:
            prefixTerm = self.log[prefixLen - 1].term

        try:
            channel = grpc.insecure_channel(node_address)
            stub = raft_pb2_grpc.RaftStub(channel)
            response = stub.AppendEntries(raft_pb2.AppendEntriesRequest(
                term=self.current_term,
                leaderId=str(self.nodeID),  # Ensure correct value is passed
                prevLogIndex=prefixLen - 1,  # Adjusted for zero-based index
                prevLogTerm=prefixTerm,
                entries=suffix,  # Adjusted as per the new suffix calculation
                leaderCommit=self.commit_length,  # Your logic
                leaseInterval=7  # Your logic
            ))
            print(f"AppendEntries response from node {node_address}: {response.success}")
            self.receive_log_acknowledgment(response, follower_id)
        except grpc.RpcError as e:
            # Logging the error with more details
            print(f"Unable to send AppendEntries to node {node_address}. Error: {e.details()}")
  
#9
    def acks(self,length):
        temp = 0
        for i in range(len(nodes)):
            if self.ackedLength[i] >= length:
                temp += 1
        return temp
    def CommitLogEntries(self):
        minAcks = (len(nodes) + 1) // 2
        ready = []
        for i in range(1, len(self.log) + 1):
            if self.acks(i) >= minAcks:
                ready.append(i)
        if ready!=[] and max(ready)>self.commit_length and self.log[max(ready)-1].term == self.current_term:
            #write changes into state machine here / ie to the DB
            self.commit_length = max(ready)

    


        



    

    

    # def ServeClient(self, request, context):
    #     try:
    #         global state, log, commit_length
            
    #         # Correctly access the 'request' field from ServeClientArgs message
    #         command = request.request.split()  # Notice the lowercase 'r' in 'request.request'
            
    #         if state != "leader":
    #             return raft_pb2.ServeClientReply(data="", leaderId="leader_id_if_known", success=False)

    #         if command[0] == "SET":
    #             _, key, value = command
    #             log.append({"term": current_term, "command": f"SET {key} {value}"})
    #             commit_length += 1  # This is simplified; real commit logic is needed
    #             return raft_pb2.ServeClientReply(data="", leaderId="", success=True)
    #         elif command[0] == "GET":
    #             key = command[1]
    #             value = next((entry["command"].split()[2] for entry in log if entry["command"].startswith(f"SET {key}")), "")
    #             return raft_pb2.ServeClientReply(data=value, leaderId="", success=True)
    #         else:
    #             return raft_pb2.ServeClientReply(data="Invalid command", leaderId="", success=False)
    #     except Exception as e:
    #         print(f"Exception in ServeClient: {e}")
    #         return raft_pb2.ServeClientReply(data="An error occurred", leaderId="", success=False)


def reset_election_timeout():
    global election_timeout
    election_timeout = random.uniform(5, 10)
    # Logic to reset election timeout

def is_candidate_log_up_to_date(candidate_last_log_index, candidate_last_log_term):
    # Logic to determine if the candidate's log is at least as up-to-date as the receiver's log
    return True # Placeholder, replace with actual logic

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_RaftServicer_to_server(RaftServicer(2), server)
    server.add_insecure_port('[::]:50052')#
    server.start()#
    print("Server started at [::]:50050")#
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()