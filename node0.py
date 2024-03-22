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
nodes = ["localhost:50050" , "localhost:50051"] 

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
            self.ackedLength = {node_index: 0 for node_index in range(len(nodes))}
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
        self.reset_election_timeout()
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
                print("Votes received : " , len(self.votesReceived))
                print(((len(nodes)//2) + 1))
                
                self.current_role = 'LEADER'
                # or we can make it nodes[self.nodeID] if we want IP
                self.current_leader = self.nodeID
                print("Node " + str(self.nodeID) + " is now the leader.")
                self.log.append(raft_pb2.LogEntry(term=self.current_term, command='NO-OP'))
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
                for i in range(len(nodes)):
                    if i != self.nodeID:
                        self.replicate_log(i)
                self.commit_log_entries()

    #4
    def broadcast_message(self, message):
        with state_lock:
            if self.current_role == 'LEADER':
                # Append the new message to the leader's log
                self.log.append(raft_pb2.LogEntry(term=self.current_term, command=message))
                self.ackedLength[self.nodeID] = len(self.log)  # Leader commits the new entry
                print(f"Appended message to leader's log: {message}")
                # Replicate the log entry to followers
                for follower_id in range(len(nodes)):
                    if follower_id != self.nodeID:
                        utils.run_thread(fn=self.replicate_log, args=(follower_id,))
            else:
                # Forward the message to the leader
                print(f"I am not the leader. Forwarding message to the leader: {self.current_leader}")
                # This forwarding logic depends on your system design; it might involve sending the message to the leader via RPC or another method.

                
    #5
    def replicate_log(self, follower_id):
        with state_lock:
            node_address = nodes[follower_id]
            prefixLen = self.sentLength.get(follower_id, 0)
            suffix = self.log[prefixLen:]  # Assuming self.log is already a list of LogEntry

            if prefixLen > 0:
                prefixTerm = self.log[prefixLen - 1].term
            else:
                prefixTerm = 0  # Default term for initial log entry

            # Create AppendEntriesRequest
            request = raft_pb2.AppendEntriesRequest(
                term=self.current_term,
                leaderId=str(self.nodeID),
                prevLogIndex=prefixLen - 1,
                prevLogTerm=prefixTerm,
                entries=suffix,
                leaderCommit=self.commit_length,
                leaseInterval=5  # Example lease interval
            )

            # Make the RPC call
            try:
                channel = grpc.insecure_channel(node_address)
                stub = raft_pb2_grpc.RaftStub(channel)
                response = stub.AppendEntries(request, timeout=10.0)  # Setting a timeout

                if response.success:
                    self.sentLength[follower_id] = prefixLen + len(suffix)
                    self.ackedLength[follower_id] = self.sentLength[follower_id]
                    print(f"Successfully replicated log to node {follower_id}")
                else:
                    self.sentLength[follower_id] = max(0, prefixLen - 1)
                    print(f"Failed to replicate log to node {follower_id}. Retrying...")
            except grpc.RpcError as e:
                print(f"RPC to node {follower_id} failed: {e}")     
    
    #6
    def AppendEntries(self, request, context):
        with state_lock:
            # If RPC request term is greater than node's current term
            if request.term > self.current_term:
                self.current_term = request.term
                self.voted_for = None
                self.current_role = 'FOLLOWER'
                self.current_leader = request.leaderId
                self.reset_election_timeout()

            # If terms are equal, ensure node is a follower and update leader
            if request.term == self.current_term:
                if self.current_role != 'LEADER':
                    self.current_role = 'FOLLOWER'
                    self.current_leader = request.leaderId

            # Prepare for log consistency check
            if request.prevLogIndex > 0 and request.prevLogIndex <= len(self.log):
                log_ok = self.log[request.prevLogIndex - 1].term == request.prevLogTerm
            else:
                log_ok = request.prevLogIndex == 0  # True if prevLogIndex is 0, indicating a heartbeat or initial log entry

            # If logs are consistent, append new entries and update commit index
            if log_ok:
                if request.entries:  # Only proceed if there are new entries
                    # Determine the starting index for new entries
                    start_index = request.prevLogIndex
                    # Replace the old entries with new ones from start_index onwards
                    self.log[start_index:] = request.entries
                    
                # Update ackedLength for self to the new length of the log
                self.ackedLength[self.nodeID] = len(self.log)

                # Update commit index if leaderCommit > commit_length
                if request.leaderCommit > self.commit_length:
                    new_commit_index = min(request.leaderCommit, len(self.log))
                    for i in range(self.commit_length, new_commit_index):
                        # Apply the log[i].command to the state machine here
                        print(f"Applying to state machine: {self.log[i].command}")
                    self.commit_length = new_commit_index

                return raft_pb2.AppendEntriesResponse(term=self.current_term, success=True)
            else:
                # If log consistency check fails, respond with failure
                return raft_pb2.AppendEntriesResponse(term=self.current_term, success=False)


    def commit_log_entries(self):
        with state_lock:
            # Check if there are any new entries we can commit
            for i in range(self.commit_length + 1, len(self.log) + 1):
                if self.acks(i) > (len(nodes) // 2):
                    # Apply log[i-1].command to the state machine (e.g., execute the command)
                    print(f"Committed log index {i-1} with term {self.log[i-1].term}")
                    self.commit_length = i
    def acks(self, length):
        with state_lock:
            # Ensure we're accessing existing keys in ackedLength.
            return sum(1 for ack in self.ackedLength.values() if ack >= length)

                        
    
    def reset_election_timeout(self):
        with state_lock:
            self.election_timeout = time.time() + random.uniform(5, 10)
            print("Election timeout reset to:", self.election_timeout)
    

    
  
#9
  
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
    raft_pb2_grpc.add_RaftServicer_to_server(RaftServicer(0), server)
    server.add_insecure_port('[::]:50050')#
    server.start()#
    print("Server started at [::]:50050")#
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()