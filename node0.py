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
nodes = ["localhost:50050","localhost:50051"]#, "localhost:50052", "localhost:50053", "localhost:50054"]


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
            self.election_timeout = time.time() + randint(5000,10000)/1000.0
        print("time out set to:" + str(self.election_timeout))

    # 1
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
        
        # wait for all threads to complete, i.e. get vote response from all nodes.
        for t in threads:
            t.join()

        return True
    
    def send_vote_req(self, node_address):
        
        channel = grpc.insecure_channel(node_address)
        stub = raft_pb2_grpc.RaftStub(channel)
        lastLogIndex = len(self.log)
        lastLogTerm = 0
        # code for updating it if log exitsts
        try:
                response = stub.RequestVote(raft_pb2.RequestVoteRequest(
                term=self.current_term,
                candidateId=str(self.nodeID),
                lastLogIndex = lastLogIndex,
                lastLogTerm=lastLogTerm
            ))
                # print('vote responses were:' + str(response))
                print('vote responses were:' + str(response.voteGranted))
                print('vote responses were:' + str(response.term))
                print('vote responses were:' + str(response.leaseDuration))
                self.collecting_votes(response, node_address)

        except grpc.RpcError as e:
                print(f"Error occurred while sending RequestVote RPC to node {node_address}: {e}")

        
        


    
    # 2
    def RequestVote(self, request, context):
        print(f"Received RequestVote RPC from candidate" + str(request.candidateId))
        # return raft_pb2.RequestVoteResponse(term=self.current_term, voteGranted=False, leaseDuration=5)
        # Perform voting logic here
        print(request.term, request.candidateId, request.lastLogIndex, request.lastLogTerm)
        try:
            if request.term > self.current_term:
                self.current_term = request.term
                self.current_role = 'FOLLOWER'
                self.voted_for = None
        except:
            print("Error occurred while sending RequestVote RPC")
        
        lastTerm = 0
        if len(self.log) > 0:
            lastTerm = text_readers.last_term(self.nodeID)
        
        logOk = (request.lastLogTerm > lastTerm) or (request.lastLogTerm == lastTerm and request.lastLogIndex >= len(self.log))
        print("is logOk:")
        print(logOk)
        # change here lease duration.
        if request.term == self.current_term and logOk and self.voted_for in (request.candidateId, None):
            print("vote granted")
            self.voted_for = request.candidateId
            return raft_pb2.RequestVoteResponse(term=self.current_term, voteGranted=True, leaseDuration=5)
        else:
            print("vote not granted")
            return raft_pb2.RequestVoteResponse(term=self.current_term, voteGranted=False, leaseDuration=5)

    # 3
    def collecting_votes(self, response, node_address):
        # Collecting votes
        if self.current_role == 'CANDIDATE' and response.term == self.current_term and response.voteGranted:
            self.votesReceived.add(node_address)
            if len(self.votesReceived) >= ((len(nodes) + 1)/2):
                self.current_role = 'LEADER'
                # or we can make it nodes[self.nodeID] if we want IP
                self.current_leader = self.nodeID

        
        elif self.current_term < response.term:
            print('stepping down')
            self.current_term = response.term
            self.current_role = 'FOLLOWER'
            self.voted_for = None
            self.set_election_timeout()

    # function to send append_enteries and heartbeats 
    def leader_append_entries(self):
        print("sending entries from leader")
        while True:
            time.sleep(2)
            if self.current_role == 'LEADER':
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
        prefixLen = self.sentLength[i]
        suffix = []
        for i in range(prefixLen, len(self.log)):
            suffix.append(self.log[i])

        prefixTerm = 0
        if prefixLen > 0:
            prefixTerm = self.log[prefixLen - 1].term
        

        channel = grpc.insecure_channel(node_address)
        stub = raft_pb2_grpc.RaftStub(channel)
        try:
                response = stub.AppendEntries(raft_pb2.AppendEntriesRequest(
                        term = self.term,
                        leaderId = nodes[self.nodeID],
                        prevLogIndex = prefixLen,
                        prevLogTerm = prefixTerm,
                        entries = suffix,
                        leaderCommit = len(self.log), # not sure about this
                        leaseInterval = 7, # need to change
                ))

                self.receive_log_acknowledgment(response, follower_id)
        
                
        except grpc.RpcError as e:
                print(f"Error occurred while sending RequestVote RPC to node {node_address}: {e}")  
        
#6 --> not written yet
#7
    def AppendEntries(self, request, context):

            if request.term < self.current_term:
                return raft_pb2.AppendEntriesResponse(term=self.current_term, success=False, ackLength = len(self.log))
            
            if self.log[request.prevLogIndex].term  != request.prevLogTerm:
                return raft_pb2.AppendEntriesResponse(term=self.current_term, success=False, ackedLength = len(self.log))


            # Update follower's log if necessary
            if request.entries and len(self.log) > request.prevLogIndex and \
            self.log[request.prevLogIndex].term != request.prevLogTerm:
                # There is a mismatch in the terms, so we find the index to truncate from
                index = min(len(self.log), request.prevLogIndex + len(request.entries)) - 1
                while index >= 0 and self.log[index].term != request.entries[index - request.prevLogIndex].term:
                    self.log = self.log[0: request.prevLogIndex -1]

            # Append any new entries not already in the log
            if request.prevLogIndex + len(request.entries) > len(self.log):
                for i in range(len(self.log) - request.prevLogIndex, len(request.entries)):
                    self.log.append(request.entries[i])

            # Update the commit length if leader's commit index is greater than node's commit length
            if request.leaderCommit > self.commit_length:
                for i in range(self.commit_length, request.leaderCommit):
                    # Deliver log[i].command to the application, assuming 'command' is a method
                    # In actual implementation, this would apply the command to the state machine
                    # For simplicity, we'll just print it out here
                    print(f"Committed entry from leader: {self.log[i].command}")
                self.commit_length = request.leaderCommit

            # Response handling
            current_term = self.current_term  # To avoid issues with self.current_term changing elsewhere
            return raft_pb2.AppendEntriesResponse(term=current_term, success=True, ackLength = len(self.log))
#8
    def receive_log_acknowledgment(self, response, follower_id):
            # Check if the response term is equal to the leader's current term and the node is the leader
            if response.term == self.current_term and self.current_role == 'LEADER':
                # If the response is successful and the ack is greater or equal to what we have recorded
                if response.success and response.ackLength >= self.ackedLength[follower_id]:
                    # Update the sent and acked lengths
                    self.sentLength[follower_id] = response.ackLength
                    self.ackedLength[follower_id] = response.ackLength
                    # Commit any new entries if possible
                    self.CommitLogEntries()
                # If the response indicates a failure, decrement the sent length and retry replication
                elif self.sentLength[follower_id] > 0:
                    self.sentLength[follower_id] -= 1
                    self.send_entry_req(self.nodeID, follower_id)

            # If the response contains a greater term, step down to follower
            elif response.term > self.current_term:
                self.current_term = response.term
                self.current_role = 'FOLLOWER'
                self.voted_for = None
                self.set_election_timeout()
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