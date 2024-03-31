import threading
import time
import random
from concurrent import futures
import grpc
import raft_pb2
import raft_pb2_grpc
from temp import setup_directories
import text_readers
from text_readers import log_to_dump,write_logs,write_metadata_file
import os
from random import randint
import utils
# Global variable for stpring IP of all nodes.
nodes = ["34.131.182.182:50050", "34.131.29.154:50050", "34.131.170.187:50050", "34.131.44.163:50050", "34.131.38.113:50050"]
# nodes = ["localhost:50050" , "localhost:50051", "localhost:50052" , "localhost:50053" , "localhost:50054"]

# Lock for state changes
state_lock = threading.Lock()

class RaftServicer(raft_pb2_grpc.RaftServicer):
    def __init__(self, nodeID):
        # Create a directory on initialisation
        self.nodeID = nodeID
        # recover values from metadata file after crash
        if os.path.exists(f'logs_node_{self.nodeID}'):
            values = text_readers.read_metadata_file(self.nodeID)
            nv = 'term :{}, vote_for:{}, commitlength:{}, current_leader:{}'.format(values[0], values[1], values[2], values[3])
            print('Resetting node values from metadata:' , str(nv))
            self.current_term, self.voted_for , self.commit_length, self.current_leader = values[0], values[1], values[2], values[3]
            
            # these values are reset.
            self.log = []
            self.current_role = 'FOLLOWER'
            self.current_leader = None
            self.votesReceived = set()
            self.sentLength  = dict()
            self.ackedLength = {node_index: 0 for node_index in range(len(nodes))}
            self.election_timeout = None
            self.election_timeout_interval = 5.0 + nodeID
            self.directory = f"logs_node_{self.nodeID}"
            
            # this should also get reset right ?
            self.leader_lease_timeout = -1
            self.leader_lease_timeout_interval = 10

        
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
            self.leader_lease_timeout = -1
            self.leader_lease_timeout_interval = 10

            self.election_timeout = -1
            self.election_timeout_interval = 5.0 + nodeID
            self.directory = f"logs_node_{self.nodeID}"
            setup_directories(self.nodeID)
    
        self.init()
    def init(self):
        self.set_election_timeout()
        utils.run_thread(fn=self.on_election_timeout, args=())
        utils.run_thread(fn=self.leader_append_entries, args=())
        

    def set_election_timeout(self):
        self.election_timeout = time.time() + self.election_timeout_interval
        msg = ("time out set to:" + str(self.election_timeout))
        log_to_dump(self, msg)
        print(msg)

    # 1
    def on_election_timeout(self):
        while True:            
            # print(time.time())
            # print(self.current_role)
            time.sleep(2)
            if time.time() > self.election_timeout and \
                    (self.current_role == 'FOLLOWER' or self.current_role == 'CANDIDATE'):

                # print("Timeout....")
                # self.set_election_timeout()
                self.start_election()  

    def start_election(self):
        msg = f"Node {self.nodeID} election timer timed out, Starting election. "
        log_to_dump(self, msg)
        print(msg)
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
            return
        
        for i in range(len(nodes)):
            if i != self.nodeID:
                t = utils.run_thread(fn=self.send_vote_req, args = (nodes[i], i))
                threads += [t]
        
        # wait for all threads to complete, i.e. get vote response from all nodes.
        for t in threads:
            t.join()

        write_metadata_file(self) 

        return True
    
    def send_vote_req(self, node_address, nodeID):
        try:
            # Determine the last log index and term
            if self.log:  # Check if the log contains entries
                lastLogIndex = len(self.log)  # Last index is length of the log minus one
                lastLogTerm = self.log[lastLogIndex-1].term  # Term of the last log entry
            else:
                # Defaults if the log is empty
                lastLogIndex = 0
                lastLogTerm = 0

            # Setup the RPC call with the correct lastLogIndex and lastLogTerm
            channel = grpc.insecure_channel(node_address)
            stub = raft_pb2_grpc.RaftStub(channel)
            response = stub.RequestVote(raft_pb2.RequestVoteRequest(
                term=self.current_term,
                candidateId=str(self.nodeID),
                lastLogIndex=lastLogIndex,
                lastLogTerm=lastLogTerm
            ), timeout=1)
            
            if response.voteGranted:
                msg = f"Vote granted for Node {nodeID} in term {self.current_term}"
            else:
                msg = f"Vote denied for Node {nodeID} in term {self.current_term}"

            log_to_dump(self, msg)
            print(msg)
            self.collecting_votes(response, nodeID)
        except grpc.RpcError as e:
            msg = (f"Unable to send Vote to node {node_address}. Error: {e.details()}")
            print(msg)
            log_to_dump(self, msg)




    
    # 2
    def RequestVote(self, request, context):
        self.set_election_timeout()
        print(f"Received RequestVote RPC from candidate" + str(request.candidateId))
        # Perform voting logic here
        print(self.current_term)
        print(request.term)
        if request.term > self.current_term:
            print('Request from a candidate, set self state to follower')
            self.current_term = request.term
            self.current_role = 'FOLLOWER'
            self.voted_for = None
            self.set_election_timeout()
        
        lastTerm = 0
        if len(self.log) > 0:
            lastTerm = text_readers.last_term(self.nodeID)
        
        logOk = (request.lastLogTerm > lastTerm) or (request.lastLogTerm == lastTerm and request.lastLogIndex >= len(self.log))
        print("djfbujdfbudbfd:")
        print(request.lastLogTerm)
        print(lastTerm)
        print(request.lastLogIndex)
        print(len(self.log))
        # change here lease duration.
        print(request.term == self.current_term)
        print(logOk)
        print(self.voted_for)
        if request.term == self.current_term and logOk and self.voted_for in (request.candidateId, None):
            print(f'vote Granted to {request.candidateId}')
            log_to_dump(self, f'vote granted to {request.candidateId}')
            self.voted_for = request.candidateId
            return raft_pb2.RequestVoteResponse(
                        term=self.current_term,
                        voteGranted=True,
                        leaseDuration=self.leader_lease_timeout
                    )
        else:
            print('vote not granted')
            log_to_dump(self, f'vote not granted to {request.candidateId}')
            return raft_pb2.RequestVoteResponse(
                    term=self.current_term,
                    voteGranted=False,
                    leaseDuration=self.leader_lease_timeout
                )
        write_metadata_file(self)

    # 3
    def collecting_votes(self, response, node_address):
        with state_lock:
            self.leader_lease_timeout = max(self.leader_lease_timeout, response.leaseDuration)
            if self.current_role == 'CANDIDATE' and response.term == self.current_term and response.voteGranted:
                print("collecting votes")
                self.votesReceived.add(node_address)
                if len(self.votesReceived) >= ((len(nodes)//2) + 1):
                    print("Votes received : " , len(self.votesReceived))
                    print(((len(nodes)//2) + 1))
                    
                    now = time.time()
                    if now < self.leader_lease_timeout:
                        print("inside condition")
                        time.sleep(self.leader_lease_timeout - now)
                        print("sleep completed")
                    self.leader_lease_timeout = time.time() + self.leader_lease_timeout_interval
                    print("Node " + str(self.nodeID) + " is now the leader.")
                    self.log.append(raft_pb2.LogEntry(term=self.current_term, command='NO-OP'))
                    write_logs(self)
                    self.set_election_timeout()
                    self.current_role = 'LEADER'
                    self.current_leader = self.nodeID

            
            elif self.current_term < response.term:
                msg = f"Node {self.nodeID} stepping down"
                log_to_dump(self, msg)
                print(msg)
                self.current_term = response.term
                self.current_role = 'FOLLOWER'
                self.voted_for = None
                self.set_election_timeout()

    # function to send append_enteries and heartbeats 
                
    def step_down(self,new_term):
        with state_lock:
            self.current_term = new_term
            self.current_role = 'FOLLOWER'
            self.voted_for = None
            self.current_leader = None  # Resetting current leader since stepping down
            self.votesReceived.clear()
            self.set_election_timeout()
            print(f"Node {self.nodeID} stepped down to FOLLOWER in term {self.current_term}.")

    def leader_append_entries(self):
        while True:
            time.sleep(1)  # Control the frequency of heartbeat messages
            print("Current Time:",time.time())
            print("Lease End Time:",self.leader_lease_timeout)
            if self.current_role == 'LEADER':
                # Check if the leader's lease has expired before sending out new heartbeats
                now = time.time()
                if now > self.leader_lease_timeout:
                    print(f"Leader {self.nodeID}'s lease has expired. Stepping down.")
                    self.step_down(self.current_term)
                    continue

                msg = f"Leader {self.nodeID} sending heartbeat & attempting to renew lease"
                print(msg)
                log_to_dump(self, msg)
                
                acks_received = 0
                for i in range(len(nodes)):
                    if i != self.nodeID:
                        ack = self.replicate_log(i)
                        print("ack response was:" , ack)
                        if ack:
                            acks_received += 1

                print('total acks received are:' , str(acks_received))
                # Check if a majority of acks have been received to renew the lease
                if acks_received >= (len(nodes) // 2):
                    self.leader_lease_timeout = time.time() + self.leader_lease_timeout_interval
                    print(f"Leader {self.nodeID}'s lease renewed until {self.leader_lease_timeout}")

                self.commit_log_entries()

    #4
    def broadcast_message(self, message):
        with state_lock:
            if self.current_role == 'LEADER':
                # Append the new message to the leader's log
                self.log.append(raft_pb2.LogEntry(term=self.current_term, command=message))
                write_logs(self)
                self.ackedLength[self.nodeID] = len(self.log)  # Leader commits the new entry
                print(f"Appended message to leader's log: {message}")
                # Replicate the log entry to followers
                for follower_id in range(len(nodes)):
                    if follower_id != self.nodeID:
                        utils.run_thread(fn=self.replicate_log, args=(follower_id,))
            else:
                # Forward the message to the leader
                print(f"I am not the leader. Forwarding message to the leader: {self.current_leader}")

                
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
                prevLogIndex=prefixLen,
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
                return self.LeaderGettingAcks(response, follower_id, prefixLen, len(suffix))

            except grpc.RpcError as e:
                msg = f"RPC to node {follower_id} failed: {e}"
                log_to_dump(self, msg)
                print(f"RPC to node {follower_id} failed: {e}")  
                return False
            
            time.sleep(1)
    
            
                   
    #6 and 7
    def AppendEntries(self, request, context):
        with state_lock:
            print("Append Enteries request was received from node: " + str(request.leaderId))
            print('Processing AppendEnteries RPC')

            # Change self's election timer so as heartbeat msg was received.
            print('Incrementing nodes election timout timer: ', request.leaseInterval)
            self.set_election_timeout()
            self.leader_lease_timeout = time.time() + self.leader_lease_timeout_interval
            # If RPC request term is greater than node's current term
            if request.term > self.current_term:
                print('The request term was higher, update self leader and term')
                self.current_term = request.term
                self.voted_for = None
                self.current_role = 'FOLLOWER'
                self.current_leader = request.leaderId

            # If terms are equal, ensure node is a follower and update leader
            if request.term == self.current_term:
                if self.current_role != 'LEADER':
                    self.current_role = 'FOLLOWER'
                    self.current_leader = request.leaderId
            self.election_timeout+=self.election_timeout_interval
            # Prepare for log consistency check
            # print(self.log[request.prevLogIndex - 1].term)
            # print(request.prevLogTerm)
            if request.prevLogIndex > 0 and request.prevLogIndex <= len(self.log):
                log_ok = self.log[request.prevLogIndex - 1].term == request.prevLogTerm
            else:
                log_ok = request.prevLogIndex == 0  # True if prevLogIndex is 0, indicating a heartbeat or initial log entry

            print('The leader logs were found to be:' + str(log_ok))
            # If logs are consistent, append new entries and update commit index
            if log_ok:
                if request.entries:  # Only proceed if there are new entries
                    # Determine the starting index for new entries
                    start_index = request.prevLogIndex
                    # Replace the old entries with new ones from start_index onwards
                    self.log[start_index:] = request.entries
                    
                # Update ackedLength for self to the new length of the log
                self.ackedLength[self.nodeID] = len(self.log)
                
                write_logs(self)
                msg = f"Node {self.nodeID} accepted AppendEntries RPC from {request.leaderId}."
                print(msg)
                log_to_dump(self, msg)
                # Update commit index if leaderCommit > commit_length
                # NOTe: I DONT THINK WE NEED THIS BCOZ FOR OUR USE CASE COMMIT IS SAME AS APPEND!
                if request.leaderCommit > self.commit_length:
                    new_commit_index = min(request.leaderCommit, len(self.log))
                    for i in range(self.commit_length, new_commit_index):
                        # Apply the log[i].command to the state machine here
                        print(f"Applying to state machine: {self.log[i].command}")
                        text_readers.commit_entry(self.nodeID, self.log[i].command)
                    msg = f"Node {self.nodeID} (follower) committed the entry {self.log[i].command} to the state machine"   
                    print(msg)
                    log_to_dump(self, msg)
                    self.commit_length = new_commit_index
                    write_metadata_file(self)
                return raft_pb2.AppendEntriesResponse(term=self.current_term, success=True)
            else:
                msg = f"Node {self.nodeID} rejected AppendEntries RPC from {request.leaderId}."
                print(msg)
                log_to_dump(self, msg)
                # If log consistency check fails, respond with failure
                return raft_pb2.AppendEntriesResponse(term=self.current_term, success=False)
    #8
    def LeaderGettingAcks(self, response, follower_id, prefixLen, suffixLen):
        if response.term == self.current_term and self.current_role == 'LEADER':
            if response.success:
                print(f"Successfully replicated log to node {follower_id}")
                self.sentLength[follower_id] = prefixLen + suffixLen
                self.ackedLength[follower_id] = self.sentLength[follower_id]

                # Check if a majority of acks has been received
                # if self.has_majority_ack():
                #     # Only renew the lease after receiving majority acks
                #     self.leader_lease_timeout = time.time() + self.leader_lease_timeout_interval
                #     print(f"Lease renewed after receiving majority acks at {self.leader_lease_timeout}")
                    
                self.commit_log_entries()
                
            else:
                if self.sentLength[follower_id] > 0:
                    print(f"Failed to replicate log to node {follower_id}. Retrying...")
                    # changed here, send the whole log
                    self.sentLength[follower_id] -= 1
                    # self.sentLength[follower_id] = 0
                    # self.replicate_log(follower_id)
            return True
        elif response.term > self.current_term:
            self.step_down(response.term)
            return False

    def has_majority_ack(self):
        ack_count = sum(1 for ack in self.ackedLength.values() if ack >= len(self.log))
        return ack_count >= (len(nodes)-1) // 2



    def commit_log_entries(self):
        print('Invoked Commit enteries')
        print('Current log length:', len(self.log))
        print('Current log:', self.log)
        # Check if there are any new entries we can commit
        minAcks =((len(nodes) + 1)//2)-1
        for i in range(self.commit_length + 1, len(self.log) + 1):
            if self.acks(i) >= minAcks:
                # Apply log[i-1].command to the state machine (e.g., execute the command)
                text_readers.commit_entry(self.nodeID, self.log[i-1].command)
                print(f"Committed log index {i-1} with term {self.nodeID, self.log[i-1].term}")
                self.commit_length = i
        write_metadata_file(self)

    def acks(self, length):
            # Ensure we're accessing existing keys in ackedLength.
            return sum(1 for ack in self.ackedLength.values() if ack >= length)                   
    

    
  
#9
  
    # def CommitLogEntries(self):
    #     minAcks = (len(nodes) + 1) // 2
    #     ready = []
    #     for i in range(1, len(self.log) + 1):
    #         if self.acks(i) >= minAcks:
    #             ready.append(i)
    #     if ready!=[] and max(ready)>self.commit_length and self.log[max(ready)-1].term == self.current_term:
    #         #write changes into state machine here / ie to the DB
    #         for j in ready:
    #             print(f'Committing entry at log index:{i} to db' )
    #             text_readers.commit_entry(self.log[i].command)
    #         self.commit_length = max(ready)

    
    def ServeClient(self, request, context):
        try:
            print('hello')
            # Correctly access the 'request' field from ServeClientArgs message
            print(request.request)
            try:
                command = request.request.split()  # Notice the lowercase 'r' in 'request.request'
            except Exception as e:
                print('error in spliting {e}')
            
            if self.current_role != "LEADER":
                print("The current leader is: "+ str(self.current_leader))
                return raft_pb2.ServeClientReply(data="I am not the leader, try pinging:", leaderId=nodes[int(self.current_leader)], success=False)

            if command[0] == "SET":
                key, value = command[1] , command[2]
                initial_commit_len = self.commit_length
                obj = raft_pb2.LogEntry(term=self.current_term, command=request.request)
                self.log.append(obj)
                write_logs(self)

                # wait for request to get committed.
                while(self.commit_length < initial_commit_len + 1): 
                    continue
                    time.sleep(1)


                #  Append command to state machine.
                try:
                    # text_readers.set_value_state_machine(self.nodeID, key, value)
                    return raft_pb2.ServeClientReply(data="Command executed", leaderId="", success=True)
                except:
                    return raft_pb2.ServeClientReply(data="Failed to commit", leaderId="", success=False)

            elif command[0] == "GET":
                try:
                    key = command[1]
                    value = text_readers.get_value_state_machine(self.nodeID, key)
                    return raft_pb2.ServeClientReply(data="Value found to be" + str(value), leaderId="", success=True)
                except:
                    return raft_pb2.ServeClientReply(data="Some error occurred", leaderId="", success=False)

            else:
                return raft_pb2.ServeClientReply(data="Invalid command", leaderId="", success=False)
        except Exception as e:
            print(f"Exception in ServeClient: {e}")
            return raft_pb2.ServeClientReply(data="An error occurred", leaderId="", success=False)



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