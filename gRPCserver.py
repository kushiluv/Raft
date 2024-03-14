import threading
import time
import random
from concurrent import futures
import grpc
import raft_pb2
import raft_pb2_grpc

# Global state variables (example for one server)
current_term = 0
voted_for = None
log = []
commit_index = 0
last_applied = 0
state = "leader"  # can be follower, candidate, or leader
election_timeout = random.uniform(5, 10)

# Volatile state on leaders
next_index = {}  # initialize to leader last log index + 1
match_index = {}  # initialize to 0

# Lock for state changes
state_lock = threading.Lock()

class RaftServicer(raft_pb2_grpc.RaftServicer):
    def AppendEntries(self, request, context):
        global current_term, voted_for, commit_index, log, state
        with state_lock:
            if request.term < current_term:
                return raft_pb2.AppendEntriesResponse(success=False, term=current_term)
            
            # Reset election timeout
            reset_election_timeout()

            # More logic here for appending entries and updating state
            # ...

            return raft_pb2.AppendEntriesResponse(success=True, term=current_term)
    
    def RequestVote(self, request, context):
        global current_term, voted_for, state
        with state_lock:
            if (request.term < current_term or
                (voted_for is not None and voted_for != request.candidateId) or
                not is_candidate_log_up_to_date(request.lastLogIndex, request.lastLogTerm)):
                return raft_pb2.RequestVoteResponse(voteGranted=False, term=current_term)

            # Candidate's log is up-to-date, and either haven't voted or candidate is the one we voted for
            voted_for = request.candidateId
            current_term = request.term
            # Reset election timeout
            reset_election_timeout()

            return raft_pb2.RequestVoteResponse(voteGranted=True, term=current_term)
    

    def ServeClient(self, request, context):
        try:
            global state, log, commit_index
            
            # Correctly access the 'request' field from ServeClientArgs message
            command = request.request.split()  # Notice the lowercase 'r' in 'request.request'
            
            if state != "leader":
                return raft_pb2.ServeClientReply(data="", leaderId="leader_id_if_known", success=False)

            if command[0] == "SET":
                _, key, value = command
                log.append({"term": current_term, "command": f"SET {key} {value}"})
                commit_index += 1  # This is simplified; real commit logic is needed
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
    raft_pb2_grpc.add_RaftServicer_to_server(RaftServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Server started at [::]:50051")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()