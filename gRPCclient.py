import grpc
import raft_pb2
import raft_pb2_grpc
import time
nodes = ["localhost:50050" , "localhost:50051", "localhost:50052" , "localhost:50053" , "localhost:50054"]
leader_addr = "localhost:50051"
def send_request(stub, request_type, key, value=None):
    """Send a request and return the response object."""
    if request_type == "SET":
        request_str = f"SET {key} {value}"
    else:  # GET
        request_str = f"GET {key}"
    
    request_args = raft_pb2.ServeClientArgs(request=request_str)
    response = stub.ServeClient(request_args)
    return response

def attempt_send_request(request_type, key, value=None):
    global leader_addr
    """Attempt to send a request, handling redirection if necessary."""
    with grpc.insecure_channel(leader_addr) as channel:
        stub = raft_pb2_grpc.RaftStub(channel)
        response = send_request(stub, request_type, key, value)
        
        # Check if we need to redirect to the leader
        if not response.success and response.leaderId:
            # Redirect to the leader and resend the request
            leader_addr = response.leaderId
            time.sleep(1)
            return attempt_send_request( request_type, key, value)
        
        # Return the final response
        return response

def main():
    while True:
        global leader_addr
        print("Enter 1 for GET request, 2 for SET request, or any other key to exit:")
        choice = input().strip()

        if choice not in ["1", "2"]:
            print("Exiting...")
            break

        print("Enter key:")
        key = input().strip()
        value = None
        if choice == "2":
            print("Enter value for SET request:")
            value = input().strip()

        request_type = "GET" if choice == "1" else "SET"

        # # Try sending the request to each node until one succeeds or redirects to the leader
        # for node in nodes:
        try:
            response = attempt_send_request(request_type, key, value)
            print(f"Response: Success: {response.success}, Data: {response.data}, LeaderID: {response.leaderId}")
            # break  # A successful response (or a final failure response) was received
        except grpc.RpcError as e:
            print(f"Could not connect to {leader_addr}. Error: {e}")

if __name__ == '__main__':
    main()
