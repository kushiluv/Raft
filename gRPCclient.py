import grpc
import raft_pb2
import raft_pb2_grpc

def send_set_request(stub, key, value):
    # channel = grpc.insecure_channel("localhost:50051")
    # stub = raft_pb2_grpc.RaftStub(channel)
    response = stub.ServeClient(raft_pb2.ServeClientArgs(request=f"SET {key} {value}"))
    print(f"Set response: Success: {response.success}, Data: {response.data}, LeaderID: {response.leaderId}")

def send_get_request(stub, key):
    # channel = grpc.insecure_channel("localhost:50051")
    # stub = raft_pb2_grpc.RaftStub(channel)
    response = stub.ServeClient(raft_pb2.ServeClientArgs(request=f"GET {key}"))
    print(f"Get response: Success: {response.success}, Data: {response.data}, LeaderID: {response.leaderId}")

def main():
    with grpc.insecure_channel('localhost:50050') as channel:
        stub = raft_pb2_grpc.RaftStub(channel)
        while True:
            print("Enter 1 for GET request, 2 for SET request, or any other key to exit:")
            choice = input().strip()
            
            if choice == "1":
                print("Enter key for GET request:")
                key = input().strip()
                send_get_request(stub, key)
            elif choice == "2":
                print("Enter key for SET request:")
                key = input().strip()
                print("Enter value for SET request:")
                value = input().strip()
                send_set_request(stub, key, value)
            else:
                print("Exiting...")
                break

if __name__ == '__main__':
    main()
