import grpc
import raft_pb2
import raft_pb2_grpc

def send_set_request(stub, key, value):
    response = stub.ServeClient(raft_pb2.ServeClientArgs(request=f"SET {key} {value}"))
    print(f"Set response: Success: {response.success}, Data: {response.data}, LeaderID: {response.leaderId}")

def send_get_request(stub, key):
    response = stub.ServeClient(raft_pb2.ServeClientArgs(request=f"GET {key}"))
    print(f"Get response: Success: {response.success}, Data: {response.data}, LeaderID: {response.leaderId}")

def main():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = raft_pb2_grpc.RaftStub(channel)
        # Example client request to set and get values
        send_set_request(stub, "key1", "value1")
        send_get_request(stub, "key1")

if __name__ == '__main__':
    main()
