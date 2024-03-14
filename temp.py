import os

def setup_directories(node_id):
    dir_name = f"logs_node_{node_id}"
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    
    files = ["logs.txt", "metadata.txt", "dump.txt"]
    for file in files:
        file_path = os.path.join(dir_name, file)
        if not os.path.exists(file_path):
            open(file_path, 'w').close()

node_id = 0  # Example node_id
setup_directories(node_id)