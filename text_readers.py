# Contains utility functions for reading/writing to the text files.
import os
import json
from google.protobuf.json_format import MessageToJson

def log_entry_to_json(log_entry):
    # Convert a LogEntry object to a JSON string
    return MessageToJson(log_entry)
def read_metadata_file(nodeID):
            directory_name = f"logs_node_{nodeID}"
            file_path = os.path.join(directory_name, "metadata.txt")
            if os.path.exists(file_path) and os.path.isfile(file_path):
                with open(file_path, 'r') as file:
                    lines = file.readlines()
                    if len(lines) >= 4:
                        try:
                            current_term = int(lines[0].strip())
                            voted_for = lines[1].strip()
                            commit_length = int(lines[2].strip())
                            current_leader = lines[3].strip()
                            print("Metadata read successfully.")
                            return [current_term, voted_for, commit_length, current_leader]
                        except ValueError:
                            print("Error: Metadata file contains invalid data.")
                    else:
                        print("Error: Metadata file does not contain enough lines.")
            else:
                print(f"Error: Metadata file '{file_path}' does not exist.")

def last_term(node_id):
    directory_name = f"logs_node_{node_id}"
    file_path = os.path.join(directory_name, "logs.txt")
    if os.path.exists(file_path) and os.path.isfile(file_path):
        with open(file_path, 'r') as file:
            lines = file.readlines()
            if(len(lines)) == 0:
                return 0
            else:
                last_line = lines[-1].strip()
                return last_line[-1]
    else:
        return None  # If the file or directory does not exist

def get_value_state_machine(node_id, variable_name):
    # Construct the file path using the node_id
    file_path = os.path.join(f"logs_node_{node_id}", "state_machine.json")
    
    # Check if the file exists
    if not os.path.exists(file_path):
        print(f"File {file_path} does not exist.")
        return ""
    
    try:
        # Open and load the JSON file
        with open(file_path, 'r') as file:
            state_machine = json.load(file)
            
            # Check if the variable_name key exists in the JSON data
            if variable_name in state_machine:
                return state_machine[variable_name]
            else:
                return ""  # Return an empty string if the key does not exist
    except Exception as e:
        # Handle exceptions such as file reading errors or JSON parsing errors
        print(f"An error occurred: {e}")
        return ""

def set_value_state_machine(node_id, variable_name, value):
    # Construct the directory path and file path
    dir_path = f"logs_node_{node_id}"
    file_path = os.path.join(dir_path, "state_machine.json")

    # Ensure the directory exists
    os.makedirs(dir_path, exist_ok=True)
    
    # Load or initialize the state machine
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            try:
                state_machine = json.load(file)
            except json.JSONDecodeError:
                # If the file is not a valid JSON, start with an empty dictionary
                state_machine = {}
    else:
        state_machine = {}

    # Update or create the key with the new value
    state_machine[variable_name] = value

    # Write the updated state machine back to the file
    with open(file_path, 'w') as file:
        json.dump(state_machine, file, indent=4)    

def commit_entry(node_id, command):
    # if heartbead command
    if command == 'NO-OP':
        set_value_state_machine(node_id, 'NO-OP', 0)
        return
    command = command.split()
    key, value = command[1], command[2]
    set_value_state_machine(node_id, key, value)

def log_to_dump(self, message):
    print(self.directory)
    file_path = os.path.join(self.directory, "dump.txt")
    with open(file_path, 'a') as file:
        file.write(message + '\n')

def write_logs(self):
    # Each log entry will be saved in the format "term: {term}, command: {command}"
    logs_as_lines = [f"term: {log.term}, command: {log.command}" for log in self.log]
    file_path = os.path.join(self.directory, "logs.txt")
    with open(file_path, 'w') as file:
        for line in logs_as_lines:
            file.write(line + '\n')

def write_metadata_file(self):
    metadata_content = [
        str(self.current_term),
        self.voted_for if self.voted_for is not None else "",
        str(self.commit_length),
        self.current_leader if self.current_leader is not None else ""
    ]
    file_path = os.path.join(self.directory, "metadata.txt")
    with open(file_path, 'w') as file:
        file.write('\n'.join(metadata_content))
