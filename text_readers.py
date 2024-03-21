# Contains utility functions for reading/writing to the text files.
import os

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