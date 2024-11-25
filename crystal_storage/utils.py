import os
import pickle

def ensure_directories(node_paths):
    for path in node_paths:
        os.makedirs(path, exist_ok=True)

def store_fragment_info(num_fragments, path='data/fragment_info.pkl'):
    # Ensure the directory exists
    directory = os.path.dirname(path)
    os.makedirs(directory, exist_ok=True)

    # Store the fragment information
    with open(path, 'wb') as f:
        pickle.dump(num_fragments, f)
