# scripts/store_data.py

import yaml
import sys
import os
from crystal_storage import CRYSTALStorage
from pipeline import load_config

def load_config_file(config_path='config/config.yaml'):
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def main():
    config = load_config_file()
    storage = CRYSTALStorage(
        node_paths=config['node_paths'],
        fragment_size=config['fragment_size']
    )
    # Replace with actual data loading/generation
    data = os.urandom(config.get('data_size', 1024 * 500))  # Default ~500KB
    storage.store_data_with_proof(data)
    print("Data stored successfully with zero-knowledge proof.")

if __name__ == "__main__":
    main()
