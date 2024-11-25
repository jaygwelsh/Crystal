# scripts/verify_integrity.py

import yaml
import sys
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
    integrity = storage.verify_data_integrity()
    print(f"Data integrity verified: {integrity}")

if __name__ == "__main__":
    main()
