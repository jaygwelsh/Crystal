# scripts/recover_data.py

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
    try:
        recovered_data = storage.recover_data()
        # Replace with actual data verification or usage
        print(f"Data recovered successfully: {recovered_data[:50]}...")
    except Exception as e:
        print(f"Data recovery failed: {e}")

if __name__ == "__main__":
    main()
