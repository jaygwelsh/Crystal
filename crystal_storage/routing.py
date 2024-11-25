import os
import logging

class RoutingManager:
    def __init__(self, node_paths):
        self.node_paths = node_paths

    def store_fragment(self, fragment, node_index):
        node_path = self.node_paths[node_index % len(self.node_paths)]
        fragment_path = os.path.join(node_path, f"fragment_{node_index}.pkl")
        with open(fragment_path, "wb") as f:
            f.write(fragment)
        logging.info(f"Fragment stored at {fragment_path}")

    def retrieve_fragment(self, node_index):
        node_path = self.node_paths[node_index % len(self.node_paths)]
        fragment_path = os.path.join(node_path, f"fragment_{node_index}.pkl")
        with open(fragment_path, "rb") as f:
            fragment = f.read()
        logging.info(f"Fragment retrieved from {fragment_path}")
        return fragment
