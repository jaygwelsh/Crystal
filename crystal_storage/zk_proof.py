import pickle
import logging

class ZKProofManager:
    def generate_data_hash(self, data):
        import hashlib
        return hashlib.sha256(data).digest()

    def store_data_hash(self, data_hash, path='data/data_hash.pkl'):
        with open(path, 'wb') as f:
            pickle.dump(data_hash, f)

    def get_stored_data_hash(self, path='data/data_hash.pkl'):
        with open(path, 'rb') as f:
            return pickle.load(f)

    def verify_integrity(self, current_hash):
        stored_hash = self.get_stored_data_hash()
        logging.info(f"Stored Hash: {stored_hash.hex()}")
        logging.info(f"Current Hash: {current_hash.hex()}")
        return current_hash == stored_hash
