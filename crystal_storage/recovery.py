import pickle
import logging

class RecoveryManager:
    def __init__(self, compression_manager, fragmentation_manager, routing_manager, encryption_manager):
        self.compression_manager = compression_manager
        self.fragmentation_manager = fragmentation_manager
        self.routing_manager = routing_manager
        self.encryption_manager = encryption_manager

    def recover_data(self):
        try:
            with open('data/fragment_info.pkl', 'rb') as f:
                num_fragments = pickle.load(f)
            logging.info(f"Number of fragments to recover: {num_fragments}")
            fragments = self.routing_manager.retrieve_fragments(num_fragments, self.encryption_manager)
            compressed_data = self.fragmentation_manager.merge_fragments(fragments)
            data = self.compression_manager.decompress(compressed_data)
            return data
        except Exception as e:
            logging.error(f"Data recovery failed: {e}")
            raise
