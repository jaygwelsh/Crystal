from .encryption import EncryptionManager
from .compression import CompressionManager
from .fragmentation import FragmentationManager
from .routing import RoutingManager
from .recovery import RecoveryManager
from .zk_proof import ZKProofManager
from .utils import ensure_directories

class CRYSTALStorage:
    def __init__(self, node_paths, fragment_size=1024):
        ensure_directories(node_paths)
        self.encryption_manager = EncryptionManager()
        self.compression_manager = CompressionManager()
        self.fragmentation_manager = FragmentationManager(fragment_size)
        self.routing_manager = RoutingManager(node_paths)
        self.recovery_manager = RecoveryManager(
            self.compression_manager,
            self.fragmentation_manager,
            self.routing_manager,
            self.encryption_manager
        )
        self.zk_proof_manager = ZKProofManager()

    def store_data_with_proof(self, data):
        compressed_data = self.compression_manager.compress(data)
        fragments = self.fragmentation_manager.fragment_data(compressed_data)
        self.routing_manager.distribute_fragments(fragments, self.encryption_manager)
        self.zk_proof_manager.store_data_hash(self.zk_proof_manager.generate_data_hash(data))

    def verify_data_integrity(self):
        recovered_data = self.recovery_manager.recover_data()
        current_hash = self.zk_proof_manager.generate_data_hash(recovered_data)
        return self.zk_proof_manager.verify_integrity(current_hash)

    def recover_data(self):
        return self.recovery_manager.recover_data()
