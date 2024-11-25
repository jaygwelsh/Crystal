import time
import logging

class EncryptionManager:
    def encrypt(self, fragment):
        logging.info(f"Encrypting fragment: {fragment[:10]}...")
        time.sleep(0.1)  # Simulate encryption delay
        return f"encrypted({fragment.decode()})".encode()

    def decrypt(self, encrypted_fragment):
        logging.info(f"Decrypting fragment: {encrypted_fragment[:10]}...")
        time.sleep(0.1)  # Simulate decryption delay
        return encrypted_fragment.replace(b"encrypted(", b"").replace(b")", b"")
