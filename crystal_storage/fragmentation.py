import logging

class FragmentationManager:
    def __init__(self, fragment_size=1024):
        self.fragment_size = fragment_size

    def fragment_data(self, data):
        fragments = [data[i:i+self.fragment_size] for i in range(0, len(data), self.fragment_size)]
        for i, fragment in enumerate(fragments):
            logging.info(f"Fragment {i}: Size = {len(fragment)} bytes")
        return fragments

    def merge_fragments(self, fragments):
        data = b''.join(fragments)
        logging.info(f"Merged data size: {len(data)} bytes")
        return data
