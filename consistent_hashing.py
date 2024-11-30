# This is a consistent hashing file that will hash the instances of the KV stores and distribute the request 

import hashlib as hash
# import server as server
from key_value_store import KV_store as KV_store


class ConsistentHashing:
    def __init__(self, nodes=None, num_replicas=3):
        """Initialize the Consistent Hashing structure with a specified number of replicas."""
        self.num_replicas = num_replicas
        self.server_ring = {}
        self.sorted_keys = []

        # Adds all nodes to the ring
        if nodes:
            for node in nodes:
                self.add_instance(node)

    def _hash(self, key):
        """Generate a hash for the given key."""
        return hash.md5(key.encode("utf-8")).hexdigest()
    
    def add_instance(self, instance):
        """Add a new instance to the server ring."""
        for i in range(self.num_replicas):
            replica_key = f"{instance}:{i}"
            hashed_key = self._hash(replica_key)
            self.server_ring[hashed_key] = instance
            self.sorted_keys.append(hashed_key)
        self.sorted_keys.sort()

    def remove_instance(self, instance):
        """Remove an instance from the server ring."""
        for i in range(self.num_replicas):
            replica_key = f"{instance}:{i}"
            hashed_key = self._hash(replica_key)
            if hashed_key in self.server_ring:
                del self.server_ring[hashed_key]
                self.sorted_keys.remove(hashed_key)

    def _find_position(self, hashed_key):
        """Find the position of the hashed key on the ring."""
        for i, key in enumerate(self.sorted_keys):
            if hashed_key <= key:
                return i
        return 0

    def get_instance(self, key):
        """Get the instance responsible for the given key."""
        if not self.server_ring:
            return None
        
        hashed_key = self._hash(key)
        position = self._find_position(hashed_key)
        return self.server_ring[self.sorted_keys[position]]
    
    def instance_count(self):
        return len(self.server_ring)
    
    def get_instances(self):
        """Return all instances currently in the ring."""
        return list(self.server_ring.values())



















