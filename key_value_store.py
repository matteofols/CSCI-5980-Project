import threading
from datetime import datetime

class KV_store:

    def __init__(self):
        self.parent_dict = {}
        self.log_queue = []
        self.lock = threading.Lock()
    
    def _get_timestamp(self):
        return datetime.now().strftime("%m-%d-%Y %H:%M:%S")
    
    def GET(self, key):
        timestamp = self._get_timestamp()
        with self.lock:
            self.log_queue.append(f'GET({key}) - {timestamp}') # a queue of all the operations in the KV_store
            if (key in self.parent_dict):
                return {"value": self.parent_dict[key]} 
            # Error Handling
            else:
                return {"message": f'Key: {key} not found'}

    def SET(self, key, value):  # only used to update the values that already exist in the store
        timestamp = self._get_timestamp()
        with self.lock:
            self.log_queue.append(f'SET({key}, {value}) - {timestamp}') # a queue of all the operations in the KV_store
            if (key in self.parent_dict):
                self.parent_dict[key] = value
                return {"message": f'Key: {key} value successfully set to {value}'}
            else:
                return {"message": f'Key: {key} not found. Use the PUT({key}, {value}) to add the pair to the store'}
            
    def PUT(self, key, value):
        timestamp = self._get_timestamp()
        with self.lock:
            self.log_queue.append(f'PUT({key}, {value}) - {timestamp}') # a queue of all the operations in the KV_store
            if (key in self.parent_dict):
                return {"message": f'Key: {key} already exists. Use the Set({key}, {value}) function to update the key'}
            self.parent_dict[key] = value
            return {"message": f'Key: {key} has been successfully added'}

    def DELETE(self, key):
        timestamp = self._get_timestamp()
        with self.lock:
            self.log_queue.append(f'DELETE({key}) - {timestamp}') # a queue of all the operations in the KV_store
            if (key in self.parent_dict):
                del(self.parent_dict[key])
                return {"message": f'Key {key} has been deleted successfully'}
            else:
                return {"message": f'Key: {key} not found'}
        
    def GET_LOG(self): # displays the log of all operations
        return {"log": self.log_queue}
    
    def POP_LOG(self, item): # a method to remove items off the log. This might have to be a hidden method.
        if (item in self.log_queue):
            self.log_queue.remove(item)
            return {"message": f'item "{item}" removed.'}
        else:
            return {"message": f'item "{item}" not found'}, 404
