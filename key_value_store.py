

class KV_store:

    def __init__(self):
        self.parent_dict = {}
        self.log_queue = []

    def GET(self, key):
        self.log_queue.append(f'GET({key})') # a queue of all the operations in the KV_store
        if (key in self.parent_dict):
            return self.parent_dict[key]
        
        # Error Handling
        else:
            return f'Key: {key} not found'
    def SET(self, key, value):  # only used to update the values that already exist in the store
        self.log_queue.append(f'SET({key}, {value})') # a queue of all the operations in the KV_store
        if (key in self.parent_dict):
            self.parent_dict[key] = value
            return f'Key: {key} value successfully set to {value}'
        else:
            return f'Key: {key} not found. Use the PUT({key}, {value}) to add the pair to the store'
        
    def PUT (self, key, value):
        self.log_queue.append(f'PUT({key}, {value})') # a queue of all the operations in the KV_store
        if (key in self.parent_dict):
            return f'Key: {key} already exists. Use the Set({key}, {value}) function to update the key'
        self.parent_dict[key] = value
        return f'Key: {key} has been successfully added'

    def DELETE(self, key):
        self.log_queue.append(f'DELETE({key})') # a queue of all the operations in the KV_store
        if (key in self.parent_dict):
            del(self.parent_dict[key])
            return f'Key {key} has been deleted successfully'
        else:
            return f'Key: {key} not found'
        
    def GET_LOG(self): # displays the log of all operations
        return self.log_queue
    
    def POP_LOG(self, item): # a method to remove items off the log. This might have to be a hidden method.
        if (item in self.log_queue):
            self.log_queue.pop(item)
