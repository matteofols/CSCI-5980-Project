import threading
from key_value_store import KV_store as key_value_store
from flask import Flask, request, jsonify
from benchmark_test import run_benchmark
from consistent_hashing import ConsistentHashing

app = Flask(__name__)

# Initialize Consistent Hasishing 
ch = ConsistentHashing(num_replicas=1)
kv_stores = {}


# Endpoint to set the number of replicas for consistent hashing.
@app.route('/set_replicas', methods=['POST'])
def set_replicas():
    global ch, num_replicas
    data = request.json
    if "num_replicas" not in data:
        return jsonify({"message": "Missing 'num_replicas' in request"}), 400
    num_replicas = data["num_replicas"]

    # Reinitialize Consistent Hashing with new num_replicas
    ch = ConsistentHashing(num_replicas=num_replicas)

    for i in range(num_replicas):
        kv_stores[f'KVStore{i+1}'] = key_value_store()

    for store_name in kv_stores:
        ch.add_instance(store_name)

    return jsonify({"message": f"Number of replicas set to {num_replicas}"}), 200

@app.route('/<key>', methods=['GET', 'POST'])
def handle_key(key):
    instance_name = ch.get_instance(key) # Get the correct instance using consistent hashing

    if instance_name is None:
        return jsonify({"message": "No available instance for the key"}), 500
    
    kv_store = kv_stores[instance_name] # Get the selected KV store instance

    if request.method == 'POST':
        value_json = request.get_json()
        if value_json is None or 'value' not in value_json:
            return jsonify({"message": "Missing 'value' in request"}), 400
        value = value_json['value']
        

        if key in kv_store.parent_dict:
            return jsonify(kv_store.SET(key, value))
        else:
            return jsonify(kv_store.PUT(key, value))
          
    elif request.method == 'GET':
        return jsonify(kv_store.GET(key))
    else:
        return jsonify({"message": "Unsupported HTTP method"}), 405

@app.route('/<key>', methods=['DELETE'])
def delete_key(key):
    instance_name = ch.get_instance(key)
    if instance_name is None:
        return jsonify({"message": "No available instance for the key"}), 500
    kv_store = kv_stores[instance_name]
    return jsonify(kv_store.DELETE(key))

@app.route('/log', methods=['GET'])
def get_log():
    logs = {name: store.GET_LOG() for name, store in kv_stores.items()}
    return jsonify(logs)

if __name__ == "__main__":
    app.run(host='127.0.0.1', port=5000, threaded=True)
