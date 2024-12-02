import threading
from key_value_store import KV_store as key_value_store
from flask import Flask, request, jsonify

app = Flask(__name__)

# Create multiple instances of KV Store
kv_store = key_value_store()

@app.route('/<key>', methods=['GET', 'POST'])
def handle_key(key):
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
    return jsonify(kv_store.DELETE(key))

@app.route('/log', methods=['GET'])
def get_log():
    return jsonify(kv_store.GET_LOG())

if __name__ == "__main__":
    app.run(host='127.0.0.1', port=5000, threaded=True)
