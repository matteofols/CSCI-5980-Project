import socket
import threading
import key_value_store
# from flask import Flask, request, jsonify
from benchmark_test import run_benchmark

# app = Flask(__name__)

# kv_store = key_value_store.KV_store()
kv_store_lock = threading.Lock()

# Function to handle client requests
def request_handling(client_conn, client_addr):
    print(f"Request received: {request_split}") 
    print(f"Connection successful with {client_conn} and address {client_addr}")            
    kv_object = key_value_store.KV_store()
    
    while True:
        try:
            # Receiving request from the client
            request = client_conn.recv(1024).decode()
            if not request:
                print(f"No request received. Closing connection with {client_addr}.")
                break
            print(f"Request received from {client_addr} is {request}")

            request_split = request.split()
            response = ""

            with kv_store_lock: # Apply a lock on operations that change the KV store
                try:
                    if request_split[0] == "GET":
                        response = kv_object.GET(request_split[1])
                    elif request_split[0] == "PUT":
                        response = kv_object.PUT(request_split[1], request_split[2])
                    elif request_split[0] == "SET":
                        response = kv_object.SET(request_split[1], request_split[2])
                    elif request_split[0] == "DELETE":
                        response = kv_object.DELETE(request_split[1])
                    elif request_split[0] == "GET_LOG":
                        response = kv_object.GET_LOG()
                    elif request_split[0] == "POP_LOG":
                        response = kv_object.POP_LOG()
                    else:
                        response = "Enter only GET, SET, PUT, DELETE, GET_LOG, POP_LOG requests"
                except KeyNotFoundError as e:
                    response = str(e)  # Send error message back to client
                except KeyAlreadyExistsError as e:
                    response = str(e)  # Send error message back to client
                except Exception as e:
                    response = f'An error occurred: {str(e)}'
            # Send message back to client
            client_conn.sendall(response.encode())
        
        except ConnectionResetError:
            break

    client_conn.close()
    print(f"Connection {client_addr} closed.")

def server():
    HOST = "127.0.0.1"  # Localhost
    PORT = 5000       # Port number

    # Create a socket object
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print ("Socket created")

    # Bind the socket to the address and port
    s.bind((HOST, PORT))

    # Listen for incoming connections (max 5 clients in the queue)
    s.listen(5)
    print ("socket is listening")            

    while True:
        # Accept new connections
        client_conn, client_addr = s.accept()

        # Start a new thread to handle each client connection
        thread = threading.Thread(target=request_handling, args=(client_conn, client_addr))
        thread.start()

# @app.route('/<key>', methods=['GET'])
# def get_value(key):
#     value = kv_store.GET(key)
#     return jsonify(value)

# @app.route('/<key>', methods=['POST'])
# def set_value(key):
#     value = request.json.get('value')
#     result = kv_store.PUT(key, value)
#     return jsonify(result)



if __name__ == "__main__":
    run_benchmark()
    # server()
    app.run(host='127.0.0.1', port=5000)
