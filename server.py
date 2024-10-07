import socket
import threading
import key_value_store

# Function to handle client requests
def request_handling(client_conn, client_addr):
    print(f"Connection successful with {client_conn} and address {client_addr}")            
    kv_object = key_value_store.KV_store()
    
    while True:
        try:
            # Receiving request from the client
            request = client_conn.recv(1024).decode()
            if not request:
                break
            print(f"Request received from {client_addr} is {request}")

            request_split = request.split()

            if request_split[0] == "GET":
                return_value = kv_object.GET(request_split[1])
            elif request_split[0] == "PUT":
                return_value = kv_object.PUT(request_split[1], request_split[2])
            elif request_split[0] == "SET":
                return_value = kv_object.SET(request_split[1], request_split[2])
            elif request_split[0] == "DELTE":
                return_value = kv_object.DELETE(request_split[1])
            elif request_split[0] == "GET_LOG":
                return_value = kv_object.GET_LOG()
            elif request_split[0] == "POP_LOG":
                return_value = kv_object.POP_LOG()
            else:
                return_value = "Enter only GET, SET, PUT, DELETE, GET_LOG, POP_LOG requests"

            # Send message back to client
            client_conn.sendall(return_value.encode())
        
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
        client_conn, client_addr = server.accept()

        # Start a new thread to handle each client connection
        thread = threading.Thread(target=request_handling, args=(client_conn, client_addr))
        thread.start()

if __name__ == "__main__":
    server()
