import socket

def send_request(command):
    HOST = '127.0.0.1'
    PORT = 5000

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        s.sendall(command.encode())
        response = s.recv(1024)
        print('Response from server:', response.decode())

if __name__ == "__main__":
    send_request('SET key1 value1')  # Change this to any command you want to test
    send_request('GET key1')