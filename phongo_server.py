import phongo_unpacker, phongo_reply
from collections import OrderedDict
import socket, threading, select, functools, errno, struct, traceback, queue, sys, json
import phongo_config


def synchronized(wrpped):
    # TODO refactor
    @functools.wraps(wrpped)
    def wrapper(self, *args, **kwargs):
        with self.lock:
            return wrpped(self, *args, **kwargs)

    return wrapper

class PhongoDB(object):
    def __init__(self):
        self.listening_socket = None
        self.listening_address = phongo_config.DB_SERVER_ADDRESS
        self.listening_port = phongo_config.DB_SERVER_PORT
        self.listening_full_address = None
        self.db_up_and_running = None
        #self.request_queue = queue.Queue()
        self.lock = threading.Lock()
        self.lock2 = threading.Lock()
        self.lock2.acquire()

    @synchronized
    def start(self):

        sock_list_response = socket.getaddrinfo(self.listening_address, self.listening_port, family=socket.AF_INET, type=socket.SOCK_STREAM, proto= 0, flags=socket.AI_PASSIVE)
        family, socktype, proto, _, socket_address = sock_list_response[0]
        self.listening_socket = socket.socket(family, socktype, proto)
        self.listening_socket.bind(socket_address)
        self.listening_socket.listen()
        port_bound = self.listening_socket.getsockname()[1]
        self.listening_full_address = (self.listening_address, port_bound)

        self.accept_thread = threading.Thread(target=self.accept_clients)
        self.accept_thread.daemon = True
        self.accept_thread.start()
        self.db_up_and_running = True


    def accept_clients(self):

        print("Ready to serve client requests ...")
        self.listening_socket.setblocking(True)
        while self.db_up_and_running:
            try:
                if select.select([self.listening_socket.fileno()], [], [], 1):
                    client, client_addr = self.listening_socket.accept()
                    client.setblocking(True)
                    print(f"Connection initiated. Client address: {client_addr}")

                    server_thread = threading.Thread(target=functools.partial(self.read_client_bytes, client, client_addr))
                    server_thread.daemon = True
                    server_thread.start()

            except socket.error as err:
                print("Error:" + str(err.errno))
                if err.errno != None and err.errno in (
                        err.ENOTSOCK, err.EWOULDBLOCK, err.EBADF, err.EAGAIN):
                    raise
            except Exception as inst:
                print(type(inst))    # the exception typer
                print(inst.args)     # arguments stored in .args
                print(inst)  
                traceback.print_exc()
                raise

    
    def wait_connections_forever(self):
        print("Server is waiting for connections ...")
        self.lock2.acquire()

    @synchronized
    def read_client_bytes(self, client, client_addr):
        
        while self.db_up_and_running:
            try:

                self.lock.release()

                msg_header = self.read_from_socket_with_length(client, phongo_config.MSG_HEADER_ATTRIBUTES["headerLength"])
                MSG_HEADER_SIZE = phongo_config.MSG_HEADER_ATTRIBUTES["headerLength"]

                # decoding the message header                
                (message_body_length, request_id, response_to, op_code) = phongo_unpacker.unpack_header(msg_header)
                msg_body = self.read_from_socket_with_length(client, message_body_length - MSG_HEADER_SIZE)
                
                # op_code must be either OP_CODE_FOR_OP_MSG or OP_QUERY
                if op_code == phongo_config.OP_CODE_FOR_OP_MSG:
                    client_request_payload, database, flags = phongo_unpacker.unpack_op_msg(msg_body)
                    response_data = phongo_reply.replyto_op_msg(client_request_payload, request_id)
                    client.sendall(response_data)

 
                elif op_code == phongo_config.OP_CODE_FOR_OP_QUERY:
                    # OP_QUERY is deprecated with 5.1 BUT still being used by some commands (hello, isMaster etc.)

                    (docs, flags, command_ns) = phongo_unpacker.unpack_op_query(msg_body)
                    actual_payload = json.loads(json.dumps(docs[0]))
                    
                    if 'ismaster' in actual_payload:
                        response_data = phongo_reply.reply_bytes_op_query_of_the_request("isMaster", request_id)
                        client.sendall(response_data)
                    elif 'whatsmyuri' in actual_payload or 'buildinfo' in actual_payload or 'buildInfo' in actual_payload or 'hello' in actual_payload or 'ping' in actual_payload or 'getParameter' in actual_payload or 'getLog' in actual_payload or 'atlasVersion' in actual_payload:
                        response_data = phongo_reply.reply_bytes_op_query_of_okay_response(request_id)
                        client.sendall(response_data)

            except socket.error as error:
                if error.errno in (errno.EBADF, errno.ECONNRESET, errno.ENOTSOCK):
                    # either we or the client disconnected
                    break
                    # we just end the client thread by exiting the loop however server process still is active
            except Exception as inst:
                print(type(inst))    # the exception typer
                print(inst.args)     # arguments stored in .args
                print(inst)  
                traceback.print_exc()
                raise
            finally:
                self.lock.acquire()

        print(f"Client disconnected: {client_addr}")
        client.close()

    # can be called to stop the server loop
    def stop_the_server(self):
        self.db_up_and_running = False

    def read_from_socket_with_length(self, sock, length):
        
        message_in_bytes = b''
        while length:
            chunk = sock.recv(length)
            if chunk == b'':
                raise socket.error(errno.ECONNRESET, 'closed')
                # TODO refactor
            length -= len(chunk)
            message_in_bytes += chunk

        return message_in_bytes
    