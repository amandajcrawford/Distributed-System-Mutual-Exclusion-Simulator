import threading
import SocketServer
import requestQueue
import socket
import time
import select
import re
from ast import literal_eval

l_time = 0
network = {}
c_pattern = '(.*) \| (.*) \| (.*) \| (.*)'
b_pattern = '([a-zA-z]*) \| ([a-zA-z]*) \| (.*)'
client_parser = re.compile(c_pattern)
broadcast_parser = re.compile(b_pattern)

class DistributedServerHandler(SocketServer.BaseRequestHandler):
    NO_OP = 0
    READ_OP = 1
    WRITE_OP = 2
    B_QUEUE = 3
    B_COMPLETED = 4
    B_STARTED = 5
    B_POLL = 6
    B_VOTE = 7

    ''' Handler for server '''

    def handle(self):
        self.response = ""
        self.op_type = self.NO_OP
        self.mid = None
        self.msg = {}
        self.connection()
        self.terminate = False
        self.inputs = [self.request]
        self.request.setblocking(0)
        while not self.terminate:
            (read, write, exceptions) = select.select(self.inputs, self.inputs, self.inputs, 1)
            for r in read:
                data = r.recv(1048)
                if data:
                    self.execute(data)

            for receiver in write:
                if self.is_response_ready():
                    op = self.server.current_cs_holder['op']
                    mid = self.server.current_cs_holder['mid']
                    if op == self.WRITE_OP:
                        self.send_write_data()
                    if op == self.READ_OP:
                        self.send_read_data()
                    print("Message Sent To Client: %s" % self.response)
                    self.request.sendall(self.response)
                    self.terminate = True

            for err in exceptions:
                self.terminate = True
        return

    def execute(self, msg):
        print('Message Received From %s: %s' % (self.client_address[1], msg)) + '\n'
        if self.is_client_msg(msg):
            self.handleCSRequest(msg)
        if self.is_broadcast_msg(msg):
            self.handleBroadcast(msg)
        return

    def handleCSRequest(self, msg):
        self.parse_client_msg(msg)        # Parse Client request
        self.timestamp_msg(msg)         # Time Stamp Message
        self.generate_message_id()
        self.send_broadcast('ADD')           # Broadcast message
        self.server.add_mid(self.msg['mid'])
        self.queue_message()         # Add message to queue
        return

    def handleBroadcast(self, msg):
        self.parse_broadcast(msg)
        if self.broadcast_op(msg) == self.B_QUEUE:
            self.timestamp_msg(msg)
            self.queue_message()
        if self.broadcast_op(msg) == self.B_STARTED:
            self.server.set_cs_in_use()
        if self.broadcast_op(msg) == self.B_COMPLETED:
            self.server.messageQueue.popRequest()
            self.server.set_cs_open()
        if self.broadcast_op(msg) == self. B_POLL:
            self.handle_ballot()
        if self.broadcast_op(msg) == self. B_VOTE:
            self.handle_vote()
        self.terminate = True
        return

    def parse_client_msg(self, msg):
        global client_parser
        self.msg = {}
        matches = client_parser.search(msg)
        msg_op = matches.group(2)
        self.set_op(msg_op)
        self.msg['sid'] = self.server.id
        self.msg['op'] = self.op_type
        self.msg['port'] = self.server.my_port
        self.msg['cid'] = int(re.search('\d+', matches.group(3)).group())
        if self.op_type == self.WRITE_OP:
            self.msg['data'] = int(re.search('\d+', matches.group(4)).group())
            self.msg['actual'] = msg
        return

    def parse_broadcast(self, msg):
        global broadcast_parser
        matches = broadcast_parser.search(msg)
        self.op_type = self.broadcast_op(matches.group(2))
        self.msg = literal_eval(matches.group(3))
        return

    def handle_ballot(self):
        self.server.handle_election(self.msg)
        return

    def handle_vote(self):
        self.server.handle_vote()
        return

    def send_broadcast(self, broadcast_type):
        print("Broadcasting\n\n")
        print("=========================\n\n")
        broadcast = 'BROADCAST | %s | %s' % (broadcast_type, self.msg)
        print(broadcast)
        for port in self.server.network_ports:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                time.sleep(1)
                sock.connect((socket.gethostname(), port))
                sock.sendall(broadcast)
                print('Broadcast %s Successful'% broadcast_type)
            except socket.error as error:
                print("Failed to connect to server: %s for %s broadcast" % (port, broadcast_type))
                print(error.strerror)
        return

    def is_response_ready(self):
        if self.server.current_cs_holder:
            if 'status' in self.server.current_cs_holder:
                mid = self.server.current_cs_holder['mid']
                if mid == self.msg['mid']:
                        return True
        return False

    def queue_message(self):
        self.server.enqueue_msg(self.msg)
        return

    def send_write_data(self):
        self.response = "COMPLETED"
        return

    def send_read_data(self):
        data = self.server.current_cs_holder['cs']
        self.response = "COMPLETED | %s" % data
        return

    def timestamp_msg(self, msg):
        global l_time
        if 'time' in self.msg:
            l_time = max(self.msg['time'], l_time)
            self.msg['time'] = l_time
        else:
            self.msg['time'] = l_time
            self.msg['actual'] = msg + "| TIME: " + str(l_time)
        return

    def set_op(self, msg):
        if self.is_write_request(msg):
            self.op_type = self.WRITE_OP
        else:
            self.op_type = self.READ_OP
        return

    def broadcast_op(self, msg):
        if self.is_queue_broadcast(msg):
            return self.B_QUEUE

        if self.is_complete_broadcast(msg):
            return self.B_COMPLETED

        if self.is_reserved_broadcast(msg):
            return self.B_STARTED

        if self.is_poll_broadcast(msg):
            return self.B_POLL

        if self.is_vote_broadcast(msg):
            return self.B_VOTE
        return

    def is_queue_broadcast(self, msg):
        return -1 < str(msg).find('ADD')

    def is_complete_broadcast(self, msg):
        return -1 < str(msg).find('FINISHED')

    def is_reserved_broadcast(self, msg):
        return -1 < str(msg).find('STARTED')

    def is_poll_broadcast(self, msg):
        return -1 < str(msg).find('CANDIDATE')

    def is_vote_broadcast(self, msg):
        return -1 < str(msg).find('ELECTED')

    def is_read_request(self, msg):
        return -1 < str(msg).find('READ')

    def is_write_request(self, msg):
        return -1 < str(msg).find('WRITE')

    def is_client_msg(self, msg):
        return -1 < str(msg).find('CLIENT') < 6

    def is_broadcast_msg(self, msg):
        return -1 < str(msg).find('BROADCAST') < 9

    def connection(self):
        print 'New client added %s' % self.client_address[1] + '\n'

    def generate_message_id(self):
        global l_time
        self.mid = str(self.client_address[1]) + str(l_time + 1) + str(self.server.id)
        self.msg['mid'] = self.mid

class DistributedServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    NO_OP = 0
    READ_OP = 1
    WRITE_OP = 2
    CS_OPEN = 4
    POLLING = 5
    CS_IN_USE = 6
    CS_FAILED = 7


    def __init__(self, sid, port_number, cs_file, network=[]):
        address = ('0.0.0.0', port_number)
        self.local_requests = set()
        self.my_port = port_number
        self.network_ports = network
        self.id = sid
        self.messageQueue = requestQueue.RequestQueue()
        self.current_cs_holder = None
        self.cs_filename = cs_file
        self.cs_state = self.CS_OPEN
        self.cs_data = None
        self.votes = 0
        self.poll_session = False
        self.prev_cs_holder = True
        self.changed = False
        self.request_queue_size = 200
        SocketServer.TCPServer.__init__(self, address, DistributedServerHandler)
        return

    def server_activate(self):
        global l_time
        try:
            thread = threading.Thread(target=self.mutual_exclusion)
            thread.start()
        except:
            print('Failed')
        print("Server Ready!")
        l_time = l_time + 1
        return SocketServer.TCPServer.server_activate(self)

    def handle_request(self):
        global l_time
        l_time = l_time + 1
        return SocketServer.TCPServer.handle_request(self)

    def process_request(self, request, client_address):
        global l_time
        l_time = l_time + 1
        return SocketServer.TCPServer.process_request(self, request, client_address)

    def finish_request(self, request, client_address):
        global l_time
        l_time = l_time + 1
        return SocketServer.TCPServer.finish_request(self, request, client_address)

    def add_mid(self, mid):
        self.local_requests.add(mid)

    def remove_mid(self, mid):
        self.local_requests.remove(mid)

    def handle_election(self, msg):
        time = msg['time']
        sid = msg['sid']
        mid = msg['mid']

        if mid == self.current_cs_holder['mid']:
            self.elect_candidate(msg)

    def handle_vote(self):
        self.votes = self.votes + 1
        return

    def elect_candidate(self, msg):
        broadcast = '\nBROADCAST | ELECTED | %s' % (msg) + '\n\n'
        port = msg['port']
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            time.sleep(.5)
            sock.connect((socket.gethostname(), port))
            sock.sendall(broadcast)
        except socket.error as error:
            print("Failed to connect to server: %s " % port + '\n')
            print(error.strerror)
        return

    def set_cs_open(self):
        self.cs_state = self.CS_OPEN

    def set_cs_in_use(self):
        self.cs_state = self.CS_IN_USE

    def set_cs_fail(self):
        self.cs_state = self.CS_FAILED

    def enqueue_msg(self, msg):
        self.messageQueue.add(msg)

    def dequeue_msg(self):
        return self.messageQueue.pop()

    def read_file(self):
        global l_time
        critical_section = open(self.cs_filename, 'r')
        cs_lines = critical_section.readlines()
        if len(cs_lines) > 0:
            final_line = cs_lines[-1]
            final_sum = final_line[final_line.find(':')+1:]
        else:
            final_sum = '0'
        critical_section.close()
        print('Final sum %s' % final_sum)
        return int(final_sum)

    def write_file(self, input_int):
        global l_time
        last_sum = self.read_file()
        new_sum = last_sum + input_int
        critical_section = open(self.cs_filename, 'a')
        critical_section.write(str(l_time) + ':' + str(new_sum) + '\n')
        critical_section.close()
        return

    def poll_cs_access(self):
        elected_mid = self.current_cs_holder['mid']
        print('Broadcasting CS Access Poll for message %s ' % elected_mid)
        self.broadcast_cs_status('CANDIDATE')
        self.poll_session = True

    def process_msg(self):
        print('Broadcasting CS In Use Status \r')
        self.broadcast_cs_status('STARTED')
        op = self.current_cs_holder['op']
        mid = self.current_cs_holder['mid']
        self.set_cs_in_use()
        print("Processing %s message \n" % op)
        if op == self.WRITE_OP:
            val = self.current_cs_holder['data']
            print('Writing value to critical section: %s \r' % val)
            self.write_file(val)
        else:
            self.current_cs_holder['cs'] = self.read_file()
            print('Reading value from critical section: %s \r' )
        self.current_cs_holder['status'] = True
        self.set_cs_open()
        print('Broadcasting CS Open Status\n')
        self.broadcast_cs_status('FINISHED')
        self.messageQueue.popRequest()

    def broadcast_cs_status(self, cs_status):
        broadcast = '\nBROADCAST | %s | %s' % (cs_status, self.current_cs_holder) + '\n\n'
        for port in self.network_ports:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                time.sleep(.5)
                sock.connect((socket.gethostname(), port))
                sock.sendall(broadcast)
                print('Broadcast CS Status Successful')
            except socket.error as error:
                print("Failed to connect to server: %s " % port + '\n')
                print(error)
                print(error.strerror)
        return

    def mutual_exclusion(self):
        while True:
            # Get the message at the top of pqueue
            if self.messageQueue.queueSize > 0:
                # if self.cs_state == self.CS_OPEN:
                if self.messageQueue.get_first() != self.prev_cs_holder:
                    self.prev_cs_holder = self.current_cs_holder
                    self.polling_session = False
                    self.votes = 0
                    self.changed = True
                    # print("Next Message To Be Processed: %s " % self.messageQueue.get_first()['mid'] + '\n')
                    # print("================================= \n")
                self.current_cs_holder = self.messageQueue.get_first()
                mid = self.current_cs_holder['mid']
                # I want to handle it for my connected client
                if mid in self.local_requests:
                    if not self.poll_session:
                        self.poll_cs_access() #poll for access
                    # if receive votes - access
                    if self.votes == 2:
                        print("=========Processing Message==============")
                        self.process_msg()

                else:
                    if self.changed:
                        self.elect_candidate(self.current_cs_holder)
                        self.changed = False


