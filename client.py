# Assumptions:
# client_id starts at 0
# clients know input file path

import socket
import select
import time
import random
import argparse
import threading

class WriterClient:
    def __init__(self, client_id, port_number, file_name, num_clients, schedule):
        self.id = client_id
        self.assigned_server = port_number
        self.input_path = file_name
        self.client_count = num_clients
        self.schedule = schedule
        self.connections = []
        self.number_set = []
        self.number_set_idx = 0
        self.get_number_set()
        print("Starting writer client %s" % self.id)

    def request(self):
        while len(self.number_set) > self.number_set_idx:
            connection = self.__connect(True)
            msg = "CLIENT | WRITE | ID: "+str(self.id)+" | " + str(self.number_set[self.number_set_idx])
            connection.sendall(msg)
            print('Client %s sent write request %s to CS' % (self.id, self.number_set[self.number_set_idx]))
            self.connections.append(connection)
            self.number_set_idx = self.number_set_idx + 1
            (read, write, exceptions) = select.select(self.connections, [], [], 1)
            for conn in read:
                data = conn.recv(1024)
                if data:
                    if data == "COMPLETED":
                        print("Write request completed.")
                        self.connections.remove(conn)
                    else:
                        print(data)

            for err in exceptions:
                err.close()
            time.sleep(self.schedule)

    def get_number_set(self):
        num_file = open(self.input_path, 'r')
        lines = num_file.readlines()
        for i in range(0, len(lines)):
            if i % self.client_count == self.id:
                self.number_set.append(int(lines[i]))
        num_file.close()

    def __connect(self, retry):
        try:
            to_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            to_socket.connect((socket.gethostname(), self.assigned_server))
            print("Writer client %s connected to server %s" % (self.id, self.assigned_server))
        except socket.error:
            if retry:
                time.sleep(2)
                return self.__connect(self.assigned_server, False)
            else:
                print("Client %s Socket Error: %s" % (self.id, socket.gaierror.message))
        finally:
            return to_socket

    def close(self):
        self.connection.close()



class ReaderClient:
    def __init__(self, client_id, port_number, read_num, schedule):
        self.id = client_id
        self.assigned_server = port_number
        print("Starting reader client %s" % self.id)
        self.num_reads = read_num
        self.curr_read_idx = 0
        self.connections = []
        self.schedule = schedule

    def request(self):
        while self.curr_read_idx < self.num_reads:
            connection = self.__connect(True)
            msg = "CLIENT | READ | ID: "+str(self.id) + " | "
            connection.sendall(msg)
            self.connections.append(connection)
            print('Client %s sent read request to CS' % self.id)
            self.curr_read_idx = self.curr_read_idx + 1
            (read, write, exceptions) = select.select(self.connections, self.connections, [], 1)
            for conn in read:
                data = conn.recv(1024)
                if data:
                    print("Client %s read request completed: %s" % (self.id, data))
                    self.connections.remove(conn)

            for err in exceptions:
                err.close()
            time.sleep(self.schedule)

    def __connect(self, retry):
        try:
            to_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            to_socket.connect((socket.gethostname(), self.assigned_server))
            print("Reader client %s connected to server %s" % (self.id, self.assigned_server))
        except socket.error:
            if retry > 0:
                time.sleep(1)
                return self.__connect(self.assigned_server, False)
            else:
                print("Client %s Socket Error: %s" % (self.id, socket.gaierror.message))
        finally:
            return to_socket

    def close(self):
        self.connection.close()


ports = (9004, 9005, 9006)
random.seed(11)


def main():
    #open('cs.txt', 'w').close()
    arguments = get_parser().parse_args()
    file_name = arguments.file_name
    w_counts = arguments.num_writers
    r_counts = arguments.num_readers
    read_num = arguments.num_read_requests
    clients = list()
    client_threads = list()
    p_assign = 0
    cid = 1

    for w in range(w_counts):
        port_num = p_assign % 3
        p_assign = p_assign + 1
        port_assignment = ports[port_num]
        schedule = random.randint(20, 60)
        writer = WriterClient(cid, port_assignment, file_name, w_counts, schedule)
        clients.append(writer)
        cid = cid + 1

    for r in range(r_counts):
        port_num = p_assign % 3
        p_assign = p_assign + 1
        port_assignment = ports[port_num]
        schedule = random.randint(20, 30)
        reader = ReaderClient(cid, port_assignment, read_num, schedule)
        clients.append(reader)
        cid = cid + 1

    random.shuffle(clients)
    for c in clients:
        try:
            c_thread = threading.Thread(target=c.request)
            client_threads.append(c_thread)
            c_thread.start()
            time.sleep(5)
        except Exception as e:
            print(str(e))


def get_parser():
    parser = argparse.ArgumentParser(description='Clients')
    parser.add_argument('file_name', type=str)
    parser.add_argument('num_readers', type=int)
    parser.add_argument('num_writers', type=int)
    parser.add_argument('num_read_requests', type=int)

    return parser


if __name__ == "__main__":
    main()
