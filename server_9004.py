import socket
import threading
import argparse
import DistributedServer

ports = (9004, 9005, 9006)

def get_parser():
    parser = argparse.ArgumentParser(description='Distributed Server Node')
    parser.add_argument('critical_section', help='Filename of critical section')
    return parser


if __name__ == "__main__":
    arguments = get_parser().parse_args()
    port_number = 9004
    server_id = 1
    network = list(set(ports) - set([port_number]))
    cs_filename = arguments.critical_section
    server = DistributedServer.DistributedServer(server_id, port_number, cs_filename, network)
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Shutting down server on port %s" % server.server_address[1])
        server.shutdown()
