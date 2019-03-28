# Distributed System Mutual Exclusion Simulator


# Project Overview
This project demonstrates a basic distributed system using socket programming in Python. Nodes in the system follow the client server model to exchange messages. The simulator demonstrates an rpc protocol that allows a node to share it's identity amongst nodes, message synchronization using Lamport's algorithm, and attempts to handle deadlocks due to readers/ writers conflict while accessing a shared critical section. 

## Server Files
Each server has a file that connects to eaither 9004, 9005, or 9006. 
The name of the file denotes the server that it connects to.
server_9004.py: connects to port 9004
server_9005.py: connects to port 9005
server_9006.py: connects to port 9006

## Running a server - Server.py
Run command python server.py <cs_filename>

To run a server, run the command python server_9004.py --cs_filename 
1. Critical Section Filename (String): Name of the shared file servers will read/write from


server_9004.py critical_section.txt

## Client Files
Clients should only be ran after all servers in the system have been started
using the instructions from above. Each client will be assigned a
server that is hosted at either 9004, 6005, 9006. For write requests, the client 
will send data values from the specified input text input list.

## Running Clients - Client.py
Run command python client.py --input_filename --num_readers --num_writers --read_requests
1. Input File Name (String): Name of the critical section file or path
2. Number of Readers (Int)
3. Number of Writers  (Int)
4. Number of Read Requests per Reader (Int)

