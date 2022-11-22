
import multiprocessing
import os 
import re
from typing import List
from xmlrpc.server import SimpleXMLRPCServer
from sys import stderr
from sqlalchemy import true
#from mapper import *
import json
import xmlrpc.client
import threading


server = None

def init_rpc_master_server(master_ip,master_port):
    global server
    with SimpleXMLRPCServer((master_ip, int(master_port)),logRequests=False,allow_none=True) as localServer:
        server = localServer
        server.register_introspection_functions()
        def give_status(status):
            print(f"{status} completed.")
            
        server.register_function(give_status, 'give_status')

        server.serve_forever()

def get_preprocesses_lines(input_file_loc):
    length_all_lines = 0
    input_lines =[]
    with open(input_file_loc,"r") as fd:
        all_lines = fd.readlines()
        length_all_lines = len(all_lines)
    with open(input_file_loc,"r") as fd2:
        line_count = 0
        byte_offset = 0
        while line_count < length_all_lines:
            line = fd2.readline()
            if line != "\n":
                line = re.sub(r"[^a-zA-Z0-9 ]","",line)
                input_lines.append(tuple((line,byte_offset)))
            line_count+=1
            byte_offset += len(line)
    return input_lines

def chunk_slicing(input_lines,no_of_mappers):
    chunks = []
    total_lines = len(input_lines)
    lines_per_mapper = total_lines // no_of_mappers
    chunk_start = 0
    chunk_end = lines_per_mapper + (total_lines % no_of_mappers)
    chunk_no = 0
    while chunk_no < no_of_mappers:
        chunks.append((chunk_start,chunk_end))
        chunk_start = chunk_end
        chunk_end = chunk_end + lines_per_mapper
        chunk_no+=1
    return chunks

def handle_each_mapper(config_filename,address_config,mapper_func,mapper_input,mapper_id,no_of_reducers):
    mapper_addr = address_config["mapper_"+str(mapper_id)]
    create_dir_cmd = "ssh "+str(mapper_addr)+" 'mkdir -p mapper_"+str(mapper_id)+"'"
    args = ["rsync", "./"+mapper_func, str(mapper_addr) + ":" + "mapper_"+str(mapper_id)+"/"+mapper_func]
    args2 = ["rsync", "./"+config_filename, str(mapper_addr) + ":" + "mapper_"+str(mapper_id)+"/"+config_filename]
    cmd = " ".join(args)
    cmd2 = " ".join(args2)
    os.system(create_dir_cmd)
    os.system(cmd)
    os.system(cmd2)
    os.system(f"rsync remote_process_starter.sh {str(mapper_addr)}:remote_process_starter.sh")
    database_server_ip = address_config["database"].split(" ")[0]
    database_server_port = address_config["database"].split(" ")[1]
    database_addr = "http://"+database_server_ip+":"+database_server_port
    s = xmlrpc.client.ServerProxy(database_addr)
    status = s.set_mapper_function("mapper_"+str(mapper_id),mapper_input)

    run_mapper_cmd = "ssh "+str(mapper_addr)+" 'bash remote_process_starter.sh mapper_"+str(mapper_id)+"/"+mapper_func+" mapper_"+str(mapper_id)+"/"+config_filename+" "+str(mapper_id)+" "+no_of_reducers+"'"
    os.system(run_mapper_cmd)  

def handle_each_reducer(config_filename,address_config,reducer_func,reducer_id):
    reducer_addr = address_config["reducer_"+str(reducer_id)]
    create_dir_cmd = "ssh "+str(reducer_addr)+" 'mkdir -p reducer_"+str(reducer_id)+"'"
    args = ["rsync", "./"+reducer_func, str(reducer_addr) + ":" + "reducer_"+str(reducer_id)+"/"+reducer_func]
    args2 = ["rsync", "./"+config_filename, str(reducer_addr) + ":" + "reducer_"+str(reducer_id)+"/"+config_filename]
    reducer_file_transfer_cmd = " ".join(args)
    config_file_transfer_cmd = " ".join(args2)
    os.system(create_dir_cmd)
    os.system(reducer_file_transfer_cmd)
    os.system(config_file_transfer_cmd)
    os.system(f"rsync remote_process_starter.sh {str(reducer_addr)}:remote_process_starter.sh")

    run_reducer_cmd = "ssh "+str(reducer_addr)+" 'bash remote_process_starter.sh reducer_"+str(reducer_id)+"/"+reducer_func+" reducer_"+str(reducer_id)+"/"+config_filename+" "+str(reducer_id)+"'"
    os.system(run_reducer_cmd)
    

def master_function (input_file_loc,no_of_mappers,no_of_reducers,mapper_func,reducer_func,address_config):
    

    fd = open(address_config,'r')
    addr_json = json.load(fd)
    fd.close()

    master_addr = addr_json['master']
    master_ip = master_addr.split(" ")[0]
    master_port = master_addr.split(" ")[1]
    master_server_process = threading.Thread(target = init_rpc_master_server,args=(str(master_ip),str(master_port)))
    master_server_process.start()
    #master_server_process.join()
    

    no_of_mappers = int(no_of_mappers)
    no_of_reducers = int(no_of_reducers)
    input_lines = get_preprocesses_lines(input_file_loc)
    #print(input_lines)
    chunks = chunk_slicing(input_lines,no_of_mappers)
    #print(f"chunk indices: {chunks}")
    mapper_processes: List[multiprocessing.Process] =[]
    reducer_processes = []
    print("Starting Mappers")
    for i in range(no_of_mappers):
        mapper_function = multiprocessing.Process(target=handle_each_mapper,args=(address_config,addr_json,mapper_func,input_lines[chunks[i][0]:chunks[i][1]],i,str(no_of_reducers),))
        mapper_function.start()
        mapper_processes.append(mapper_function)

    for proc in mapper_processes:
        proc.join()
        #print(proc.exitcode)
        if proc.exitcode !=0:
            proc.start()
            proc.join()
    print("Bringing barrier down.")
    print("Starting Reducers")    
    for i in range(no_of_reducers):
        reducer_function = multiprocessing.Process(target = handle_each_reducer,args =(address_config,addr_json,reducer_func,i,))
        reducer_function.start()
        reducer_processes.append(reducer_function)

    for proc in reducer_processes:
        proc.join()
        #print(proc.exitcode)
        if proc.exitcode !=0:
            proc.start()
            proc.join()
    global server
    server.shutdown()
    master_server_process.join()

    for i in range(no_of_mappers):
        mapper_ip = addr_json["mapper_"+str(i)]
        os.system("ssh "+mapper_ip+" 'rm -rf "+"mapper_"+str(i)+"'")

    for i in range(no_of_reducers):
        reducer_ip = addr_json["reducer_"+str(i)]
        os.system("ssh "+reducer_ip+" 'rm -rf "+"reducer_"+str(i)+"'")

    database_server_ip = addr_json["database"].split(" ")[0]
    database_server_port = addr_json["database"].split(" ")[1]

    os.system("rsync "+database_server_ip+":database/solution_database.json ~")
    os.system("ssh "+database_server_ip+" 'rm -rf "+"database'")

    
    database_addr = "http://"+database_server_ip+":"+database_server_port
    s = xmlrpc.client.ServerProxy(database_addr)
    s.exit_function()

    print("Bye Bye.")
    

    

    
    
    

    

    

    
   


