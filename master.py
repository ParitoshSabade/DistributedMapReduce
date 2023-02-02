
import multiprocessing
import os 
import re
from time import sleep

from xmlrpc.server import SimpleXMLRPCServer
from sys import stderr

#from mapper import *
import json
import xmlrpc.client
import threading
import sys


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
    mapper_name = "mapper-"+str(mapper_id)
    cmd1 = f"gcloud compute scp  {config_filename} {mapper_name}:~ --zone=us-west1-c --quiet --ssh-key-file=~/.ssh/{mapper_name}"
    cmd2 = f"gcloud compute scp  --quiet {mapper_func} {mapper_name}:~ --zone=us-west1-c --ssh-key-file=~/.ssh/{mapper_name}"
    cmd3 = f"gcloud compute scp --quiet remote_process_starter.sh {mapper_name}:~ --zone=us-west1-c --ssh-key-file=~/.ssh/{mapper_name}"
    print("transfering configfile")
    os.system(cmd1)
    print("transfering mapper file")
    os.system(cmd2)
    print("transfering starter file")
    os.system(cmd3)
    database_server_ip = address_config["database"].split("\n")[0]
    database_server_port = address_config["database"].split("\n")[1]
    database_addr = "http://"+database_server_ip+":"+database_server_port
    s = xmlrpc.client.ServerProxy(database_addr)
    status = s.set_mapper_function("mapper_"+str(mapper_id),mapper_input)
    print("running mapper")
    run_mapper_cmd = f"gcloud compute ssh --ssh-key-file=~/.ssh/{mapper_name}  {mapper_name} --zone us-west1-c --quiet --command='bash remote_process_starter.sh {mapper_func} {config_filename} {mapper_id} {no_of_reducers}'  -- -t"
    os.system(run_mapper_cmd)  

def handle_each_reducer(config_filename,address_config,reducer_func,reducer_id):
    reducer_name = "mapper-"+str(reducer_id)
    cmd1 = f"gcloud compute scp {config_filename} {reducer_name}:~ --zone=us-west1-c --quiet --ssh-key-file=~/.ssh/{reducer_name}"
    cmd2 = f"gcloud compute scp --quiet {reducer_func} {reducer_name}:~ --zone=us-west1-c --ssh-key-file=~/.ssh/{reducer_name}"
    cmd3 = f"gcloud compute scp --quiet remote_process_starter.sh {reducer_name}:~ --zone=us-west1-c --ssh-key-file=~/.ssh/{reducer_name}"
    os.system(cmd1)
    os.system(cmd2)
    os.system(cmd3)
    print("Please wait this will take few minutes")
    run_reducer_cmd = f"gcloud compute ssh --ssh-key-file=~/.ssh/{reducer_name} --quiet {reducer_name} --zone us-west1-c --command='bash remote_process_starter.sh {reducer_func} {config_filename} {reducer_id}'"
    os.system(run_reducer_cmd)  
    
def create_VM_instance(project_id,instance_name):
    instance_create_command = f"gcloud compute --project {project_id} instances create {instance_name} --image-family=debian-11 --image-project=debian-cloud --machine-type=e2-medium --zone us-west1-c"
    os.system(instance_create_command)    

def master_function (input_file_loc,no_of_mappers,no_of_reducers,mapper_func,reducer_func,address_config):
    

    fd = open(address_config,'r')
    addr_json = json.load(fd)
    fd.close()

    master_addr = addr_json['master']
    master_ip = master_addr.split("\n")[0]
    master_port = master_addr.split("\n")[1]
    master_server_process = threading.Thread(target = init_rpc_master_server,args=(str(master_ip),str(master_port)))
    master_server_process.start()
    #master_server_process.join()
    

    no_of_mappers = int(no_of_mappers)
    no_of_reducers = int(no_of_reducers)
    input_lines = get_preprocesses_lines(input_file_loc)
    #print(input_lines)
    chunks = chunk_slicing(input_lines,no_of_mappers)
    #print(f"chunk indices: {chunks}")
    mapper_processes = []
    reducer_processes = []
    print("Starting Mappers")
    for i in range(no_of_mappers):
        mapper_function = multiprocessing.Process(target=handle_each_mapper,args=(address_config,addr_json,mapper_func,input_lines[chunks[i][0]:chunks[i][1]],i,str(no_of_reducers),))
        mapper_function.start()
        mapper_processes.append(mapper_function)

    for proc in mapper_processes:
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
        
    global server
    server.shutdown()
    master_server_process.join()

    # for i in range(no_of_mappers):
    #     mapper_ip = addr_json["mapper_"+str(i)]
    #     os.system("ssh "+mapper_ip+" 'rm -rf "+"mapper_"+str(i)+"'")

    # for i in range(no_of_reducers):
    #     reducer_ip = addr_json["reducer_"+str(i)]
    #     os.system("ssh "+reducer_ip+" 'rm -rf "+"reducer_"+str(i)+"'")

    database_server_ip = addr_json["database"].split("\n")[0]
    database_server_port = addr_json["database"].split("\n")[1]

    # os.system("rsync "+database_server_ip+":database/solution_database.json ~")
    # os.system("ssh "+database_server_ip+" 'rm -rf "+"database'")

    
    database_addr = "http://"+database_server_ip+":"+database_server_port
    s = xmlrpc.client.ServerProxy(database_addr)
    s.exit_function()

    print("Bye Bye.")
def create_mappers_reducers(project_id,no,instance_type):
    if instance_type == "mappers":
        instance_str = "mapper-"
    else:
        instance_str = "reducer-"
    for i in range(no):
        instance_create_command = f"gcloud compute --project {project_id} instances create {instance_str+str(i)} --image-family=debian-11 --image-project=debian-cloud --machine-type=e2-medium --zone us-west1-c"
        os.system(instance_create_command)


def main():
    input_info = sys.argv[1]
    ip_config = sys.argv[2]
    input_fd = open(input_info,'r')
    input_json = json.load(input_fd)
    input_fd.close() 

    project_id = input_json["project_id"]
    
    os.system(f"gcloud auth activate-service-account paritosh-service-account@{project_id}.iam.gserviceaccount.com --key-file=key.json --project={project_id}")
    #no_of_arguments = len(sys.argv)
    input_file= input_json["input_file_location"]
    no_of_mappers = input_json["no_of_mappers"]
    no_of_reducers = input_json["no_of_reducers"]
    mapper_func = input_json["mapper_file"]
    reducer_func = input_json["reducer_file"]
    create_mappers_reducers(project_id,int(no_of_mappers),"mappers")
    sleep(5)
    create_mappers_reducers(project_id,int(no_of_reducers),"reducers")
    sleep(10)
    master_function(input_file,no_of_mappers,no_of_reducers,mapper_func,reducer_func,ip_config)

if __name__ == "__main__":
    main()
    

    

    
    
    

    

    

    
   


