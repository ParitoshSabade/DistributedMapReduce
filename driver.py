import sys
import multiprocessing
import time
from master import *
import threading

lock = threading.Lock()

def handle_database_server(address_config):
    
    fd = open(address_config,'r')
    addr_json = json.load(fd)
    fd.close() 

    database_ip = addr_json["database"].split(" ")[0]
    database_port = addr_json["database"].split(" ")[1]
    create_dir_cmd = "ssh "+str(database_ip)+" 'mkdir -p database'"
    args = ["rsync", "./database_server.py", str(database_ip) + ":" + "database/database_server.py"]
    args2 = ["rsync", "./"+address_config, str(database_ip) + ":" + "database/"+address_config]
    os.system(f"rsync database_starter.sh {database_ip}:database_starter.sh")
    cmd = " ".join(args)
    cmd2 = " ".join(args2)
    database_run_cmd = "ssh "+database_ip+" 'bash database_starter.sh "+"database/"+address_config+" 2>&1 >>database_log'"
    os.system(create_dir_cmd)
    os.system(cmd)
    os.system(cmd2)
    os.system(database_run_cmd)
    time.sleep(5)
    

def main():
    no_of_arguments = len(sys.argv)
    input_file_loc = sys.argv[1]
    no_of_mappers = sys.argv[2]
    no_of_reducers = sys.argv[3]
    mapper_func = sys.argv[4]
    reducer_func = sys.argv[5]
    address_config = sys.argv[6]

    # print(f"Input file location: {input_file_loc}")
    # print(f"No of mappers: {no_of_mappers}")
    # print(f"No of reducer: {no_of_reducers}")
    # print(f"Mapper func: {mapper_func}")
    # print(f"Reducer func: {reducer_func}")
    # print(f"Address config: {address_config}")
    
    proc = multiprocessing.Process(target=handle_database_server, args=(address_config,))
    proc.start()
    proc.join()
    

    proc2 = multiprocessing.Process(target=master_function, args=(input_file_loc,no_of_mappers,no_of_reducers,mapper_func,reducer_func,address_config,))
    proc2.start()
    proc2.join()

    

if __name__ == "__main__":
    main()