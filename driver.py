import sys
import multiprocessing
from time import sleep
# from master import *
import threading
import json
import os

lock = threading.Lock()

def create_network():
    os.system("gcloud compute networks create default")
    os.system("gcloud compute firewall-rules create default-allow-icmp --network default --allow icmp --source-ranges 0.0.0.0/0")
    os.system("gcloud compute firewall-rules create default-allow-ssh --network default --allow tcp:22 --source-ranges 0.0.0.0/0")
    os.system("gcloud compute firewall-rules create default-allow-internal --network default --allow tcp:0-65535,udp:0-65535,icmp --source-ranges 10.128.0.0/9")

def create_service_account(name, display_name):
    """Creates a service account."""

    run_command = f'gcloud iam service-accounts create {name} --display-name="{display_name}"'
    os.system(run_command)

def add_role(service_account_name,project_id):
    service_account_email = service_account_name+"@"+project_id+".iam.gserviceaccount.com"
    add_role_command = "gcloud projects add-iam-policy-binding "+project_id+" --member='serviceAccount:"+service_account_email+"' --role='roles/owner'"
    os.system(add_role_command)
    
def create_VM_instance(project_id,instance_name):
    instance_create_command = f"gcloud compute --project {project_id} instances create {instance_name} --image-family=debian-11 --image-project=debian-cloud --machine-type=e2-medium --zone us-west1-c"
    os.system(instance_create_command)

def get_instance_ip(instance_name):
    ip_command = f"gcloud compute instances describe {instance_name} --format='get(networkInterfaces[0].networkIP)' --zone=us-west1-c"
    ip_address = os.popen(ip_command).read()
    return ip_address

def handle_database_server(address_config):
    
    cmd1 = f"gcloud compute scp database_server.py database-server:~ --zone=us-west1-c --quiet"
    cmd2 = f"gcloud compute scp {address_config} database-server:~ --zone=us-west1-c"
    cmd3 = f"gcloud compute scp database_starter.sh database-server:~ --zone=us-west1-c"
    database_run_cmd = "gcloud compute ssh database-server --zone us-west1-c --command='bash database_starter.sh "+address_config+" 2>&1 >>database_log'"
    #os.system(create_dir_cmd)
    sleep(5)
    os.system(cmd1)
    os.system(cmd2)
    os.system(cmd3)
    os.system(database_run_cmd)
    
    sleep(5)


def master_start(input_info,address_config):
    input_fd = open(input_info,'r')
    input_json = json.load(input_fd)
    input_fd.close() 
    
    #no_of_arguments = len(sys.argv)
    input_file_loc = input_json["input_file_location"] 
    mapper_func = input_json["mapper_file"]
    reducer_func = input_json["reducer_file"]
    cmd1 = f"gcloud compute scp {address_config} master-server:~ --zone=us-west1-c --quiet"
    cmd2 = f"gcloud compute scp {input_info} master-server:~ --zone=us-west1-c"
    cmd3 = f"gcloud compute scp {input_file_loc} master-server:~ --zone=us-west1-c"
    cmd4 = f"gcloud compute scp {mapper_func} master-server:~ --zone=us-west1-c"
    cmd5 = f"gcloud compute scp {reducer_func} master-server:~ --zone=us-west1-c"
    cmd6 = f"gcloud compute scp master.py master-server:~ --zone=us-west1-c"
    cmd7 = f"gcloud compute scp key.json master-server:~ --zone=us-west1-c"
    cmd8 = f"gcloud compute scp remote_process_starter.sh master-server:~ --zone=us-west1-c"
    
    cmd_master_run = f"gcloud compute ssh master-server --zone us-west1-c --command='python3 master.py {input_info} {address_config}' -- -t"

    os.system(cmd1)
    os.system(cmd2)
    os.system(cmd3)
    os.system(cmd4)
    os.system(cmd5)
    os.system(cmd6)
    os.system(cmd7)
    os.system(cmd8)
    
    os.system(cmd_master_run)
    sleep(5)
    

def main():
    input_fd = open("input_info.json",'r')
    input_json = json.load(input_fd)
    input_fd.close() 
    ip_dict = {}
    input_info = "input_info.json"
    #no_of_arguments = len(sys.argv)
    input_file_loc = input_json["input_file_location"]
    no_of_mappers = int(input_json["no_of_mappers"])
    no_of_reducers = int(input_json["no_of_reducers"])
    mapper_func = input_json["mapper_file"]
    reducer_func = input_json["reducer_file"]
    #address_config = sys.argv[6]

    project_id = input_json["project_id"]
    service_name = "paritosh-service-account"
    service_display_name = "paritosh-sa"

    print("Creating google cloud network")
    create_network()

    print("Creating service account")
    create_service_account(service_name, service_display_name)
    print("Assigning role to service account")
    add_role(service_name,project_id)

    service_account_email = service_name+"@"+project_id+".iam.gserviceaccount.com"
    os.system(f"gcloud iam service-accounts keys create key.json --iam-account={service_account_email}")

    create_VM_instance(project_id,"database-server")
    create_VM_instance(project_id,"master-server")
    sleep(10)
    db_server_ip = get_instance_ip("database-server")
    master_server_ip = get_instance_ip("master-server")
    ip_dict["master"] = master_server_ip + "8090"
    ip_dict["database"] = db_server_ip + "5000"
    fd_config = open("ip_config.json",'w')
    json.dump(ip_dict,fd_config)
    fd_config.close()
    address_config = "ip_config.json"
    
    # print(f"Input file location: {input_file_loc}")
    # print(f"No of mappers: {no_of_mappers}")
    # print(f"No of reducer: {no_of_reducers}")
    # print(f"Mapper func: {mapper_func}")
    # print(f"Reducer func: {reducer_func}")
    # print(f"Address config: {address_config}")
    
    proc = multiprocessing.Process(target=handle_database_server, args=(address_config,))
    proc.start()
    proc.join()
    

    proc2 = multiprocessing.Process(target=master_start, args=(input_info,address_config,))
    proc2.start()
    proc2.join()

    print("Deleting servers and cleaning . . .")
    os.system("gcloud compute scp database-server:~/solution_database.json . --zone=us-west1-c")
    os.system(f'gcloud projects remove-iam-policy-binding {project_id} --member="serviceAccount:{service_account_email}" --role="roles/owner" --quiet')
    os.system(f'gcloud iam service-accounts delete {service_account_email} --quiet')
    zone = "us-west1-c"
    instance_names = ["master-server","database-server"]
    os.system(f"gcloud compute instances delete {' '.join(instance_names)} --project={project_id} --quiet --zone {zone}")
    for i in range(no_of_mappers):
        os.system(f"gcloud compute instances delete mapper-{str(i)} --project={project_id} --quiet --zone {zone}")

    for i in range(no_of_reducers):
        os.system(f"gcloud compute instances delete reducer-{str(i)} --project={project_id} --quiet --zone {zone}")

    

if __name__ == "__main__":
    main()