import sys
import json
import xmlrpc.client


def get_input(s,reducer_id):
    key = "reducer_"+str(reducer_id)
    reducer_input = s.get_reducer_function(key)
    return reducer_input

def count_ocurrence(reducer_input):
    count_dict = {}
    for word in reducer_input:
        if word[0] not in count_dict:
            count_dict[word[0]] = [word[1]]
        else:
            count_dict[word[0]].append(word[1])
    return count_dict
def add_to_database(s,count_dict):
    for key,value in count_dict.items():
        s.set_solution_function(key,value)



def main():
    config_file_addr = str(sys.argv[1])
    reducer_id = sys.argv[2]
    reducer_id = int(reducer_id)
    fd = open(config_file_addr,'r')
    addr_json = json.load(fd)
    fd.close() 

    database_server_ip = addr_json["database"].split("\n")[0]
    database_server_port = addr_json["database"].split("\n")[1]
    database_server_addr = "http://"+database_server_ip+":"+database_server_port
    s = xmlrpc.client.ServerProxy(database_server_addr)
    reducer_input = get_input(s,reducer_id)
    word_count = count_ocurrence(reducer_input)
    add_to_database(s,word_count)

    master_server_ip = addr_json["master"].split("\n")[0]
    master_server_port = addr_json["master"].split("\n")[1]
    master_server_addr = "http://"+master_server_ip+":"+master_server_port
    master_s = xmlrpc.client.ServerProxy(master_server_addr)
    master_s.give_status("reducer_"+str(reducer_id))

if __name__ == "__main__":
    main()