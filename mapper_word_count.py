import sys
import json
import xmlrpc.client


def get_input(s,mapper_id):
    key = "mapper_"+str(mapper_id)
    mapper_input = s.get_mapper_function(key)
    return mapper_input

def get_words(mapper_input):
    words = []
    for line in mapper_input:
        temp_list2 = [] 
        offset = line[1]
        temp_list = line[0].split(" ")
        for i in temp_list:
            if i != "":
                temp_list2.append((i,offset))
                offset+=len(i)+1
        words.extend(temp_list2)
    return words

def dist_to_reducer(s,words,no_of_reducers):
    mapper_result = [[] for i in range(no_of_reducers)]
    for word in words:
        if word[0] != "":
            hash_key = (len(word[0]))%no_of_reducers
            #print(f"word:hash  {word}:{hash_key}")
            mapper_result[hash_key].append(word)
    i = 0        
    for i in range(no_of_reducers):
        key = "reducer_"+str(i)
        value = mapper_result[i]
        s.set_reducer_function(key,value)






def main():
    config_file_addr = str(sys.argv[1])
    mapper_id = sys.argv[2]
    no_of_reducers = int(sys.argv[3])
    mapper_id = int(mapper_id)
    fd = open(config_file_addr,'r')
    addr_json = json.load(fd)
    fd.close() 

    database_server_ip = addr_json["database"].split(" ")[0]
    database_server_port = addr_json["database"].split(" ")[1]
    database_server_addr = "http://"+database_server_ip+":"+database_server_port
    s = xmlrpc.client.ServerProxy(database_server_addr)
    mapper_input = get_input(s,mapper_id)
    words = get_words(mapper_input)

    #print(f"words:          {words}")
    dist_to_reducer(s,words,no_of_reducers)
    
    master_server_ip = addr_json["master"].split(" ")[0]
    master_server_port = addr_json["master"].split(" ")[1]
    master_server_addr = "http://"+master_server_ip+":"+master_server_port
    master_s = xmlrpc.client.ServerProxy(master_server_addr)
    master_s.give_status("mapper_"+str(mapper_id))


if __name__ == "__main__":
    main()