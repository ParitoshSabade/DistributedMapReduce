import socket
import threading
import json
from json.decoder import JSONDecodeError
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer
import sys


lock = threading.Lock()

f = open("mapper_database.json",'w')
f.close()
f = open("reducer_database.json",'w')
f.close()
f = open("solution_database.json",'w')
f.close()
class SimpleThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass


def handle_set_mapper(key,value):
    lock.acquire()
    with open('mapper_database.json','r') as json_file:
        try:
            db = json.load(json_file)
            json_file.close()
        except JSONDecodeError:
            db = {}
    db[key] = value

    with open("mapper_database.json", "w") as outfile:
        json.dump(db, outfile)
        outfile.close()
    lock.release()
    return True

def handle_get_mapper(key):
    lock.acquire()
    with open('mapper_database.json','r') as json_file:
        try:
            db = json.load(json_file)
            json_file.close()
        except JSONDecodeError:
            db = {}
    lock.release()
    if key in db.keys():
        return db[key]
    else:
        return False

def handle_set_reducer(key,value):
    lock.acquire()
    with open('reducer_database.json','r') as json_file:
        try:
            db = json.load(json_file)
            json_file.close()
        except JSONDecodeError:
            db = {}

    if key in db.keys():
        temp_arr = db[key]
        temp_arr.extend(value)
        db[key] = temp_arr
    else:
        db[key] = value

    with open("reducer_database.json", "w") as outfile:
        json.dump(db, outfile)
        outfile.close()
    lock.release()
    return True

def handle_get_reducer(key):
    lock.acquire()
    with open('reducer_database.json','r') as json_file:
        try:
            db = json.load(json_file)
            json_file.close()
        except JSONDecodeError:
            db = {}
    lock.release()
    if key in db.keys():
        return db[key]
    else:
        return False

def handle_set_solution(key,value):
    lock.acquire()
    with open('solution_database.json','r') as json_file:
        try:
            db = json.load(json_file)
            json_file.close()
        except JSONDecodeError:
            db = {}
    db[key] = value

    with open("solution_database.json", "w") as outfile:
        json.dump(db, outfile)
        outfile.close()
    lock.release()
    return True

def handle_get_solution(key):
    lock.acquire()
    with open('solution_database.json','r') as json_file:
        try:
            db = json.load(json_file)
            json_file.close()
        except JSONDecodeError:
            db = {}
    lock.release()
    if key in db.keys():
        return db[key]
    else:
        return False
        

def main():
    config_file_name = sys.argv[1]
    fd = open(config_file_name,'r')
    addr_json = json.load(fd)
    fd.close()
    database_server_ip = addr_json["database"].split("\n")[0]
    database_port_no = addr_json["database"].split("\n")[1]
    with SimpleThreadedXMLRPCServer((str(database_server_ip), int(database_port_no))) as server:
        server.register_introspection_functions()
        print("Database server formed")
        def set_mapper_function(key,value):
            completion_flag = handle_set_mapper(key,value)
            return completion_flag
        server.register_function(set_mapper_function,'set_mapper_function')

        def get_mapper_function(key):
            value = handle_get_mapper(key)
            return value
        server.register_function(get_mapper_function,'get_mapper_function')

        def set_reducer_function(key,value):
            completion_flag = handle_set_reducer(key,value)
            return completion_flag
        server.register_function(set_reducer_function,'set_reducer_function')

        def get_reducer_function(key):
            completion_flag = handle_get_reducer(key)
            return completion_flag
        server.register_function(get_reducer_function,'get_reducer_function')

        def set_solution_function(key,value):
            completion_flag = handle_set_solution(key,value)
            return completion_flag
        server.register_function(set_solution_function,'set_solution_function')

        def get_solution_function(key):
            completion_flag = handle_get_solution(key)
            return completion_flag
        server.register_function(get_solution_function,'get_solution_function')

        def exit_function():
            server.shutdown()
            return("Database closed.")
        server.register_function(exit_function,'exit_function')

        server.serve_forever()
        
if __name__ == "__main__":
    main()