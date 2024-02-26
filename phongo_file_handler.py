'''
It implements the json file based storage for PhongoDB. 
'''
from bson import json_util
import fileinput
import os

root_datafiles_directory = "datafiles/"
def insert_records_into_file(database_name, collection_name, documents):
    full_directory = root_datafiles_directory+ "/" + database_name
    if (not os.path.isdir(full_directory)):
        # create the folder
        #os.mkdir(full_directory)
        os.makedirs(full_directory, exist_ok=True)

    file_name = full_directory + "/" + collection_name

    with open(file_name, "a") as f:
        for document in documents:
            f.write(json_util.dumps(json_util.loads(json_util.dumps(document)))+"\n")
    return True

def delete_records_in_file(database_name, collection_name, delete_filter, deleteMany):
    file_name = root_datafiles_directory + "/" + database_name + "/" + collection_name
    delete_count = 0
    
    # ineffective delete operations, rewrite the entire file
    for line in fileinput.input(file_name, inplace=True):
        line_object = json_util.loads(line)

        if delete_filter.items() <= line_object.items():
            if (((deleteMany == False) and (delete_count == 0)) or (deleteMany == True)):
                delete_count += 1
            else:
                print(json_util.dumps(json_util.loads(json_util.dumps(line_object))))    
        else:
            print(json_util.dumps(json_util.loads(json_util.dumps(line_object))))
        
    return delete_count

def update_records_in_file(database_name, collection_name, update_filter, update_fields, multi):
    # inefficient update
    file_name = root_datafiles_directory + "/" + database_name + "/" + collection_name
    update_count = 0
    matched_count = 0
    for line in fileinput.input(file_name, inplace=True):
        line_object = json_util.loads(line)

        if update_filter.items() <= line_object.items():
            matched_count += 1
            if (((multi == False) and (update_count == 0)) or (multi == True)):
                for key in update_fields.keys():
                    if (line_object[key] != update_fields[key]):    
                        line_object[key] = update_fields[key]
                update_count += 1

            print(json_util.dumps(json_util.loads(json_util.dumps(line_object))))
        else:
            print(json_util.dumps(json_util.loads(json_util.dumps(line_object))))
    # print(f"matched count, {matched_count}, update count {update_count}")
    return {"matched_count": matched_count, "update_count": update_count}

def read_records_from_the_file(database_name, collection_name, query_filter):
    file_name = root_datafiles_directory + "/" + database_name + "/" + collection_name

    # for every query, it does full collection scan only, inefficient search
    results = []
    with open(file_name, "r") as f:
        for line in f:
            line_object = json_util.loads(line)
            for item in line_object.items():
                pass
            
            # TODO: if query filter empty?
            if query_filter.items() <= line_object.items():
                results.append(line_object)

    return results