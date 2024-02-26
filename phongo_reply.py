from bson import json_util
from bson.objectid import ObjectId
import phongo_file_handler
import phongo_packer
import phongo_config
from pathlib import Path
import os 

root_datafiles_directory = phongo_config.ROOT_DATAFILES_DIRECTORY
def get_collections_info(db_name):
    collection_files = []
    for x in os.walk(f"{root_datafiles_directory}/{db_name}"):
        collection_files = x[2]
    
    collections = []
    for collection_file in collection_files:
        collection = {
            "name": collection_file,
            "type": "collection"
        }
        collections.append(collection)

    return collections

def get_databases_info():

    folders = []
    for x in os.walk(root_datafiles_directory):
        folders = x[1]
        break

    databases = []
    for folder in folders:
        root_directory = Path(f'{root_datafiles_directory}/{folder}')
        sumsize = sum(f.stat().st_size for f in root_directory.glob(f'**/*') if f.is_file())
        db_obj = {
            "name": folder,
            "sizeOnDisk": sumsize
        }
        databases.append(db_obj)

    return_object = {"databases": databases, "ok": 1}
    return return_object;

def replyto_op_msg(payload_document, request_id):
    
    if ('endSessions' in dict(payload_document)):
        response_doc = {"ok": 1}
    else:
        
        actual_payload = json_util.loads(json_util.dumps(payload_document))
        print("Message received from the client:"+str(dict(actual_payload)))
        if 'ismaster' in actual_payload:
            response_doc = {"ismaster": True, "minWireVersion": 0, "maxWireVersion": 100, "ok": 1, "version": 17, "serverVersion": 18, "apiVersion": 19, "buildInfo": {"version":11}}
        elif 'whatsmyuri' in actual_payload or 'hello' in actual_payload or 'ping' in actual_payload or 'getParameter' in actual_payload or 'getLog' in actual_payload or 'atlasVersion' in actual_payload:
            response_doc = {"ok": 1}
        elif 'connectionStatus' in actual_payload or 'showPrivileges' in actual_payload: # redundant
            response_doc = {"ok": 1} 
        elif 'hostInfo' in actual_payload: 
            response_doc = {"ok": 1}
        elif 'authInfo' in actual_payload: # redundant
            response_doc = {"ok": 1, 'authInfo': {"authenticatedUsers": {}, "authenticatedUserRoles": {}, "authenticatedUserPrivileges":{}}}
        elif 'top' in actual_payload: # redundant
            response_doc = {"ok": 1}    
        elif 'buildInfo' in actual_payload or 'buildinfo' in actual_payload:
            #response_doc = {"version":"7.0.1", "ok": 1}
            response_doc = {"version": "7.2.0", "ok": 1} # change it as you wish 
        elif 'listDatabases' in actual_payload:
            response_doc = get_databases_info()
        elif 'listCollections' in actual_payload:
            db_name = actual_payload['$db']
            docs = get_collections_info(db_name)
            #print("list databases")
            #response_doc = {"collections": [{"name": "a1b2c3", "type": "collection"}, {"name": "xyz", "type": "collection"}], "ok": 1}
            
            #docs = [{"name": "aaa", "type": "collection"}, {"name": "dsadsadbbbc", "type": "collection"}]
            response_doc = {'cursor': {'firstBatch': docs, 'id': 0, 'ns': 'test.aaa'}, 'ok': 1.0}
        elif 'aggregate' in actual_payload:
            if actual_payload['aggregate'] == 'atlascli':
                response_doc = {"ok": 1}
            else:
                docs = []
                pipeline = actual_payload['pipeline']
                database_name = actual_payload['$db']
                collection_name = actual_payload['aggregate']

                stage_name = list(pipeline[0].keys())[0]

                if (stage_name) == "$match":
                    query_filter = pipeline[0]['$match']
                    docs = phongo_file_handler.read_records_from_the_file(database_name, collection_name, query_filter)
                    response_doc = {'cursor': {'firstBatch': docs, 'id': 0, 'ns': 'test.aaa'}, 'ok': 1.0}
                else:
                    response_doc = { "ok": 0, "code":"001", "errmsg": f"Unrecognized pipeline stage name: '{stage_name}'"}


        elif 'insert' in actual_payload:
            # and there should be insert, documents, ordered and $db in the payload
            # example: {'insert': 'aaa', 'documents': [{'a': 4, '_id': ObjectId('651e42159d7f696b839ab6ff')}], 'ordered': True, '$db': 'test'}
            collection_name = actual_payload['insert']
            database_name = actual_payload['$db']
            result = phongo_file_handler.insert_records_into_file(database_name, collection_name, actual_payload['documents'])
            if (result):
                #print("Yes, insert successful")
                response_doc = {"ok": 1}
            else:
                pass
                # we don't know what to implement here now
        elif 'find' in actual_payload:
            # and there should be filter, limit and $db in the payload
            # {'find': 'aaa', 'filter': {'a': 4}, 'limit': 1, '$db': 'test'}
            collection_name = actual_payload['find']
            database_name = actual_payload['$db']
            query_filter = actual_payload['filter']
            docs = phongo_file_handler.read_records_from_the_file(database_name, collection_name, query_filter)

            #response_doc = {'cursor': {'firstBatch': [{'_id': ObjectId('651fdc7ca0f2c7f1b30e7a42'), 'x': 65, 'y': 2312321, 'z': [1, 2]}], 'id': 0, 'ns': 'test.aaa'}, 'ok': 1.0}
            response_doc = {'cursor': {'firstBatch': docs, 'id': 0, 'ns': 'test.aaa'}, 'ok': 1.0}
            
        elif 'update' in actual_payload:
            #response_doc = { "ok": 0, "code":"321321", "errmsg": "message yok"}
            #op msg:{'update': 'fff', 'updates': [{'q': {}, 'u': {'$set': {'x': 1}}, 'multi': True}], 'ordered': True, '$db': 'db001'}
            database_name = actual_payload['$db']
            collection_to_update = actual_payload['update']
            update_filter = actual_payload['updates'][0]['q']
            what_to_update = actual_payload['updates'][0]['u']
            multi = False
            if ( 'multi' in actual_payload['updates'][0]):
                multi = actual_payload['updates'][0]['multi']
            update_operator = stage_name = list(what_to_update.keys())[0]
            if (update_operator) == "$set":
                update_fields = actual_payload['updates'][0]['u']['$set']
                response = phongo_file_handler.update_records_in_file(database_name, collection_to_update, update_filter, update_fields, multi)
                response_doc = {"ok":1, "acknowledged": True, "n": response['matched_count'], "nModified": response['update_count'], "upserted": 0}
            else:
                response_doc = { "ok": 0, "code":"001", "errmsg": f"Unrecognized update operator: '{update_operator}'"}
            
        elif 'delete' in actual_payload:
            database_name = actual_payload['$db']
            collection_to_delete = actual_payload['delete']
            delete_filter = actual_payload['deletes'][0]['q']
            limit = actual_payload['deletes'][0]['limit'] 
            
            delete_many = True if limit == 0 else False
            delete_count = phongo_file_handler.delete_records_in_file(database_name, collection_to_delete, delete_filter, delete_many)
            response_doc = {"ok":1, "acknowledged": True, "n":delete_count}
            
        elif 'replace' in actual_payload:
            pass    

    # response_doc["ok"] = 1
    return phongo_packer.replyto_op_msg_in_bytes(response_doc, request_id)

def reply_bytes_op_query_of_the_request(message, request_id):
    if message == "isMaster":
        docs = []
        doc01 = {"ismaster": True, "minWireVersion": 0, "maxWireVersion": 100, "ok": 1}
        docs.append(doc01)
        return phongo_packer.reply_bytes_op_query(request_id, docs)

def reply_bytes_op_query_of_okay_response(request_id):
        doc01 = {"ok": 1}
        docs = []
        docs.append(doc01)
        return phongo_packer.reply_bytes_op_query(request_id, docs)