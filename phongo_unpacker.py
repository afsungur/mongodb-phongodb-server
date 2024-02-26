import struct, bson
import codecs
import phongo_config
import collections

def unpack_header(msg):
    MSG_HEADER_ATTRIBUTES = phongo_config.MSG_HEADER_ATTRIBUTES
    message_body_length = struct.unpack("<i",msg[MSG_HEADER_ATTRIBUTES["bodyLength"]["start"]:MSG_HEADER_ATTRIBUTES["bodyLength"]["end"]])[0]
    request_id = struct.unpack("<i",msg[MSG_HEADER_ATTRIBUTES["requestId"]["start"]:MSG_HEADER_ATTRIBUTES["requestId"]["end"]])[0]
    response_to = struct.unpack("<i",msg[MSG_HEADER_ATTRIBUTES["responseTo"]["start"]:MSG_HEADER_ATTRIBUTES["responseTo"]["end"]])[0]
    op_code = struct.unpack("<i",msg[MSG_HEADER_ATTRIBUTES["opCode"]["start"]:MSG_HEADER_ATTRIBUTES["opCode"]["end"]])[0]
    return (message_body_length, request_id, response_to, op_code)
              
def unpack_op_query(msg):
    # It is (OP_QUERY) obsolete after MongoDB 5.1 however, for hello and isMaster commands, still being used. 
    # https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/#ref-op-query-footnote-id4
    size_of_the_flags = 4
    flags, = struct.unpack("<I",msg[:size_of_the_flags])
    # https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/#kind-1--document-sequence
    

    end_position = msg.index(b"\x00", size_of_the_flags)
    namespace = codecs.utf_8_decode(msg[size_of_the_flags:end_position], None, True)[0] # BSON Cstring to Python unicode
    current_position = end_position + 1

    is_command = namespace.endswith('.$cmd')
    current_position += 8
    docs = bson.decode_all(msg[current_position:], phongo_config.CODEC_OPTIONS)
    
    if is_command:
        assert len(docs) == 1
        command_namespace = namespace[:-len('.$cmd')]
        return (docs, flags, command_namespace)
    else:
        print("!!!It's not an op-query!!! = There's something wrong")
        
def unpack_op_msg(msg):
        current_position = 0
        size_of_the_flags = 4
        flags, = struct.unpack("<I",msg[:size_of_the_flags])
        
        current_position += size_of_the_flags # 4 
        checksum_present = flags & phongo_config.FLAGS_OP_MSG['checksumPresent'] # whether the checksum is present or not
        msg_len_without_checksum = len(msg) - 4 if checksum_present else len(msg) # calculate total message length
        client_request = collections.OrderedDict() # we'll build this in the loop
        
        size_of_the_payload_type = 1
        size_of_the_payload_size = 4
        
        while current_position < msg_len_without_checksum:

            type_of_the_payload, = struct.unpack("<b",msg[current_position:current_position + size_of_the_payload_type])
            current_position += size_of_the_payload_type
            payload_size, = struct.unpack("<i", msg[current_position:current_position + size_of_the_payload_size])

            if type_of_the_payload == 0:
                # https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/#kind-0--body
                docs = bson.decode_all(msg[current_position:current_position + payload_size],phongo_config.CODEC_OPTIONS)
                doc = docs[0]
                client_request.update(doc)
                current_position += payload_size
            elif type_of_the_payload == 1:
                # https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/#kind-1--document-sequence
                section_size_in_bytes, = struct.unpack("<i", msg[current_position:current_position + 4])
                current_position += 4
                
                end_position = msg.index(b"\x00", current_position)
                identifier = codecs.utf_8_decode(msg[current_position:end_position], None, True)[0]
                current_position = end_position + 1
                
                documents_len = section_size_in_bytes - len(identifier) - 1 - 4
                documents = bson.decode_all(msg[current_position:current_position + documents_len],
                                            phongo_config.CODEC_OPTIONS)
                client_request[identifier] = documents
                current_position += documents_len
            elif type_of_the_payload == 2:
                # https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/#kind-2
                pass # internal

        database = client_request['$db']
        return (client_request, database, flags)