import struct, bson, random
import phongo_config

def reply_bytes_op_query(request_id, docs):
    
    flags = struct.pack("<i", 0)
    
    cursor_id = struct.pack("<q", 0)
    starting_from = struct.pack("<i", 0)
    number_returned = struct.pack("<i", len(docs))
    reply_id = random.randint(0, 1000000)
    response_to = request_id

    body_in_bytes = b''.join([flags, cursor_id, starting_from, number_returned])

    docs_tuple = (tuple(docs)) # add returned documents to the message
    # print(docs_tuple)
    body_in_bytes += b''.join([bson.BSON.encode(doc) for doc in docs_tuple])

    header_in_bytes = b''.join([
        struct.pack("<i", phongo_config.MSG_HEADER_ATTRIBUTES["headerLength"] + len(body_in_bytes)),
        struct.pack("<i", reply_id),
        struct.pack("<i", response_to),
        struct.pack("<i", phongo_config.OP_CODE_FOR_OP_REPLY)
    ])
    return header_in_bytes + body_in_bytes

# wrap the message 
def replyto_op_msg_in_bytes(document_to_return_to_the_client, request_id):
    
    flags = struct.pack("<I", 0)
    payload_type = struct.pack("<b", 0)
    payload_data = bson.BSON.encode(document_to_return_to_the_client)
    body_in_bytes = b''.join([flags, payload_type, payload_data])

    reply_id = random.randint(0, 1000000)
    response_to = request_id

    header_length = phongo_config.MSG_HEADER_ATTRIBUTES["headerLength"]
    body_length = len(body_in_bytes)
    total_message_length = header_length + body_length

    header_in_bytes = b''.join([
        struct.pack("<i", total_message_length),
        struct.pack("<i", reply_id),
        struct.pack("<i", response_to),
        struct.pack("<i", phongo_config.OP_CODE_FOR_OP_MSG)
        ])
    
    return header_in_bytes + body_in_bytes