
from collections import OrderedDict
import sys
from bson import codec_options
import collections

CODEC_OPTIONS = codec_options.CodecOptions(document_class=collections.OrderedDict)
MSG_HEADER_ATTRIBUTES = {
    "headerLength" : 16,
    "bodyLength": {
        "start": 0,
        "end": 4
    },
    "requestId": {
        "start": 4,
        "end": 8
    },
    "responseTo": {
        "start": 8,
        "end": 12
    },
    "opCode": {
        "start": 12,
        "end": 16
    }

}

ROOT_DATAFILES_DIRECTORY = "datafiles/"
DATAFILES_FOLDER="./datafiles/"
REQUEST_TIMEOUT = 30
DB_SERVER_ADDRESS = "localhost"
DB_SERVER_PORT = sys.argv[1] # default but can be changed
LISTENING_SOCKET = None
DB_UP_AND_RUNNING = False
DB_IS_BEING_SHUT_DOWN = False

FLAGS_OP_MSG = {
    "checksumPresent": 0,
    "moreToCome": 1,
    "exhaustAllowed": 16
}

OP_CODE_FOR_OP_MSG = 2013
OP_CODE_FOR_OP_QUERY = 2004
OP_CODE_FOR_OP_REPLY = 1


QUERY_FLAGS = OrderedDict([
    ('TailableCursor', 2),
    ('SlaveOkay', 4),
    ('OplogReplay', 8),
    ('NoTimeout', 16),
    ('AwaitData', 32),
    ('Exhaust', 64),
    ('Partial', 128)])
