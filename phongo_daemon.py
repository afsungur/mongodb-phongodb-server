'''
    Phongo Daemon Process

    Eveything starts here.
'''

import phongo_server

server = phongo_server.PhongoDB()
server.start()
server.wait_connections_forever()
