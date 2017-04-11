#!/usr/bin/env python

import sys, os
import logging
import threading

sys.path.append('gen-py')

# Thrift specific imports
from thrift import Thrift
from thrift.transport import TSocket
from thrift.server import TServer
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

# Protocol specific imports
from metadataServer import MetadataServerService
from metadataServer.ttypes import *
from shared import *
from shared.ttypes import *
from blockServer import *
from blockServer.ttypes import *

logging.disable(logging.CRITICAL)

class MetadataServerHandler():

    def __init__(self, config_path, my_id):
        # Initialize block
        self.config_path = config_path
        self.id = my_id
        self.port = self.readServerPort(self.id)
        self.hashFileName = {}

    def getFile(self, filename):
        # Function to handle download request from file
        print "--Client requesting to download--"

        # Check if filename is there 
        if filename in self.hashFileName:
            hashedFile = self.hashFileName[filename]
            hashedFile.status = responseType.OK
            print "File is in the server"
        else:
            hashedFile = file()
            hashedFile.hashList = []
            hashedFile.status = responseType.ERROR
            print "File does not exist in the server"
            
        # Return blocklist to client
        return self.downloadResponse(hashedFile)

    def downloadResponse(self, hashedFile):

        print "Sending downloadResponse to Client"
        print ""
        # Return downloadResponse
        return hashedFile


    def storeFile(self, filename, hashlist, version):
        # Function to handle upload request
        print "--Client requesting to upload--"

        # Initialize list of missing blocks
        listOfEntireBlocks = []
        listOfMissingBlocks = []

         # Create connection between metadata server and block server
        print "--Metadata Server <-> BlockServer--"
        blockServerPort = getPortNumber(config_path, 'block')
        blockServerSock, bTrans = getSocket(blockServerPort, 'block')

        # If there is connection to blockserver 
        if blockServerSock != None and bTrans != None:

            for i in range(len(hashlist)):
                # Get the list of all blocks
                listOfEntireBlocks.append(hashlist[i])

                # Check if every hash is already present in BlockServer
                print "Checking if blocks", hashlist[i], "is in BlockServer..."
                status = blockServerSock.hasBlock(hashlist[i]).num
                if status == 1:
                    print hashlist[i], "is not in BlockServer"     
                    listOfMissingBlocks.append(hashlist[i])
                else:
                    print hashlist[i], "is already in BlockServer"  

            # Close socket connection after use
            bTrans.close()

            # Since we already have versioning, my implementation, if
            # this function get called only when really need to store (Overwrite dictionary)
            f = file()
            f.filename = filename
            f.version = version
            f.hashList = hashlist
            f.status = responseType.OK
            self.hashFileName[filename] = f
                              
        # If no connection to block server
        else:
            print "Error: There is no connection to block server\n"
            f = file()
            f.status = responseType.Error
            
        # StoreFileResponse
        return self.storeFileResponse(listOfMissingBlocks, f)

    def storeFileResponse(self, missingList, f):

        # Assign the missing list 
        ur = uploadResponse()
        ur.hashList = missingList

        # Response cases 
        if f.status == 1:
            ur.status = uploadResponseType.OK

        elif f.status == 2:
            ur.status = uploadResponseType.ERROR  
           
        print "Sending this storeFileResponse to Client..\n"
        # Return storeFileResponse
        return ur


    def deleteFile(self, filename):
        print "--Client requesting to delete--"
        r = response()

        # Check if filename is there 
        if filename in self.hashFileName:
            del self.hashFileName[filename]
            r.message = responseType.OK
            print filename, "has been deleted"
        else:
            print "File requested to delete does not exist in the server"
            r.message = responseType.ERROR

        print ""

        return r


    def readServerPort(self, metaId):
        # Get the server port from the config file.
        # id field will determine which metadata server it is 1, 2 or n
        # Your details will be then either metadata1, metadata2 ... metadatan
        # return the port
        print "\nChecking validity of the config path"
        if not os.path.exists(config_path):
            print "ERROR: Config path is invalid"
            exit(1)
        if not os.path.isfile(config_path):
            print "ERROR: Config path is not a file"
            exit(1)

        # Later need to change ** to match id 
        print "Reading config file"
        with open(config_path, 'r') as conffile:
            lines = conffile.readlines()
            for line in lines:
                if ('metadata'+ metaId) in line:
                    # Important to make port as an integer
                    return int(line.split()[1].lstrip().rstrip())

        # Exit if you did not get metadata information
        print "ERROR: metadata information not found in config file"
        exit(1)

    # Add other member functions if needed

    # This function will trigger getFile (DOWNLOAD)
    def download(self, filename, metaName):

        print "Here"

        # HARDCODE: Connection
        if metaName == 'metadata1':
            useMeta = ['metadata2', 'metadata3']

        elif metaName == 'metadata2':
            useMeta = ['metadata3', 'metadata1']

        elif metaName == 'metadata3':
            useMeta = ['metadata2', 'metadata3']

        for i in range(len(useMeta)):

            print "Trying Server:", useMeta[i]
            port = getPortNumber(config_path, useMeta[i])
            sock, trans = getSocket(port, useMeta[i])

            if sock == None and trans == None:
                print "No Connection to server:", useMeta[i]
                print ""
            else:
                print "Success Connection to server:", useMeta[i]
                print ""
                serverName = useMeta[i]
                break

        # This handle when only current meta server is up, then if file exist, download directly
        if sock == None and trans == None:
            print "Only current metaserver is up"
            print "Downloading requested file to current metadata:", metaName
            retF = self.getFile(filename)

            # Check the file status 
            if retF.status == 1:
                print "File requested to download has successfully downloaded!\n"
            else:
                print "File requested to download does not exist in this server!\n"

            return retF

        # At this point, we have got the socket of which metadata server to connect and check
        print "Checking for file and version in metadata server:", serverName
        checkFile = sock.getFile(filename)
        trans.close()

        if checkFile.status == 1:
            print "File present in checked metadata server:", serverName
            print ""
            # Get current meta server file version
            if filename in self.hashFileName:
                curFile = self.hashFileName[filename]
                curV = curFile.version
                checkV = checkFile.version

                # If version in the other server is greater
                if checkV > curV:
                    return checkFile
                # Case handle if current version is greater or same compare to other meta
                else:
                    return curFile

            # Do we need this case when we have Gossip Protocol?
            elif filename not in self.hashFileName:
                return checkFile

        else:
            print "File is not present in checked metadata server:", serverName

            # Not sure if we need this case when we have Gossip Protocol?
            if filename in self.hashFileName:
                print "File present in current metadata server:", metaName
                print ""
                curFile = self.hashFileName[filename]
                return curFile
            else:
                print "File is not present in current metadata server:", metaName
                print ""
                return self.getFile(filename)


    # This function will trigger storeFile by contacting other metadata (Versioning)
    def upload(self, filename, hashList, clientVersion, numberOfM, metaName):

        # Check for file version on current socket
        stat = self.checkFileVersion(filename, clientVersion)
        
        # CASE 1: If current socket version is already greater then, 
        # we indicate error to client, don't need to check other
        if stat.message == 2:

            # Return response back to client 
            fr2 = uploadResponse()
            fr2.status = uploadResponseType.ERROR
            fr2.hashList = []
            
            return fr2

        # CASE 2: If current meta version is odler or same or no file 
        elif stat.message == 1 or stat.message == 3 or stat.message == 4:

            #####################################Connection#############################################
            # I HARDCODE socket connection (Handling if only one connection is up then upload to itself)
            if metaName == 'metadata1':
                useMeta = ['metadata2', 'metadata3']

            elif metaName == 'metadata2':
                useMeta = ['metadata3', 'metadata1']

            elif metaName == 'metadata3':
                useMeta = ['metadata2', 'metadata3']

            for i in range(len(useMeta)):

                print "Trying Server:", useMeta[i]
                port = getPortNumber(config_path, useMeta[i])
                sock, trans = getSocket(port, useMeta[i])

                if sock == None and trans == None:
                    print "No Connection to server:", useMeta[i]
                    print ""
                else:
                    print "Success Connection to server:", useMeta[i]
                    print ""
                    serverName = useMeta[i]
                    break

            # This handle when only current meta server is up, then store directly
            if sock == None and trans == None:
                print "Only current metaserver is up"
                print "Storing requested file to current metadata:", metaName
                retUr = self.storeFile(filename, hashList, clientVersion)
                fr = uploadResponse()
                if retUr.status == 1:
                    print "File requested to upload has successfully stored!\n"
                    fr.status = uploadResponseType.OK
                    fr.hashList = retUr.hashList
                else:
                    print "Error occured when storing file!\n"
                    fr.status = uploadResponseType.ERROR
                    fr.hashList = []

                return fr
            ######################################################################################

            ####################################Operation#########################################
            # At this point, we know which socket of metadata to connect
            print "Meta Socket Checked:", serverName
            # Get the version checker 
            print "Meta Socket Checking version..."
            otherStat = sock.checkFileVersion(filename, clientVersion)
            print "Meta Socket Checked Stat:", otherStat.message
            print ""
            trans.close()

            # Case 1: Checked server Version < client Version
            if otherStat.message == 1:

                # Get socket 
                c2Port = getPortNumber(config_path, serverName)
                c2Sock, c2Trans = getSocket(c2Port, serverName)

                print ""

                # Current server has older version or no file // OK to client 
                if stat.message == 1 or stat.message == 4:

                    if stat.message == 1:
                        print "Description: Client > CheckedMeta, CurrMeta < Client"
                    elif stat.message == 4:
                        print "Description: Client > CheckedMeta, CurrMeta: No File"

                    print "Updating file version on CheckedMeta:", serverName

                    # Store the updated file in that checked server
                    c2Ur = c2Sock.storeFile(filename, hashList, clientVersion)
                    c2Trans.close()

                    print "Response from CheckedMeta:", c2Ur.status

                    # Store the file in current meta server
                    print "Storing this file on current server: CurrMeta"
                    ur2 = self.storeFile(filename, hashList, clientVersion)

                    fr2 = uploadResponse()

                    # Error check 
                    if c2Ur.status == 1 and ur2.status == 1:
                        print "File is successfully stored in both server\n"
                        fr2.status = uploadResponseType.OK                  # OK
                        fr2.hashList = ur2.hashList   
                    else:
                        print "Internal Error occured when storing file\n"
                        fr2.status = uploadResponseType.ERROR               # ERROR 
                        fr2.hashList = []

                    return fr2

                # Current server has same version as client // ERROR to Client
                elif stat.message == 3:
                    print "Description: Client > CheckedMeta, CurrMeta = Client"
                    print "Updating file version on CheckedMeta:", serverName

                    # Store the updated file in that checked server
                    c2Ur = c2Sock.storeFile(filename, hashList, clientVersion)
                    c2Trans.close()

                    print "Response from CheckedMeta:", c2Ur.status

                    # Send response back to client 
                    fr2 = uploadResponse()
                    fr2.hashList = []

                    # Error check
                    if c2Ur.status == 1:
                        print "File already present at current server: CurrMeta\n"
                        fr2.status = uploadResponseType.FILE_ALREADY_PRESENT
                    else:
                        print "Internal Error occured when storing file\n"
                        fr2.status = uploadResponseType.ERROR
                    
                    return fr2

            # Case 2: If Checked server Version > client version
            elif otherStat.message == 2:

                # Get socket 
                c2Port = getPortNumber(config_path, serverName)
                c2Sock, c2Trans = getSocket(c2Port, serverName)

                # Current server has older version or same version to client // ERROR to client 
                if stat.message == 1 or stat.message == 3 or stat.message == 4:

                    if stat.message == 1:
                        print "Description: Client < CheckedMeta, CurrMeta < Client"
                    elif stat.message == 3:
                        print "Description: Client < CheckedMeta, CurrMeta = Client"
                    elif stat.message == 4:
                        print "Description: Client < CheckedMeta, CurrMeta: No File"

                    print "CheckedMeta has the newest version:", serverName

                    # Get the newest version from checkedMeta
                    v = c2Sock.getVersion(filename)
                    newestVersion = v.version
                    l = c2Sock.getHashList(filename)
                    newestList = l.hashList
                    c2Trans.close()

                    # Store the file in current meta server
                    print "Storing this file version on current server: CurrMeta"
                    ur2 = self.storeFile(filename, newestList, newestVersion)

                    fr2 = uploadResponse()

                    # Error check
                    if ur2.status == 1:
                        print "Current Meta server file version has been updated\n"
                        fr2.status = uploadResponseType.ERROR # ERROR
                        fr2.hashList = ur2.hashList           # Contains the missing list 
                    else:
                        print "Internal Error occured when storing file\n"    
                        fr2.status = uploadResponseType.ERROR
                        fr2.hashList = []

                    return fr2

            # Case 3: If Checked server Version == client version
            elif otherStat.message == 3:
                print ""
                # Current server has older version or no file // ERROR to client 
                if stat.message == 1 or stat.message == 4:

                    if stat.message == 1:
                        print "Description: Client = CheckedMeta, CurrMeta < Client"
                    elif stat.message == 4:
                        print "Description: Client = CheckedMeta, CurrMeta: No File"

                    print("CheckedMeta: %s and Client has the newest version" % serverName)

                    # Store the file in current meta server
                    print "Storing this file version on current server: CurrMeta"
                    ur2 = self.storeFile(filename, hashList, clientVersion)

                    fr2 = uploadResponse()

                    if ur2.status == 1:
                        print "File has successfully updated on current server: CurrMeta\n"
                        fr2.status = uploadResponseType.ERROR   # ERROR, File already present 
                        fr2.hashList = ur2.hashList             # Contains misisngList if any
                    else:
                        print "Internal Error occured when storing file\n"
                        fr2.status = uploadResponseType.ERROR
                        fr2.hashList = []

                    return fr2

                # Current server has same version as client // ERROR to Client
                elif stat.message == 3:
                    print "Description: Client Version = CheckedMeta Version, CurrMeta = Client"
                    print "File already present: All server already has the same version\n"
                    
                    # Send response back to client 
                    fr2 = uploadResponse()
                    fr2.status = uploadResponseType.FILE_ALREADY_PRESENT
                    fr2.hashList = []

                    return fr2

            # Case 4: If file does not exist in checkedMeta
            elif otherStat.message == 4:

                # Get socket
                c2Port = getPortNumber(config_path, serverName)
                c2Sock, c2Trans = getSocket(c2Port, serverName)

                print ""

                # Current server has older version or no file // OK to client 
                if stat.message == 1 or stat.message == 4:

                    if stat.message == 1:
                        print "Description: Client > CurrMeta, CheckedMeta: No File"
                    elif stat.message == 4:
                        print "Description: Client: has File, CheckedMeta: No File, CurrMeta: No File"

                    print "Client has the newest version"
                    
                    # Store to checkedMeta
                    print "Storing the file to checkedMeta:", serverName
                    ur2 = c2Sock.storeFile(filename, hashList, clientVersion)
                    c2Trans.close()

                    print "checkedMeta Status:", ur2.status

                    # Store to currMeta
                    print "Storing the file version to current server: CurrMeta\n"
                    ur3 = self.storeFile(filename, hashList, clientVersion)

                    fr2 = uploadResponse()

                    # Error check
                    if ur2.status == 1 and ur3.status == 1:
                        print "Both server has successfully stored the file\n"        
                        fr2.status = uploadResponseType.OK
                        fr2.hashList = ur3.hashList             # Contains missing list 
                    else:
                        print "Internal Error occurred when storing file\n"
                        fr2.status = uploadResponseType.ERROR
                        fr2.hashList = []     

                    return fr2

                # Current server has same version as client // ERROR to Client
                elif stat.message == 3:
                    print "Description: Client = CurrMeta, CheckedMeta: No File"
                    
                     # Store to checkedMeta
                    print "Storing the file to checkedMeta:", serverName
                    ur2 = c2Sock.storeFile(filename, hashList, clientVersion)
                    c2Trans.close()

                    print "checkedMeta Status:", ur2.status

                    fr2 = uploadResponse()
                    fr2.status = uploadResponseType.ERROR
                    fr2.hashList = []

                    # Error check
                    if ur2.status == 1:
                        print "CheckedMeta has successfully store the file\n"  
                    else:
                        print "Internal Error occured when storing file\n"

                    return fr2
        ######################################################################################

    # Helper function to get version
    def getVersion(self, filename):
        v = version()
        f = self.hashedFile[filename]
        v.version = f.version
        return v

    # Helper function to get HashList
    def getHashList(self, filename):
        l = listHash()
        f = self.hashedFile[filename]
        l.hashList = f.hashList
        return l

    # This function check the file version with other metadata socket stored file
    def checkFileVersion(self, filename, clientVersion):

        r = response()

        # When file is in server
        if filename in self.hashFileName:

            f = self.hashFileName[filename]
            
            # CASE: When version client has is smaller (ERROR)
            if f.version > clientVersion:
                print "Client has older version of file\n"
                r.message = responseType.ERROR                   # 2

            # CASE: When client has newer version (OK)
            elif f.version < clientVersion:
                print "Client has newer version of file\n"
                r.message = responseType.OK                      # 1

            # CASE: When client has same version (ERROR)
            elif f.version == clientVersion:
                print "Client has same version as current server\n"
                r.message = responseType.FILE_ALREADY_PRESENT    # 3

        # When file not in server
        else:
            # Indicate file does not exist
            r.message = responseType.NO_FILE                     # 4

        # Return response 
        return r


    # This function triggers the deleteFile, it contacts the other server
    # to delete the file
    def triggerDelete(self, filename, numberOfM, metaName):

        # To store the response
        rStore = []
        # Delete the file in self meta server
        rStore.append(self.deleteFile(filename))

        # Check online metadata
        for i in range(numberOfM):
            serverName = 'metadata'+str(i+1)
            print ""
            

            # If port is not itself
            if serverName != metaName:
                port = getPortNumber(config_path, serverName)
                print "Trying Server:", serverName
                # Get socket connection
                sock, trans = getSocket(port, serverName)
                # Delete the file on all online meta server 
                if sock != None and trans != None:
                    rStore.append(sock.deleteFile(filename))
                    # Close socket connection
                    trans.close()
            else:
                print "Ignoring Self\n"

        # Final response to client
        fr = response()

        # Count if file is deleted in at least one up server
        countOk = 0
        for each in rStore:
            # Count if message is OK
            if each.message == 1:
                countOk += 1

        # Check if file is deleted from the server
        if countOk > 0:
            fr.message = responseType.OK
        else:
            fr.message = responseType.ERROR

        # Return the final response
        return fr

    # This will be triggered by the gossip function
    def triggerGossip(self, metaName):
        r = response()
        for name in self.hashFileName:
            f = self.hashFileName[name]
            gossUr = self.upload(name, f.hashList, f.version, 3, metaName)
        # Since it's gossiping, I will just assume it always return OK
        r.message = responseType.OK

        return r 

# Add additional classes and functions here if needed

# Function to obtain the block server port
def getPortNumber(config_path, serverName):
    # This function reads config file and gets the port for block server
    print "Checking validity of the config path"
    if not os.path.exists(config_path):
        print "ERROR: Config path is invalid"
        exit(1)
    if not os.path.isfile(config_path):
        print "ERROR: Config path is not a file"
        exit(1)

    print "Reading config file"
    with open(config_path, 'r') as conffile:
        lines = conffile.readlines()
        for line in lines:
            if serverName in line:
                # Important to make port as an integer
                return int(line.split()[1].lstrip().rstrip())

    # Exit if you did not get blockserver information
    print ("ERROR: %s information not found in config file" % serverName)
    exit(1)

# Function to obtain the block server socket
def getSocket(port, serverName):
    # This function creates a socket to block server and returns it
    # Make socket
    transport = TSocket.TSocket('localhost', port)
    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)
    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    if serverName == 'block':
        # Block socket
        client = BlockServerService.Client(protocol)
    elif serverName == serverName:
        # Meta socket
        client = MetadataServerService.Client(protocol)

    # Connect!
    print ("Trying connection to %s server on port: %d" % (serverName, port))

    try:
        transport.open()
    except Exception:
        return None, None

    return client, transport

# This function is the gossip protocol
def gossipProtocol(port, config_path):
    t = threading.Timer(5.0, gossipProtocol, [port, config_path])
    t.daemon = True
    t.start()
    print "Starting Gossiping..."

    print "Config:", config_path
    print 'Port:', port

    metaNum = 3
    # Get current metadata name:
    for i in range(metaNum):
        serverName = 'metadata' + str(i+1)
        getPort = getPortNumber(config_path, serverName)

        if port == getPort:
            getName = serverName
            break

    # Search for another socket
    if getName == 'metadata1':
        tryName = ['metadata3', 'metadata2']

    elif getName == 'metadata2':
        tryName = ['metadata1', 'metadata3']

    elif getName == 'metadata3':
        tryName = ['metadata2', 'metadata1']

    # Try to connect to all possible socket
    for i in range(len(tryName)):
        print "Gossiping: Trying Server:", tryName[i]
        port = getPortNumber(config_path, tryName[i])
        sock, trans = getSocket(port, tryName[i])

        if sock == None and trans == None:
            print "Gossiping: No Connection to server:", tryName[i]
            print ""
        else:
            print "Gossiping: Success Connection to server:", tryName[i]
            print ""
            serverName = tryName[i]
            break

    # Do the socket connect only if at least one other server is up, else do nothing
    if sock != None and trans != None:
        # Here comes the operation
        r = sock.triggerGossip(getName)

        if r.message == 1:
            word = 'OK'

        print "Gossiping:", word


if __name__ == "__main__":

    if len(sys.argv) < 3:
        print "Invocation <executable> <config_file> <id>"
        exit(-1)

    config_path = sys.argv[1]
    my_id = sys.argv[2]

    print "Initializing metadata server"
    handler = MetadataServerHandler(config_path, my_id)
    port = handler.readServerPort(my_id)

    # Define parameters for thrift server
    processor = MetadataServerService.Processor(handler)
    transport = TSocket.TServerSocket(port=port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    # Create a server object
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    print "Starting server on port:", port
    print("")

    try:
        # GOSSIPING 
        gossipProtocol(port, config_path)
        server.serve()
    except (Exception, KeyboardInterrupt) as e:
        print "\nExecption / Keyboard interrupt occured: ", e
        exit(0)
