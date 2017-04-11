#!/usr/bin/env python

import sys, os, hashlib
import glob
import math
import logging

sys.path.append('gen-py')

from blockServer import *
from blockServer.ttypes import *
from shared import *
from shared.ttypes import *
from metadataServer import *
from metadataServer.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

# Set logging to critical (Thanks from PIAZZA)
logging.disable(logging.CRITICAL)

# Get either metadata or block server port number
def getPortNumber(config_path, whatServer):

    # This function reads config file and gets the port for block server
    # print "Checking validity of the config path"
    if not os.path.exists(config_path):
        sys.stderr.write("ERROR: Config path is invalid\n")
        sys.stdout.write("ERROR\n")
        exit(1)
    if not os.path.isfile(config_path):
        sys.stderr.write("ERROR: Config path is not a file\n")
        sys.stdout.write("ERROR\n")
        exit(1)

    with open(config_path, 'r') as conffile:
        lines = conffile.readlines()
        for line in lines:
            if whatServer in line:
                # Important to make port as an integer
                return int(line.split()[1].lstrip().rstrip())

    # Just for the error string 
    if whatServer == 'block':
        serverType = 'blockserver'
    else:
        serverType = whatServer

    # Exit if you did not get blockserver information
    sys.stderr.write("ERROR: %s information not found in config file\n" % serverType)
    sys.stdout.write("ERROR")
    exit(1)

# Return the socket connection of block server or metadata 
def getSocket(port, whatServer):

    # This function creates a socket to server and returns it
    transport = TSocket.TSocket('localhost', port)
    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)
    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    if whatServer == 'block':
        # Create a client to use the protocol encoder (Block server)
        client = BlockServerService.Client(protocol)
    elif whatServer == 'metadata':
        # Create a client to use the protocol encoder (Metadata server)
        client = MetadataServerService.Client(protocol)

    # Connect!
    try:
        transport.open()
    except Exception as e:
        return None, None

    return client, transport

# Additional function to search for blocks in the local directory (Download)
def searchLocalBlock(base_dir):
    
    # localHashList will store the blocks of each file
    localHashList = []
    # localBinaryBlock will store the (K: hashString, V: data)
    localBinaryBlock = {}

    # Look through all the files in the directory
    for files in os.listdir(base_dir):
        if os.path.isfile(os.path.join(base_dir, files)):
            f = open(files, 'rb')
        else:
            # Ignore a not file type (such as dir)
            pass

        # Read 4MB at a time 
        read4MB = 4 * int(math.pow(2,20))      
        data = f.read(read4MB)

        # Read the data while it is not null
        while(data):
            # Generating SHA-256 hash
            m = hashlib.sha256()
            m.update(data)
            hashString = m.hexdigest()

            # Hash the hashString to its corresponding data
            localBinaryBlock[hashString] = data
            # Append the list of blocks
            localHashList.append(hashString)
            # Read 4MB
            data = f.read(read4MB)
    
    # Only contains unique hash in the list
    setLocalList = set(localHashList)
    newLocalList = list(setLocalList)

    # Return the list of local block available, and its corresponding dictionary
    return newLocalList, localBinaryBlock


# This function will check which blocks are available locally, and which are not (Download)
def checkLocalBlocks(existingHash, newLocalList, localBinaryBlock):

    # The availableLocal will store list of blocks that are already available locally
    availableLocal = []
    # The missingLocal will store list of hashstring missing locally
    missingLocal = []

     # Look for blocks client doesn't already have
    for i in range(len(existingHash)):
        # Check if block is available in local list 
        if existingHash[i] in newLocalList:
            # Put in list of available local blocks
            availableLocal.append(localBinaryBlock[existingHash[i]])
        else:
            # Put in list that are to be sent to server to get the missing block
            missingLocal.append(existingHash[i])

    # Return the lists
    return availableLocal, missingLocal


# This function will get the missing blocks that are not available locally (Download)
def getMissingBlock(blockSock, availableLocal, missingLocal):

    # Get the missing block in block server 
    for i in range(len(missingLocal)):
        try:
            hb_dwnld = blockSock.getBlock(missingLocal[i])
        except Exception as e:
            sys.stderr.write("Received exception while trying getBlock\n")
            sys.stderr.write("%s\n" % e)
            sys.stdout.write("ERROR")
            exit(1)

        # When error in retrieving block we exit
        if hb_dwnld.status == "ERROR":
            # Error while retrieving block server, block is not in the server
            sys.stdout.write("ERROR\n")
            exit(1)

        # Hashing the downloaded block
        m = hashlib.sha256()
        m.update(hb_dwnld.block)
        hashString_dwnld = m.hexdigest()
            
        # At the end of the iteration, this will contain list of blocks of the requested download file
        availableLocal.append(hb_dwnld.block)

    return availableLocal


# This function is to merge the binary data into an output file (Download)
def mergeBlockWrite(filename, finalBlockList, base_dir):

    # Get the filename and path to be written
    newFile = open(os.path.join(base_dir, filename), 'wb')
    
    # Write to a new file from the list of binary blocks
    for b in finalBlockList:
        newFile.write(b)
    

# Additional function to search for blocks in the local directory (Upload)
def readOpenHash(filename, base_dir):

    # Make sure it is a valid file format
    if os.path.isfile(os.path.join(base_dir, filename)):
        f = open(filename, 'rb')
    else:
        # File is not filetype or does not exist in directory
        sys.stderr.write("Not a file type or file does not exist in directory\n")
        sys.stdout.write("ERROR\n")
        exit(1)

    # Read 4MB at a time 
    read4MB = 4 * int(math.pow(2,20))      
    data = f.read(read4MB)
    # HashList will store the blocks of the data to be send to metadata
    hashList = []
    # dataHash will store the corresponding hashstring and its data (K: hashstring, V: data)
    dataHash = {}

    # Read the data while it is not null
    while(data):
        # Generating SHA-256 hash
        m = hashlib.sha256()
        m.update(data)
        hashString = m.hexdigest()

        # Hash the data corresponding to its hashstring as key
        dataHash[hashString] = data
        # Append the list of blocks
        hashList.append(hashString)
        # Read 4MB
        data = f.read(read4MB)

    # Close the opened file after reading them
    f.close()

    # Return the list of block created 4MB each and its corresponding dictionary
    return hashList, dataHash


# Function to store the missing block on the missing list (Upload)
def storeMissingBlock(blockSock, missingList, dataHash):

    # Store all the missing block from the list 
    for i in range(len(missingList)):
        # Storing hashblock RPC
        hb = hashBlock()
        hb.hash = missingList[i]

        # Get the corresponding data 
        hb.block = dataHash[missingList[i]] 
        hb.status = "OK"

        try:
            resp = blockSock.storeBlock(hb)
        except Exception as e:
            sys.stderr.write("Received exception while trying storeBlock\n")
            sys.stderr.write("%s\n" % e)
            sys.stdout.write("ERROR")
            exit(1)

        # Received response from block server
        if resp.message != responseType.OK:
            # Will print error when storeblock is not succesfull
            sys.stdout.write("ERROR\n")
            exit(1)


# This function checks which metadata server is up
def checkOnlineMetaServer(config_path, numberOfM):

    # Check which metadata server is up
    for i in range(numberOfM):
        # Only 1,2,3
        metadataName = 'metadata'+str(i+1)

        # Try which metadata server is up 
        metaPort = getPortNumber(config_path, metadataName)
        metaSock, trans = getSocket(metaPort, 'metadata')
           
        # Which ever is up, use that one, and break
        if(metaSock != None and trans != None):
            break

    # Check if at least one meta is up
    checkServerConnection(trans, 'metadata')

    return metaSock, metadataName, trans


# This function will check if server is up
def checkServerConnection(trans, server):

    # If none of the metadata server is online, give ERROR
    if trans == None:
        sys.stderr.write("None of the %s server is up\n" % server)
        sys.stdout.write("ERROR\n")
        exit(1)


# The big functions doing all the commands
def do_operations(config_path, base_dir, command, filename):

    # Get the number of metadata server 
    numberOfM = getPortNumber(config_path, 'M')         # In this case is 3

    # Before starting anything, we check if blockserver is up, if not we give error

    # Start blockserver socket connection then close it when unuse
    blockPort = getPortNumber(config_path, 'block')
    blockSock, bTrans = getSocket(blockPort, 'block')

    # Check if socket connection is succesful, if not will give error, else close it
    checkServerConnection(bTrans, 'block')
    bTrans.close()

    # DOWNLOAD CASE
    if(command == "download"):

        # GETFILE will return list of the corresponding file requested 
        fi = file()
        try:
            # Start meta socket connection then close it when unuse
            # Check for online metadata server
            metaSock, metaName, mTrans = checkOnlineMetaServer(config_path, numberOfM)
            fi = metaSock.download(filename, metaName)
            mTrans.close()
        except Exception as e:
            sys.stderr.write("Received exception while trying download\n")
            sys.stderr.write("%s\n" % e)
            sys.stdout.write("ERROR")
            exit(1)

        # This contains the list of block existing from the metadata server 
        existingHash = fi.hashList

        # Status message error when file does not exist in server
        if fi.status == 2:
            sys.stdout.write("ERROR\n")
            exit(1) 

        # Search for blocks in local directory will return the available local list of blocks, 
        # and the dictionary of block in local directory 
        newLocalList, localBinaryBlock = searchLocalBlock(base_dir)

        # Contain list of blocks that are available locally, and that are missing
        availableLocal, missingLocal = checkLocalBlocks(existingHash, newLocalList, localBinaryBlock)

        if len(missingLocal) == 0:
            # All blocks are available locally
            finalBlockList = availableLocal
        else:
            # Start blockserver socket connection then close it when unuse
            blockPort = getPortNumber(config_path, 'block')
            blockSock, bTrans = getSocket(blockPort, 'block')

            # Check if socket connection is succesful
            checkServerConnection(bTrans, 'block')

            # Contain list of blocks file that is requested to download (Retrieved by getBlock)
            # During the call to this function, if error occured, it will exit and print "ERROR"
            finalBlockList = getMissingBlock(blockSock, availableLocal, missingLocal)
            bTrans.close()

        # When the code passes the above then succesfully obtained the blocks wanted to write the file
        # Merge the blocks and write the output file 
        mergeBlockWrite(filename, finalBlockList, base_dir)

        # Status message 
        sys.stdout.write("OK\n")

    # UPLOAD CASE
    elif (command == "upload"):

        # Reading, Open, and Hash SHA256 
        hashList, dataHash = readOpenHash(filename, base_dir)

        # Get the file version
        fileVersion = os.stat(os.path.join(base_dir, filename)).st_mtime 

        # Call StoreFile by passing the list of blocks and filename
        ur = uploadResponse()
        try:
            # Check for online metadata server
            metaSock, metaName, mTrans = checkOnlineMetaServer(config_path, numberOfM)
            ur = metaSock.upload(filename, hashList, int(fileVersion), numberOfM, metaName)
        except Exception as e:
            sys.stderr.write("Received exception while triggering upload\n")
            sys.stderr.write("%s\n" % e)
            sys.stdout.write("ERROR\n")
            exit(1)

        # Here is the list of missing block
        missingList = ur.hashList

        # Start blockserver socket connection then close it when unuse
        blockPort = getPortNumber(config_path, 'block')
        blockSock, bTrans = getSocket(blockPort, 'block')

        # Check if socket connection is succesful
        checkServerConnection(bTrans, 'block')

        # Will store each blocks that are on the missing list 
        # If some error happens during, storing, it will exit and gives "ERROR"
        storeMissingBlock(blockSock, missingList, dataHash)
        bTrans.close()

        # Status Response
        if ur.status == 1:          # OK
            sys.stdout.write("OK\n")
        elif ur.status == 4:        # ERROR
            sys.stdout.write("ERROR\n")
            exit(1)
        elif ur.status == 3:        # FILE_ALREADY_PRESENT
            sys.stdout.write("ERROR\n")
            exit(1)

    # DELETE CASE
    elif (command == "delete"):
        
        # Getting the delete response
        r = response()
        try:
            # Start meta socket connection then close it when unuse
            # Check for online metadata server
            metaSock, metaName, mTrans = checkOnlineMetaServer(config_path, numberOfM)
            r = metaSock.triggerDelete(filename, numberOfM, metaName)                
            mTrans.close()
        except Exception as e:
            sys.stderr.write("Received exception while triggering delete\n")
            sys.stderr.write("%s\n" % e)  
            sys.stdout.write("ERROR\n")
            exit(1)

        # Output Client message 
        if r.message == 1:
            sys.stdout.write("OK\n")
        elif r.message == 2:
            sys.stdout.write("ERROR\n")
            exit(1)

    # Other commands than upload, delete, and download will give ERROR 
    else:
        # No such command 
        sys.stderr.write("ERROR: Wrong command\n")
        sys.stdout.write("ERROR\n")
        exit(1)

# MAIN
if __name__ == "__main__":

    if len(sys.argv) < 5:
        sys.stderr.write("ERROR: Usage <executable> <path to config file>\n")
        sys.stdout.write("ERROR\n")
        exit(1)

    # Take arguments
    config_path = sys.argv[1]
    base_dir = sys.argv[2]          # Local directory
    command = sys.argv[3]           # Download, upload, delete
    filename = sys.argv[4]          # Name of the file

    # Time to do some operations!
    do_operations(config_path, base_dir, command, filename)
