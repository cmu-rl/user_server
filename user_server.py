#!/usr/bin/python3.6
import boto3
import time
import json
import random
import string
import hashlib
import datetime
import mySQLLib
import hriLib
import socketserver

FIREHOSE_STREAM_MIN_AVAILABLE = 5

def generateUserID(minecraftUUID):
    return hashlib.md5(minecraftUUID.encode('utf-8')).hexdigest()

def generateSecureString(len):
    return ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(len))

def generateSecureFruitString():
    return hriLib.getString()

def crateFirehoseStream(playerDB, firehoseClient, inUse = False, uid = None):
    # Role for firehose needs to have access to S3 - make policy that includes this
    roleARN =   'arn:aws:iam::215821069683:role/firehose_delivery_role'
    bucketARN = 'arn:aws:s3:::deepmine-alpha-data'

    firehoseStreamName = 'player_stream_' + generateSecureFruitString()   
    try:
        createdFirehose = firehoseClient.create_delivery_stream(
            DeliveryStreamName = firehoseStreamName,
            S3DestinationConfiguration = {
                'RoleARN': roleARN,
                'BucketARN': bucketARN,
                'BufferingHints': {
                    'SizeInMBs': 128,
                    'IntervalInSeconds': 60 # TODO set to 900 for deploy MUST update minDelay in mysqllib!
                }
            })
    except Exception as E:
        print (E)
        return None
    else:
        streamStatus = firehoseClient.describe_delivery_stream(DeliveryStreamName=firehoseStreamName)
        versionID = streamStatus['DeliveryStreamDescription']['VersionId']
        playerDB.addFirehoseStream(firehoseStreamName,versionID, inUse=inUse, uid=uid)
        return firehoseStreamName

def returnFirehoseStream(playerDB, firehoseClient, streamName, uid):
    # TODO Validate UID
    if False:
        return

    if streamName is None:
        print("StreamName is none!")
        return

    # TODO Validate stream is checked out

    streamStatus = firehoseClient.describe_delivery_stream(DeliveryStreamName=streamName)
    print("Status before update: ")
    print(streamStatus)
    status = streamStatus['DeliveryStreamDescription']['DeliveryStreamStatus']
    versionID = streamStatus['DeliveryStreamDescription']['VersionId']
    currentStreamVersion = playerDB.getFirehoseStreamVersion(streamName)
    print("Stream version is " +  versionID + " and database says " + currentStreamVersion)

    if (currentStreamVersion is None or versionID != currentStreamVersion):
        print("Stream version is inconsistent! Actual is " +  versionID + " but database says " + currentStreamVersion)
        return None
    elif status is None:
        return None
    elif status != 'ACTIVE':
        print("Stream not active! Status is " +  status)
        playerDB.returnFirehoseStream(streamName, versionID, outdated = False)
        return versionID
    
    playerDB.returnFirehoseStream(streamName, versionID, outdated = True)

    return versionID

# Update an outdated firehose stream that has been dormate long enough for its buffer to be flushed
def updateFirehoseStream(playerDB, firehoseClient, streamName):
    streamStatus = firehoseClient.describe_delivery_stream(DeliveryStreamName=streamName)
    status = streamStatus['DeliveryStreamDescription']['DeliveryStreamStatus']
    versionID = streamStatus['DeliveryStreamDescription']['VersionId']
    destinationId = streamStatus['DeliveryStreamDescription']['Destinations'][0]['DestinationId']
    currentStreamVersion = playerDB.getFirehoseStreamVersion(streamName)

    # #TODO remove debug
    # print("Updating stream")
    # print(streamStatus)
    # print("Stream version is " +  versionID + " and database says " + currentStreamVersion)

    if (currentStreamVersion is None or versionID != currentStreamVersion):
        print("Stream version is inconsistent! Actual is " +  versionID + " but database says " + currentStreamVersion)
        return None
    elif status is None:
        return None
    elif status != 'ACTIVE':
        print("Stream not active! Status is " +  status)
        # Any previous owner could not have written records to an inactive stream - safe to mark uptodate
        playerDB.returnFirehoseStream(streamName, versionID, outdated = False)
        return versionID

    firehoseClient.update_destination(
        DeliveryStreamName=streamName,
        CurrentDeliveryStreamVersionId = versionID,
        DestinationId = destinationId,
        S3DestinationUpdate = {
            'BufferingHints': {
                'SizeInMBs': 128,
                'IntervalInSeconds': 60 # TODO set to 900 for deploy
            }
        })
    # Return key to pool

    streamStatus = firehoseClient.describe_delivery_stream(DeliveryStreamName=streamName)
    newVersionID = streamStatus['DeliveryStreamDescription']['VersionId']
    
    for _ in range(10):
        if (versionID != newVersionID):
            playerDB.returnFirehoseStream(streamName, newVersionID, outdated = False)
            return
        else:
            streamStatus = firehoseClient.describe_delivery_stream(DeliveryStreamName=streamName)
            newVersionID = streamStatus['DeliveryStreamDescription']['VersionId']
            time.sleep(1)

    if (versionID == newVersionID):
        playerDB.returnFirehoseStream(streamName, newVersionID, outdated = True)
    else:
        playerDB.returnFirehoseStream(streamName, newVersionID, outdated = False)
        
class MyUDPHandler(socketserver.BaseRequestHandler):
    """
    This class works similar to the TCP handler class, except that
    self.request consists of a pair of data and client socket, and since
    there is no connection the client address must be given explicitly
    when sending data back via sendto().
    """

    def handle(self):
        data = self.request[0]
        socket = self.request[1]

        try:
            request = json.loads(data, encoding="utf-8")
        except ValueError as error:
            #TODO return error
            return
        
        if 'cmd' in request:
            response = {}
            response['timestamp'] = datetime.datetime.now().strftime("%m_%d_%H_%M_%S")

            playerDB = mySQLLib.mySQLLib()
            playerDB.Open("user_database")


            ## If there was no error in executing the command the packet will be sent at the end
            ## if an error was encountered set 'error' = true, send the packet, and return immediately
            
            # Form the response
            if request['cmd'] == 'echo':
                response['cmd'] = request['cmd']

            #######          List Users          ########
            elif request['cmd'] == 'list_users':
                response['list'] = playerDB.listUsers()
                print(response)

            ########           Add User          ########  
            elif request['cmd'] == 'add_user':
                # TODO check if they exist but are 'removed' or 'banned', etc.
        
                if 'email' in request and 'uid' in request and 'mcusername' in request:
                    try:
                        unique_elements = playerDB.isUnique( request['email'], request['mcusername'], request['uid'])
                        if not all(unique_elements.values()):
                            
                            # User's info is not unique - return a helpfull message
                            response['unique'] = False
                            status = playerDB.getStatus(request['uid'])

                            # User is allready added
                            if unique_elements['minecraft_username'] == False:
                                response['error'] = True
                                response['message'] = 'Username is taken'
                                socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)
                                return
                            elif unique_elements['email'] == False:
                                response['error'] = True
                                response['message'] = 'Email address is taken'
                                socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)
                                return
                            elif unique_elements['uid'] == False:
                                response['error'] = True
                                response['message'] = 'Hash collision in minecraft uuid'
                                socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)
                                return
                            else:
                                response['error'] = True
                                response['message'] = 'Unknown error'
                                socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)
                                return

                        # Add the user to the database
                        playerDB.addUser( \
                        request['email'],
                        request['mcusername'],
                        request['uid'])

                        response['error'] = False
                        #response['uid'] = uid
                        response['message'] =  'User {} has been successfully added!'.format(request['mcusername'])
                        socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)
                        return
                    except Exception as e:
                        response['error'] = True
                        response['message'] = repr(e)
                        socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)
                        return
                    
                else:    
                    response['error'] = True
                    response['message'] = 'Required fields not populated, must supply email and mcusername'
                    socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)
                    return

            ########           Add to Queue          ########
            elif request['cmd'] == 'add_to_queue':
                # TODO Remove user by marking them as removed (not deleting them)
                if 'uid' in request:
                        
                    #playerDB.deleteUserViaUID(request['uid'])
                    response['message'] = 'Nothing happend - we this command will be supported at a later date'
                else:
                    response['error'] = True
                    response['message'] = 'Request needs one of uid, mcusername, email'

            ########           Remove User          ########
            elif request['cmd'] == 'remove_user':
                # TODO Remove user by marking them as removed (not deleting them)
                if 'uid' in request:
                        
                    #playerDB.deleteUserViaUID(request['uid'])
                    response['message'] = 'Nothing happend - we this command will be supported at a later date'
                else:
                    response['error'] = True
                    response['message'] = 'Request needs one of uid, mcusername, email'

            ########           Validate Key          ########
            elif request['cmd'] == 'validate_minecraft_key':

                if 'uid' in request and 'minecraft_key' in request:
                    

                    try:
                        mcKey = playerDB.getMinecraftKeyViaUID(request['uid'])
                    except Exception:
                        response['error'] = True
                        response['message'] = 'Error getting key by uid'
                        socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)
                        return
                    else: 
                        if not mcKey is None and mcKey == request['minecraft_key']:
                            response['key_is_valid'] = True
                        else:
                            response['key_is_valid'] = False
                else:
                    response['error'] = True
                    response['message'] = 'Request needs both <uid> and <key>'

            ########         Player Disconnect         ########
            elif request['cmd'] == 'disconnect_user':
                if 'uid' in request:
                    try:
                        streamName = playerDB.getFirehoseStreamNameViaUID(request['uid'])
                    except Exception:
                        response['error'] = True
                        response['message'] = 'Error getting stream name by uid'
                        socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)
                        return
                    else: 
                        uid = request['uid']

                        playerDB.clearMinecraftKeyViaUID(uid)
                        
                        response['message'] = 'Will try to return stream {} and minecraft key for user {}'.format(streamName, uid)
                        socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)

                        # TODO manage stale streams in another server
                        time.sleep(10) 
                        newSteamName = playerDB.getFirehoseStreamNameViaUID(request['uid'])
                        if streamName == newSteamName and not streamName is None:
                            firehoseClient = boto3.client('firehose', region_name='us-east-1')
                            returnFirehoseStream(playerDB, firehoseClient, streamName, uid)

                else:
                    response['error'] = True
                    response['message'] = 'Request needs both <uid> and <key>'

            ########           Get Status            ########
            elif request['cmd'] == 'get_status':
                if 'uid' in request: 
                    try:
                        status = playerDB.getStatus(request['uid'])
                        response.update(status)
                    except Exception:
                        response['error'] = True
                        response['message'] = 'Could not find user by uid'
                        socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)
                        return
                    else:       
                        response['command_status'] = 'Success'
                        response['message'] = 'Status returned sucessfully'
                else:
                    response['error'] = True
                    response['message'] = 'Request needs one of uid, mcusername, email'

            ########           Change Status        ########
            elif request['cmd'] == 'make_awesome':
                # TODO Remove user by marking them as removed (not deleting them)
                if 'uid' in request: 
                    response['banned'] = False 
                    response['awesome'] = True
                    response['queue_position'] = 120
                    response['message'] = 'Command not supported yet'
                else:
                    response['error'] = True
                    response['message'] = 'Request needs valid uid'

            ########           Get MC Key           ########
            elif request['cmd'] == 'get_minecraft_key':
                if 'uid' in request and request['uid'] != None:
                    uid = request['uid']

                    status = playerDB.getStatus(uid)

                    if 'invalid' in status:
                        if status['invalid']:
                            # UID is invalid - TODO don't respond 
                            response['error'] = True
                            response['message'] = 'Failed, uid is invalid'
                            socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)
                            return
                        elif status['banned']:
                            response['error'] = False
                            response['message'] = 'User is banned from play'
                            socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)
                            return
                        elif status['removed']:
                            response['error'] = False
                            response['message'] = 'User has been removed from the database'
                            socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)
                            return
                    else:
                        print('error retreving status for user')

                    minecraft_key = generateSecureString(45)
                    playerDB.setMinecraftKeyViaUID(request['uid'], minecraft_key)
                    response['minecraft_key'] = minecraft_key
                else:
                    response['error'] = True
                    response['message'] = 'Request needs valid uid'

            ########        Get FireHose Key        ########
            elif request['cmd'] == 'get_firehose_key':
                if 'uid' in request:
                    uid = request['uid']

                    status = playerDB.getStatus(uid)

                    if 'invalid' in status:
                        if status['invalid']:
                            # UID is invalid - TODO don't respond 
                            response['error'] = True
                            response['message'] = 'Failed, uid is invalid'
                            socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)
                            return
                        elif status['banned']:
                            response['error'] = False
                            response['message'] = 'User is banned from play'
                            socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)
                            return
                        elif status['removed']:
                            response['error'] = False
                            response['message'] = 'User has been removed from the database'
                            socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)
                            return
                    else:
                        print('error retreving status for user')
                
                    # Get Session Token via AssumeRole
                    sts = boto3.client('sts')
                    role = {}
                    try:
                        sessionName = str(uid) + datetime.datetime.now().strftime("_%m_%d_%H_%M_%S")
                        role = sts.assume_role(RoleArn='arn:aws:iam::215821069683:role/iam_client_streamer', RoleSessionName=sessionName)
                    except sts.exceptions.ClientError as e:
                        print('Error assuming role!')
                        playerDB.returnFirehoseStream(streamName, 'err')
                        return

                    credentials = role['Credentials']

                    # Get key from pool
                    streamName = playerDB.getFirehoseStream(uid)
                    if not streamName is None:
                        response['stream_name'] = streamName

                    # If we failed to find a stream in the pool open a new FireHose Stream
                    if not 'stream_name' in response:
                        # Open FireHose Client
                        firehoseClient = boto3.client('firehose', region_name='us-east-1')
                        response['stream_name'] = crateFirehoseStream(playerDB, firehoseClient,inUse = True, uid = uid)

                    response['access_key'] = credentials['AccessKeyId']
                    response['secret_key'] = credentials['SecretAccessKey']
                    response['session_token'] = credentials['SessionToken']

                    # Record stream and session id in database and send to player right away
                    playerDB.setFirehoseCredentialsViaUID(
                        uid, 
                        response['stream_name'],
                        response['access_key'], 
                        response['secret_key'], 
                        response['session_token'])
                    socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)

                    # Ask for an outdated stream and try update it
                    outOfDate = playerDB.getOutOfDateFirehoseStream()
                    if not outOfDate is None:
                        firehoseClient = boto3.client('firehose', region_name='us-east-1')
                        updateFirehoseStream(playerDB, firehoseClient, outOfDate)

                    # Ask for another outdated stream and try update it
                    outOfDate = playerDB.getOutOfDateFirehoseStream()
                    if not outOfDate is None:
                        firehoseClient = boto3.client('firehose', region_name='us-east-1')
                        updateFirehoseStream(playerDB, firehoseClient, outOfDate)

                    # Expand the pool if below the minimum size
                    if playerDB.getFirehoseStreamCount() < FIREHOSE_STREAM_MIN_AVAILABLE:
                        firehoseClient = boto3.client('firehose', region_name='us-east-1')
                        crateFirehoseStream(playerDB, firehoseClient)
                    return


                else:
                    response['error'] = True
                    response['message'] = 'Request needs valid uid'

            ########       Return FireHose Key       ########
            elif request['cmd'] == 'return_firehose_key':
                if 'uid' in request and 'stream_name' in request:
                    # TODO Validate uid that checked it out is returning it (maybe)

                    uid = request['uid']
                    streamName = request['stream_name']
                    firehoseClient = boto3.client('firehose', region_name='us-east-1')

                    versionID = returnFirehoseStream(playerDB, firehoseClient, streamName, uid)

                    if versionID is None:
                        response['error'] = True
                        response['message'] = 'Was not able to properly return firehose stream ' + streamName
                    else:
                        response['message'] = 'Stream ' + streamName + ' v' + versionID + ' returned sucessfully'
                else:
                    response['error'] = True
                    response['message'] = 'Stream name or uid not provided'



            ########          Default Error          ########
            else:
                response['error'] = True
                response['message'] = 'Failed, cmd not understood'
                socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)
                return

            ###    Send response    ###
            if not 'error' in response:
                response['error'] = False
            socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)
        else:
            return

        print("{} wrote:".format(self.client_address[0]))
        print(data)

    def handle_error(self, request, client_address):
        data = self.request[0]
        socket = self.request[1]
        
        response = {}
        response['error'] = True
        response['message'] = "Exception processing request " + data
        socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)

        print (data)
        socketserver.ThreadingUDPServer.handle_error(self, request, client_address)

            

if __name__ == "__main__":
    HOST, PORT = "0.0.0.0", 9999
    with socketserver.ThreadingUDPServer((HOST, PORT), MyUDPHandler) as server:
        server.serve_forever()