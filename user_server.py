#!/usr/bin/python3.6
import boto3
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
                    'IntervalInSeconds': 60 # TODO set to 900 for deploy
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

            ########         Player Dissconnect         ########
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
                        #TODO remove minecraftkey from player 
                        playerDB.returnFirehoseStream(streamName, '12945920')
                        playerDB.clearFirehoseStreamNameViaUID(request['uid'])
                        response['message'] = 'Returned stream {}'.format(streamName)
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

                    # Get key from pool
                    streamName = playerDB.getFirehoseStream(uid)
                    if not streamName is None:
                        response['stream_name'] = streamName
                
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

                    # TODO if there was an error creating sesion credentials give the stream back

                    # If we failed to find a stream in the pool open a new FireHose Stream
                    if not 'stream_name' in response:
                        # Open FireHose Client
                        firehoseClient = boto3.client('firehose', region_name='us-east-1')
                        response['stream_name'] = crateFirehoseStream(playerDB, firehoseClient,inUse = True, uid = uid)


                    response['access_key'] = credentials['AccessKeyId']
                    response['secret_key'] = credentials['SecretAccessKey']
                    response['session_token'] = credentials['SessionToken']
                    #TODO serialize expiriation object (below)
                    #response['expiration'] = credentials['Expiration']

                    # Record stream and session id in database and send to player
                    playerDB.setFirehoseCredentialsViaUID(
                        uid, 
                        response['stream_name'],
                        response['access_key'], 
                        response['secret_key'], 
                        response['session_token'])
                    socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)

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

                    uid = request['uid']
                    streamName = request['stream_name']

                    # TODO Validate UID
                    if False:
                        return
                    firehoseClient = boto3.client('firehose', region_name='us-east-1')
                    streamStatus = firehoseClient.describe_delivery_stream(DeliveryStreamName=streamName)
                    versionID = streamStatus['DeliveryStreamDescription']['VersionId']
                    destinationId = streamStatus['DeliveryStreamDescription']['Destinations'][0]['DestinationId']
                    currentStreamVersion = playerDB.getFirehoseStreamVersion(request['stream_name'])
                    print("Stream version is " +  versionID + " and database says " + currentStreamVersion)

                    if (currentStreamVersion is None or versionID != currentStreamVersion):
                        print("Stream version is inconsistent! Actual is " +  versionID + " but database says " + currentStreamVersion)
                        # response['error'] = True
                        # response['message'] = "Stream name is invalid"
                        # socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)
                        # return

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

                    # TODO validate that stream name was in the pool allready

                    # TODO Validate uid that checked it out is returning it (maybe)

                    # Return key to pool
                    streamStatus = firehoseClient.describe_delivery_stream(DeliveryStreamName=streamName)
                    newVersionID = streamStatus['DeliveryStreamDescription']['VersionId']
                    if (versionID == newVersionID):
                        playerDB.returnFirehoseStream(streamName, newVersionID, outdated = True)
                    else:
                        playerDB.returnFirehoseStream(streamName,newVersionID, outdated = False)

                    playerDB.clearFirehoseStreamNameViaUID(uid)
                    print("Stream version is now " +  newVersionID)

                    response['message'] = "Stream " + streamName + " returned sucessfully"
                else:
                    response['error'] = True
                    response['message'] = "Stream name or uid not provided"



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

        

if __name__ == "__main__":
    HOST, PORT = "0.0.0.0", 9999
    with socketserver.ThreadingUDPServer((HOST, PORT), MyUDPHandler) as server:
        server.serve_forever()