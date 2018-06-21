import boto3
import json
import random
import string
import hashlib
import datetime
import mySQLLib
import socketserver

def generateUserID(minecraftUUID):
    return hashlib.md5(minecraftUUID.encode('utf-8')).hexdigest()

def generateSecureString(len):
    return ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(len))

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

            ########           Add User          ########  
            elif request['cmd'] == 'add_user':
                # TODO check if they exist but are 'removed' or 'banned', etc.
        
                if 'email' in request and 'uid' in request and 'mcusername' in request:

                    uid = request['uid']

                    #Check if user info is unique
                    if not all( playerDB.isUnique( request['email'], request['mcusername'], uid)):
                        response['unique'] = False
                        status = playerDB.getStatus(uid)

                        # User is allready added if they have a status 
                        if status != 'invalid' and status != 'removed':
                            # TODO support re-adding users who have been removed
                            response['error'] = True
                            response['message'] = 'Username is taken'
                            socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)
                            return

                        elif status == 'invalid':
                            # User has an invalid status - they likely don't exist yet
                            response['error'] = True
                            response['message'] = 'Email address is taken'
                            socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)
                            return

                    # Add the user to the database
                    playerDB.addUser( \
                       request['email'],
                       request['mcusername'],
                       uid)

                    response['error'] = False
                    #response['uid'] = uid
                    response['message'] =  'User {} has been successfully added!'.format(request['mcusername'])
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
                minecraft_key = generateSecureString(45)
                if 'uid' in request:
                    playerDB.setMinecraftKeyViaUID(request['uid'], minecraft_key)
                    response['minecraft_key'] = minecraft_key
                else:
                    response['error'] = True
                    response['message'] = 'Request needs valid uid'

            ########        Get FireHose Key        ########
            elif request['cmd'] == 'get_firehose_key':
                if 'uid' in request:

                    uid = request['uid']

                    # TODO Validate UID
                    if False:
                        return

                    # Get key from pool
                    # TODO store who checked out which key
                    streamName = playerDB.getFirehoseStream()

                    # Check that we have a good stream
                    if not streamName is None:
                        response['stream_name'] = streamName
                
                    # Get Session Token via AssumeRole
                    sts = boto3.client('sts')
                    # TODO handle error when role cannot be assumed
                    sessionName = str(uid) + datetime.datetime.now().strftime("_%m_%d_%H_%M_%S")
                    role = sts.assume_role(RoleArn='arn:aws:iam::058861212628:role/iam_client_streamer', RoleSessionName=sessionName)

                    credentials = role['Credentials']

                    # TODO if there was an error creating sesion credentials give the stream back

                    # If we failed to find a stream in the pool or
                    # if the pool is too low, open a new FireHose Stream
                    # TODO remove magic number (5)
                    if not 'stream_name' in response:
                        # Open FireHose Stream 
                        # TODO handle error when stream is unable to be created
                        firehoseClient = boto3.client('firehose', region_name='us-east-1')

                        # the role for firehose needs to have access to S3 - make policy that includes this
                        roleARN =   'arn:aws:iam::058861212628:role/firehose_delivery_role'
                        bucketARN = 'arn:aws:s3:::rickyfubar'


                        # TODO stop naming streams with UID - think of better scheme
                        firehoseStreamName = 'player_stream_' + str(uid) + datetime.datetime.now().strftime("_%m_%d_%H_%M_%S")   
                        try:
                            createdFirehose = firehoseClient.create_delivery_stream(
                                DeliveryStreamName = firehoseStreamName,
                                S3DestinationConfiguration = {
                                    'RoleARN': roleARN,
                                    'BucketARN': bucketARN
                                })
                        except Exception as E:
                            # TODO handle exception with error message
                            # TODO still give user a stream if this is an error
                            print (E)
                            return

                        # If pool was empty, give the stream to the user
                        if not ('stream_name' in response):
                            response['stream_name'] = firehoseStreamName
                            playerDB.addFirehoseStream(firehoseStreamName,'12345678', inUse=True)
                            
                        else: # Otherwise add this new stream to the pool
                            # TODO get actual version
                            playerDB.addFirehoseStream(firehoseStreamName,'12345678')



                    # Send access tokens to requestor
                    response['access_key'] = credentials['AccessKeyId']
                    response['secret_key'] = credentials['SecretAccessKey']
                    response['session_token'] = credentials['SessionToken']
                    #TODO serialize expiriation object (below)
                    #response['expiration'] = credentials['Expiration']

                    # Record stream and session id in database
                    playerDB.setFirehoseCredentialsViaUID(
                        uid, 
                        response['stream_name'],
                        response['access_key'], 
                        response['secret_key'], 
                        response['session_token'])


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

                    # TODO process returned stream name

                    # TODO validate that stream name was in the pool allready

                    # TODO Validate uid that checked it out is returning it (maybe)

                    # Return key to pool
                    playerDB.returnFirehoseStream(streamName, '123456')



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
    with socketserver.UDPServer((HOST, PORT), MyUDPHandler) as server:
        server.serve_forever()