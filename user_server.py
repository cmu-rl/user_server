import boto3
import json
import random
import string
import hashlib
import datetime
import mySQLLib
import socketserver

def generateUserID(minecraftID):
    return hashlib.md5(minecraftID.encode('utf-8')).hexdigest()

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
        
                if 'email' in request and 'mcusername' in request:

                    uid = generateUserID(request['mcusername'])

                    #Check if user info is unique
                    if not all( playerDB.isUnique( request['email'], 
                            request['mcusername'], uid)):
                        response['unique'] = False

                        status = playerDB.getStatus(uid)

                        # User is allready added if they have a status 
                        if status != 'invalid' and status != 'removed':
                            # TODO support re-adding users who have been removed
                            response['status'] = 'failure'
                            response['message'] = 'Username is taken'
                            response['status'] = status
                            socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)
                            return

                        elif status == 'invalid':
                            # User has an invalid status - they likely don't exist yet
                            response['status'] = 'failure'
                            response['message'] = 'Email address is taken'
                            socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)
                            return

                    # Add the user to the database
                    playerDB.addUser( \
                       request['email'],
                       request['mcusername'],
                       uid)

                    response['status'] = 'success'
                    #response['uid'] = uid
                    response['message'] =  'User {} has been successfully added!'.format(request['mcusername'])
                else:    
                    response['error'] = True
                    response['status'] = 'failure'
                    response['message'] = 'Required fields not populated, must supply email and mcusername'
                    socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)
                    return

            ########           Remove User          ########
            elif request['cmd'] == 'remove_user':
                # TODO Remove user by marking them as removed (not deleting them)
                if 'uid' in request:
                        
                    #playerDB.deleteUserViaUID(request['uid'])
                    response['status'] = 'Success'
                    response['message'] = 'Really nothing happend - we this command will be supported at a later date'
                else:
                    response['status'] = 'Failure'
                    response['message'] = 'Request needs one of uid, mcusername, email'

            ########           Get Status            ########
            elif request['cmd'] == 'get_status':
                # TODO Remove user by marking them as removed (not deleting them)
                # TODO Implement actual status return
                if 'uid' in request: 
                    response['banned'] = False 
                    response['awesome'] = True
                    response['queue_position'] = 120
                    response['message'] = 'Request needs one of uid, mcusername, email'
                else:
                    response['status'] = 'Failure'
                    response['message'] = 'Request needs one of uid, mcusername, email'

            ########           Change Status        ########
            elif request['cmd'] == 'make_awesome':
                # TODO Remove user by marking them as removed (not deleting them)
                if 'uid' in request: 
                    response['banned'] = False 
                    response['awesome'] = True
                    response['queue_position'] = 120
                    response['message'] = 'Request needs valid uid'
                else:
                    response['status'] = 'Failure'
                    response['message'] = 'Request needs valid uid'

            ########           Get MC Key           ########
            elif request['cmd'] == 'get_minecraft_key':
                minecraft_key = generateSecureString(45)
                if 'uid' in request:
                    playerDB.setMinecraftKeyViaUID(request['uid'], minecraft_key)
                    response['minecraft_key'] = minecraft_key
                else:
                    response['status'] = 'Failed'
                    response['message'] = 'Request needs valid uid'

            ########        Get FireHose Key        ########
            elif request['cmd'] == 'get_firehose_key':
                # TODO Check if UID is valid                
                # TODO Check if user has a key allready

                if 'uid' in request:

                    uid = request['uid']

                    sts = boto3.client('sts')
                
                    # Get Session Token via AssumeRole
                    sessionName = str(uid) + datetime.datetime.now().strftime("_%m_%d_%H_%M_%S")
                    role = sts.assume_role(RoleArn='arn:aws:iam::058861212628:role/iam_client_streamer', RoleSessionName=sessionName)

                    print (role)
                    credentials = role['Credentials']

                    # Open FireHose Stream with key
                    firehoseClient = boto3.client('firehose', region_name='us-east-1')

                    # the role for firehose needs to have access to S3 - make policy that includes this
                    roleARN =   'arn:aws:iam::058861212628:role/firehose_delivery_role'
                    bucketARN = 'arn:aws:s3:::rickyfubar'

                    firehoseStreamName = 'player_stream_' + str(uid) + datetime.date.today().strftime("_%H_%M_%S")   
                    try:
                        createdFirehose = firehoseClient.create_delivery_stream(
                            DeliveryStreamName = firehoseStreamName,
                            S3DestinationConfiguration = {
                                'RoleARN': roleARN,
                                'BucketARN': bucketARN
                            })
                    except Exception as E:
                        print (E)
                        return

                    # Record stream and session id in database
                    # playerDB.setFirehoseKeyViaUID(
                    #   uid, 
                    #   credentials['AccessKeyId'], 
                    #   credentials['SecretAccessKey'], 
                    #   credentials['SessionToken'],
                    #   firehoseStreamName)

                    # Send access tokens to requestor
                    response['stream_name'] = firehoseStreamName
                    response['access_key'] = credentials['AccessKeyId']
                    response['secret_key'] = credentials['SecretAccessKey']
                    response['session_token'] = credentials['SessionToken']
                    #TODO serialize expiriation object (below)
                    #response['expiration'] = credentials['Expiration']

                else:
                    response['status'] = 'Failed'
                    response['message'] = 'Request needs valid uid'


            ########          Default Error          ########
            else:
                response['error'] = True
                response['status'] = 'Failed, cmd not understood'
                socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)
                return

            ###    Command executed correctly     ###
            response['error'] = False

            ###    Send response    ###
            socket.sendto(bytes(json.dumps(response), "utf-8"), self.client_address)
        else:
            return

        print("{} wrote:".format(self.client_address[0]))
        print(data)

        

if __name__ == "__main__":
    HOST, PORT = "0.0.0.0", 9999
    with socketserver.UDPServer((HOST, PORT), MyUDPHandler) as server:
        server.serve_forever()