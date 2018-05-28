import boto3
import json
import random
import string
import mySQLLib
import socketserver

#TODO Use config file for keys
AWS_ACCESS_KEY = AKIAI62AJGHAKX7LJHAQ
AWS_SECRET_KEY = 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   

def generateUserID(minecraftID):
    return hash(minecraftID)

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
            
            # Form the response
            if request['cmd'] == 'echo':
                response['cmd'] = request['cmd']

            elif request['cmd'] == 'list_users':
                playerDB = mySQLLib.mySQLLib()
                playerDB.Open("player_database")
                response['list'] = playerDB.listUsers()

            ########           Add User          ########  
            elif request['cmd'] == 'add_user':
                if 'email' in request and 'mcusername' in request:
                    playerDB.addUser( \
                        request['email'],
                        request['mcusername'],
                        generateUserID(request['mcusername']))
                    response['status'] = 'Success'
                else:    
                    response['status'] = 'Failed'
                    response['message'] = 'Required fields not populated, must supply email and mcusername'

            ########           Remove User          ########
            elif request['cmd'] == 'remove_user':
                if 'uid' in request: 
                    playerDB.deleteUserViaUID(request['uid'])
                    response['status'] = 'Success'
                elif 'mcusername' in request:
                    playerDB.deleteUserViaEmail(request['email'])
                    response['status'] = 'Success'
                elif 'email' in request:
                    playerDB.deleteUserViaEmail(request['email'])
                    response['status'] = 'Success'
                else:
                    response['status'] = 'Failure'
                    response['message'] = 'Request needs one of uid, mcusername, email'

            ########           Get MC Key           ########
            elif request['cmd'] == 'get_minecraft_key':
                minecraft_key = generateSecureString(45)
                if 'uid' in request:
                    playerDB.setMinecraftKeyViaUID(request['uid'], minecraft_key)
                elif 'mcusername' in request:
                    playerDB.setMinecraftKeyViaMinecraftUsername(request['mcusername'], minecraft_key)
                elif 'email' in request: 
                    playerDB.setMinecraftKeyViaEmail(request['email'], minecraft_key)
                else:
                    response['status'] = 'Failed'
                    response['message'] = 'Request needs one of [uid, mcusername, email]'

            ########        Get FireHose Key        ########
            elif request['cmd'] == 'get_firehose_key':
                # Determine user's UID if not given
                if 'uid' in request:
                    uid = request['uid']
                elif 'mcusername' in request:
                    uid = playerDB.getUIDViaMinecraftUsername(request['mcusername'])
                elif 'email' in request:
                    uid = playerDB.getUIDViaEmail(request['email'])
                else
                    uid = None

                if not uid is None:
                    # Create AWS UserName
                    username = 'mc_client_' + str(uid)
                    # Generate IAM User
                    client = boto3.client('iam')
                    client.create_user(Path='/players/',UserName=username)
                    
                    # Retrive User
                    iam = boto3.resource('iam')
                    user = iam.user(UserName=username)

                    # Add them to firehose_restricted security group
                    user.add_group(GroupName='firehose_restricted')
                    # Generate key pair for user 
                    access_key_pair = user.create_access_key_pair()
                
                    playerDB.setFirehoseKeyViaUID(uid, access_key_pair.id, access_key_pair.secret)

                else:
                    response['status'] = 'Failed'
                    response['message'] = 'Request needs one of uid, mcusername, email'


            ########          Default Error          ########
            else:
                response['error'] = True
                response['status'] = 'Failed, cmd not understood'

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