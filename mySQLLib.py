import os
import time
import datetime
import mysql.connector




# Remote access 
#SERVER= '73.214.249.180'
#PORT = 13306

## Iam Specific SQLite class
#    
# 
class mySQLLib:

    dbUser ='alpha'
    dbPasswd = 'alphapassword'
    dbServer = 'cmu-rl.c2gld0sydy91.us-east-1.rds.amazonaws.com'
    dbPort = 3306

    minDelta = datetime.timedelta(minutes = 10)

    ## The constructor
    def __init__(self):
        pass
    
    ## Opens the database - validates that the database exists as a file
    #  Don't know how to validate that open was successful
    def Open(self,database):
        try:
            self.conn = mysql.connector.connect(user=self.dbUser, password=self.dbPasswd, host=self.dbServer,db=database, port=self.dbPort,buffered=True)
            self.cursor = self.conn.cursor()
            return 0
        except mysql.connector.Error as e:
            print ("\tError code:", e.errno)        # error number
            print ("\tSQLSTATE value:", e.sqlstate) # SQLSTATE value
            print ("\tError message:", e.msg)       # error message
            print ("\tError:", e)                   # errno, sqlstate, msg values
            s = str(e)
            print ("\tError:", s)                   # errno, sqlstate, msg values
            self.conn = None
            self.cursor = None
            return e.errno

    # This connects to the server, but doesn't open a database on the server
    # This may be a mySQL specific thing
    # The primary use is for creating databases that don't exist on the server
    def Connect(self):
        print ("Connecting to server")
        try:
            self.conn = mysql.connector.connect(user=self.dbUser, password=self.dbPasswd, host=self.dbServer,buffered=True)
            self.cursor = self.conn.cursor()
            return 0
        except mysql.connector.Error as e:
            print ("\tError code:", e.errno)        # error number
            print ("\tSQLSTATE value:", e.sqlstate) # SQLSTATE value
            print ("\tError message:", e.msg)       # error message
            print ("\tError:", e)                   # errno, sqlstate, msg values
            s = str(e)
            print ("\tError:", s)                   # errno, sqlstate, msg values
            self.conn = None
            self.cursor = None
            return e.errno        

    ## Close the connection to the database     
    def Close(self):
        if self.conn is None:
            pass
        else:
            try:
                self.conn.close()
            except:
                pass
        self.conn = None

    #########################
    # USER Commands
    ##########################

    def listUsers(self):
        if self.conn is None:
            # error
            print ("Player Database is not open")
            pass
        else:
            minecraftUsernames = []

            cur = self.conn.cursor()
            cur.execute("SELECT minecraftUsername FROM user_table ORDER by id") # We aren't achieving sorting as expected- why is this?
            for row in cur:
                minecraftUsernames.append(row[0])
            return minecraftUsernames
 
    # Add user to the database
    # No error checking - throws exception if user already exists
    def addUser(self,email,minecraftUsername,uid):
        if self.conn is None:
            # error
            pass
        else:
            cur= self.conn.cursor()
            try:
                cur.execute ("INSERT INTO user_table (email,minecraftUsername,uid) VALUES(%s,%s,%s)",(email,minecraftUsername,uid,))
                self.conn.commit()
            except:
                # Error
                pass
            cur.close()

    # Deletes a user from the database
    # No error checking - throws exception if user does not exist
    def deleteUserviaEmail(self,value):
        if self.conn is None:
            # error
            pass
        else:
            cur= self.conn.cursor()
            try:
                cur.execute ("DELETE FROM user_table WHERE email='%s'"%(value))
                self.conn.commit()
            except:
                # Error
                pass
            cur.close()

    # Deletes a user from the database
    # No error checking - throws exception if user does not exist
    def deleteUserviaMinecraftUsername(self,value):
        if self.conn is None:
            # error
            pass
        else:
            cur= self.conn.cursor()
            try:
                cur.execute ("DELETE FROM user_table WHERE minecraftUsername='%s'"%(value))
                self.conn.commit()
            except:
                # Error
                pass
            cur.close()



    #########################
    # MINECRAFT Commands
    ##########################

    # Set (UPDATE) minecraft Key with UID
    def setMinecraftKeyViaUID(self,uid,key):
        if self.conn is None:
            # error
            pass
        else:
            cur= self.conn.cursor()
            cur.execute ("UPDATE user_table SET minecraftKey='%s' WHERE uid='%s'" % (key, uid))
            self.conn.commit()
            cur.close()

    def clearMinecraftKeyViaUID(self,uid):
        if self.conn is None:
            # error
            pass
        else:
            cur= self.conn.cursor()
            cur.execute ("UPDATE user_table SET minecraftKey=null WHERE uid='%s'" % (uid))
            self.conn.commit()
            cur.close()

    # Set (UPDATE) minecraft Key with minecraft username
    def setMinecraftKeyViaMinecraftUsername(self,minecraftUsername,key):
        if self.conn is None:
            # error
            pass
        else:
            cur= self.conn.cursor()
            cur.execute ("UPDATE user_table SET minecraftKey='%s' WHERE minecraftUsername='%s'" % (key,minecraftUsername))
            self.conn.commit()
            cur.close()

    # Get minecraft Key with user id
    def getMinecraftKeyViaUID(self,uid):
        if self.conn is None:
            # error
            pass
        else:
            cur= self.conn.cursor()
            cur.execute ("SELECT minecraftKey FROM user_table WHERE uid='%s'"%(uid))
            key = cur.fetchone()
            cur.close()
            if key is None:                 
                return None             
            else:                 
                return key[0]

    # Get minecraft Key with email address
    def getMinecraftKeyViaEmail(self,email):
        if self.conn is None:
            # error
            pass
        else:
            cur= self.conn.cursor()
            cur.execute ("SELECT minecraftKey FROM user_table WHERE email='%s'"%(email))
            key = cur.fetchone()
            cur.close()
            if key is None:
                return None
            else:
                return key[0]

    # Get minecraft Key with Username
    def getMinecraftKeyViaMinecraftUsername(self,minecraftUsername):
        if self.conn is None:
            # error
            pass
        else:
            cur= self.conn.cursor()
            cur.execute ("SELECT minecraftKey FROM user_table WHERE minecraftUsername='%s'"%(minecraftUsername))
            key = cur.fetchone()
            cur.close()
            if key is None:
                return None
            else:
                return key[0]

    # #########################
    # # AWS Stream Commands
    # ##########################

    def listStreams(self):
        if self.conn is None:
            # error
            print ("Player Database is not open")
            pass
        else:
            playerStreams = []

            cur = self.conn.cursor()
            cur.execute("SELECT streamName FROM stream_table ORDER by id") # We aren't achieving sorting as expected- why is this?
            for row in cur:
                playerStreams.append(row[0])
            return playerStreams

    # Add firehose stream to the pool
    def addFirehoseStream(self, name, version, inUse=0, uid=None):
        if self.conn is None or name is None:
            # error
            pass
        else:
            cur = self.conn.cursor()

            dateTimeStr = (datetime.datetime.utcnow() - self.minDelta).strftime('%Y-%m-%d %H:%M:%S')
            
            cur.execute("INSERT INTO stream_table (streamName, streamVersion, inUse, lastReturned) VALUES(%s,%s,%s,%s)",(name, version, inUse, dateTimeStr))
            self.conn.commit()

            if not uid is None:
                cur.execute("UPDATE user_table SET firehoseStreamName=%s WHERE uid=%s",(name,uid))
                self.conn.commit()
            cur.close()

    # Get firehose stream from pool
    # Returns None if no such stream is available
    #   or a valid streamName otherwise
    def getFirehoseStream(self, uid):
        if self.conn is None:
            # error
            pass
        else:
            cur= self.conn.cursor()

            dateTimeStr = (datetime.datetime.utcnow() + self.minDelta).strftime('%Y-%m-%d %H:%M:%S')
            cur.execute ("SELECT streamName FROM stream_table WHERE inUse=0 AND outdated=0 AND lastReturned < '%s'"%(dateTimeStr))
            name = cur.fetchone()

            if name is None:
                print('Found no streams returned before ' + dateTimeStr)
                return None
            else:
                # BAH testing 
                print(name[0])
                cur.execute ("UPDATE stream_table SET inUse=1 WHERE streamName='%s'" % (name[0]))
                self.conn.commit()
                cur.execute("UPDATE user_table SET firehoseStreamName='%s' WHERE uid='%s'" % (name[0],uid))
                self.conn.commit()
                cur.close()
                return name[0]

    def getOutOfDateFirehoseStream(self):
        if self.conn is None:
            # error
            pass
        else:
            cur= self.conn.cursor()

            dateTimeStr = (datetime.datetime.utcnow() + self.minDelta).strftime('%Y-%m-%d %H:%M:%S')
            cur.execute ("SELECT streamName FROM stream_table WHERE inUse=0 AND outdated=1 AND lastReturned < '%s'"%(dateTimeStr))
            name = cur.fetchone()

            if name is None:
                print('Found no out of date streams that were returned before ' + dateTimeStr)
                return None
            else:
                return name[0]

    # Takes a stream name that has been configured and puts it back in the pool
    # Streams must be versioned correctly! You must not add stream to pool until
    # stream version matches that returnd by the changeStream function (if called)
    # name must be unique
    def returnFirehoseStream(self, name, version, outdated=False):
        if self.conn is None:
            # error
            pass
        else:
            cur= self.conn.cursor()
            
            if outdated:
                timeStr = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                cur.execute ("UPDATE stream_table SET inUse=0,outdated=1,streamVersion='%s',lastReturned='%s' WHERE streamName='%s'" % (version, timeStr, name))
            else:
                cur.execute ("UPDATE stream_table SET inUse=0,outdated=0,streamVersion='%s' WHERE streamName='%s'" % (version, name))
 
            self.conn.commit()
            cur.close()

    def getFirehoseStreamCount(self):
        if self.conn is None:
            # error
            pass
        else:
            cur= self.conn.cursor()
            cur.execute ("SELECT COUNT(inUse) from stream_table WHERE inUse=0 AND outdated=0")
            (countNotInUse,) = cur.fetchone()
            cur.close()
            return countNotInUse

    def deleteFirehoseStream(self, streamName):
        if self.conn is None:
            pass
        else:
            cur = self.conn.cursor()
            cur.execute("DELETE FROM stream_table WHERE streamName='%s';"%(streamName))
            self.conn.commit()
            cur.close()

    #########################
    # FIREHOSE Commands 
    ##########################

    # Get firehose stream version
    def getFirehoseStreamVersion(self, streamName):
        if self.conn is None:
                pass
        else:
            cur= self.conn.cursor()
            cur.execute ("SELECT streamVersion FROM stream_table WHERE streamName='%s'"%(streamName))
            name = cur.fetchone()
            cur.close()
            if name is None:
                return None
            else:
                return name[0]
                    
    # Get firehose Key with UID
    def getFirehoseStreamNameViaUID(self,uid):
        if self.conn is None:
            # error
            pass
        else:
            cur= self.conn.cursor()
            cur.execute ("SELECT firehoseStreamName FROM user_table WHERE uid='%s'"%(uid))
            key = cur.fetchone()
            cur.close()
            if key is None:
                return None
            else:
                return key[0]

    # Get firehose Key with UID
    def clearFirehoseStreamNameViaUID(self,uid):
        if self.conn is None:
            # error
            pass
        else:
            cur= self.conn.cursor()
            cur.execute ("UPDATE user_table SET firehoseStreamName = NULL WHERE uid='%s'"%(uid))
            self.conn.commit()

   # Set (UPDATE) firehose Key with UID
    def setFirehoseStreamNameViaUID(self,uid,key):
        if self.conn is None:
            # error
            pass
        else:
            cur= self.conn.cursor()
            cur.execute ("UPDATE user_table SET firehoseStreamName='%s' WHERE uid='%s'" % (key,uid))
            self.conn.commit()
            cur.close()

    # Set all firehose credentials with UID
    def setFirehoseCredentialsViaUID(self,uid, streamName, accessKey, secretKey, sessionToken):
        if self.conn is None:
            # error
            pass
        else:
            return
            cur= self.conn.cursor()
            # TODO send the rest of credentials to server
            cur.execute ("UPDATE user_table SET firehoseStreamName='%s' WHERE uid='%s'" % (streamName,uid))
            self.conn.commit()
            cur.close()

    #########################
    # Is Unique 
    #########################
    def isUnique (self,email,minecraftUsername,uid):
        status = {}
        if self.conn is None:
            # error
            pass
        else:
            cur= self.conn.cursor()
            try:
                cur.execute ("SELECT id FROM user_table WHERE email='%d'"%(email))
                (tmp,) = cur.fetchone()
                print(tmp)
                if tmp is None:
                    
                    tmp = True
                else:
                    tmp = False
            except:
                tmp = True
            status['email'] = tmp
            try:
                cur.execute ("SELECT id FROM user_table WHERE minecraftUsername='%d'"%(minecraftUsername))
                (tmp,) = cur.fetchone()
                print(tmp)
                if tmp is None:
                    tmp = True
                else:
                    tmp = False
            except:
                tmp = True
            status['minecraft_username'] = tmp
            try:
                cur.execute ("SELECT id FROM user_table WHERE uid='%s'"%(uid))
                (tmp,) = cur.fetchone()
                print(tmp)
                if tmp is None:
                    tmp = True
                else:
                    tmp = False
            except:
                tmp = True
            status['uid'] = tmp
            cur.close()
        return status

    #########################
    # Get User Status
    #########################  

    # Query and return if the user is 'valid', 'banned', 'removed', etc.
    # default status is 'invalid' indicating user does not exist
    def getStatus (self, uid):
        if self.conn is None:
            # error
            pass
        else:
            cur= self.conn.cursor()
            try:
                cur.execute ("SELECT removed, banned, awesome, offQueue, id FROM user_table WHERE uid='%s'"%(uid))
                try:
                    (removed, banned, awesome, offQueue, id) = cur.fetchone()
                except ValueError() as e:
                    cur.close()
                    return {'invalid':True}                    
                else:
                    cur.close()
                    status = {}
                    status['invalid'] = False
                    status['removed'] = bool(removed)
                    status['banned'] = bool(banned)
                    status['awesome'] = bool(awesome)
                    status['off_queue'] = bool(offQueue)
                    status['queue_position'] = int(id)
                    return status
            except:
                return {'invalid':True}
                cur.close()



if __name__ == '__main__':    
    playerDB = mySQLLib ()
    playerDB.Open("user_database")
    print (playerDB.listUsers())
    playerDB.addUser('ricky.houghton@gmail.com','mountainBiker',2345)
    print (playerDB.listUsers())

    print ('Minecraft Key via UID               ',playerDB.getMinecraftKeyViaUID('1234'))
    print ('Minecraft Key via email             ',playerDB.getMinecraftKeyViaEmail('imushroom1@gmail.com'))
    print ('Minecraft Key via minecraft username',playerDB.getMinecraftKeyViaMinecraftUsername('imushroom1'))
    # print ('Firehose Key via UID                ',playerDB.getFirehoseKeyViaUID('1234'))


    playerDB.setMinecraftKeyViaUID(2345,'mnopqrstuv')
    playerDB.setMinecraftKeyViaMinecraftUsername('mountainBiker','stopit')

    print (playerDB.isUnique('fubar@gmail','mountainBiker',2345))
    print (playerDB.isUnique('imushroom1@gmail.com','imushroom1',1234))
    print (playerDB.isUnique('imushroom2@gmail.com','imushroom2',12345))

    print (playerDB.getFirehoseStreamCount())

    # FirehsoeKey = '987654'
    # playerDB.setFirehoseKeyViaMinecraftUsername('mountainBiker', FirehsoeKey)
    # tmp = playerDB.getFirehoseKeyViaMinecraftUsername('mountainBiker')
    # if tmp == FirehsoeKey:
    #     print ("setAWSViaMineCraftUsername: Success")
    # else:
    #     print (FirehsoeKey,tmp)
    #     print ("setAWSViaMineCraftUsername: FAILURE")

    #playerDB.deleteUserViaEmail('ricky.houghton@gmail.com')
    playerDB.deleteUserviaMinecraftUsername('mountainBiker')

    playerDB.Close()