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
            cur.execute ("UPDATE user_table SET minecraftKey='%s' WHERE uid='%s'" % (key, uid,))
            self.conn.commit()
            cur.close()

    # Set (UPDATE) minecraft Key with minecraft username
    def setMinecraftKeyViaMinecraftUsername(self,minecraftUsername,key):
        if self.conn is None:
            # error
            pass
        else:
            cur= self.conn.cursor()
            cur.execute ("UPDATE user_table SET minecraftKey='%s' where minecraftUsername='%s'" % (key,minecraftUsername))
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
    # # AWSKey Commands
    # ##########################

    # # Set (UPDATE) AWS Key with UID
    # def setAWSKeyViaUID(self,uid,key):
    #     if self.conn is None:
    #         # error
    #         pass
    #     else:
    #         cur= self.conn.cursor()
    #         cur.execute ("UPDATE user_table SET AWSKey='%s' WHERE uid='%s'" % (key,uid,))
    #         self.conn.commit()
    #         cur.close()

    # # Set (UPDATE) AWS Key with minecraft username
    # def setAWSViaMinecraftUsername(self,minecraftUsername,key):
    #     if self.conn is None:
    #         # error
    #         pass
    #     else:
    #         cur= self.conn.cursor()
    #         cur.execute ("UPDATE user_table SET AWSKey='%s' where minecraftUsername='%s'" % (key,minecraftUsername))
    #         self.conn.commit()
    #         cur.close()

    # # Get AWS Key with user id
    # def getAWSKeyViaUID(self,uid):
    #     if self.conn is None:
    #         # error
    #         pass
    #     else:
    #         cur= self.conn.cursor()
    #         cur.execute ("SELECT AWSKey FROM user_table WHERE uid='%s'"%(uid))
    #         key = cur.fetchone()
    #         cur.close()
    #         return key

    # # Get minecraft Key with email address
    # def getAWSKeyViaEmail(self,email):
    #     if self.conn is None:
    #         # error
    #         pass
    #     else:
    #         cur= self.conn.cursor()
    #         cur.execute ("SELECT AWSKey FROM user_table WHERE email='%s'"%(email))
    #         key = cur.fetchone()
    #         cur.close()
    #         return key

    # # Get minecraft Key with Username
    # def getAWSKeyViaMinecraftUsername(self,minecraftUsername):
    #     if self.conn is None:
    #         # error
    #         pass
    #     else:
    #         cur= self.conn.cursor()
    #         cur.execute ("SELECT AWSKey FROM user_table WHERE minecraftUsername='%s'"%(minecraftUsername))
    #         key = cur.fetchone()
    #         cur.close()
    #         return key

    #########################
    # FIREHOSE Commands
    ##########################


    # Get firehose Key with UID
    def getFirehoseKeyViaUID(self,uid):
        if self.conn is None:
            # error
            pass
        else:
            cur= self.conn.cursor()
            cur.execute ("SELECT firehoseKey FROM user_table WHERE uid='%s'"%(uid))
            key = cur.fetchone()
            cur.close()
            if key is None:
                return None
            else:
                return key[0]

   # Set (UPDATE) firehose Key with UID
    def setFirehoseKeyViaUID(self,uid,key):
        if self.conn is None:
            # error
            pass
        else:
            cur= self.conn.cursor()
            cur.execute ("UPDATE user_table SET firehose='%s' WHERE uid='%s'" % (key,uid))
            self.conn.commit()
            cur.close()

    #########################
    # Is Unique 
    #########################
    def isUnique (self,email,minecraftUsername,uid):
        status = []
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
            status.append(tmp)
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
            status.append(tmp)
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
            status.append(tmp)
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
                cur.execute ("SELECT removed, banned, awesome FROM user_table WHERE uid='%s'"%(uid))
                (removed, banned, awesome,) = cur.fetchone()
                if removed is None:
                    cur.close()
                    return 'invalid'
                else:
                    cur.close()
                    status = {}
                    status['removed'] = bool(removed)
                    status['banned'] = bool(banned)
                    status['awesome'] = bool(awesome)
                    return status
            except:
                return 'invalid'
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
    print ('Firehose Key via UID                ',playerDB.getFirehoseKeyViaUID('1234'))
    print ('Firehose Key via email              ',playerDB.getFirehoseKeyViaEmail('imushroom1@gmail.com'))
    print ('Firehose Key via minecraft username ',playerDB.getFirehoseKeyViaMinecraftUsername('imushroom1'))

    playerDB.setMinecraftKeyViaUID(2345,'mnopqrstuv')
    playerDB.setMinecraftKeyViaMinecraftUsername('mountainBiker','stopit')

    print (playerDB.isUnique('fubar@gmail','mountainBiker',2345))
    print (playerDB.isUnique('imushroom1@gmail.com','imushroom1',1234))
    print (playerDB.isUnique('imushroom2@gmail.com','imushroom2',12345))

    FirehsoeKey = '987654'
    playerDB.setFirehoseKeyViaMinecraftUsername('mountainBiker',FirehsoeKey)
    tmp = playerDB.getFirehoseKeyViaMinecraftUsername('mountainBiker')
    if tmp == FirehsoeKey:
        print ("setAWSViaMineCraftUsername: Success")
    else:
        print (FirehsoeKey,tmp)
        print ("setAWSViaMineCraftUsername: FAILURE")

    #playerDB.deleteUserViaEmail('ricky.houghton@gmail.com')
    playerDB.deleteUserviaMinecraftUsername('mountainBiker')

    playerDB.Close()