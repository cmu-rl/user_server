import boto3
import mySQLLib

playerDB = mySQLLib.mySQLLib()
playerDB.Open("user_database")

streams = playerDB.listStreams()

firehoseClient = boto3.client('firehose', region_name='us-east-1')


for streamName in streams:
    if 'player_stream' in streamName:
        streamStatus = firehoseClient.describe_delivery_stream(DeliveryStreamName=streamName)
        versionID = streamStatus['DeliveryStreamDescription']['VersionId']
        print ("Deleting stream: " + streamName + ' v' + str(versionID))

        firehoseClient.delete_delivery_stream(DeliveryStreamName=streamName)
        playerDB.deleteFirehoseStream(streamName)

        


