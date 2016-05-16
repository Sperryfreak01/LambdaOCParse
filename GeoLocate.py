from __future__ import print_function
import logging

logger = logging.getLogger()
logger.setLevel(logging.WARNING)
logger.debug('Loading function')


import json
import records
import sys
import datetime
import pymysql
import boto3
import geocoder

sqs = boto3.resource('sqs')

cityList = {'AV':'ALISO VIEJO', 'AN':'ANAHEIM', 'BR':'BREA', 'BP':'BUENA PARK', 'CN':'ORANGE COUNTY',
           'CS':'ORANGE COUNTY', 'CM':'COSTA MESA', 'CZ':'COTO DE CAZA', 'ON':'ORANGE COUNTY',
           'OS':'ORANGE COUNTY', 'CY':'CYPRESS', 'DP':'DANA POINT', 'DH':'DANA POINT', 'FA':'ORANGE COUNTY',
           'FV':'FOUNTAIN VALLEY', 'FU':'FULLERTON', 'GG':'GARDEN GROVE', 'HB':'HUNTINGTON BEACH', 'IR':'IRVINE',
           'JW':'JOHN WAYNE AIRPORT', 'LR':'LA HABRA', 'LM':'LA MIRADA', 'LP':'LA PALMA', 'LD':'LADERA RANCH',
           'LB':'LAGUNA BEACH', 'LH':'LAGUNA HILLS', 'LN':'LAGUNA NIGUEL', 'LW':'LAGUNA WOODS', 'LF':'LAKE FOREST',
           'FL':'LAS FLORES', 'LA':'LOS ALAMITOS', 'MC':'MIDWAY CITY', 'MV':'MISSION VIEJO', 'NB':'NEWPORT BEACH',
           'NH':'NEWPORT BEACH', 'OR':'ORANGE', 'OC':'ORANGE COUNTY', 'PL':'PLACENTIA',
           'RV':'RANCHO MISSION VIEJO', 'RS':'RANCHO SANTA MARGARITA','RO':'ROSSMOOR', 'SC':'SAN CLEMENTE',
           'SJ':'SAN JUAN CAPISTRANO', 'SA':'SANTA ANA', 'SB':'SEAL BEACH', 'SI':'SILVERADO CANYON', 'ST':'STANTON',
           'SN':'SUNSET BEACH', 'TC':'TRABUCO CANYON', 'TU':'TUSTIN', 'VP':'VILLA PARK', 'WE':'WESTMINSTER',
           'YL':'YORBA LINDA'}


def processQueue(context):
    db = records.Database('mysql+pymysql://Sperryfreak01:Matthdl13@192.168.5.185:3306/BlotBlotBlot')
    #sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='GeoLocation')

    retry_counter = 6
    while context.get_remaining_time_in_millis() > 30000 and retry_counter > 0:
        message_bodies = []
        messages_to_delete = []
        for message in queue.receive_messages(MaxNumberOfMessages=10):
            # process message body
            body = json.loads(message.body)
            message_bodies.append(body)
            # add message to delete
            messages_to_delete.append({
                'Id': message.message_id,
                'ReceiptHandle': message.receipt_handle
            })

        # if you don't receive any notifications the
        # messages_to_delete list will be empty
        if len(messages_to_delete) == 0:
            retry_counter -= 1
            print ('retries left: %s' % retry_counter)
        # delete messages to remove them from SQS queue
        # handle any errors
        else:
            dbUpdate(db, message_bodies)
            delete_response = queue.delete_messages(Entries=messages_to_delete)
    db.close()



def getLocation(textLocation, city):
    textLocation = ('%s %s CA') % (textLocation.replace("//", " and "), city)
    #logger.debug(textLocation)
    g = geocoder.google(textLocation, key='AIzaSyAjPgRQbIeHjaOX7FT8lap0r6M2TRHsZyw')
    if g.json['ok'] == True:
        splitedLat = str.split(str(g.json['lat']), '.')
        joinedLat = '%s.%s' %(splitedLat[0],splitedLat[1][:5])  # truncate the lat to 5 decimal points

        splitedLon = str.split(str(g.json['lng']), '.')
        joinedLon = '%s.%s' %(splitedLon[0],splitedLon[1][:5])  # truncate the long to 5 decimal points
        return joinedLat, joinedLon, g.json['confidence']
    else:
        logger.warning('Unable to geocode incident location: %s' % g.json)
        return 37.8267, -122.4233, -1


def dbUpdate(db, messages):
    for message in messages:
        #logger.debug ('%s' % (message))
        if message[u'type'] == u'Location':
            print ('Location')
            lat, lon, confidence = getLocation(message[u'IncidentLocation'],cityList[message[u'city']])

            try:
                'UPDATE Incidents SET notes=:note WHERE CaseNumber=:CaseNum'
                db.query('UPDATE Incidents SET lat=:Lat, lon=:Lon, confidence=:Confidence WHERE CaseNumber=:CaseNum',
                         CaseNum=message[u'CaseNum'],
                         Lat=lat,
                         Lon=lon,
                         Confidence=confidence
                         )

            except ValueError as e:
                logger.warning(e)


            except records.IntegrityError as e:
                logger.warning('got an integrity error from records %s' % e)



def lambda_handler(event, context):
    print ('event - %s' % event)
    print ('context - %s' % context)

    processQueue(context)
