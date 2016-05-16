from __future__ import print_function
import logging

logger = logging.getLogger()
logger.setLevel(logging.WARNING)
logger.debug('Loading function')


import json
import requests  # https://github.com/kennethreitz/requests
import records
import sys
from BeautifulSoup import BeautifulSoup
import datetime
import pymysql
import boto3

sqs = boto3.resource('sqs')


def processQueue(context):
    db = records.Database('mysql+pymysql://Sperryfreak01:Matthdl13@192.168.5.185:3306/BlotBlotBlot')
    #sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='Incidents')
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
            logger.debug('retries left: %s' % retry_counter)
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
    geolocationqueue = sqs.get_queue_by_name(QueueName='GeoLocation')
    for message in messages:
        #logger.debug ('%s' % (message))
        if message[u'type'] == u'incident':
            geoLookup = False
            try:
                db.query('INSERT INTO Incidents (CaseNumber, incidentdescription, location, incidentdate, city, arrest) VALUES(:CaseNum,:Description, :IncidentLocation, :IncidentDate, :city, :arrested)',
                         CaseNum=message[u'CaseNum'],
                         Description=message[u'Description'],
                         IncidentLocation=message[u'IncidentLocation'],
                         IncidentDate=message[u'IncidentDate'],
                         city=message[u'city'],
                         arrested=message[u'arrested']
                         )
                geoLookup = True

            except ValueError as e:
                logger.warning(e)


            except records.IntegrityError as e:
                logger.debug('got an integrity error from records %s' % e)

            if geoLookup:
                incident = {'type': 'Location',
                            'CaseNum': message[u'CaseNum'],
                            'IncidentLocation': message[u'IncidentLocation'],
                            'city': message[u'city']
                            }
                logger.debug("adding location to queue %s" % json.dumps(incident))
                response = geolocationqueue.send_message(MessageBody=json.dumps(incident))
                logger.debug(response)

            else:
                logger.debug("incident exsisted no need to geolocate")

        if message[u'type'] == u'notes':
            try:
                db.query('UPDATE Incidents SET notes=:note WHERE CaseNumber=:CaseNum',
                         CaseNum=message[u'CaseNum'],
                         note=message[u'notes'])
            except ValueError as e:
                logger.warning(e)


def arrestparse(db, casenumber):
    logger.debug('Arrest parsing case# %s' %casenumber)

    BlotterURL = "http://ws.ocsd.org/Whoisinjail/search.aspx?FormAction=CallNo&CallNo=%s" % casenumber
    name = dob = sex = race = status = height = bail = weight = hair = location = eye = occupation = None

    r = requests.get(BlotterURL)
    if r.status_code != requests.codes.ok:
        sys.exit()

    try:
        soup = BeautifulSoup(r.content)
    except AttributeError as e:
        logger.error(e)

    if 'ERROR - This page cannot be displayed at this time.' in soup.getText():
        logger.warning('error on page while parseing case# %s \n%s' % (casenumber, soup.getText()))
        try:
            db.query('''INSERT INTO Arrests (casenumber, arrestname, dob, sex, race, arreststatus, height, bail, weight, hair, location,
                     eye, occupation) VALUES (:casenum, :name, :dob, :sex, :race, :status, :height, :bail, :weight, :hair,
                     :location, :eye, :occupation)
                     ''',
                     casenum=casenumber,
                     name=name,
                     dob=dob,
                     sex=sex,
                     race=race,
                     status=status,
                     height=height,
                     bail=bail,
                     weight=weight,
                     hair=hair,
                     location=location,
                     eye=eye,
                     occupation=occupation
                     )
        except ValueError as e:
                    logger.warning(e)
        return

    table = soup.find("table", cellpadding=4)
    rows = table.findAll("tr")
    heading = rows[0].findAll('th')
    for prev, title, next in tableparse(heading):
        if title.getText() == 'Inmate Name:':
            name = " ".join(next.getText().split()).replace(' ,', ',')
            logger.debug('%s %s' % (title.getText(), name))

    for row in rows:
        cells = row.findAll("td")
        for prev, cell, next in tableparse(cells):
            # logger.debug( '%s, \n%s' % (cell.getText(), next))
            if cell.getText() == "Date of Birth:":
                 dob = next.getText().replace("&nbsp;", "")
                 date_object = datetime.datetime.strptime(dob, '%m-%d-%Y')
                 dob = date_object.strftime('%Y-%m-%d')
                 logger.debug('%s %s' % (cell.getText(), next.getText().replace("&nbsp;", "")))
            if cell.getText() == "Sex:":
                 sex = next.getText().replace("&nbsp;", "")
                 logger.debug('%s %s' % (cell.getText(), next.getText().replace("&nbsp;", "")))
            if cell.getText() == "Race:":
                race = next.getText().replace("&nbsp;", "")
                logger.debug('%s %s' % (cell.getText(), next.getText().replace("&nbsp;", "")))
            if cell.getText() == "Custody Status:":
                status = next.getText().replace("&nbsp;", "")
                logger.debug('%s %s' % (cell.getText(), next.getText().replace("&nbsp;", "")))
            if cell.getText() == "Height:":
                height  = next.getText().replace("&nbsp;", "")
                logger.debug('%s %s' % (cell.getText(), next.getText().replace("&nbsp;", "")))
            if cell.getText() == "Bail Amount:":
                bail = next.getText().replace('$', '').replace(',', '').split('.')
                bail =int(bail[0])
                logger.debug('%s %s' % (cell.getText(), next.getText().replace("&nbsp;", "").replace('$', '').replace(',', '')))
            if cell.getText() == "Weight:":
                weight = next.getText().replace("&nbsp;", "")
                logger.debug('%s %s' % (cell.getText(), next.getText().replace("&nbsp;", "")))
            if cell.getText() == "Hair Color:":
                hair = next.getText().replace("&nbsp;", "")
                logger.debug('%s %s' % (cell.getText(), next.getText().replace("&nbsp;", "")))
            if cell.getText() == "Housing Location:":
                location = next.getText().replace("&nbsp;", "")
                logger.debug('%s %s' % (cell.getText(), next.getText().replace("&nbsp;", "")))
            if cell.getText() == "Eye Color:":
                eye = next.getText().replace("&nbsp;", "")
                logger.debug('%s %s' % (cell.getText(), next.getText().replace("&nbsp;", "")))
            if cell.getText() == "Occupation:":
                occupation = next.getText().replace("&nbsp;", "")
                logger.debug('%s %s' % (cell.getText(), next.getText().replace("&nbsp;", "")))

    logger.debug('inserting new arrest info %s' % casenumber)

    try:
        db.query('''INSERT INTO Arrests (casenumber, arrestname, dob, sex, race, arreststatus, height, bail, weight,
                  hair, location, eye, occupation) VALUES (:casenum, :name, :dob, :sex, :race, :status, :height, :bail,
                  :weight, :hair, :location, :eye, :occupation)''',
                 casenum=casenumber,
                 name=name,
                 dob=dob,
                 sex=sex,
                 race=race,
                 status=status,
                 height=height,
                 bail=bail,
                 weight=weight,
                 hair=hair,
                 location=location,
                 eye=eye,
                 occupation=occupation
                 )
    except ValueError as e:
                logger.warning(e)

    return


def lambda_handler(event, context):
    processQueue(context)
