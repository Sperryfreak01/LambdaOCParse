from __future__ import print_function
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.debug('Loading function')

import json
import requests  # https://github.com/kennethreitz/requests
import records
import sys
from BeautifulSoup import BeautifulSoup
import datetime
import pymysql
import boto3


def processQueue(context):
    db = records.Database('mysql+pymysql://Sperryfreak01:Matthdl13@192.168.5.185:3306/BlotBlotBlot')
    sqs = boto3.resource('sqs')
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
            print ('retries left: %s' % retry_counter)
        # delete messages to remove them from SQS queue
        # handle any errors
        else:
            dbUpdate(db, message_bodies)
            delete_response = queue.delete_messages(Entries=messages_to_delete)
    db.close()


def dbUpdate(db, messages):
    for message in messages:
        logger.debug ('%s' % (message))
        if message[u'type'] == u'incident':
            print ('incident')
            try:

                db.query('INSERT INTO Incidents (CaseNumber, incidentdescription, location, incidentdate,lat, lon, city, confidence, arrest) VALUES(:CaseNum,:Description, :IncidentLocation, :IncidentDate, :lat, :lon, :city, :confidence, :arrested)',
                         CaseNum=message[u'CaseNum'],
                         Description=message[u'Description'],
                         IncidentLocation=message[u'IncidentLocation'],
                         IncidentDate=message[u'IncidentDate'],
                         lat=message[u'lat'],
                         lon=message[u'lon'],
                         city=message[u'city'],
                         confidence=message[u'confidence'],
                         arrested=message[u'arrested']
                         )
            except ValueError as e:
                logger.warning(e)

            except records.IntegrityError as e:
                print ('got an integrity error from records')
                print (e)


        if message[u'type'] == u'notes':
            print ('notes')
            return
            try:
                db.query('UPDATE Incidents SET notes=:note WHERE CaseNumber=:CaseNum',
                         CaseNum=noteCaseNum,
                         note=notes)
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
    print ('event - %s' % event)
    print ('context - %s' % context)
    #db = records.Database('mysql+pymysql://Sperryfreak01:Matthdl13@ec2-52-38-243-136.us-west-2.compute.amazonaws.com:3306/BlotBlotBlot')
    #logger.debug('db connected')
    #logger.debug('lambda event = %s' % event)
    processQueue(context)
    #crimeParser(db, webpage, parserCity)
    #sqs = boto3.client('sqs')
    #message = response = sqs.receive_message(QueueUrl='https://sqs.us-west-2.amazonaws.com/685804734565/Incidents',)
    #print (message)


#lambda_handler(None, None)