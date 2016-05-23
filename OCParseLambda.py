from __future__ import print_function
import logging

logger = logging.getLogger()
logger.setLevel(logging.WARNING)
logger.debug('Loading function')

import json
import geocoder
import requests  # https://github.com/kennethreitz/requests
import records
import sys
from BeautifulSoup import BeautifulSoup
import datetime
import pymysql
import boto3


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


def getWebpages(city, ViewState, ViewStateGen, EventValidation):
    BlotterURL = 'http://ws.ocsd.org/Blotter/BlotterSearch.aspx'

    headers = {
        'accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        'origin': "http//ws.ocsd.org",
        'x-devtools-emulate-network-conditions-client-id': "3D54FA71-2ADC-4CB2-8140-B266EC5E9596",
        'upgrade-insecure-requests': "1",
        'user-agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36",
        'content-type': "application/x-www-form-urlencoded",
        'dnt': "1",
        'referer': "http//ws.ocsd.org/Blotter/BlotterSearch.aspx",
        'accept-encoding': "gzip, deflate",
        'accept-language': "en-US,en;q=0.8",
        'cookie': "ASP.NET_SessionId=a0ooc555m13vlofbtiljc055",
        'cache-control': "no-cache",
        }

    payload ={'SortBy': '',
               '__EVENTVALIDATION': EventValidation,
               '__VIEWSTATE': ViewState,
               '__VIEWSTATEGENERATOR': ViewStateGen,
               '__EVENTARGUMENT': '',
               '__EVENTTARGET': '',
               '__SCROLLPOSITIONX': '0',
               '__SCROLLPOSITIONY': '0',
               'btn7Days.x': '15',
               'btn7Days.y': '8',
               'ddlCity': city
                }

    logger.info('getting webpage')
    try:
        response = requests.post(BlotterURL, data=payload, headers=headers, timeout=10)
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.HTTPError) as e:
        logger.warning(e)
        return -1
    logger.info('got the webpage for %s' % city)
    return response



def crimeParser(db, response, city):
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='Incidents')
    logger.info('Parsing through %s' % city)
    getNotes = False

    try:
        soup = BeautifulSoup(response.content)
    except AttributeError as e:
        logger.error(e)
        return

    # OCBlotter stores incidents in odd/even rows in table, columns are known so we don't need the column titles
    # Only discerning property of the table is the cell padding, this is weak and easily broken.
    # To get the notes we have too look for "read" in the notes column and then get the next row (not odd or even)
    # Arrest info is scrapped from a second webpage when there is "Arrest Info" in the Incident column
    table = soup.find("table", cellpadding=4)
    rows = table.findAll("tr")
    incidents = []
    sqsnotes = []
    for row in rows:
        arrested = trimmeddate = Description = IncidentLocation = lat = lon = confidence = notes = ''
        # Stepping through the incident table and parsing each row
        if row.attrs[0] == (u'class', u'trEven') or row.attrs[0] == (u'class', u'trOdd'):
            cells = row.findAll("td")
            CaseNum = cells[2].getText()
            IncidentDate = cells[0].getText()
            date_object = datetime.datetime.strptime(IncidentDate, '%m/%d/%Y %I:%M:%S %p')

            if date_object > datetime.datetime.now()-datetime.timedelta(hours=13):
                trimmeddate = date_object.strftime('%Y-%m-%d %H:%M')  # convert to a format that is easier to search for
                Description = cells[3].getText().replace("&nbsp;", "")  # get rid of the pesky web formating
                IncidentLocation = cells[4].getText()
                # Logging the incident to the database
                #lat, lon, confidence = getLocation(IncidentLocation, cityList[city])

                if 'Arrest Info' in Description:
                    logging.info('subject in case Number: %s arrested' % CaseNum)
                    #arrestparse(db, CaseNum)
                    arrested = 1
                else:
                    arrested = 0
                    logging.debug('No arrest in case Number: %s' % CaseNum)

                incident = {'type': 'incident',
                            'CaseNum': CaseNum,
                            'Description': Description,
                            'IncidentLocation': IncidentLocation,
                            'IncidentDate': trimmeddate,
                            'city': city,
                            'arrested': arrested
                            }
                if len(incidents) < 10:
                    incidents.append({'Id': str(len(incidents)+1), 'MessageBody': json.dumps(incident)})
                else:
                    response = queue.send_messages(Entries=incidents)
                    logger.debug('queued casenumber: %s \n%s' % (CaseNum, response))
                    incidents[:] = []

            if cells[5].getText().replace("&nbsp;", "") == 'read':  # seeing if we need to check for updated notes
                logger.debug('Case number: %s has notes' % CaseNum)
                getNotes = True  # Since there are notes we set the flag to get the notes, stored in next row
                noteCaseNum = CaseNum
            else:
                logger.debug('Case number: %s does not have notes' % CaseNum)
                getNotes = False  # No notes, no flag, don't need the next row


        # Parsing through the notes if a note row exits and it was flagged for logging
        if row.attrs[0] == (u'id', u'trNotes') and getNotes:
            if date_object > datetime.datetime.now()-datetime.timedelta(hours=42):
                notes = row.getText()

                note = {'type': 'notes',
                        'CaseNum': CaseNum,
                        'notes': notes
                        }
                if len(sqsnotes) < 10:
                    sqsnotes.append({'Id': str(len(sqsnotes)+1), 'MessageBody': json.dumps(note)})
                else:
                    response = queue.send_messages(Entries=sqsnotes)
                    logger.debug('queued casenumber: %s \n%s' % (CaseNum, response))
                    sqsnotes[:] = []

    if len(incidents):
        queue.send_messages(Entries=incidents)
    if len(sqsnotes):
        queue.send_messages(Entries=sqsnotes)

def databaseupdate(db):
    webpages, cityIndex  = getWebpages()
    for response, city in zip(webpages, cityIndex):
        crimeParser(db, response, city)


def tableparse(iterable):
    iterator = iter(iterable)
    prev = None
    item = iterator.next()  # throws StopIteration if empty.
    for next in iterator:
        yield (prev,item,next)
        prev = item
        item = next
    yield (prev,item,None)


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
    db = records.Database('mysql+pymysql://Sperryfreak01:Matthdl13@192.168.5.185:3306/BlotBlotBlot')
    #db = records.Database('mysql+pymysql://Sperryfreak01:Matthdl13@ec2-52-38-243-136.us-west-2.compute.amazonaws.com:3306/BlotBlotBlot')
    logger.debug('db connected')
    logger.debug('lambda event = %s' % event)
    snsmessage = json.loads(event[u'Records'][0][u'Sns'][u'Message'])
    parserCity = snsmessage['city']
    ViewState = snsmessage['viewstate']
    ViewStateGen = snsmessage['viewstategen']
    EventValidation = snsmessage['eventvalidation']
    logger.info ("recived %s as the city" % cityList[parserCity])
    webpage = getWebpages(parserCity, ViewState, ViewStateGen, EventValidation)
    if webpage != -1:
        crimeParser(db, webpage, parserCity)
    db.close()
    logger.debug('db closed')


#lambda_handler(None, None)
