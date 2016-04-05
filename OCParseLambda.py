from __future__ import print_function
import geocoder
import requests  # https://github.com/kennethreitz/requests
import records
import sys
from BeautifulSoup import BeautifulSoup
import datetime
import logging
import pymysql


logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
logging.info('Starting BLOTblotBLOT')
logger = logging.getLogger(__name__)

db = records.Database('mysql+pymysql://Sperryfreak01:Matthdl13@192.168.5.185:3306/BlotBlotBlot')
print('db connected')


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


def getWebpages(city):
    logger.info('getting webpage headers')
    BlotterURL = 'http://ws.ocsd.org/Blotter/BlotterSearch.aspx'

    r = requests.get(BlotterURL)

    if r.status_code != requests.codes.ok:
        sys.exit()

    soup = BeautifulSoup(r.content)

    # ASP validation and session fields
    input_fields = soup.findAll("input", {'type':'hidden'})

    for inputs in input_fields:  # gets the form validation variables the server presents, needed for submitting the form
        if inputs['id'] == '__VIEWSTATE':
            ViewState = inputs['value']

        if inputs['id'] == '__VIEWSTATEGENERATOR':
            ViewStateGen = inputs['value']

        if inputs['id'] == '__EVENTVALIDATION':
            EventValidation = inputs['value']

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
               '__EVENTARGUMENT': '',
               '__EVENTTARGET': '',
               '__EVENTVALIDATION': EventValidation,
               '__SCROLLPOSITIONX': '0',
               '__SCROLLPOSITIONY': '0',
               '__VIEWSTATE': ViewState,
                '__VIEWSTATEGENERATOR': ViewStateGen,
               'btn7Days.x': '15',
               'btn7Days.y': '8',
               'ddlCity': city
                }

    logger.info('getting webpage headers')
    response = requests.post(BlotterURL, data=payload, headers=headers)
    #responses = grequests.map(request)
    logger.info('got the webpage for %s' % city)
    return response


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


def crimeParser(db, response, city):
    logger.info('Parsing through %s' % city)
    getNotes = False
    noteCaseNum = ''

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

    for row in rows:
        # Stepping through the incident table and parsing each row
        if row.attrs[0] == (u'class', u'trEven') or row.attrs[0] == (u'class', u'trOdd'):
            cells = row.findAll("td")
            IncidentDate = cells[0].getText()
            date_object = datetime.datetime.strptime(IncidentDate, '%m/%d/%Y %I:%M:%S %p')
            trimmeddate = date_object.strftime('%Y-%m-%d %H:%M')  # convert to a format that is easier to search for
            CaseNum = cells[2].getText()
            Description = cells[3].getText().replace("&nbsp;", "")  # get rid of the pesky web formating
            IncidentLocation = cells[4].getText()

            # Check to see if the incident we are processing is already in the db
            exist = db.query('SELECT CaseNumber FROM Incidents WHERE CaseNumber=:CaseNum', CaseNum=CaseNum)
            exist = exist.all(as_dict=True)
            print('checking the db to see if %s exsists, got %s' %(CaseNum, exist))
            if not exist:
                print('Case number not in DB')
                # Arrest parsing
                if 'Arrest Info' in Description:
                    logging.debug('subject in case Number: %s arrested' % CaseNum)
                    #arrestparse(db, CaseNum)
                    arrested = 1
                else:
                    arrested = 0
                    logging.debug('No arrest in case Number: %s' % CaseNum)

                if cells[5].getText().replace("&nbsp;", "") == 'read':  # seeing if we need to check for updated notes
                    logger.debug('Case number: %s has notes' % CaseNum)
                    getNotes = True  # Since there are notes we set the flag to get the notes, stored in next row
                    noteCaseNum = CaseNum
                else:
                    logger.debug('Case number: %s does not have notes' % CaseNum)
                    getNotes = False  # No notes, no flag, don't need the next row
                logger.debug('Found a new incident %s' % CaseNum)

               # Logging the incident to the database
                lat, lon, confidence = getLocation(IncidentLocation, cityList[city])
                db.query('INSERT INTO Incidents (CaseNumber, incidentdescription, location, incidentdate,lat, lon, city, confidence, arrest) VALUES(:CaseNum,:Description, :IncidentLocation, :IncidentDate, :lat, :lon, :city, :confidence, :arrested)',
                          CaseNum=CaseNum, Description=Description, IncidentLocation=IncidentLocation, IncidentDate=trimmeddate, lat=lat, lon=lon, city=city, confidence=confidence, arrested=arrested,)

            else:
                logger.debug('the case number already exists in the DB, case: %s, city:%s' % (CaseNum, cityList[city]))
                if cells[5].getText().replace("&nbsp;", "") == 'read':  # seeing if we need to check for updated notes
                    logger.debug('Case number: %s has notes' % CaseNum)
                    getNotes = True  # Since there are notes we set the flag to get the notes, stored in next row
                    noteCaseNum = CaseNum
                else:
                    logger.debug('Case number: %s does not have notes' % CaseNum)
                    getNotes = False  # No notes, no flag, don't need the next row.

        # Parsing through the notes if a note row exits and it was flagged for logging
        if row.attrs[0] == (u'id', u'trNotes') and getNotes:
            #cells = row.findAll("td")
            notes = row.getText()
            exist = db.query('SELECT notes FROM Incidents WHERE CaseNumber=:CaseNum', CaseNum=noteCaseNum)
            exist = exist.all(as_dict=True)
            if not exist:
                logger.debug('db has no notes')
                logger.debug('scrapped notes say: %s' % notes)
                db.query('UPDATE Incidents SET notes=:note WHERE CaseNumber=:CaseNum', CaseNum=noteCaseNum, note=notes)
            elif exist[0]['notes'] == notes:
                logger.debug('scrapped notes say: %s' % notes)
                logger.debug('notes are up to date')
            else:
                logger.debug('notes out of date')
                logger.debug('scrapped notes say: %s \n db says: %s' % (notes, exist[0]['notes']))
                db.query('UPDATE Incidents SET notes=:note WHERE CaseNumber=:CaseNum', CaseNum=noteCaseNum, note=notes)


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
        sql = '''INSERT INTO Arrests (casenumber, arrestname, dob, sex, race, arreststatus, height, bail, weight, hair, location,
                 eye, occupation) VALUES (:casenum, :name, :dob, :sex, :race, :status, :height, :bail, :weight, :hair,
                 :location, :eye, :occupation)
              '''
        db.query(sql, casenum=casenumber, name=name, dob=dob, sex=sex, race=race, status=status, height=height,
                 bail=bail, weight=weight, hair=hair, location=location, eye=eye, occupation=occupation )
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
                bail = next.getText().replace("&nbsp;", "")
                logger.debug('%s %s' % (cell.getText(), next.getText().replace("&nbsp;", "")))
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

    db.query('''INSERT INTO Arrests (casenumber, arrestname, dob, sex, race, arreststatus, height, bail, weight, hair, location,
                 eye, occupation) VALUES (:casenum, :name, :dob, :sex, :race, :status, :height, :bail, :weight, :hair,
                 :location, :eye, :occupation)
              ''',
             casenum=casenumber, name=name, dob=dob, sex=sex, race=race, status=status, height=height,
             bail=bail, weight=weight, hair=hair, location=location, eye=eye, occupation=occupation )

    return


print('Loading function')


def lambda_handler(event, context):
    print('lambda event = %s' %event)

    #print("Received event: " + json.dumps(event, indent=2))
    #print("value1 = " + event['key1'])
    #print("value2 = " + event['key2'])
    #print("value3 = " + event['key3'])
    #return event['key1']  # Echo back the first key value
    #raise Exception('Something went wrong')
    #databaseupdate(db)
    webpage = getWebpages('AN')
    #for response, city in zip(webpages, cityIndex):
    #    crimeParser(db, response, city)
    crimeParser(db, webpage, 'AN')
#    db.close()



