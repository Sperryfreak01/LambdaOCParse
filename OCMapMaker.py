from __future__ import print_function
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.debug('Loading function')

import boto3
from folium import folium
import records
import datetime
import folium.plugins
import folium.element
import cgi
import json

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

def arrestinfogen(arrestinfo, casenumber):
    for arrest in arrestinfo:
        if arrest['CaseNumber'] == casenumber:
            return arrest
    return None



def createMap(db, city, days=0, month=None):
    if days > 0:
        mapstartdate =  datetime.datetime.now() - datetime.timedelta(days=days)
        #2016-02-04T23:15:00
        mapstartdate = mapstartdate.strftime('%Y-%m-%dT%H:%M:%S')
        logger.debug('starting date range: %s' % mapstartdate)

    datapoints = []
    popups = []
    markers = []

    calldatapoints = []
    callpopups = []
    callmarkers = []

    arrestdatapoints = []
    arrestpopups = []
    arrestmarkers = []

    logger.info('creating %s day map for %s' % (days, city))
    #WHERE incidentdate > :daterange AND location =:city
    #  Get all of the incidents and arrests since teh specified date
    dbIncidents = db.query('SELECT * from Incidents WHERE incidentdate > :daterange AND city =:city',
                           daterange=mapstartdate,
                           city=city
                           )
    dbArrests = db.query('''SELECT Arrests.* FROM Arrests
                            INNER JOIN Incidents ON Arrests.casenumber = Incidents.CaseNumber
                            WHERE Incidents.incidentdate > :daterange''',
                            daterange =mapstartdate
                         )
    logger.debug(dbIncidents.all())
    for entry in dbIncidents:
        logger.debug('entry: %s' % entry)
        arrestnotes = ''
        arrest = False
        if entry and entry['arrest'] == 1:
            dbarrestinfo = arrestinfogen(dbArrests, entry['CaseNumber'])
            if dbarrestinfo is not None: #If there is a matching incident get details and assemble the html
                arrest = True
                arrestnotes = '''<dt><b>Arrest Info</b></dt>
                                 <dd> Name: %s </dd>
                                 <dd> Status: %s </dd>
                                 <dd> Location: %s </dd>
                                 <dd> Bail: %s </dd>
                                 <dd> DOB: %s </dd>
                                 <dd> Sex: %s </dd>
                                 <dd> Race: %s </dd>
                                 <dd> Height: %s </dd>
                                 <dd> Weight: %s </dd>
                                 <dd> Hair: %s </dd>
                                 <dd> Eye: %s </dd>
                                 <dd> Occupation: %s </dd>
                              ''' % (dbarrestinfo['arrestname'],
                                     dbarrestinfo['arreststatus'],
                                     dbarrestinfo['location'],
                                     dbarrestinfo['bail'],
                                     dbarrestinfo['dob'],
                                     dbarrestinfo['sex'],
                                     dbarrestinfo['race'],
                                     dbarrestinfo['height'],
                                     dbarrestinfo['weight'],
                                     dbarrestinfo['hair'],
                                     dbarrestinfo['eye'],
                                     dbarrestinfo['occupation']
                                     )

        formatedNotes = str(entry['notes']).replace('\r','')
        formatedNotes = cgi.escape(formatedNotes)
        html = '''<dl>
                   <dt><b> %s </b></dt>
                    <dd> Case Number: %s </dd>
                    <dd> Description: %s </dd>
                    <dd> Reported Location: %s </dd>
                   <dt><b>Notes</b></dt>
                    <dd> %s </dd>
                    %s
                 </dl>''' % (entry['incidentdescription'],
                             entry['CaseNumber'],
                             entry['incidentdate'],
                             entry['location'],
                             formatedNotes,
                             arrestnotes)

        iframe = folium.element.IFrame(html=html, width=500, height=300)

        # Create 2 different datasets, arrests and calls
        if arrest:
            arrestdatapoints.append((entry['lat'], entry['lon']))
            arrestpopups.append(folium.Popup(iframe, max_width=2650))
            if dbarrestinfo['sex'] == 'Female':
                arrestmarkers.append(folium.Icon(color='lightred', icon='flash'))
            elif dbarrestinfo['sex'] == 'Male':
                arrestmarkers.append(folium.Icon(color='blue', icon='flash'))
            else:
                arrestmarkers.append(folium.Icon(color='lightgray', icon='flash'))

        else:
            calldatapoints.append((entry['lat'], entry['lon']))
            callpopups.append(folium.Popup(iframe, max_width=2650))
            callmarkers.append(folium.Icon(color='green', icon='phone-alt'))

    datapoints = [arrestdatapoints, calldatapoints]
    popups = [arrestpopups, callpopups]
    markers = [arrestmarkers, callmarkers]

    logger.debug('appending multiprocessing job %s' % city)

    #TODO center map on city and fix zoom
    map_osm = folium.Map(location=[33.6700, -117.7800], width='75%', height='75%')
    for datapoint,popup,marker in zip(datapoints, popups, markers):
        map_osm.add_children(folium.plugins.MarkerCluster(datapoint, popup, icons=marker))
    map_osm.save('/tmp/%sMap%s.html' % (city, days))
    logger.info('%s map complete' % city)

    s3bucket = 'blotblotblot'
    s3key = 'OCParser/Maps/%sMap%s.html' % (city, days)
    s3field = {'ContentType': 'text/html', 'ACL': 'public-read'}
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file('/tmp/%sMap%s.html' % (city, days), s3bucket, s3key, ExtraArgs=s3field)
    logger.info('upload complete')





def lambda_handler(event, context):
    db = records.Database('mysql+pymysql://Sperryfreak01:Matthdl13@192.168.5.185:3306/BlotBlotBlot')
    #db = records.Database('mysql+pymysql://Sperryfreak01:Matthdl13@ec2-52-38-243-136.us-west-2.compute.amazonaws.com:3306/BlotBlotBlot')
    print('db connected')
    logger.info('lambda event = %s' % event)
    snsmessage = json.loads(event[u'Records'][0][u'Sns'][u'Message'])
    mapCity = snsmessage['city']
    dateRange =  snsmessage['days']
    logger.info ("recived %s as the city" % cityList[mapCity])
    createMap(db, str(mapCity), int(dateRange))
    db.close()

#logging.basicConfig()
#event = {'city': 'MV', 'daterange': 7}
#event = {u'Records': [{u'EventVersion': u'1.0', u'EventSubscriptionArn': u'arn:aws:sns:us-west-2:685804734565:BotterMap:9fac6364-901c-44b4-bb0a-036db6dbe3fd', u'EventSource': u'aws:sns', u'Sns': {u'SignatureVersion': u'1', u'Timestamp': u'2016-04-19T04:12:30.758Z', u'Signature': u'f1DJZbWNzGdDP5GWpFOBUW0H2bRLmObl0MO44B7wskPkwTXIGy2h6ZF6yG1ni+tw8oYLnS0fBBMlPtqBSwLrHyxL7WxJ9s1BH0BDbmOCftcwqP5j3ZHeF6OGKXZ0g9Ssn3scHps2YRzzw9ItJPS3W0mVVjdyRRofupVHrjWXwm9Etzl1QgSdxZaVJdH0fOFmt4Xsz1+4SHd9cO/3Q73t/Lp6XoHyfIfajDpGVfBaJQV1KdP/0JAjtsTT2Wvo44TmY/YOghuMmLOHst9MsEOaI4v1E933zEyQk54VSXaFAL3I33za8B8T7BnyUqdCfe8kjUVfzy4gCc1WrOYrxJinvA==', u'SigningCertUrl': u'https://sns.us-west-2.amazonaws.com/SimpleNotificationService-bb750dd426d95ee9390147a5624348ee.pem', u'MessageId': u'136c761f-0e91-5b0d-af4c-3990bcffc6e4', u'Message': u'{"city": "YL", "days": 7}', u'MessageAttributes': {}, u'Type': u'Notification', u'UnsubscribeUrl': u'https://sns.us-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-2:685804734565:BotterMap:9fac6364-901c-44b4-bb0a-036db6dbe3fd', u'TopicArn': u'arn:aws:sns:us-west-2:685804734565:BotterMap', u'Subject': None}}]}
#lambda_handler(event, None)
