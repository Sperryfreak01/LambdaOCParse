from __future__ import print_function

import boto3
from folium import folium
import records
import datetime
import folium.plugins
import folium.element
import cgi
import logging


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

def arrestinfogen(arrestinfo, casenumber):
    for arrest in arrestinfo:
        if arrest['CaseNumber'] == casenumber:
            return arrest
    return None



def createMap(db, city, days=0, month=None):
    if days > 0:
        mapstartdate =  datetime.datetime.now() - datetime.timedelta(days=days)
        mapstartdate = mapstartdate.strftime('%Y-%m-%d %H:%M')

    datapoints = []
    popups = []
    markers = []

    calldatapoints = []
    callpopups = []
    callmarkers = []

    arrestdatapoints = []
    arrestpopups = []
    arrestmarkers = []

    print('creating %s day map for %s' % (days, city))

    #  Get all of the incidents and arrests since teh specified date
    dbIncidents = db.query('SELECT * from Incidents WHERE "Date" > :daterange AND City=:city', daterange=mapstartdate, city=city)
    dbArrests = db.query('''SELECT Arrests.* FROM Arrests
                            INNER JOIN Incidents ON Arrests.CaseNumber = Incidents.CaseNumber
                            WHERE Incidents.Date > :daterange''',
                            daterange =mapstartdate
                         )

    for entry in dbIncidents:
        arrestnotes = ''
        arrest = False
        if entry['Aresst'] == 1:
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
                              ''' % (dbarrestinfo['Name'], dbarrestinfo['Status'], dbarrestinfo['Location'],
                                    dbarrestinfo['Bail'], dbarrestinfo['DOB'], dbarrestinfo['Sex'], dbarrestinfo['Race'],
                                    dbarrestinfo['Height'], dbarrestinfo['Weight'], dbarrestinfo['Hair'],
                                    dbarrestinfo['Eye'], dbarrestinfo['Occupation'])

        formatedNotes = str(entry['Notes']).replace('\r','')
        formatedNotes = cgi.escape(formatedNotes)
        html = '''<dl>
                   <dt><b> %s </b></dt>
                    <dd> Case Number: %s </dd>
                    <dd> Description: %s </dd>
                    <dd> Reported Location: %s </dd>
                   <dt><b>Notes</b></dt>
                    <dd> %s </dd>
                    %s
                 </dl>''' % (entry['Incident'], entry['CaseNumber'], entry['Date'], entry['Location'],
                            formatedNotes, arrestnotes)

        iframe = folium.element.IFrame(html=html, width=500, height=300)

        # Create 2 different datasets, arrests and calls
        if arrest:
            arrestdatapoints.append((entry['Lat'], entry['Lon']))
            arrestpopups.append(folium.Popup(iframe, max_width=2650))
            if dbarrestinfo['Sex'] == 'Female':
                arrestmarkers.append(folium.Icon(color='lightred', icon='flash'))
            elif dbarrestinfo['Sex'] == 'Male':
                arrestmarkers.append(folium.Icon(color='blue', icon='flash'))
            else:
                arrestmarkers.append(folium.Icon(color='lightgray', icon='flash'))

        else:
            calldatapoints.append((entry['Lat'], entry['Lon']))
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
        map_osm.save('./maps/%sMap%s.html' % (city, days))
        jobname = ('%s %sday' % (city, days))
        s3bucket = 'blotblotblot'
        s3key = 'OCParser/Maps/%sMap%s.html' % (city, days)
        s3 = boto3.resource('s3')
        s3.meta.client.upload_file('./maps/%sMap%s.html', s3bucket, s3key)
        logger.debug('%s map complete' % city)




def lambda_handler(event, context):
    print('lambda event = %s' % event)
    mapCity = event[u'city']
    dateRange = event[u'daterange']
    print ("recived %s as the city" % cityList[parserCity])
    createMap(db, mapCity, dateRange)



