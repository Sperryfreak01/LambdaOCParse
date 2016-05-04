from __future__ import print_function

import boto3
import json
import logging
import requests
import sys
from BeautifulSoup import BeautifulSoup
from time import sleep
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def startParser():
    cityList = {'MV':'MISSION VIEJO'}
    cityback = {'AV':'ALISO VIEJO', 'AN':'ANAHEIM', 'BR':'BREA', 'BP':'BUENA PARK', 'CN':'ORANGE COUNTY',
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


    session = boto3.session.Session()
    sns = session.resource('sns')
    topic = sns.Topic('arn:aws:sns:us-west-2:685804734565:BlotterParse')
    for city in cityList:
        logger.info('queing parser for %s' % cityList[city])
        payload = json.dumps({'default': 'Parser Trigger Notification',
                              'lambda': json.dumps({'city': city,
                                                    'eventvalidation': EventValidation,
                                                    'viewstate': ViewState,
                                                    'viewstategen': ViewStateGen
                                                    })
                              })
        response = topic.publish(
                MessageStructure='json',
                Message=payload
                )
        logger.debug('lambda status: %s' % response)
        sleep(2)

def lambda_handler(event, context):
    logger.info('Starting OC Parser Cordinator')
    logger.debug(event)
    startParser()
    logger.info('Stopping OC Parser Cordinator')


#lambda_handler(None,None)

