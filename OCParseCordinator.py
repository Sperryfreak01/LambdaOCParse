from __future__ import print_function

import boto3
import json
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def startParser():
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

    session = boto3.session.Session()
    sns = session.resource('sns')
    topic = sns.Topic('arn:aws:sns:us-west-2:685804734565:BlotterParse')
    for city in cityList:
        logger.info('queing parser for %s' % cityList[city])
        payload = json.dumps({"default": "Message body text here.", 'lambda': city})
        response = topic.publish(
                MessageStructure='json',
                Message=payload
                )
        logger.debug('lambda status: %s' % response)

def lambda_handler(event, context):
    logger.info('Starting OC Parser Cordinator')
    logger.debug(event)
    startParser()
    logger.info('Stopping OC Parser Cordinator')


#lambda_handler(None,None)

