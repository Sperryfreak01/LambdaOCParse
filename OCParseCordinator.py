from __future__ import print_function

from boto3 import client
import requests  # https://github.com/kennethreitz/requests
import logging
import grequests
import json

lambdaclient = client('lambda')


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



    reqs = []
    for city in cityList:
        print('queing parser for %s' % cityList[city])
        ParserAPI = 'https://l553cybdyh.execute-api.us-west-2.amazonaws.com/prod/CityUpdate?city=%s' % city
        #requests.get(ParserAPI)
        #reqs.append(grequests.get(ParserAPI))
        payload = json.dumps({'city': city})
        response = lambdaclient.invoke(
                                FunctionName='arn:aws:lambda:us-west-2:685804734565:function:OCParser',
                                InvocationType='Event',
                                Payload=payload
                                )
    print ('Executing Parser Tasks')
    #grequests.map(reqs)


def lambda_handler(event, context):
    print ('Starting OC Parser Cordinator')
    startParser()
    print ('Stopping OC Parser Cordinator')

