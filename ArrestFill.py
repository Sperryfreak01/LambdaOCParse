import requests # https://github.com/kennethreitz/requests
import records # https://github.com/kennethreitz/records
from BeautifulSoup import BeautifulSoup
from folium import plugins, folium
import geocoder
import datetime
import  logging

from sqlalchemy.exc import IntegrityError


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
    exist = db.query('SELECT casenumber FROM Arrests WHERE casenumber=:CaseNum', CaseNum=casenumber, fetchall=True)
    exsisting = False
    try:
        if exist[0]['casenumber'] == casenumber:
            exsisting = True
    except:
        pass

    BlotterURL = "http://ws.ocsd.org/Whoisinjail/search.aspx?FormAction=CallNo&CallNo=%s" % casenumber
    logger = logging.getLogger(__name__)


    r = requests.get(BlotterURL)

    if r.status_code != requests.codes.ok:
        sys.exit()

    getNotes = False
    noteCaseNum = ''
    try:
        soup = BeautifulSoup(r.content)
    except AttributeError as e:
        logger.error(e)

    if 'ERROR - This page cannot be displayed at this time.' in soup.getText():
        print 'error on page'
        print 'case# %s' %casenumber
        exist = db.query('SELECT casenumber FROM Arrests WHERE casenumber=:CaseNum', CaseNum=casenumber, fetchall=True)

        if exsisting:
            return
        else:
            try:
                db.query('''INSERT INTO Arrests (casenumber, arrestname, dob, sex, race, arreststatus, height, bail, weight, hair, location,
                         eye, occupation) VALUES (:casenum, :name, :dob, :sex, :race, :status, :height, :bail, :weight, :hair,
                         :location, :eye, :occupation)
                         ''',
                         casenum=None,
                         name=None,
                         dob=None,
                         sex=None,
                         race=None,
                         status=None,
                         height=None,
                         bail=None,
                         weight=None,
                         hair=None,
                         location=None,
                         eye=None,
                         occupation=None
                         )
            except IntegrityError as e:
                        logger.warning(e)

        return  # TODO stuff here
    name = dob = sex = race = status = height = bail = weight  = hair = location = eye = occupation = None

    table = soup.find("table", cellpadding=4)
    rows = table.findAll("tr")
    heading = rows[0].findAll('th')
    for prev, title, next in tableparse(heading):
        if title.getText() == 'Inmate Name:':
            name = " ".join(next.getText().split()).replace(' ,', ',')
            print '%s %s' % (title.getText(), name)

    for row in rows:
        cells = row.findAll("td")
        for prev, cell, next in tableparse(cells):
            #print '%s, \n%s' % (cell.getText(), next)
            if cell.getText() == "Date of Birth:":
                dob = next.getText().replace("&nbsp;", "")
                date_object = datetime.datetime.strptime(dob, '%m-%d-%Y')
                dob = date_object.strftime('%Y-%m-%d')
                print '%s %s' % (cell.getText(), next.getText().replace("&nbsp;", ""))
            if cell.getText() == "Sex:":
                sex = next.getText().replace("&nbsp;", "")
                print '%s %s' % (cell.getText(), next.getText().replace("&nbsp;", ""))
            if cell.getText() == "Race:":
                race = next.getText().replace("&nbsp;", "")
                print '%s %s' % (cell.getText(), next.getText().replace("&nbsp;", ""))
            if cell.getText() == "Custody Status:":
                status = next.getText().replace("&nbsp;", "")
                print '%s %s' % (cell.getText(), next.getText().replace("&nbsp;", ""))
            if cell.getText() == "Height:":
                height  = next.getText().replace("&nbsp;", "")
                print '%s %s' % (cell.getText(), next.getText().replace("&nbsp;", ""))
            if cell.getText() == "Bail Amount:":
                bail = int(float(next.getText().replace("&nbsp;", "").replace('$', '').replace(',', '')))
                print '%s %s' % (cell.getText(), next.getText().replace("&nbsp;", "").replace('$', '').replace(',', ''))
            if cell.getText() == "Weight:":
                weight = next.getText().replace("&nbsp;", "")
                print '%s %s' % (cell.getText(), next.getText().replace("&nbsp;", ""))
            if cell.getText() == "Hair Color:":
                hair = next.getText().replace("&nbsp;", "")
                print '%s %s' % (cell.getText(), next.getText().replace("&nbsp;", ""))
            if cell.getText() == "Housing Location:":
                location = next.getText().replace("&nbsp;", "")
                print '%s %s' % (cell.getText(), next.getText().replace("&nbsp;", ""))
            if cell.getText() == "Eye Color:":
                eye = next.getText().replace("&nbsp;", "")
                print '%s %s' % (cell.getText(), next.getText().replace("&nbsp;", ""))
            if cell.getText() == "Occupation:":
                occupation = next.getText().replace("&nbsp;", "")
                print '%s %s' % (cell.getText(), next.getText().replace("&nbsp;", ""))

    if exsisting:
        print casenumber
        print 'case already in db updating arest info'
        try:
            db.query('''UPDATE Arrests SET arrestname=:name, dob=:dob, sex=:sex, race=:race, arreststatus=:status,
                        height=:height, bail=:bail, weight=:weight, hair=:hair, location=:location,
                        eye=:eye, occupation=:occupation WHERE casenumber=:casenum''',
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
    else:
        print 'casenew inserting arrest info'
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




#db = records.Database('mysql+pymysql://Sperryfreak01:Matthdl13@192.168.5.185:3306/BlotBlotBlot')
db = records.Database('mysql+pymysql://Sperryfreak01:Matthdl13@ec2-52-38-243-136.us-west-2.compute.amazonaws.com:3306/BlotBlotBlot')
#db = records.Database('sqlite:///blot.db')
#i=0
#entries = db.query('SELECT CaseNumber,Incident FROM Incidents WHERE Incident LIKE :arrestinfo', arrestinfo='%Arrest Info')
#entries = db.query('SELECT CaseNumber from Incidents WHERE arrest=:arrest', arrest=1)
entries = db.query('''SELECT Incidents.CaseNumber
                        FROM Incidents
                        LEFT JOIN Arrests ON Arrests.casenumber = Incidents.CaseNumber
                        WHERE Arrests.casenumber IS NULL AND Incidents.arrest = 1
                        ORDER BY Incidents.incidentdate DESC''')

#entries = db.query('SELECT casenumber from Arrests WHERE DOB=:null', null='NULL')


for entry in entries:
    #splitstring = str.split(str(entry['Incident']), 'Arrest Info')
    #print splitstring[0]
    #db.query('UPDATE Incidents SET Incident=:incident, Aresst=:arrested WHERE CaseNumber=:CaseNum', CaseNum=entry['CaseNumber'], incident=splitstring[0], arrested=1)

    arrestparse(db,entry['CaseNumber'])



