import psycopg2, random, datetime
from datetime import timedelta, timezone

con = psycopg2.connect(
  database="mentol", 
  user="lk", 
  password="lk2017", 
  host="192.168.80.233", 
  port="5432"
)
print("Database opened successfully")

global idr, routelist, sitenamelist, directionlist, staffnamelist
cur = con.cursor()
idr=int("1")
routelist=['Внешний', 'Внутренний', 'Транзитный']
#sitenamelist=['Test Collector', 'Test Collector2', 'Test Collector3']
directionlist=['Входящий', 'Исходящий']
staffnamelist=["Кононов Михаил Дмитриевич","Ципулин Владимир Игоревич", "Череватова Евгения Владимровна", "Каширина Алеся Николаевна", "Коноплицкий Даниил Игоревич", "Чижина Юлия Андреевна", "Горшкова Карина Олеговна", "Пенкина Алина Азатовна", "Васильева Анастасия Александровна", "Туровская Валерия Витальневна", "Дроботенко Артем Николаевич", "Череватова Евгения Владимировна"]

def data_gen():
  global idr, routelist, sitenamelist, directionlist, staffnamelist
  data=[]
  idr+=1
  day=random.randint(1,27)
  mounth=random.randint(1,1)
  hours=random.randint(0,23)
  minutes=random.randint(0,59)
  secondss=random.randint(0,59)
  datestart=datetime.datetime(2020, mounth, day, hours, minutes, secondss)
  second=random.randint(1, 59)
  dateend=datestart+timedelta(seconds=second)
  duration=dateend-datestart
  zerodate=str('1900-01-01 ')
  duration=str(duration)
  duration=zerodate+duration
  phonea=random.randint(1000000,1050000)
  phoneb=random.randint(10000000000,10000050000)
  outtrunk=random.randint(1,10)
  inctrunk=random.randint(1,10)
  route=random.choice(routelist)
  sitesid=random.randint(4,6)
  if sitesid==4:
    sitename='Test Collector'
  if sitesid==5:
    sitename='Test Collector2'
  if sitesid==6:
    sitename='Test Collector3'
  #sitename=random.choice(sitenamelist)
  direction=random.choice(directionlist)
  primaryrateduration='1900-01-01 00:00:00'
  staffname=random.choice(staffnamelist)
  #data.append(idr)
  data.append(datestart)
  data.append(dateend)
  data.append(duration)
  data.append(phonea)
  data.append(phoneb)
  data.append(outtrunk)
  data.append(inctrunk)
  data.append(route)
  data.append(sitesid)
  data.append(sitename)
  data.append(direction)
  data.append(primaryrateduration)
  data.append(staffname)
  return(data)

def check_idr ():
    sql="SELECT MAX(idr) FROM dbo.tarratedcalls_202001"
    cur.execute(sql)
    con.commit()
    idrnow=cur.fetchall()
    idrnow=idrnow[0]
    idrnow=idrnow[0]
    return(idrnow)

def check_count():
  sql="SELECT count(*) FROM dbo.tarratedcalls_202001;"
  cur.execute(sql)
  con.commit()
  countnow=cur.fetchall()
  countnow=countnow[0]
  countnow=countnow[0]
  return(countnow)

def start_insert ():
  global idr
  timecheck=0
  counttocommit=100
  countnow=int(check_count())

  while countnow<20000000:
    if timecheck==0:
      timestart=datetime.datetime.now()
      timecheck=1
    if (idr % counttocommit)==0:
      con.commit()
      timecheck=0
      timeend=datetime.datetime.now()

      idrnow=str(check_idr())
      if idrnow=='None':
        idrnow=int(0)
      else:
        idrnow=int(idrnow)

      countnow=int(check_count())
      duration=timeend-timestart
      onemln=(duration/counttocommit)*1000000
      print('---------------------')
      print ("Пройдено итераций: ",counttocommit)
      print('---              ---')
      print ("Count rows:" ,countnow)
      #print ("Last idr:" , idrnow)
      #print ("Time start: ", timestart)
      #print ("Time end: ", timeend)
      print('---              ---')
      print ("Duration: ", duration)
      print ("Speed 1 Mln: ", onemln)
    data=data_gen()
    sql="INSERT INTO dbo.tarratedcalls_202001 (datestart, dateend, duration, phonea, phoneb, outtrunk, inctrunk, route, sitesid, sitename, direction, primaryrateduration, staffname) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cur.execute(sql, data)
    
start_insert()
con.commit()
