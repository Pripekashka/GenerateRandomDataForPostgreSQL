import psycopg2, random, datetime
from datetime import timedelta

con = psycopg2.connect(
  database="mentol", 
  user="postgres", 
  password="postgres", 
  host="192.168.80.237", 
  port="5432"
)
print("Database opened successfully")

global idr, routelist, sitenamelist, directionlist, staffnamelist
cur = con.cursor()
idr=int("1")
routelist=['Внешний', 'Внутренний', 'Транзитный']
sitenamelist=['Test Collector', 'Test Collector2', 'Test Collector3']
directionlist=['Входящий', 'Исходящий']
staffnamelist=["Кононов Михаил Дмитриевич","Ципулин Владимир Игоревич", "Череватова Евгения Владимровна", "Каширина Алеся Николаевна", "Коноплицкий Даниил Игоревич", "Чижина Юлия Андреевна", "Горшкова Карина Олеговна", "Пенкина Алина Азатовна", "Васильева Анастасия Александровна", "Туровская Валерия Витальневна", "Дроботенко Артем Николаевич", "Череватова Евгения Владимировна"]

def data_gen():
  global idr, routelist, sitenamelist, directionlist, staffnamelist
  data=[]
  idr+=1
  datestart=datetime.datetime.now()
  second=random.randint(1, 59)
  dateend=datestart+timedelta(seconds=second)
  duration=dateend-datestart
  zerodate=str('1900-01-01 ')
  duration=str(duration)
  duration=zerodate+duration
  phonea=random.randint(10000000,99999999)
  phoneb=random.randint(10000000000,99999999999)
  outtrunk=random.randint(1,10)
  inctrunk=random.randint(1,10)
  route=random.choice(routelist)
  sitesid=random.randint(4,6)
  sitename=random.choice(sitenamelist)
  direction=random.choice(directionlist)
  primaryrateduration='1900-01-01 00:00:00'
  staffname=random.choice(staffnamelist)
  data.append(idr)
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

def start_insert ():
  global idr
  while idr<80000000:
    #check=idr/1000
    if (idr % 1000)==0:
      print ("Пройдено итераций: ",idr)
    data=data_gen()
    sql="INSERT INTO dbo.tarratedcalls_202009 (idr, datestart, dateend, duration, phonea, phoneb, outtrunk, inctrunk, route, sitesid, sitename, direction, primaryrateduration, staffname) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cur.execute(sql, data)
    con.commit()
start_insert()
con.commit()