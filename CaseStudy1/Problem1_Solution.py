############### PROBLEM 1 PART 1 & 2 ############### 
#Description:

############### Import Libraries ###############
import urllib.request
import urllib #for web scraping
from bs4 import BeautifulSoup #for web scraping
#from pandas.io.parsers import TextParser #used in web scraping
#from lxml.html import parse #used in web scraping
#from urllib.request import urlopen #for writing csv
import csv #for writing csv
#import pandas as pd #for dataframes
import logging #for logging
import os
import zipfile
#from odo.backends import aws
import boto
import boto.s3
#from boto.s3.connection import S3Connection
import sys
from boto.s3.key import Key
import shutil

############### Initializing logging file ###############
root = logging.getLogger()
root.setLevel(logging.DEBUG)

ch1 = logging.FileHandler('problem1_log.log') #output the logs to a file
ch1.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch1.setFormatter(formatter)
root.addHandler(ch1)

ch = logging.StreamHandler(sys.stdout ) #print the logs in console as well
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s - %(message)s')
ch.setFormatter(formatter)
root.addHandler(ch)


############### Create the URL by inputted CIK and ACC_No ###############
cik = input("Please enter CIK: ")
accessionNumber = input("Please enter AccessionNumber: ")
url_pre = "https://www.sec.gov/Archives/edgar/data/"
if not cik or not accessionNumber:
    logging.warning('CIK or AccessionNumber was not given, assuming it to be 0000051143 and 0000051143-13-000007 respectively')
    cik='0000051143'
    accessionNumber = '0000051143-13-000007'
    cik_striped = cik.lstrip("0")
    accno_striped = accessionNumber.replace("-","")
    url1 = url_pre + cik_striped + "/" + accno_striped + "/" + accessionNumber + "-index.html"
else:
    logging.info('CIK: %s and AccessionNumber: %s given', cik, accessionNumber)
    cik_striped = cik.lstrip("0") #strip leading zeroes
    accno_striped = accessionNumber.replace("-","")
    url1 = url_pre + cik_striped + "/" + accno_striped + "/" + accessionNumber + "-index.html"
        
logging.info("URL generated is: "+ url1)


###############  fetch the form's URL ###############
url2=''
try:
    page = urllib.request.urlopen(url1)
    soup = BeautifulSoup(page,"lxml") # parse the page and save it in soup format 
    form = soup.find(id='formName').get_text() # find the form name according to accession no
    formname = form[6:10]
    # find the td which has the above form name
    formtype = soup.findAll('td', text = formname)[0]
    # fetch the url for that form.. Argh!!! find all siblings and then child href ? maybe
    #formprevious = formtype.findAllPrevious
    #print(formprevious)
    all_links = soup.find_all('a')
    for link in all_links:
        if "10q.htm" in link.get("href"):
            url2 = "https://www.sec.gov/" + link.get("href")
            logging.info("Form's URL is: "+ url2)

except urllib.error.HTTPError as err:
    logging.warning("Invalid CIK or AccNo")
    exit()


############### Access the form and fetch the tables ###############
if not os.path.exists('extracted_csvs'):
    os.makedirs('extracted_csvs')
else:
    shutil.rmtree(os.path.join(os.path.dirname(__file__),'extracted_csvs'), ignore_errors=False)
    os.makedirs('extracted_csvs', mode=0o777)
    
page_10q = urllib.request.urlopen(url2)
soup = BeautifulSoup(page_10q,"lxml")
all_tables = soup.select('div["bclpageborder"] table')
for tab in all_tables:
    records = []
    for tr in tab.find_all('tr'):
        rowString=[]
        for td in tr.findAll('td'):
            p = td.find_all('p')
            if len(p)>0:
                for ps in p:
                    ps_text = ps.get_text().replace("\n"," ") 
                    ps_text = ps_text.replace("\xa0","")                 
                    rowString.append(ps_text)
            else:
                td_text=td.get_text().replace("\n"," ")
                td_text = td_text.replace("\xa0","")
                rowString.append(td_text)
        records.append(rowString)        
    with open(os.path.join('extracted_csvs' , str(all_tables.index(tab)) + 'tables.csv'), 'w') as f:
        writer = csv.writer(f)
        writer.writerows(records)
            
logging.info('Tables successfully extracted to csv')
    
############### Zip the csvs and logs ###############
def zipdir(path, ziph, all_Tables):
    # ziph is zipfile handle
    for tab in all_Tables:
        ziph.write(os.path.join('extracted_csvs', str(all_tables.index(tab))+'tables.csv'))
    ziph.write(os.path.join('problem1_log.log'))   

zipf = zipfile.ZipFile('Problem1CsvLogs.zip', 'w', zipfile.ZIP_DEFLATED)
zipdir('/', zipf, all_tables)
zipf.close()
logging.info('csv and log file zipped')


############### Upload the zip to AWS S3 ###############

AWS_ACCESS_KEY_ID = input("Please enter your AWS access key: ")
#AWS_ACCESS_KEY_ID='AKIAI2VML2OZ42S5OWBA'
AWS_SECRET_ACCESS_KEY = input("Please enter your AWS secret access key: ")
#AWS_SECRET_ACCESS_KEY='c6Xa2BuZZAXKfQQ1sL81HQRuEtvq9np7A95RyYhZ'

if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
    print('ERROR: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY were not entered.')
    print('Program ended. Please run the program again with valid keys.')
    exit()
    
bucket_name = 'mybucketqwerty'
conn = boto.connect_s3(AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY)

print("Connected to S3")

try:
    bucket = conn.create_bucket(bucket_name, location=boto.s3.connection.Location.USWest)
except:
    print('AWS keys were not correct. Please re-run the program with correct keys.')
    exit()
    
print("bucket created")
testfile = 'Problem1CsvLogs.zip'
print ("Uploading %s to Amazon S3 bucket %s", testfile, bucket_name)

def percent_cb(complete, total):
    sys.stdout.write('.')
    sys.stdout.flush()

k = Key(bucket)
k.key = 'Problem1CsvLogs'
k.set_contents_from_filename(testfile,
    cb=percent_cb, num_cb=10)
print("Zip File successfully uploaded to S3")

## 