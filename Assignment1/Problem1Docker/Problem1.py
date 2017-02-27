############### PROBLEM 1 PART 1 & 2 ############### 
#Description: Refer Report

############### Import Libraries ###############
import urllib.request
from bs4 import BeautifulSoup #for web scraping
import csv #for writing csv
import logging #for logging
import os
import zipfile
import boto.s3
import sys
from boto.s3.key import Key
import time
import datetime


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

############## Fetch all the command line arguments ###################
argLen=len(sys.argv)
cik=''
accessionNumber=''
accessKey=''
secretAccessKey=''
inputLocation=''

for i in range(1,argLen):
    val=sys.argv[i]
    if val.startswith('cik='):
        pos=val.index("=")
        cik=val[pos+1:len(val)]
        continue
    elif val.startswith('accessionNumber='):
        pos=val.index("=")
        accessionNumber=val[pos+1:len(val)]
        continue
    elif val.startswith('accessKey='):
        pos=val.index("=")
        accessKey=val[pos+1:len(val)]
        continue
    elif val.startswith('secretKey='):
        pos=val.index("=")
        secretAccessKey=val[pos+1:len(val)]
        continue
    elif val.startswith('location='):
        pos=val.index("=")
        inputLocation=val[pos+1:len(val)]
        continue

print("CIK=",cik)
print("Accession Number=",accessionNumber)
print("Access Key=",accessKey)
print("Secret Access Key=",secretAccessKey)
print("Location=",inputLocation)

############### Validate amazon keys ###############
if not accessKey or not secretAccessKey:
    logging.warning('Access Key and Secret Access Key not provided!!')
    print('Access Key and Secret Access Key not provided!!')
    exit()

AWS_ACCESS_KEY_ID = accessKey
AWS_SECRET_ACCESS_KEY = secretAccessKey

try:
    conn = boto.connect_s3(AWS_ACCESS_KEY_ID,
            AWS_SECRET_ACCESS_KEY)

    print("Connected to S3")

except:
    logging.info("Amazon keys are invalid!!")
    print("Amazon keys are invalid!!")
    exit()


############### Create the URL by inputed CIK and ACC_No ###############
url_pre = "https://www.sec.gov/Archives/edgar/data/"
if not cik or not accessionNumber:
    logging.warning('CIK or AccessionNumber was not given, assuming it to be 0000051143 and 0000051143-13-000007 respectively')
    cik='0000051143'
    accessionNumber = '0000051143-13-000007'
else:
    logging.info('CIK: %s and AccessionNumber: %s given', cik, accessionNumber)

cik_striped = cik.lstrip("0")
accno_striped = accessionNumber.replace("-","")
url1 = url_pre + cik_striped + "/" + accno_striped + "/" + accessionNumber + "-index.html"        
logging.info("URL generated is: "+ url1)


###############  fetch the form's URL either 10q or 10-k ###############
url2=''
try:
    page = urllib.request.urlopen(url1)
    soup = BeautifulSoup(page,"lxml") # parse the page and save it in soup format 
    form = soup.find(id='formName').get_text() # find the form name according to accession no
    formname = form[6:10]
    # find the td which has the above form name
    formtype = soup.findAll('td', text = formname)[0]
    # fetch the url for that form.. find all siblings and then child href ? maybe
    all_links = soup.find_all('a')
    for link in all_links:
        href=link.get("href")
        if "10q.htm" in href:
            url2 = "https://www.sec.gov/" + href
            logging.info("Form's URL is: "+ url2)
            break;
        elif "10-k" in href:
            url2 = "https://www.sec.gov/" + href
            logging.info("Form's URL is: "+ url2)
            break;
        else:
            url2=""
            
            
    if url2 is "":
        logging.info("Invalid URL!!!")
        print("Invalid URL!!!")
        exit()  
         
except urllib.error.HTTPError as err:
    logging.warning("Invalid CIK or AccNo")
    exit()


############### Access the form and fetch the tables ###############
if not os.path.exists('extracted_csvs'):
    os.makedirs('extracted_csvs')
        
page = urllib.request.urlopen(url2)
soup = BeautifulSoup(page,"lxml")
all_tables = soup.select('div table')

############### Finding tables that contain statistical information ###############
############### Fetched by checking presence of '$' or '%' ###############
refined_tables=[]

for tab in all_tables:
    for tr in tab.find_all('tr'):
        f=0
        for td in tr.findAll('td'):
            if('$' in td.get_text() or '%' in td.get_text()):
                refined_tables.append(tab)
                f=1;
                break;
        if(f==1):
            break;    
############### For all the refined tables, following is performed ##############
############### Fetching the data inside <td> tags and removing unwanted characters such as '\n','\xa0' ###############
############### After cleaning, writing the table into a csv file ###############

for tab in refined_tables:
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
    with open(os.path.join('extracted_csvs' , str(refined_tables.index(tab)) + 'tables.csv'), 'w') as f:
        writer = csv.writer(f)
        writer.writerows(records)
            
logging.info('Tables successfully extracted to csv')
    
############### Zip the csvs and logs ###############
def zipdir(path, ziph, refined_tables):
    # ziph is zipfile handle
    for tab in refined_tables:
        ziph.write(os.path.join('extracted_csvs', str(refined_tables.index(tab))+'tables.csv'))
    ziph.write(os.path.join('problem1_log.log'))   

zipf = zipfile.ZipFile('Problem1.zip', 'w', zipfile.ZIP_DEFLATED)
zipdir('/', zipf, refined_tables)
zipf.close()
logging.info('csv and log file zipped')


############### Upload the zip to AWS S3 ###############
############### Fetch the location argument if provided, else user's system location is taken ############### 
loc=''

if inputLocation == 'APNortheast':
    loc=boto.s3.connection.Location.APNortheast
elif inputLocation == 'APSoutheast':
    loc=boto.s3.connection.Location.APSoutheast
elif inputLocation == 'APSoutheast2':
    loc=boto.s3.connection.Location.APSoutheast2
elif inputLocation == 'CNNorth1':
    loc=boto.s3.connection.Location.CNNorth1
elif inputLocation == 'EUCentral1':
    loc=boto.s3.connection.Location.EUCentral1
elif inputLocation == 'EU':
    loc=boto.s3.connection.Location.EU
elif inputLocation == 'SAEast':
    loc=boto.s3.connection.Location.SAEast
elif inputLocation == 'USWest':
    loc=boto.s3.connection.Location.USWest
elif inputLocation == 'USWest2':
    loc=boto.s3.connection.Location.USWest2
try:   
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts)    
    bucket_name = AWS_ACCESS_KEY_ID.lower()+str(st).replace(" ", "").replace("-", "").replace(":","").replace(".","")
    bucket = conn.create_bucket(bucket_name, location=loc)
    print("bucket created")
    zipfile = 'Problem1.zip'
    print ("Uploading %s to Amazon S3 bucket %s", zipfile, bucket_name)
    
    def percent_cb(complete, total):
        sys.stdout.write('.')
        sys.stdout.flush()
    
    k = Key(bucket)
    k.key = 'Problem1'
    k.set_contents_from_filename(zipfile,
        cb=percent_cb, num_cb=10)
    print("Zip File successfully uploaded to S3")
except:
    logging.info("Amazon keys are invalid!!")
    print("Amazon keys are invalid!!")
    exit()


############ EOF ############