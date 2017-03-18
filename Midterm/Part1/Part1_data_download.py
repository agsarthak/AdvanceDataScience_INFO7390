'''
Created on Mar 15, 2017

@author: Aashri
'''
############### Import Libraries ###############
import requests
from lxml import html
import os
import sys
import logging # for logging
import shutil #to delete the directory contents
import zipfile

############### Initializing logging file ###############
root = logging.getLogger()
root.setLevel(logging.DEBUG)

ch1 = logging.FileHandler('midterm_part1_log.log') #output the logs to a file
ch1.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch1.setFormatter(formatter)
root.addHandler(ch1)

ch = logging.StreamHandler(sys.stdout ) #print the logs in console as well
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s - %(message)s')
ch.setFormatter(formatter)
root.addHandler(ch)

############### Cleanup required directories ###############
def cleanup_dir():
    try:
        if not os.path.exists('downloaded_zips'):
            os.makedirs('downloaded_zips', mode=0o777)
        else:
            shutil.rmtree(os.path.join(os.path.dirname(__file__),'downloaded_zips'), ignore_errors=False)
            os.makedirs('downloaded_zips', mode=0o777)
        
        if not os.path.exists('downloaded_zips_unzipped'):
            os.makedirs('downloaded_zips_unzipped', mode=0o777)
        else:
            shutil.rmtree(os.path.join(os.path.dirname(__file__), 'downloaded_zips_unzipped'), ignore_errors=False)
            os.makedirs('downloaded_zips_unzipped', mode=0o777)
        
        if not os.path.exists('cleanFiles'):
            os.makedirs('cleanFiles', mode=0o777)
        else:
            shutil.rmtree(os.path.join(os.path.dirname(__file__), 'cleanFiles'), ignore_errors=False)
            os.makedirs('cleanFiles', mode=0o777) 
        if not os.path.exists('cleanFilesWithSummaries'):
            os.makedirs('cleanFilesWithSummaries', mode=0o777)  
        else:
            shutil.rmtree(os.path.join(os.path.dirname(__file__), 'cleanFilesWithSummaries'), ignore_errors=False)
            os.makedirs('cleanFilesWithSummaries', mode=0o777) 
        logging.info('Directories cleanup complete.')
    except Exception as e:
        print(str(e))
        exit()
    

############### Create Session ###############
def data_download(uname,pwd):
    USERNAME='tandon.a@husky.neu.edu'
    PASSWORD='o<m|N{e4'
    print('username='+uname)
    print('password='+pwd)
    
    payload = {
        "username": USERNAME, 
        "password": PASSWORD
    }
    
    
    session_requests = requests.session()
    
    login_url = "https://freddiemac.embs.com/FLoan/secure/auth.php"
    
    result = session_requests.post(
        login_url, 
        data = payload, 
        headers = dict(referer=login_url)
    )
    
    url = 'https://freddiemac.embs.com/FLoan/Data/download.php'
    agreement_payload={
        "accept":"Yes",
        "action":"acceptTandC",
        "acceptSubmit":"Continue"
        }
    result = session_requests.post(
        url, 
        agreement_payload,
        headers = dict(referer = url)
    )
    
    tree = html.fromstring(result.content)
    all_links = tree.findall(".//a")
    
    ############### Download zips ###############
    for link in all_links:
        href=link.get("href")
        if "sample" in href:
            url= 'https://freddiemac.embs.com/FLoan/Data/'+href
            r = session_requests.get(url,stream=True)
            with open(os.path.join('downloaded_zips',link.text), 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk: # filter out keep-alive new chunks
                        f.write(chunk)
    
    
    ############### Unzip and extract text files ###############
    try:
        zip_files = os.listdir('downloaded_zips')
        for f in zip_files:
            z = zipfile.ZipFile(os.path.join('downloaded_zips', f), 'r')
            for file in z.namelist():
                if file.endswith('.txt'):
                    z.extract(file, r'downloaded_zips_unzipped')
        logging.info('Zip files successfully extracted to folder: downloaded_zips_unzipped.')
    except Exception as e:
            logging.error(str(e))
            exit()
