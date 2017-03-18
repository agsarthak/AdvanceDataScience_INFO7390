import requests
from lxml import html
import os
import zipfile

def get_data(quarter):
    ############### Cleanup required directories ###############
    def cleanup_dir():
        if not os.path.exists('part2_data_downloaded_zips'):
            os.makedirs('part2_data_downloaded_zips', mode=0o777)
                
        if not os.path.exists('part2_data_downloaded_zips_unzipped'):
            os.makedirs('part2_data_downloaded_zips_unzipped', mode=0o777)
    
    cleanup_dir()
    
    
    ############### Create Session ###############
    USERNAME='tandon.a@husky.neu.edu'
    PASSWORD='o<m|N{e4'
    
    
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
    
    
    def get_next_quarter(QUARTER):
        quarterNumber = int(QUARTER[1])
        quarterYear= int(QUARTER[2:6])
        
        if quarterNumber < 4:
            newQuarterNumber = quarterNumber + 1
            newQuarterYear = quarterYear
        else:
            newQuarterNumber = quarterNumber - 3
            newQuarterYear = quarterYear + 1
            
        QUARTER2 = "Q" + str(newQuarterNumber) + str(newQuarterYear)
            
        return QUARTER2
    
    ############### Download zips ###############
    def download_zip(quaterInput):
        for link in all_links:
            href=link.get("href")
            if quaterInput in href:
                url= 'https://freddiemac.embs.com/FLoan/Data/'+href
                print(url)
                r = session_requests.get(url,stream=True)
                with open(os.path.join('part2_data_downloaded_zips',link.text), 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024):
                        if chunk: # filter out keep-alive new chunks
                            f.write(chunk)
        print('Data downloaded for %s', quaterInput )
    
    download_zip(quarter)
    download_zip(get_next_quarter(quarter))
    
    ############### Unzip and extract the quarter text files ###############
    try:
        zip_files = os.listdir('part2_data_downloaded_zips')
        for f in zip_files:
            z = zipfile.ZipFile(os.path.join('part2_data_downloaded_zips', f), 'r')
            for file in z.namelist():
                if file.endswith('.txt'):
                    z.extract(file, r'part2_data_downloaded_zips_unzipped')
        print('Zip files successfully extracted to folder: part2_data_downloaded_zips_unzipped.')
    except Exception as e:
            print(str(e))
            exit()