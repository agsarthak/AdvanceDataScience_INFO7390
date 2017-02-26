############### PROBLEM 2 PART 1 & 2 ############### 
#Description: Refer report

############### Import Libraries ###############
import urllib.request
import zipfile
import os
import pandas as pd
import logging # for logging
import sys
import shutil #to delete the directory contents
import glob

############### Initializing logging file ###############
root = logging.getLogger()
root.setLevel(logging.DEBUG)

ch1 = logging.FileHandler('problem2_log.log') #output the logs to a file
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
    logging.info('Directories cleanup complete.')
except Exception as e:
    logging.error('Error occurred while cleaning up directories. Check console.')
    print(str(e))
    exit()     
    
############### Function to Download zips ###############
def download_zip(url):
    zips = []
    try:
        zips.append(urllib.request.urlretrieve(url, filename= 'downloaded_zips/'+url[-15:]))
        if os.path.getsize('downloaded_zips/'+url[-15:]) <= 4515: #catching empty file
            os.remove('downloaded_zips/'+url[-15:])
            logging.warning('Log file %s is empty. Attempting to download for next date.', url[-15:])
            return False
        else:
            logging.info('Log file %s successfully downloaded', url[-15:])
            return True
    except Exception as e: #Catching file not found
        #print(str(e))
        logging.warning('Log %s not found...Skipping ahead!', url[-15:])
        return True

############### Generate URLs and download zip for the inputted year ###############
#http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2016/Qtr1/log20160101.zip
url_pre = "http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/"
qtr_months = {'Qtr1':['01','02','03'], 'Qtr2':['04','05','06'], 'Qtr3':['07','08','09'], 'Qtr4':['10','11','12']}
year = input("Please enter the year for which you want to analyze the logs:")
valid_years = range(2003,2017)
days = range(1,32)

if not year:
    year = 2003
    logging.warning('Program running for 2003 by default since you did not enter any Year.')

if int(year) not in valid_years:
    logging.error("Invalid year. Please enter a valid year between 2003 and 2016.")
    exit()

logging.info('Initializing zip download.')

url_final = []
for key, val in qtr_months.items():
    for v in val:
        for d in days:
            url = url_pre +str(year) +'/' +str(key) +'/' +'log' +str(year) +str(v) + str(format(d,'02d')) +'.zip'
            if download_zip(url):
                break
            else:
                continue
logging.info('All log files downloaded for %s', year)


############### Unzip the logs and extract csv ###############
try:
    zip_files = os.listdir('downloaded_zips')
    for f in zip_files:
        z = zipfile.ZipFile(os.path.join('downloaded_zips', f), 'r')
        for file in z.namelist():
            if file.endswith('.csv'):
                z.extract(file, r'downloaded_zips_unzipped')
    logging.info('Zip files successfully extracted to folder: downloaded_zips_unzipped.')
except Exception as e:
        logging.error(str(e))
        exit()


############### Load the csvs into dataframe ###############
try:
    filelists = glob.glob('downloaded_zips_unzipped' + "/*.csv")
    all_csv_df_dict = {period: pd.read_csv(period) for period in filelists}
    logging.info('All the csv read into individual daaframes')
except Exception as e:
    logging.error('Error occurred while loading csv to dataframe. Check console for more info.')
    print(str(e))
    exit()
                   
                   
############### HANDLING MISSING VALUES for one dataframe at a time ###############
try:
    for key, val in all_csv_df_dict.items():
        df = all_csv_df_dict[key]
        #remove rows which have no ip, date, time, cik or accession
        df.dropna(subset=['cik'])
        df.dropna(subset=['accession'])
        df.dropna(subset=['ip'])
        df.dropna(subset=['date'])
        df.dropna(subset=['time'])
        
        #replace nan with the most used browser in data.
        max_browser = pd.DataFrame(df.groupby('browser').size().rename('cnt')).idxmax()[0]
        df['browser'] = df['browser'].fillna(max_browser)
        
        # replace nan idx with max idx
        max_idx = pd.DataFrame(df.groupby('idx').size().rename('cnt')).idxmax()[0]
        df['idx'] = df['idx'].fillna(max_idx)
        
        # replace nan code with max code
        max_code = pd.DataFrame(df.groupby('code').size().rename('cnt')).idxmax()[0]
        df['code'] = df['code'].fillna(max_code)
        
        # replace nan norefer with zero
        df['norefer'] = df['norefer'].fillna('1')
        
        # replace nan noagent with zero
        df['noagent'] = df['noagent'].fillna('1')
        
        # replace nan find with max find
        max_find = pd.DataFrame(df.groupby('find').size().rename('cnt')).idxmax()[0]
        df['find'] = df['find'].fillna(max_find)
        
        
        # replace nan crawler with zero
        df['crawler'] = df['crawler'].fillna('0')
        
        
        # replace nan extention with max extention
        max_extention = pd.DataFrame(df.groupby('extention').size().rename('cnt')).idxmax()[0]
        df['extention'] = df['extention'].fillna(max_extention)
        
        # replace nan extention with max extention
        max_zone = pd.DataFrame(df.groupby('zone').size().rename('cnt')).idxmax()[0]
        df['zone'] = df['zone'].fillna(max_zone)
    
        # find mean of the size and replace null values with the mean
        df['size'] = df['size'].fillna(df['size'].mean(axis=0))
        
        #Compute mean size
        df['size_mean'] = df['size'].mean(axis=0)
        
        #Compute maximum used browser
        df['max_browser'] = pd.DataFrame(df.groupby('browser').size().rename('cnt')).idxmax()[0]
    
    logging.info('Rows removed where ip, date, time, cik or accession were null.')
    logging.info('NaN values in browser replaced with maximum count browser.')
    logging.info('NaN values in idx replaced with maximum count idx.')
    logging.info('NaN values in code replaced with maximum count code.')
    logging.info('NaN values in norefer replaced with 0.')
    logging.info('NaN values in noagent replaced with 0.')
    logging.info('NaN values in find replaced with maximum count find.')
    logging.info('NaN values in crawler replaced with 0.')
    logging.info('NaN values in extension replaced with maximum count extension.')
    logging.info('NaN values in zone replaced with maximum count zone.')
    logging.info('NaN values in size replaced with mean value of size.')
    logging.info('New column added to dataframe: Mean of size.')
    logging.info('New column added to dataframe: Max count of browser.')
except Exception as e:
    logging.error("Error occurred while handling missing data. Check console for more info.")
    print(str(e))
    exit()
    
############### Combining all dataframe and computing overall summary metric ###############
# writing csv for all data
try:
    master_df = pd.concat(all_csv_df_dict)
    master_df.to_csv('master_csv.csv')
    logging.info('All dataframes of csvs are combined and exported as csv: master_csv.csv.')
except Exception as e:
    logging.error('Error occurred while exporting merged csv. Check console for more info.')
    print(str(e))
    exit()
    
# write csv for summary of combined data.
try:
    master_df_summary = master_df.describe()
    master_df_summary.to_csv('master_df_summary.csv')
    logging.info('The summary metric of combined csv is generated and exported as csv: master_df_summary.csv .')
except Exception as e:
    logging.error('Error occurred while exporting summary statistics for merged csv. Check console for more info.')
    print(str(e))
    exit()
    
