'''
Created on Mar 15, 2017

@author: Aashri
'''

############### Import Libraries ###############
import os
import glob
import pandas as pd
import numpy as np
import sys
from Part1_data_download import cleanup_dir
from Part1_data_download import data_download

cleanup_dir()
arg_len=len(sys.argv)
uname=''
pwd=''
if arg_len == 1:
    print("Arguments not entered will run for default")
    uname='tandon.a@husky.neu.edu'
    pwd='o<m|N{e4'
elif arg_len == 3:
    uname=sys.argv[1]
    pwd=sys.argv[2]
else:
    uname='tandon.a@husky.neu.edu'
    pwd='o<m|N{e4'

data_download(uname,pwd)

### fnction to calculate weighted avg
def wavg(group, avg_name, weight_name):
    d = group[avg_name]
    w = group[weight_name]
    try:
        return (d * w).sum() / w.sum()
    except ZeroDivisionError:
        return d.mean()


col_names_orig=['credit_score', 'first_pay_date', 'first_time_homebuyer',
                'maturity_date', 'msa', 'mi_percentage', 'no_of_units','occupance_status',
                'original_cltv', 'original_dti_ratio', 'original_upb', 'original_ltv',
                'original_interest_rate', 'channel', 'ppm_flag', 'product_type',
                'property_state', 'property_type', 'postal_code', 'loan_sequence_no', 
                'loan_purpose', 'original_loan_term', 'no_of_borrowers', 'seller_name',
                'servicer_name', 'super_conforming_flag']

col_names_svcg=['loan_sequence_no', 'monthly_reporting_period', 'current_actual_upb', 'current_loan_delinquency_status',
                  'loan_age', 'remaning_months_on_legal_maturity', 'repurchase_flag', 'modification_flag', 'zero_bal_code',
                  'zero_bal_eff_date', 'current_interest_rate', 'current_deferred_upb', 'ddlpi', 'mi_recoveries', 'net_sales_proceeds',
                  'non_mi_recoveries', 'expenses', 'legal_costs', 'maintenance_preservation_cost', 'taxes_insurance', 'misc_expenses',
                  'actual_loss_calc', 'modification_cost']
   
####### Fnc to read each orig file, clean it and save the cleaned file ##########
def clean_orig_files():
    try:
        orig_filelists = glob.glob('downloaded_zips_unzipped' + "/sample_orig*.txt")
        for file in orig_filelists:
            df=pd.read_table(file, sep='|', names= col_names_orig, low_memory=False)
            #replacing with mode
            df.credit_score=df.credit_score.replace(r'\s+', np.nan, regex=True).astype('float64')
            max_cs = pd.DataFrame(df['credit_score'])
            max_cs=max_cs.mode()
            df['credit_score'] = df['credit_score'].fillna(max_cs.iloc[0]['credit_score'])
            
            #replacing with NA
            df.first_time_homebuyer=df.first_time_homebuyer.str.strip()
            df['first_time_homebuyer'] = df['first_time_homebuyer'].fillna('NA')
            
            #replacing with 00000 as its unknown
            df.msa=df.msa.replace(r'\s+', np.nan, regex=True)
            df['msa'] = df['msa'].fillna('00000').astype('float64')
            
            #replacing with mean
            df.mi_percentage=df.mi_percentage.replace(r'\s+', np.nan, regex=True).astype('float64')
            mean_mi = pd.DataFrame(df['mi_percentage'])
            mean_mi=mean_mi.mean()
            df['mi_percentage'] = df['mi_percentage'].fillna(mean_mi['mi_percentage'])
            
            #replacing with 0 number of units
            df.no_of_units=df.no_of_units.replace(r'\s+', np.nan, regex=True).astype('float64')
            df['no_of_units'] = df['no_of_units'].fillna(0)
            
            #replacing with NA
            df.occupance_status=df.occupance_status.str.strip()
            df['occupance_status'] = df['occupance_status'].fillna('NA')
            
            #replacing with mean
            df.original_cltv=df.original_cltv.replace(r'\s+', np.nan, regex=True).astype('float64')
            mean_ocltv = pd.DataFrame(df['original_cltv'])
            mean_ocltv=mean_ocltv.mean()
            df['original_cltv'] = df['original_cltv'].fillna(mean_ocltv['original_cltv'])
            
            #replacing with mean
            df.original_dti_ratio = df.original_dti_ratio.replace(r'\s+', 66, regex=True).astype('float64')
            df['original_dti_ratio'] = df['original_dti_ratio'].fillna(0)
            
            #replacing with mean
            df.original_ltv=df.original_ltv.replace(r'\s+', np.nan, regex=True).astype('float64')
            mean_oltv = pd.DataFrame(df['original_ltv'])
            mean_oltv=mean_oltv.mean()
            df['original_ltv'] = df['original_ltv'].fillna(mean_oltv['original_ltv'])
            
            #replacing with NA
            df.channel=df.channel.str.strip()
            df['channel'] = df['channel'].fillna('NA')
            
            #replacing with N as its a boolean value
            df.ppm_flag=df.ppm_flag.str.strip()
            df['ppm_flag'] = df['ppm_flag'].fillna('N')
            
            #replacing with NA
            df.property_type=df.property_type.str.strip()
            df['property_type'] = df['property_type'].fillna('NA')
            
            #replacing with 00000 as its unknown
            df.postal_code=df.postal_code.replace(r'\s+', np.nan, regex=True)
            df['postal_code'] = df['postal_code'].fillna('00000').astype('float64')
            
            #replacing with NA
            df.loan_purpose=df.loan_purpose.str.strip()
            df['loan_purpose'] = df['loan_purpose'].fillna('NA')
            
            #replacing with mode
            max_borrowers = pd.DataFrame(df['no_of_borrowers'])
            max_borrowers=max_borrowers.mode()
            df['no_of_borrowers'] = df['no_of_borrowers'].fillna(max_borrowers.iloc[0]['no_of_borrowers'])
            df.no_of_borrowers=df.no_of_borrowers.replace(r'\s+', np.nan, regex=True).astype('float64')
            
            #replacing with NA
            df.super_conforming_flag=df.super_conforming_flag.replace(r'\s+', np.nan, regex=True)
            df['super_conforming_flag'] = df['super_conforming_flag'].fillna('N')
            name=file[-20:]
            df.to_csv(os.path.join('cleanFiles',name),index=False)
        print('Origination files cleaned.')
    except Exception as e:
        print(str(e))
        exit()

#### Fnc to read each performance file, clean it and save the cleaned file #######
def clean_svcg_files():
    try:
        svcg_filelists = glob.glob('downloaded_zips_unzipped' + "/sample_svcg*.txt")
        for file in svcg_filelists:
            df=pd.read_table(file, sep='|', names= col_names_svcg, low_memory=False)
            df.dropna(how='all')
            name=file[-20:]
            df.to_csv(os.path.join('cleanFiles',name), index=False)
        print('Performance files cleaned.')
    except Exception as e:
        print(str(e))
        exit()
    

###################### Summaries of Origination file #################
def generate_orig_summaries():
    try:
        clean_filelists = glob.glob('cleanFiles' + "/sample_orig*.txt")
        for file in clean_filelists:
            df=pd.read_csv(file)
            loanCount=np.count_nonzero(df['loan_sequence_no'])
            creditScore_wavg=wavg(df, "credit_score", "original_upb")
            totalOriginalUPB=np.sum(df['original_upb'])
            avgOriginalUPB=totalOriginalUPB/(np.count_nonzero(df['original_upb']))
            originalCLTV_wavg=wavg(df, "original_cltv", "original_upb")
            originalLTV_wavg=wavg(df, "original_ltv", "original_upb")
            originaldti_wavg=wavg(df, "original_dti_ratio", "original_upb")
            df['Loan Count']=pd.Series(loanCount, index=df.index)
            df['Total Original UPB']=pd.Series(totalOriginalUPB, index=df.index)
            df['Average Original UPB']=pd.Series(avgOriginalUPB, index=df.index)
            df['Credit Score']=pd.Series(creditScore_wavg, index=df.index)
            df['Original CLTV']=pd.Series(originalCLTV_wavg, index=df.index)
            df['Original LTV']=pd.Series(originalLTV_wavg, index=df.index)
            df['Original DTI']=pd.Series(originaldti_wavg, index=df.index)
            fname=file[-20:]
            year=file[-8:-4]
            print(year,loanCount,creditScore_wavg,totalOriginalUPB,avgOriginalUPB,originalCLTV_wavg,originalLTV_wavg,originaldti_wavg)
            df.to_csv(os.path.join('cleanFilesWithSummaries',fname),index=False)
        print('Originating files summaries created')
    except Exception as e:
        print(str(e))
        exit()    

###################### Summaries of Performance file #################
def generate_svcg_summaries():
    try:
        clean_filelists = glob.glob('cleanFiles' + "/sample_svcg*.txt")
        for file in clean_filelists:
            df = pd.read_csv(file)
            # create new column - year
            df['year'] = file[-8:-4]
            # calculate the number of distinct loans per year
            df['cnt_distinct_loans_per_year'] = df['loan_sequence_no'].nunique()
            
            # % of loans paid voluntarily => for each year.. count(zero_bal_code = 1.0)/total_no_loans
            cnt_zero_bal_code_1 = df[df['zero_bal_code'] == 1.0]['loan_sequence_no'].nunique()
            total_loans = df['loan_sequence_no'].nunique()
            df['loans_paid_percent'] =  100 * cnt_zero_bal_code_1 / total_loans
            
            # Loans acquired by REO => count(current_loan_delinquency_status = R)
            total_reos = df[df['current_loan_delinquency_status'].astype(str) == 'R']['loan_sequence_no'].nunique()
            df['total_reos_per_year_percent'] = 100 * total_reos/total_loans
            
            # Mean of unpaid balance
            df['mean_upb_year'] = df['current_actual_upb'].mean()
            
            # Average loan-age per year
            df['avg_loan_age'] = df['loan_age'].mean()
        
            # Average Current Interest Rate per year
            df['avg_current_interest_rate'] = df['current_interest_rate'].mean()
            
            # Actual Loss Incurred by Friedie&Mac per year => Sum(Actual_loss) per year
            df['total_actual_loss'] = df['actual_loss_calc'].sum()
            
            # Total no of modified loans = Count(Modification_flag = Y) per year
            df['modified_loans_count'] = df[df['modification_flag'].astype('str')=='Y']['loan_sequence_no'].nunique()
            
            fname=file[-20:]
            df.to_csv(os.path.join('cleanFilesWithSummaries',fname),index=False)
        print('performance files summaries created')
    except Exception as e:
        print(str(e))
        exit()
############### combining all the cleaned Origination files with summaries to one file ###############
def combine_files(orig=True): 
    try: 
        print("Started combining the cleaned files of all the years with summaries to one file")
        if orig == True:  
            summary_files = glob.glob(os.path.join('cleanFilesWithSummaries',"*orig*.txt"))
            summaryCol_names=['credit_score', 'first_pay_date', 'first_time_homebuyer',
                    'maturity_date', 'msa', 'mi_percentage', 'no_of_units','occupance_status',
                    'original_cltv', 'original_dti_ratio', 'original_upb', 'original_ltv',
                    'original_interest_rate', 'channel', 'ppm_flag', 'product_type',
                    'property_state', 'property_type', 'postal_code', 'loan_sequence_no', 
                    'loan_purpose', 'original_loan_term', 'no_of_borrowers', 'seller_name',
                    'servicer_name', 'super_conforming_flag','Loan Count','Total Original UPB',
                    'Average Original UPB','Credit Score','Original CLTV','Original LTV','Original DTI']
        if orig == False:
            summary_files = glob.glob(os.path.join('cleanFilesWithSummaries',"*svcg*.txt"))
            summaryCol_names=['loan_sequence_no', 'monthly_reporting_period', 'current_actual_upb', 'current_loan_delinquency_status',
                      'loan_age', 'current_interest_rate','actual_loss_calc', 'cnt_distinct_loans_per_year',
                      'loans_paid_percent', 'total_reos_per_year_percent', 'mean_upb_year',
                      'avg_loan_age', 'avg_current_interest_rate', 'total_actual_loss',
                      'modified_loans_count']
        full_df = pd.DataFrame(columns=summaryCol_names)
        df_list = []
        for filename in sorted(summary_files):
            df_list.append(pd.read_csv(filename))
        full_df = pd.concat(df_list)
        if orig == True:
            full_df.to_csv(os.path.join('combinedFileWithSummaries_orig.csv'))
        if orig == False:
            full_df.to_csv(os.path.join('combinedFileWithSummaries_svcg.csv'))
        print("Completed combining the cleaned files of all the years with summaries to one file")
    except Exception as e:
        print(str(e))
        exit()

clean_orig_files()
clean_svcg_files()
generate_orig_summaries()
generate_svcg_summaries()
combine_files(True)
combine_files(False)
dashboardURL='https://public.tableau.com/profile/publish/Team9Midterm/ExploratoryDataAnalysis#!/publish-confirm'
print('Tableau Public URL: ',dashboardURL)