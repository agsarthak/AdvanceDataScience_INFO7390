############### Import Libraries ###############
import operator
import pandas as pd
import numpy as np
from sklearn import preprocessing
from sklearn.linear_model import LinearRegression
from sklearn.feature_selection import RFE
from sklearn.model_selection import cross_val_score
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.ensemble import RandomForestRegressor
from sklearn.neighbors import KNeighborsRegressor
from Part2_get_data import get_data
from sklearn.neural_network import MLPRegressor
# this allows plots to appear directly in the notebook %matplotlib inline
import os, sys

############### Inputs  ###############

arg_len=len(sys.argv)
quarters=[]
downloadQuarters=[]
end=''
if arg_len == 2:
    year=sys.argv[1]
    nextYear=int(year)+1
    for i in range(1,5):
        y='Q'+str(i)+year
        quarters.append(y)
        downloadQuarters.append(y)
    downloadQuarters.append('Q1'+str(nextYear))
else:
    print("running for default 2007")
    for i in range(1,5):
        y='Q'+str(i)+'2007'
        quarters.append(y)
        downloadQuarters.append(y)
    downloadQuarters.append('Q12008')
    
get_data(downloadQuarters)

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

for quarter in quarters:
    next_quarter = get_next_quarter(quarter)
    # Load data into DataFrame
    col_names_orig=['credit_score', 'first_pay_date', 'first_time_homebuyer',
                    'maturity_date', 'msa', 'mi_percentage', 'no_of_units','occupance_status',
                    'original_cltv', 'original_dti_ratio', 'original_upb', 'original_ltv',
                    'original_interest_rate', 'channel', 'ppm_flag', 'product_type',
                    'property_state', 'property_type', 'postal_code', 'loan_sequence_no', 
                    'loan_purpose', 'original_loan_term', 'no_of_borrowers', 'seller_name',
                    'servicer_name', 'super_conforming_flag']
    
    df1 = pd.read_table(os.path.join('part2_data_downloaded_zips_unzipped/historical_data1_'+quarter+".txt"), 
                            delimiter='|', names=col_names_orig)
    df2 = pd.read_table(os.path.join('part2_data_downloaded_zips_unzipped/historical_data1_'+next_quarter+".txt"), 
                            delimiter='|', names=col_names_orig )
    
    #df1 = pd.read_table('part2_data_downloaded_zips_unzipped/historical_data1_Q12005.txt', delimiter='|', names=col_names_orig)
    #df2 = pd.read_table('part2_data_downloaded_zips_unzipped/historical_data1_Q22005.txt', delimiter='|', names=col_names_orig)
    
    
    
    ##################### clean the dataframe and fill missing values #####################
    def clean_df(df):
        # Credit_score
        df.credit_score = df.credit_score.replace(r'\s+', np.nan, regex=True).astype('float64')
        max_cs = pd.DataFrame(df['credit_score'])
        max_cs = max_cs.mode()
        df['credit_score'] = df['credit_score'].fillna(max_cs.iloc[0]['credit_score'])
    
        # First_time_homebuyer
        #df.first_time_homebuyer = df.first_time_homebuyer.str.strip()
        df.first_time_homebuyer.replace(np.nan, 0, inplace=True)
        df.first_time_homebuyer.replace('N', 1, inplace=True)
        df.first_time_homebuyer.replace('Y', 2, inplace=True)
        df.first_time_homebuyer = df.first_time_homebuyer.astype('category')
    
        # MSA
        df.msa = df.msa.replace(np.nan, 0, regex=True).astype('float64')
    
        # mi_percentage
        df.mi_percentage = df.mi_percentage.replace(r'\s+', np.nan, regex=True).astype('float64')
        df.mi_percentage.replace(np.nan, df.mi_percentage.mean(), inplace=True)
    
        # no_of_units
        df.no_of_units=df.no_of_units.replace(np.nan, 0, regex=True).astype('float64')
        df.no_of_units = df.no_of_units.astype('category')
        
        # occupancy_status
        df.occupance_status.replace('', 0, inplace=True)
        df.occupance_status.replace('O', 1, inplace=True)
        df.occupance_status.replace('S', 2, inplace=True)
        df.occupance_status.replace('I', 3, inplace=True)
        df.occupance_status = df.occupance_status.astype('category')
        
        # original_cltv
        df.original_cltv.replace(np.nan, df.original_cltv.mean(), inplace=True)
    
        # original_dti_ratio
        df.original_dti_ratio = df.original_dti_ratio.replace(r'\s+', 66, regex=True).astype('float64')
        df.original_dti_ratio = df.original_dti_ratio.replace(np.nan, 0).astype('float64')
    
        # original_ltv
        df.original_ltv = df.original_ltv.replace(r'\s+', np.nan, regex=True).astype('float64')
        df.original_ltv.replace(np.nan, df.original_ltv.mean(), inplace=True)
        df.original_ltv = df.original_ltv.astype('category')
        
        # Channel
        df.channel = df.channel.str.strip()
        df.channel.replace('', 0, inplace=True)
        df.channel.replace('R', 1, inplace=True)
        df.channel.replace('T', 2, inplace=True)
        df.channel.replace('C', 3, inplace=True)
        df.channel.replace('B', 4, inplace=True)
        df.channel = df.channel.astype('category')
    
        # ppm_flag
        df.ppm_flag.replace(np.nan, 0, inplace=True)
        df.ppm_flag.replace('N', 0, inplace=True)
        df.ppm_flag.replace('Y', 1, inplace=True)
        df.ppm_flag = df.ppm_flag.astype('category')
    
        # product_type
        df.product_type.replace('FRM', 0, inplace=True)
    
        # property_type
        df.property_type = df.property_type.str.strip()
        df.property_type.replace('', 0, inplace=True)
        df.property_type.replace('SF', 1, inplace=True)
        df.property_type.replace('CO', 2, inplace=True)
        df.property_type.replace('PU', 3, inplace=True)
        df.property_type.replace('MH', 4, inplace=True)
        df.property_type.replace('LH', 5, inplace=True)
        df.property_type.replace('CP', 6, inplace=True)
        df.property_type = df.property_type.astype('category')
    
        # postal_code
        df.postal_code = df.postal_code.replace(np.nan, 0, regex=True).astype('float64')
    
        # loan_purpose
        df.loan_purpose = df.loan_purpose.str.strip()
        df.loan_purpose.replace('', 0, inplace=True)
        df.loan_purpose.replace('C', 1, inplace=True)
        df.loan_purpose.replace('N', 2, inplace=True)
        df.loan_purpose.replace('P', 3, inplace=True)
        df.loan_purpose = df.loan_purpose.astype('category')
    
        # no_of_borrowers
        df.no_of_borrowers=df.no_of_borrowers.replace(r'\s+', np.nan, regex=True).astype('float64')
        df.no_of_borrowers = df.no_of_borrowers.replace(r'\s+', np.nan, regex=True).astype('float64')
        max_b = pd.DataFrame(df['no_of_borrowers'])
        max_b = max_b.mode()
        df['no_of_borrowers'] = df['no_of_borrowers'].fillna(max_b.iloc[0]['no_of_borrowers'])
    
        #super_conforming_flag
        df.super_conforming_flag=df.super_conforming_flag.replace(r'\s+', np.nan, regex=True).astype('float64')
    
    
    clean_df(df1)
    print('Finished cleaning df1')
    clean_df(df2)
    print('Finished cleaning df2')
    
    ####################### Features and Labels #######################
    X_train_org = df1.drop(['original_interest_rate',
                 'property_state', 
                 'loan_sequence_no', 
                 'seller_name', 'servicer_name', 'super_conforming_flag'], axis=1)
    y_train = df1['original_interest_rate']
    
    X_test_org = df2.drop(['original_interest_rate',
                 'property_state', 
                 'loan_sequence_no', 
                 'seller_name', 'servicer_name', 'super_conforming_flag'], axis=1)
    y_test = df2['original_interest_rate']
    
    X_train = preprocessing.minmax_scale(X_train_org) # scale between 0 and 1
    X_test = preprocessing.minmax_scale(X_test_org)
    
    ####################### Build Regression Model #######################
    # Scaling all the features
    #X = preprocessing.scale(X)
    
    error_metric = pd.DataFrame({'rms_train':[], 
                                 'rms_test': [],
                                 'mae_train': [],
                                 'mae_test':[],
                                 'mape_train':[],
                                 'mape_test':[]})
    
    rmse_dict = {}    
        
    def calc_error_metric(modelname, model, X_train, y_train, X_test, y_test):
        global error_metric
        y_train_predicted = model.predict(X_train)
        y_test_predicted = model.predict(X_test)
        
        #MAE, RMS, MAPE
        rms_train = mean_squared_error(y_train, y_train_predicted)
        rms_test = mean_squared_error(y_test, y_test_predicted)
        
        mae_train = mean_absolute_error(y_train, y_train_predicted)
        mae_test = mean_absolute_error(y_test, y_test_predicted)
        
        mape_train = np.mean(np.abs((y_train - y_train_predicted) / y_train)) * 100
        mape_test = np.mean(np.abs((y_test - y_test_predicted) / y_test)) * 100
        
        rmse_dict[modelname] = rms_test
        
        df_local = pd.DataFrame({'Model':[modelname],
                                 'rms_train':[rms_train], 
                                 'rms_test': [rms_test],
                                 'mae_train': [mae_train],
                                 'mae_test':[mae_test],
                                 'mape_train':[mape_train],
                                 'mape_test':[mape_test]})
        
        error_metric = pd.concat([error_metric, df_local])
        return error_metric
    
    # Regression
    clf = LinearRegression()
    clf.fit(X_train, y_train)
    calc_error_metric('Regression', clf, X_train, y_train, X_test, y_test)
    print('Regression completed')
    
    # Random Forest
    rf = RandomForestRegressor(n_estimators=20)
    rf.fit(X_train, y_train)
    calc_error_metric('RandomForest', rf, X_train, y_train, X_test, y_test)
    print('RandomForest completed')
    
    # KNN
    knn = KNeighborsRegressor(n_neighbors=13)
    knn.fit(X_train_org, y_train)
    calc_error_metric('KNN', knn, X_train, y_train, X_test, y_test)
    print('KNN completed')
    
    # Neural network
    nn = MLPRegressor()
    nn.fit(X_train, y_train)
    calc_error_metric('Nueral Network', nn, X_train_org, y_train, X_test_org, y_test)
    print('Nueral Network completed')
    
    #### Calculate best model
    best_model =  min(rmse_dict.items(),key=operator.itemgetter(1))[0]
    print('Best Model is ', best_model)
    
    #### Write the error
    error_metric.to_csv(quarter+'Error_metrics.csv')