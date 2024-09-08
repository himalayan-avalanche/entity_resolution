# Databricks notebook source

##### Step 0: Installing python libs if not already installed on cluster



class installPyLibs:
  
  def __init__(self):
    
    import warnings

    def fxn():
      warnings.warn("deprecated", DeprecationWarning)

    with warnings.catch_warnings():
      warnings.simplefilter("ignore")
      
      !pip install pip --upgrade
      !pip install editdistance
      !pip install fastDamerauLevenshtein
      !pip install fuzzy
      !pip install genderize
      !pip install jellyfish
      !pip install Levenshtein
      !pip install nltk
      !pip install phonenumbers
      !pip install phonetics
      !pip install pyblake2
      !pip install rapidfuzz
      !pip install uszipcode
      !pip install xgboost

# installPyLibs()

##### Step 1: Define the snowflake and databricks scopes and connection parameters



incremental_update=True
using_deterministic_algo=False
tab_prefix='database_name.schema_name'

app_id='app-id'

sf_dict={"sfUrl":".../snowflakecomputing.com/","sfUser": dbutils.secrets.get(scope="secretscope-for_snowflake"+app_id, key="suer_name"),"pem_private_key": dbutils.secrets.get(scope="secretscope-for_databricks-"+app_id, key="key_name")}

db_dict={"sfDatabase": "database_name", "sfSchema":"schema_name", "sfWarehouse": "warehouse_name","sfRole":"role_user"}

########################. Define PII and other columns mapping in the code ########################
### This allows flexibility to define the pii column names as it be different across tables or in future version of PII tables

cols={'card_nbr_col':'card_nbr','household_col':'household_id','phone_col':'phone', 'first_nm_col':'first_nm', 'last_nm_col':'last_nm', 'address_line1_col':'address_line1', 'address_line2_col':'address_line2', 'email_address_col':'email_address'}

##### Step 2: Load absmlir library containg ID graph codes

from absmlir import mlir

snow=mlir.snowflakeConnector(app_id=app_id)
# snow=snowflakeConnector(app_id=app_id)

#################### Setting the snowrflake connection parameters.  ####################

for k,v in sf_dict.items():
  snow.set_sf(k,v)
  
for k,v in db_dict.items():
  snow.set_sf(k,v)

##### Step 3. Incremental Table Updates and creating incremental cards pii table
This step will extract the cards numbers as appear new (and old ones using blocking keys) in the PII table. This steps creates tables in snowflake to be used later during various data analysis steps.

# ### call incremental table updates class

tableUpdate=mlir.incrementalTableUpdates(app_id, sf_dict, db_dict)

################################## create updated PII current and prev tables  #################################
########  This creats extract of original PII tables from Fraud schema, each card having one record pair #######

### table created in snowflake: 
### 1. current   pii table: 'database_name.schema_name.table_name_smv_hhid_current'
### 2. previous pii table : 'database_name.schema_name.table_name_smv_hhid_previous'

tab_tc_prefix=tab_prefix+'table_name_smv_hhid_'
tab_cur=tab_prefix+'cur_table_name'
tab_prev=tab_prefix+'prev_table_name'

# tableUpdate.get_updated_tables(tab_tc_prefix=tab_tc_prefix, tab_cur=tab_cur, tab_prev=tab_prev)

################################## create incremental cards table ##############################################
####################  This contains new cards, and the connected ones using phone, households, email ###########

### table created in snowflake: 
### 1. incremental cards: 'database_name.schema_name.table_name_incremental_cards'
### 2. incremental combined cards : 'database_name.schema_name.table_name_incremental_cards_combined'

tab_tc=tab_prefix+'table_name_incremental_cards_combined'
tab_updated_cur=tab_prefix+'table_name_smv_hhid_current'
tab_updated_prev=tab_prefix+'table_name_smv_hhid_previous'
tab_full_shopper_id=tab_prefix+'table_name_all_shopper_ids'

# tableUpdate.create_incremental_cards_table(tab_prefix, tab_tc=tab_tc, tab_updated_cur=tab_updated_cur, tab_updated_prev=tab_updated_prev, tab_full_shopper_id=tab_full_shopper_id, incremental_update=incremental_update)

################################## Create table with txn PIIs   ###############################################

## table created in snowflake: 'database_name.schema_name.incremental_cards_pos_table'

tab_tc=tab_prefix+'table_name_incremental_cards_pos_data'
tab_cur_store_details=tab_prefix+'table_name_cards_store_details'
tab_txn_facts='database_name.schema_name.txn_facts'

# tableUpdate.get_incremental_cards_pos_data(tab_tc=tab_tc, tab_txn_facts=tab_txn_facts,tab_cur_store_details=tab_cur_store_details)

###################################### Create table with txn PIIs   ##################################################

## table created in snowflake: 
### 1. incremental cards.         : 'table_name for incremental cards'
### 2. incremental combined cards : 'database_name.schema_name.table_name_stores_zip'
### 3. incremental combined cards : 'database_name.schema_name.table_name_incremental_cards_zip_details'
### 4. incremental combined cards : 'database_name.schema_name.table_name_incremenetal_cards_store_details'
### 4. Updated combined cards     : 'database_name.schema_name.table_name_cards_store_details'

tab_tc = tab_prefix + "table_name_incremenetal_cards_store_details"
tab_pos = tab_prefix + "table_name_incremental_cards_pos_data"
tab_cur_store_details = tab_prefix+'table_name_cards_store_details'
tab_store = 'database_name.schema_name.store_details_data'

tableUpdate.update_incremental_cards_txn_pii(tab_prefix=tab_prefix, tab_tc=tab_tc, tab_cur_store_details=tab_cur_store_details, tab_pos=tab_pos, tab_store=tab_store)

############################## Create table with incremental cards pii   ##################################

## table created in snowflake: 'database_name.schema_name.table_name_incremental_cards_info'

tab_tc=tab_prefix+'table_name_incremental_cards_info'
tab_incr_comb_cards=tab_prefix+'table_name_incremental_cards_combined'
tab_cur_store_details=tab_prefix+'table_name_cards_store_details'
tab_updated_cur=tab_prefix+'table_name_smv_hhid_current'

tableUpdate.update_incremental_info(tab_tc=tab_tc, tab_incr_comb_cards=tab_incr_comb_cards, tab_updated_cur=tab_updated_cur, tab_cur_store_details=tab_cur_store_details)

############################## Create table with mob dev id   ##################################

### table created in snowflake: 
### 1. incremental mob dev cards: 'database_name.schema_name.table_name_incremental_mob_dev_data'
### 2. all mov dev id data: 'database_name.schema_name.table_name_mobile_dev_data' (will be updated)

tab_tc=tab_prefix+'table_name_incremental_mob_dev_data'
tab_incr_comb_cards=tab_prefix+'table_name_incremental_cards_combined'
tab_cur_store_details=tab_prefix+'table_name_cards_store_details'
tab_mob_dev=tab_prefix+'table_name_mobile_dev_data'
tab_updated_prev=tab_prefix+'table_name_smv_hhid_previous'
tab_click_hit_data='database_name.schema_name.click_hit_data'

# # tableUpdate.update_mob_dev_table(tab_tc=tab_tc, tab_incr_comb_cards=tab_incr_comb_cards, tab_mob_dev=tab_mob_dev, tab_click_hit_data=tab_click_hit_data, tab_cur_store_details=tab_cur_store_details, tab_updated_prev=tab_updated_prev)

#### Step 4. Data cleaning
###### This step requires loading the data from snowflake, and run the methods for various data cleaups

### load data cleaning module 

dataCleaning=mlir.piiDataCleaning(cols=cols)
# dataCleaning=piiDataCleaning(cols=cols)

## Load snowflake tables used in data cleaning process

tab_pii=tab_prefix+'table_name_incremental_cards_info'
tab_add_suff=tab_prefix+'table_name_add_suff'
tab_profane=tab_prefix+'table_name_profane_words'

## load the pii table from snowflake

df_pii=snow.read_snowflake(f"{tab_pii}")

## load the address suffix data: map of street short names to full names such as 'st' >> 'street' etc

add_suff=snow.read_snowflake(f"{tab_add_suff}")

### Load the table containining profane worlds. This is used to clean up email address

df_profane_words=snow.read_snowflake(f"{tab_profane}")

### lowercase dataframe

df_pii=dataCleaning.get_lowercase_columns_df(df_pii)

### phone cleaup

df_phone=dataCleaning.get_phone_number_validity(df_pii)

### name cleanp

df_name=dataCleaning.get_name_cleanedup(df_phone)

# ### email cleanup

df_email=dataCleaning.get_cleanedup_email(df_name,df_profane_words)

# ### address cleanup

df_address=dataCleaning.get_cleanedup_address(df_email,add_suff)

## Write to snowflake

snow.write_snowflake(df_address,tab_prefix+'table_name_incremental_df_address')

##### Step 5. Gender Prediction

### Load gender prediction class

genderPredict=mlir.genderPredictions()
# genderPredict=genderPredictions()

### load the names gender mapping data from snowflake, it contains US/Canada (CA) and 
# ROW(rest of world) names gender mapings

tab_address=tab_prefix+'table_name_incremental_df_address'
tab_names_us_ca_df=tab_prefix+'cust_names_us_ca_df'
tab_names_row_df=tab_prefix+'cust_names_row_df'

df_address=snow.read_snowflake(f'{tab_address}')
df_names_us_ca=snow.read_snowflake(f"{tab_names_us_ca_df}")
df_names_row=snow.read_snowflake(f"{tab_names_row_df}")

#### two steps:
#### 1. Using the name, gender mapping for US/CA and Rest of world (ROW), map the names to gender. 
#### 2. For remaning valid names, use Naive base model to predict the gender

df_gender_predicted=genderPredict.get_gender_prediction(df_address, df_names_us_ca=df_names_us_ca, df_names_row=df_names_row)

#### save the cleaned data (phone, name, email, address, gender) as snowflake table

snow.write_snowflake(df_gender_predicted,tab_prefix+'table_name_incremental_gender_predicted')

##### Step 6. Generate Record pairs

### Specify the pii columns for this stage

cols_updated={'card_nbr_col':'card_nbr', 'household_col':'household_id', 'phone_col':'phone', 'first_nm_col':'first_nm', 'last_nm_col':'last_nm', 'address_col':'address',
'email_address_col':'email_address', 'gender_col':'gender','email_user_col':'email_user',
'email_user_col':'email_user',  'first_txn_dte_col':'first_txn_dte',  'first_upc_col':'first_upc', 'first_used_store_id_col':'first_used_store_id',  'first_used_zip_code_col':'first_used_zip_code'}

### Load generating record pairs class 

generateRecordPairs=mlir.generateRecordPairs(app_id, sf_dict, db_dict, cols_updated)
# generateRecordPairs=generateRecordPairs(app_id, sf_dict, db_dict, cols_updated)

### Snowflake table names

tab_prefix='database_name.schema_name.'
tab_tc_prefix=tab_prefix+'table_name_incremental_record_pairs_'
tab_pii=tab_prefix+'table_name_incremental_gender_predicted'

### Define blocking keys (one can include email as well)

blocking_cols=['phone', 'email_address', 'household_id']

## cards appearning in different households but not having same phone, and email, will be mapped to different shopper id


### Call the methods to generte record pairs for each blocking keys. It will generate and save the tables in snowflake.

for key_col in blocking_cols:
  tab_tc=tab_tc_prefix+key_col
  generateRecordPairs.get_record_pairs_pii(tab_tc=tab_tc, tab_pii=tab_pii, key_col=key_col)

### concatenate snowflake table names

tab_blocking_keys=[]

for key_col in blocking_cols:
  tmp=tab_tc_prefix+key_col
  tab_blocking_keys.append(tmp)

### Next, Truncate, and then insert into the snowflake table to get union of record pairs for each blocking key. This table will contain union of record pair piis

tab_tc=tab_prefix+'table_name_incremental_record_pairs'

generateRecordPairs.get_union_record_pairs(tab_tc=tab_tc, tab_sf_blocking_keys=tab_blocking_keys)

##### Step 7.  Prediction using hybrid model

### Load deterministic and probabilistic algorithm class 

detModel=mlir.deterministicAlgorithm()
probModel=mlir.probabilisticAlgorithm()

# detModel=deterministicAlgorithm()
# probModel=probabilisticAlgorithm()

###############################################################################################
####################### Predictions Using Deterministic Model    ##############################
###############################################################################################

## load device data and pii data from snowflake. Mandatory columns are card_nbr, and mobile dev id, and either or both of names and email columns

tab_prefix='database_name.schema_name.'
tab_mov_dev=tab_prefix+'table_name_mobile_dev_data'
tab_gender=tab_prefix+'table_name_incremental_gender_predicted'

## read pii table

df_pii=snow.read_snowflake(tab_gender).dropDuplicates()

## if using deterministic algo, run the following which happens by default as using_deterministic_algo should be set to True.
if using_deterministic_algo:
  
  df_dev=snow.read_snowflake(tab_mov_dev)
  ## since subset on card nbr and mob dev, it can result in many duplicates since ignored user agent and other columns
  df_dev=df_dev.select('card_nbr','mobile_device_id').dropDuplicates() 
  
  ### Join the PII columns to dev id data

  cols_dev_pii={'card_nbr_col':'card_nbr','mobile_device_id_col':'mobile_device_id'}

  if cols['first_nm_col'] in df_pii.columns:
    cols_dev_pii.update({'first_nm_col':'first_nm'})
  if cols['last_nm_col'] in df_pii.columns:
    cols_dev_pii.update({'last_nm_col':'last_nm'})
  if cols['email_address_col'] in df_pii.columns:
    cols_dev_pii.update({'email_address_col':'email_address'})

  if not 'card_nbr' in df_dev.columns:
    df_dev=df_dev.withColumn('card_nbr',F.col(cols_dev_pii['card_nbr_col']))


  df_dev_pii=df_dev.join(df_pii,on=[cols['card_nbr_col']],how='left')
  df_dev_pii=df_dev_pii.select(*cols_dev_pii.values())
  df_dev_pii=df_dev_pii.filter( (df_dev_pii.first_nm.isNotNull() & df_dev_pii.first_nm.isNotNull() ) | (df_dev_pii.first_nm.isNotNull()))

  ### Predict using deterministic model algorithm for mob dev id

  cols_dev_pii={'card_nbr_col':'card_nbr','mobile_device_id_col':'mobile_device_id', 'first_nm_col':'first_nm', 'last_nm_col':'last_nm', 'email_address_col':'email_address'}

  df_dev_rp=detModel.get_dev_id_record_pairs_pii(df_dev_pii)
  df_dev_rp_f=detModel.get_features(df_dev_rp)
  df_dev_predictions=detModel.deterministic_algo(df_dev_rp_f)

###############################################################################################
#########################     Predictions Using probabilistic Model ###########################
###############################################################################################

tab_prob_rp=tab_prefix+'table_name_incremental_record_pairs'

df_rp=snow.read_snowflake(tab_prob_rp)

#### Load the Xgboost model

from pyspark.ml import PipelineModel
rfPath = "model_path"
classifier = PipelineModel.load(rfPath)

## remove the deterministic predicted card pairs which were predicted to be match

if using_deterministic_algo:
  df_dev_predictions_matched=df_dev_predictions.filter(df_dev_predictions.prediction==1).dropDuplicates().select('c1c2','card_nbr_1','card_nbr_2')
  df_dev_predictions_matched.createOrReplaceTempView('df_dev_predictions_matched_view')
  df_rp.createOrReplaceTempView('df_rp_view')

if using_deterministic_algo:
  df_rp=spark.sql("""select A.* from df_rp_view A where not exists (select distinct c1c2 from df_dev_predictions_matched_view B where a.c1c2=b.c1c2)""")
else:
  pass 

### predict using probabilistic model (xgboost pyspark model)

feature_cols={'card_nbr_col':'card_nbr','phone_col':'phone','first_nm_col':'first_nm','last_nm_col':'last_nm','email_address_col':'email_address', 'email_user_col':'email_user','address_col':'address','gender_col':'gender'}

x_cols=['same_fn', 'same_ln', 'same_email_address', 'same_address', 'same_gender', 'approx_fn', 'approx_ln', 'approx_email_address', 'same_first_nm_sdx', 'same_last_nm_sdx','same_first_nm_mp', 'same_last_nm_mp', 'first_nm_in_email', 'lev_ratio_fn',  'lev_ratio_ln',  'jw_simi_fn',  'jw_simi_ln', 'jw_simi_address','damerau_lev_simi_fn','damerau_lev_simi_ln']

df_rp_f=probModel.get_model_features(df_rp, featureCols=feature_cols)

### Call this method if one needs to train classifier. For this, one must have labeled data

### classifier=probModel.train_xgboost(trainDF,x_cols=x_cols,targetCol='target')

### Making predictions using earlier data

df_rp_pred=probModel.get_predictions(classifier, df_rp_f, threshold=0.75)

### keep the relevant columns

p_cols=['c1c2', 'phone_1','phone_2', 'card_nbr_1', 'card_nbr_2', 'first_nm_1','first_nm_2','last_nm_1','last_nm_2','prob_1','prediction']+['same_fn', 'same_ln', 'same_email_address', 'same_address', 'same_gender', 'approx_fn', 'approx_ln', 'approx_email_address', 'same_first_nm_sdx', 'same_last_nm_sdx','same_first_nm_mp', 'same_last_nm_mp', 'first_nm_in_email', 'lev_ratio_fn',  'lev_ratio_ln',  'jw_simi_fn',  'jw_simi_ln', 'jw_simi_address','damerau_lev_simi_fn','damerau_lev_simi_ln']

df_rp_pred=df_rp_pred.select(*p_cols).dropDuplicates()

#### save the predictions as snowflake table

if using_deterministic_algo:
  print('writing deterministic predictions to snowflake')
  snow.write_snowflake(df_dev_predictions,"table_name_incremental_deterministic_predictions")

print('writing probabilistic predictions to snowflake')
snow.write_snowflake(df_rp_pred,"table_name_incremental_probabilistic_predictions")

##### Step 8. Connected components

### Load connected componnets algorithm

connComp=mlir.connectedComponents()
# connComp=connectedComponents()

#### save to snowflake
tab_prefix='database_name.schema_name.'

det_preds=tab_prefix+'table_name_incremental_deterministic_predictions'
prob_preds=tab_prefix+'table_name_incremental_probabilistic_predictions'

## changes for deterministic logic

if using_deterministic_algo:
  df_det_predictions=snow.read_snowflake(det_preds)

df_prob_predictions=snow.read_snowflake(prob_preds)

### create src and dst columns as needed for connected component algorithm 

## changes for deterministic logic

if using_deterministic_algo:
  if not 'card_nbr_1' in df_det_predictions.columns:
    raise Exception('card_nbr_1 and card_nbr_2 must be present in dataframe!!!')
  if 'prediction' in df_det_predictions.columns:
    df_det_predictions=df_det_predictions.filter(df_det_predictions.prediction==1).select('card_nbr_1','card_nbr_2')
  else:
    if 'card_nbr_1' in df_det_predictions.columns:
      df_det_predictions=df_det_predictions.select('card_nbr_1','card_nbr_2')
    df_det_predictions=df_det_predictions.withColumnRenamed('card_nbr_1','src').withColumnRenamed('card_nbr_2','dst')

if not 'card_nbr_1' in df_prob_predictions.columns:
  raise Exception('card_nbr_1 and card_nbr_2 must be present in dataframe!!!')
if 'prediction' in df_prob_predictions.columns:
  df_prob_predictions=df_prob_predictions.filter(df_prob_predictions.prediction==1).select('card_nbr_1','card_nbr_2')
else:
  if 'card_nbr_1' in df_prob_predictions.columns:
    df_prob_predictions=df_prob_predictions.select('card_nbr_1','card_nbr_2')
df_prob_predictions=df_prob_predictions.withColumnRenamed('card_nbr_1','src').withColumnRenamed('card_nbr_2','dst')

## changes for deterministic logic

if using_deterministic_algo:
  dataframes=[df_det_predictions,df_prob_predictions]
else:
  dataframes=[df_prob_predictions]

### run the connected components algorithm

df_conn=connComp.get_connected_components(dataframes=dataframes)

### save to snowflake
snow.write_snowflake(df_conn,"table_name_incremental_connected_components")

##### Step 9. Shopper ID generation

### load generate shopper iD class

# Shopper=mlir.generateShopperID(app_id, sf_dict, db_dict, tab_prefix)
Shopper=generateShopperID(app_id, sf_dict, db_dict, tab_prefix)

#### snowflake df_conn and cleaned pii table

tab_prefix='database_name.schema_name.'

tab_sf_df_conn=tab_prefix+'table_name_incremental_connected_components'
tab_sf_df_pii=tab_prefix+'table_name_incremental_gender_predicted'

cols_shopper_id=['shopper_id', 'card_nbr', 'phone', 'household_id', 'shopper_first_card_nbr', 'first_nm', 'last_nm', 'address', 'email_address', 'gender',  'email_user', 'first_used_store_id', 'first_used_zip_code', 'first_txn_dte', 'first_upc','first_nm_orig', 'last_nm_orig', 'address_old', 'address_line1', 'address_line2',  'email_address_old', 'len_add', 'len_add_comp', 'dw_last_update_ts']

# df_shopper_ids=Shopper.get_shopper_ids(tab_sf_df_pii=tab_sf_df_pii,tab_sf_df_conn=tab_sf_df_conn, cols_shopper_id=[])

### Write the incremental shopper id table to snowflake
 
df_shopper_ids=df_shopper_ids.select(*cols_shopper_id)
snow.write_snowflake(df_shopper_ids,tab_prefix+"table_name_incremental_shopper_ids")

### Update the full shopper id table

tab_sf_incr_shopper_table=tab_prefix+'table_name_incremental_shopper_ids'
tab_sf_full_shopper_table=tab_prefix+'table_shopper_ids'

Shopper.update_shopper_id_full_table(tab_sf_incr_shopper_table, tab_sf_full_shopper_table, cols_shopper_id=cols_shopper_id)

##### Step 10. HHID generation

### Define the pii columns 

cols_updated={'card_nbr_col':'card_nbr', 'household_col':'household_id', 'phone_col':'phone', 'first_nm_col':'first_nm', 'last_nm_col':'last_nm', 'address_col':'address',
'email_address_col':'email_address', 'gender_col':'gender','email_user_col':'email_user',
'email_user_col':'email_user',  'first_txn_dte_col':'first_txn_dte',  'first_upc_col':'first_upc', 'first_used_store_id_col':'first_used_store_id',  'first_used_zip_code_col':'first_used_zip_code',
'len_add_col':'len_add', 'len_add_comp_col':'len_add_comp'}

### 10.1 -- Load generating record pairs class 

# HHID=mlir.generateHHIDs(app_id, sf_dict, db_dict, cols_updated)

HHID=generateHHIDs(app_id, sf_dict, db_dict, cols_updated)


### Snowflake table names

tab_prefix='database_name.schema_name.'
tab_tc_prefix=tab_prefix+'table_name_incremental_hhid_record_pairs_'
tab_shopper_id=tab_prefix+'table_name_incremental_shopper_ids'

### Define blocking keys 
### Note that: multiple existing household can map to same shopper ids, similar to that multiple phones, emails, address can also map to same shopper id, 
# example shopper id=248ae0eb54e7aca82926ace25a3094f6ab7c22a8a73a1b23d3d07bbb24c2e2c268462cd948f4fd4e

blocking_cols=['shopper_id', 'phone', 'email_address', 'household_id', 'multi_key']

### Call the methods to generte record pairs for each blocking keys. It will generate and save the tables in snowflake.

for key_col in blocking_cols:
  tab_tc=tab_tc_prefix+key_col
  HHID.get_record_pairs(tab_tc=tab_tc, tab_shopper_id=tab_shopper_id, key_col=key_col)
  
### concatenate snowflake table names

tab_blocking_keys=[]

for key_col in blocking_cols:
  tmp=tab_tc_prefix+key_col
  tab_blocking_keys.append(tmp)

### Next, Truncate, and then insert into the snowflake table to get union of record pairs for each blocking key. This table will contain union of record pair piis

tab_tc=tab_prefix+'table_name_incremental_hhid_record_pairs'

HHID.get_union_record_pairs(tab_tc=tab_tc, tab_sf_blocking_keys=tab_blocking_keys)

#### Run hhid connected components

dataframes_data=[]
dataframes=[]

for t in tab_blocking_keys:
  tmp=snow.read_snowflake(t)
  dataframes_data.append(tmp)

for d in dataframes_data:
  if not 'card_nbr_1' in d.columns:
    raise Exception('card_nbr_1 and card_nbr_2 must be present in dataframe!!!')
  else:
    if 'card_nbr_1' in d.columns:
      d=d.select('card_nbr_1','card_nbr_2')
  d=d.withColumnRenamed('card_nbr_1','src').withColumnRenamed('card_nbr_2','dst')
  dataframes.append(d)

### 10.2 -- Run the connected components algorithm

df_hhid_conn=HHID.get_hhid_connected_components(dataframes=dataframes)

### drop the first row as if df_edges first row was blank inside connected compo algorithm

df_hhid_conn=df_hhid_conn.filter(df_hhid_conn.id!='')

tab_sf_hhid_conn_comp=tab_prefix+'table_name_incremental_hhid_components'

snow.write_snowflake(df_hhid_conn,tab_sf_hhid_conn_comp)

##### 10.3 -- generate the incremental hhid tables

cols_hhid={'hhid_col': 'hhid',  'shopper_id_col': 'shopper_id',  'card_nbr_col': 'card_nbr',  'phone_col': 'phone',  'household_col': 'household_id',  'first_nm_col': 'first_nm',  'last_nm_col': 'last_nm',  'address_col': 'address',  'email_address_col': 'email_address',  'gender_col': 'gender',  'email_user_col': 'email_user',  'shopper_first_card_nbr_col': 'shopper_first_card_nbr',  'hhid_first_card_nbr_col': 'hhid_first_card_nbr',  'first_used_store_id_col': 'first_used_store_id',  'first_used_zip_code_col': 'first_used_zip_code',  'first_txn_dte_col': 'first_txn_dte',  'first_upc_col': 'first_upc',  'first_nm_orig_col': 'first_nm_orig',  'last_nm_orig_col': 'last_nm_orig',  'address_line1_col': 'address_line1',  'address_line2_col': 'address_line2',  'address_old_col': 'address_old',  'email_address_old_col': 'email_address_old', 'len_add_col':'len_add', 'len_add_comp_col':'len_add_comp',  'dw_last_update_ts_col': 'dw_last_update_ts'}

### Snowflake table names

tab_prefix='database_name.schema_name.'
tab_sf_shopper_id=tab_prefix+'table_name_incremental_shopper_ids'
tab_sf_hhid_conn_comp=tab_prefix+'table_name_incremental_hhid_components'

df_hhid=HHID.get_hhids(tab_sf_shopper_id, tab_sf_hhid_conn_comp)
snow.write_snowflake(df_hhid,tab_prefix+'table_name_incremental_hhids')

### 10.4 -- Update the full hhid table

cols_hhid_final={'hhid_col': 'hhid',  'shopper_id_col': 'shopper_id',  'card_nbr_col': 'card_nbr',  'household_col': 'household_id', 
'shopper_first_card_nbr_col': 'shopper_first_card_nbr',  'hhid_first_card_nbr_col': 'hhid_first_card_nbr', 
'first_used_store_id_col': 'first_used_store_id',  'first_used_zip_code_col': 'first_used_zip_code',   
'first_txn_dte_col': 'first_txn_dte',  'first_upc_col': 'first_upc', 'dw_last_update_ts_col': 'dw_last_update_ts'}


tab_prefix='database_name.schema_name.'
tab_sf_incr_hhid_table=tab_prefix+'table_name_incremental_hhids'
tab_sf_full_hhid_table=tab_prefix+'table_name_all_hhids'

HHID.update_hhid_full_table(tab_sf_incr_hhid_table, tab_sf_full_hhid_table, cols_hhid_final)

###### Post Analysis

df_all_shopper_ids=snow.read_snowflake('select * from table_name_all_shopper_ids')
df_all_shopper_ids.count()

df_all_hhids=snow.read_snowflake('select * from table_name_all_hhids')
df_all_hhids.count()

import pyspark.sql.functions as F

df_all_shopper_ids.select(F.countDistinct('shopper_id')).show()

import pyspark.sql.functions as F

df_all_hhids.select(F.countDistinct('shopper_id')).show()

import pyspark.sql.functions as F

df_all_hhids.select(F.countDistinct('hhid')).show()

import pyspark.sql.functions as F

df_all_hhids.select(F.countDistinct('household_id')).show()

display(df_all_shopper_ids)


display(df_all_hhids)


