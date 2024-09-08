########################################################################################################
#################### Package code for ID graph for generating shopper id and hhid ######################
########################################################################################################

"""Copyright © 2023 Ashwini Maurya. All rights reserved."""

########################################################################################################
#################### 1. Module for connecting to snowflake from databricks #############################
########################################################################################################

class snowflakeConnector:

  """
      This class has methods to connect to snowflake databases. The inputs is app id.
      The various methods can be called to set connection parameters
      It can be used to read, write from/to snowflake. Exceute stored procedure, and snowflake DDL statements
      Check table existence checks etc. see below
  """

  def __init__(self, app_id='', sc=''):

    if app_id=='':
      app_id='app_id'

    self.__sf_conf = {
      "sfUrl": '',
      "sfUser": '',
      "pem_private_key": '',
      "sfdatabase": '',
      "sfSchema": '',
      "sfWarehouse": '',
      "sfRole": ''
    }

    self.__sf_setters = ["sfUrl", "sfUser", "pem_private_key", "sfDatabase", "sfSchema", "sfWarehouse", "sfRole"]

    import pyspark
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import udf

    self.spark = SparkSession.builder.master("local[1]").appName("MLIR").getOrCreate()

    if sc=='':
      try:
        from databricks.sdk.runtime import sc
        self.sc=sc
      except Exception:
        raise Exception(f' {sc} is not defined')
    else:
      self.sc=sc

  def __str__(self):
    return str(self.__dict__)


  def config_sf(self, name):
    return self.__sf_conf[name]

  def set_sf(self, name, value):
    if name in self.__sf_setters:
      self.__sf_conf[name] = value
    else:
      raise NameError("Name not accepted in set_sf() method")

  def read_snowflake(self, query):
    if not 'select' in query.lower():
      df = self.spark.read.format("snowflake").options(**self.__sf_conf).option("dbtable", query).load()
    else:
      df = self.spark.read.format("snowflake").options(**self.__sf_conf).option("query", query).load()
    df = df.toDF(*[c.lower() for c in df.columns])
    return df

  def write_snowflake(self, df_to_write, snowflake_table_name, mode_type="overwrite"):
    df_to_write.write.format("snowflake").options(**self.__sf_conf).mode(mode_type).option("dbtable", snowflake_table_name).save()

  ### Run snowflake procedure within snowflake
  def call_snowflake_procedure(self, proc_name):
    #from databricks.sdk.runtime import sc
    return self.sc._gateway.jvm.net.snowflake.spark.snowflake.Utils.runQuery(options,proc_name)

  # use to alter the state of a table by the query, not returning a df

  def alter_snowflake_table(self, query):
    #from databricks.sdk.runtime import sc
    sfUtils = self.sc._jvm.net.snowflake.spark.snowflake.Utils
    sfUtils.runQuery(self.__sf_conf, query)

  ## run snowflake query

  def run_snowflake_query(self, query, table_name, is_create=True):
    ### This methods creates/update the tables in snowflake and does not return any results
    if query.split()[0].lower()=='drop':
      create='droppe'
    elif query.split()[0].lower() in ['update','insert']:
      create='update'
    elif query.split()[0].lower()=='delete':
        create='delete update'
    else:
      create = 'create'
    try:
      self.alter_snowflake_table(query)
    except Exception as e:
      raise ValueError(f'Failed to {create} table {table_name}, check error: {e}')
    else:
      print(f'Table {table_name} {create}d successfully')

  def run_snowflake_query(self, query, table_name, is_create=True):
    ### This methods creates/update the tables in snowflake and does not return any results
    if query.split()[0].lower()=='drop':
      create='droppe'
    elif query.split()[0].lower().strip()=='update':
      create='update'
    elif query.split()[0].lower().strip()=='insert':
      create='inserte'
    elif query.split()[0].lower()=='delete':
        create='delete update'
    elif query.split()[0].lower()=='truncate':
      create='truncate'
    else:
      create = 'create'
    try:
      self.alter_snowflake_table(query)
    except Exception as e:
      raise ValueError(f'Failed to {create} table {table_name}, check error: {e}')
    else:
      print(f'Table {table_name} {create}d successfully')

  def table_existence_check(self, table_name):

    query=f"""select count(*) as cnt from {table_name}"""

    record_existence = 0

    try:
      record_existence =  self.read_snowflake(query).collect()[0][0]
      if record_existence > 0:
        return """Pre-requsite met for {}""".format(table_name)
      else:
        raise Exception("""Pre-requsite not met for {}. Record not found.""".format(table_name))
    except Exception as e:
      raise Exception('Pre-requsite not met for {}.'.format(table_name))

  ######### Function to check if table has most recent updated data in snowfake EDW ######

  def table_update_check(self, table_name, table_update_ts_col=''):

    """
    Method to check table update from snwoflake
    """

    if table_update_ts_col=='':
      tab=self.read_snowflake(table_name)
      try:
        table_update_ts_col=[x for x in tab.columns if 'last_update_ts' in x][0]
      except Exception:
        raise Exception(f'table_update_ts_col must be defined or not present in the table!!!')

    query=f"""select case when date(max({table_update_ts_col}::timestamp_ntz))>=current_date then 1 else 0 end as is_latest_txn_date from {table_name}"""

    try:
      is_latest_run =   int(self.read_snowflake(query).collect()[0][0])
      if is_latest_run > 0:
        return f"""Pre-requsite met for {table}"""
      else:
        raise Exception(f""" {table} may not be upto date""")
    except Exception as e:
      raise Exception(f'{table} may not be upto date.')


#########################################################################################################
###################  Similarity and Distance Functions   ################################################
#########################################################################################################

### Utility functions: extract clean numbers/strings

class similarityFunctions:

  """
  Implementation of various similarity and distance functions such as levenshtein distance, levenshtein ratio, damerauLevenshtein simi, jaro
  winkler simi, and generalized version of these which is weighted measure by taking subsets of strings as components and thier simi as measure.
  Also implemented hamming distance, and various string cleaning methods such as get cleaned version of alpha, alphnumeric strings
  """

  import Levenshtein
  import re
  import hashlib

  def __init__(self):
    pass

  def get_cleaned_alpha(self,s):
    ### remove special chaarcters, numbers, punctuation etc
    return re.sub('[^A-Za-z]+', '', s) ## just keep the letters


  def get_cleaned_numeric(self,s):
    ### just keep the numbers
    return re.sub('[^0-9]+', '', s)


  def get_cleaned_int(self,s):
    ### find valid int, assume at most one decimal
    if isinstance(s,int):
        return s
    elif '.' in s:
        return int(s.split(".")[0])
    else:
        return int(re.sub('[^0-9]+', '', s))


  def get_cleaned_alpha_numeric(self,s):
    ### just alpha and numeric
    return re.sub('[^A-Za-z0-9]+', '', s)

  ### Hunction to get numerical hash function  (15 digit)


  def get_hash_15(self,s):

    return int(hashlib.sha1(s.encode("utf-8")).hexdigest(), 16) % (10 ** 15)

  #### Similarity functions¶


  def get_out_of_order_alpha_score(self,fn1,fn2):
    fn1=self.get_cleaned_alpha(fn1)
    fn2=self.get_cleaned_alpha(fn2)
    fn1s = re.split("\W+|_", fn2)
    fn2s = re.split("\W+|_", fn2)
    score = len(Counter(fn2s) & Counter(fn1s))/float(max(len(fn1s), len(fn2s)))
    return score


  def get_out_of_order_alpha_numeric_score(self,fn1,fn2):
    import re
    fn1=self.get_cleaned_alpha_numeric(fn1)
    fn2=self.get_cleaned_alpha_numeric(fn2)
    fn1s = re.split("\W+|_", fn2)
    fn2s = re.split("\W+|_", fn2)
    score = len(Counter(fn2s) & Counter(fn1s))/float(max(len(fn1s), len(fn2s)))
    return score


  def get_alpha_hamming_distance(self,s1,s2):
    s1=get_cleaned_alpha(s1);
    s2=get_cleaned_alpha(s2);
    if len(s1)==len(s2):
        return Levenshtein.hamming(s1,s2)
    if len(s1)>len(s2):
        t1=" "*(len(s1)-len(s2))+s2
        d1=Levenshtein.hamming(s1,t1)
        return d1
    else:
        t2=" "*(len(s2)-len(s1))+s1
        d2=Levenshtein.hamming(s2,t2)
    return d2


  def get_alpha_numeric_hamming_distance(self,s1,s2):
      s1=get_cleaned_alpha_numeric(s1);
      s2=get_cleaned_alpha_numeric(s2);
      if len(s1)==len(s2):
          return Levenshtein.hamming(s1,s2)
      if len(s1)>len(s2):
          t1=" "*(len(s1)-len(s2))+s2
          d1=Levenshtein.hamming(s1,t1)
          return d1
      else:
          t2=" "*(len(s2)-len(s1))+s1
          d2=Levenshtein.hamming(s2,t2)
      return d2


  def get_normalized_alpha_hamming_distance(self,s1,s2):
      fn1=self.get_cleaned_alpha(s1);
      fn2=self.get_cleaned_alpha(s2)
      hd=self.get_alpha_hamming_distance(fn1,fn2)
      if len(fn1)==0 and len(fn2)==0:
          return 0
      if len(s1)>len(s2):
          return hd/len(s1)
      else:
          return hd/len(s2)
      return d2


  def get_normalized_alpha_numeric_hamming_distance(self,s1,s2):
      fn1=self.get_cleaned_alpha_numeric(s1);
      fn2=self.get_cleaned_alpha_numeric(s2)
      hd=self.get_alpha_numeric_hamming_distance(fn1,fn2)
      if len(fn1)==0 and len(fn2)==0:
          return 0
      if len(s1)>len(s2):
          return hd/len(s1)
      else:
          return hd/len(s2)
      return d2


  def get_alpha_levenshtein_distance(self,s1,s2):
      fn1=self.get_cleaned_alpha(s1);
      fn2=self.get_cleaned_alpha(s2)
      return Levenshtein.distance(fn1,fn2)


  def get_alpha_numeric_levenshtein_distance(self,s1,s2):
      fn1=self.get_cleaned_alpha_numeric(s1);
      fn2=self.get_cleaned_alpha_numeric(s2)
      return Levenshtein.distance(fn1,fn2)


  def get_alpha_levenshtein_ratio(self,s1,s2):
      fn1=self.get_cleaned_alpha(s1);
      fn2=self.get_cleaned_alpha(s2)
      return Levenshtein.ratio(fn1,fn2)


  def get_alpha_numeric_levenshtein_ratio(self,s1,s2):
      fn1=self.get_cleaned_alpha_numeric(s1);
      fn2=self.get_cleaned_alpha_numeric(s2)
      return Levenshtein.ratio(fn1,fn2)


  def get_alpha_damerauLevenshtein_similarity(self,s1,s2):
      import fastDamerauLevenshtein
      fn1=self.get_cleaned_alpha(s1);
      fn2=self.get_cleaned_alpha(s2)
      return fastDamerauLevenshtein.damerauLevenshtein(self,fn1,fn2)


  def get_alpha_numeric_damerauLevenshtein_similarity(self,s1,s2):
      import fastDamerauLevenshtein
      fn1=self.get_cleaned_alpha_numeric(s1);
      fn2=self.get_cleaned_alpha_numeric(s2)
      return fastDamerauLevenshtein.damerauLevenshtein(fn1,fn2)


  def get_alpha_jaro_winkler_similarity(self,s1,s2):
      import Levenshtein
      fn1=self.get_cleaned_alpha(s1);
      fn2=self.get_cleaned_alpha(s2)
      return Levenshtein.jaro_winkler(fn1,fn2)


  def get_alpha_numeric_jaro_winkler_similarity(self,s1,s2):
      import Levenshtein
      fn1=self.get_cleaned_alpha_numeric(s1);
      fn2=self.get_cleaned_alpha_numeric(s2)
      return Levenshtein.jaro_winkler(fn1,fn2)


  def get_alpha_numeric_levenshtein_ratio(self,s1,s2):
    import Levenshtein
    fn1=self.get_cleaned_alpha_numeric(s1);
    fn2=self.get_cleaned_alpha_numeric(s2)
    return Levenshtein.ratio(fn1,fn2)


  def get_alpha_numeric_fuzz_Levenshtein_ratio(self,s1,s2):
    from rapidfuzz import fuzz
    fn1=self.get_cleaned_alpha_numeric(s1);
    fn2=self.get_cleaned_alpha_numeric(s2)
    return fuzz.ratio(fn1,fn2)/100


  def get_alpha_numeric_fuzz_jw_similarity(self,s1,s2):
    import rapidfuzz
    fn1=self.get_cleaned_alpha_numeric(s1);
    fn2=self.get_cleaned_alpha_numeric(s2)
    return rapidfuzz.distance.JaroWinkler.normalized_similarity(fn1,fn2)



  def get_generalized_jw_similarity(self,s1,s2):
    from itertools import zip_longest
    if s1=='' or s2=='':
      return 0
    s1=s1.lower()
    s2=s2.lower()
    t1=''
    t2=''
    jw_dist=0; cnt=0
    for a,b in zip_longest(s1,s2):
      if a:
        t1=t1+a
      if b:
        t2=t2+b
      cnt+=1
      jw_dist+=self.get_alpha_numeric_jaro_winkler_similarity(t1,t2)
    return jw_dist/cnt


  def get_generalized_levenshtein_ratio(self,s1,s2):
    from itertools import zip_longest
    if s1=='' or s2=='':
      return 0
    s1=s1.lower()
    s2=s2.lower()
    t1=''
    t2=''
    lv_ratio=0; cnt=0
    for a,b in zip_longest(s1,s2):
      if a:
        t1=t1+a
      if b:
        t2=t2+b
      cnt+=1
      lv_ratio+=self.get_alpha_numeric_levenshtein_ratio(t1,t2)
    return lv_ratio/cnt


  def get_generalized_damerauLevenshtein_similarity(self,s1,s2):
    from itertools import zip_longest
    if s1=='' or s2=='':
      return 0
    s1=s1.lower()
    s2=s2.lower()
    t1=''
    t2=''
    simi=0; cnt=0
    for a,b in zip_longest(s1,s2):
      if a:
        t1=t1+a
      if b:
        t2=t2+b
      cnt+=1
      simi+=self.get_alpha_numeric_damerauLevenshtein_similarity(t1,t2)
    return simi/cnt


  def get_generalized_fuzz_Levenshtein_ratio(self,s1,s2):
    from itertools import zip_longest
    if s1=='' or s2=='':
      return 0
    s1=s1.lower()
    s2=s2.lower()
    t1=''
    t2=''
    fuzz_ratio=0; cnt=0
    for a,b in zip_longest(s1,s2):
      if a:
        t1=t1+a
      if b:
        t2=t2+b
      cnt+=1
      fuzz_ratio+=self.get_alpha_numeric_fuzz_Levenshtein_ratio(t1,t2)
    return fuzz_ratio/cnt


  def get_generalized_fuzz_jw_similarity(self,s1,s2):
    from itertools import zip_longest
    if s1=='' or s2=='':
      return 0
    s1=s1.lower()
    s2=s2.lower()
    t1=''
    t2=''
    fuzz_ratio=0; cnt=0
    for a,b in zip_longest(s1,s2):
      if a:
        t1=t1+a
      if b:
        t2=t2+b
      cnt+=1
      fuzz_ratio+=self.get_alpha_numeric_fuzz_jw_similarity(t1,t2)
    return fuzz_ratio/cnt

################################################################################################################
###########################################   Data Cleaning Code   #############################################
################################################################################################################

class piiDataCleaning:

  """
  Data cleaning code: implements phone, name, email, address clean up codes
  """

  def __init__(self,pii_cols={}):

    if not pii_cols or pii_cols=={}:
      self.pii_cols={'card_nbr_col':'card_nbr','household_col':'household_id','phone_col':'phone',
                    'first_nm_col':'first_nm', 'last_nm_col':'last_nm', 'address_line1_col':'address_line1',
                    'address_line2_col':'address_line2', 'email_address_col':'email_address'}
    else:
      self.pii_cols=pii_cols

    self.card_nbr_col=self.pii_cols['card_nbr_col']
    self.household_col=self.pii_cols['household_col']
    self.phone_col=self.pii_cols['phone_col']
    self.first_nm_col=self.pii_cols['first_nm_col']
    self.last_nm_col=self.pii_cols['last_nm_col']
    self.address_line1_col=self.pii_cols['address_line1_col']
    self.address_line2_col=self.pii_cols['address_line2_col']
    self.email_address_col=self.pii_cols['email_address_col']

    import nltk

    nltk.download('wordnet', quiet=True)
    nltk.download('punkt', quiet=True)
    nltk.download('averaged_perceptron_tagger', quiet=True)

    ## define lemmatizer

    self.lemmatizer=nltk.stem.WordNetLemmatizer()


  #####################################################################################
  ################################## Phone Cleanup ####################################
  #####################################################################################

  def get_lowercase_columns_df(self,df):

    """

    df          : is spark dataframe.

    This method returns the transformed spark dataframe with column names lower cased, as well as column values lower cases. i.e. ABC --> abc

    """

    import pyspark.sql.functions as F
    from pyspark.sql.types import IntegerType, FloatType, BooleanType, StringType
    import pandas, string

    for c in self.pii_cols:
      if c in df.columns:
        df=df.withColumn(c,F.lower(F.col(c)))
    for c in df.columns:
      df=df.withColumnRenamed(c,c.lower())
    return df


  def get_phone_number_validity(self,tmp):

    """

    tmp          : is spark dataframe containing phone numbers values into self.phone_col column. See above constructor method.

    This method validates all phone numbers using python phonenumbers library.
    It checks if a number is valid US, Canada or Mexico phone number.
    Toll free numbers and not 100 digit numbers are considered invalid

    phonenumbers lib reference: https://pypi.org/project/phonenumbers/

    """

    import pyspark.sql.functions as F
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType, FloatType, BooleanType, StringType

    def is_valid_phone_number(x):

      if x=='' or x=='N/A' or (x is None) or len(x)<10:
        return False

      x="".join([t for t in x if t.isdigit()])

      def is_length_ten(x):
        return True if len(x)==10 else False

      def is_valid_phone_number(x,country_code='US'):
        import phonenumbers
        return phonenumbers.is_valid_number(phonenumbers.parse(x,country_code))

      def is_tollfree_phone_number(x):

        return True if x[:3] in ['800', '888', '877', '866', '855', '844'] else False

      is_length_ten=is_length_ten(x)
      is_valid_us=is_valid_phone_number(x,'US')
      is_valid_ca=is_valid_phone_number(x,'CA')
      is_valid_mx=is_valid_phone_number(x,'MX')
      is_toll_free=is_tollfree_phone_number(x)

      return  True if (is_length_ten) and (is_valid_us or is_valid_ca or is_valid_mx) and not is_toll_free else False

    isvalidPhoneNumberUDF=udf(lambda x: is_valid_phone_number(x), BooleanType())

    tmp=tmp.withColumn('is_valid_phone_number',isvalidPhoneNumberUDF(F.col(self.phone_col)))
    tmp=tmp.withColumn('phone',F.when(F.col('is_valid_phone_number')==True,F.col(self.phone_col)).otherwise(''))

    return tmp

  #####################################################################################
  ################################## Name Cleanup #####################################
  #####################################################################################

  def get_profane_words(self):

      profane_words_1=['4r5e', '5h1t', '5hit', 'len', 'a$$', 'a$$hole', 'a2m', 'a55', 'a55hole', 'a_s_s', 'adult', 'aeolus', 'ahole', 'amateur', 'anal', 'analprobe', 'anilingus', 'anus', 'ar5e', 'areola', 'areole', 'arrse', 'arse', 'arsehole', 'ass', 'ass-fucker', 'assbang', 'assbanged', 'assbangs', 'asses', 'assfuck', 'assfucker', 'assfukka', 'assh0le', 'asshat', 'assho1e', 'asshole', 'assholes', 'assmaster', 'assmucus ', 'assmunch', 'asswhole', 'asswipe', 'asswipes', 'autoerotic', 'azazel', 'azz', 'b!tch', 'b00bs', 'b17ch', 'b1tch', 'babe', 'babes', 'ballbag', 'ballsack', 'bang', 'bangbros', 'banger', 'bareback', 'barf', 'bastard', 'bastards', 'bawdy', 'beaner', 'beardedclam', 'beastial', 'beastiality', 'beatch', 'beater', 'beaver', 'beer', 'beeyotch', 'bellend', 'beotch', 'bestial', 'bestiality', 'bi+ch', 'biatch', 'bigtits', 'bimbo', 'bimbos', 'birdlock', 'bitch', 'bitched', 'bitcher', 'bitchers', 'bitches', 'bitchin', 'bitching', 'bitchy', 'bloody', 'blow', 'blowjob', 'blowjobs', 'blumpkin ', 'bod', 'bodily', 'boink', 'boiolas', 'bollock', 'bollocks', 'bollok', 'bone', 'boned', 'boner', 'boners', 'bong', 'boob', 'boobies', 'boobs', 'booby', 'booger', 'bookie', 'booobs', 'boooobs', 'booooobs', 'booooooobs', 'bootee', 'bootie', 'booty', 'booze', 'boozer', 'boozy', 'bosom', 'bosomy', 'bowel', 'bowels', 'bra', 'brassiere', 'breast', 'breasts', 'buceta', 'bugger', 'bukkake', 'bullshit', 'bullshits', 'bullshitted', 'bullturds', 'bum', 'bung', 'busty', 'butt', 'buttfuck', 'buttfucker', 'butthole', 'buttmuch', 'buttplug', 'c-0-c-k', 'c-o-c-k', 'c-u-n-t', 'c.0.c.k', 'c.o.c.k.', 'c.u.n.t', 'c0ck', 'c0cksucker', 'caca', 'cahone', 'cameltoe', 'carpetmuncher', 'cawk', 'cervix', 'chinc', 'chincs', 'chink', 'choade ', 'chode', 'chodes', 'cipa', 'cl1t', 'climax', 'clit', 'clitoris', 'clitorus', 'clits', 'clitty', 'clusterfuck', 'cnut', 'cocain', 'cocaine', 'cock', 'cock-sucker', 'cockblock', 'cockface', 'cockhead', 'cockholster', 'cockknocker', 'cockmunch', 'cockmuncher', 'cocks', 'cocksmoker', 'cocksuck', 'cocksucked', 'cocksucker', 'cocksucking', 'cocksucks', 'cocksuka', 'cocksukka', 'coital', 'cok', 'cokmuncher', 'coksucka', 'commie', 'condom', 'corksucker', 'cornhole ', 'crack', 'cracker', 'crackwhore', 'crap', 'crappy', 'cum', 'cumdump ', 'cummer', 'cummin', 'cumming', 'cums', 'cumshot', 'cumshots', 'cumslut', 'cumstain', 'cunilingus', 'cunillingus', 'cunnilingus', 'cunny', 'cunt', 'cunt-struck ', 'cuntbag ', 'cuntface', 'cunthunter', 'cuntlick', 'cuntlick ', 'cuntlicker', 'cuntlicker ', 'cuntlicking ', 'cunts', 'cuntsicle ', 'cyalis', 'cyberfuc', 'cyberfuck ', 'cyberfucked', 'cyberfucker', 'cyberfuckers', 'cyberfucking', 'd0ng', 'd0uch3', 'd0uche', 'd1ck', 'd1ld0', 'd1ldo', 'dago', 'dagos', 'dammit', 'damn', 'damned', 'damnit', 'dawgie-style', 'dick', 'dick-ish', 'dickbag', 'dickdipper', 'dickface', 'dickflipper', 'dickhead', 'dickheads', 'dickish', 'dickripper', 'dicksipper', 'dickweed', 'dickwhipper', 'dickzipper', 'diddle', 'dike', 'dildo', 'dildos', 'diligaf', 'dillweed', 'dimwit', 'dingle', 'dink', 'dinks', 'dipship', 'dirsa', 'dlck', 'dog-fucker', 'doggie-style', 'doggiestyle', 'doggin', 'dogging', 'doggy-style', 'donkeyribber', 'doofus', 'doosh', 'dopey', 'douch3', 'douche', 'douchebag', 'douchebags', 'douchey', 'drunk', 'duche', 'dumass', 'dumbass', 'dumbasses', 'dummy', 'dyke', 'dykes', 'ejaculate', 'ejaculated', 'ejaculates', 'ejaculating', 'ejaculatings', 'ejaculation', 'ejakulate', 'enlargement', 'erect', 'erection', 'erotic', 'essohbee', 'extacy', 'extasy', 'f-u-c-k', 'f.u.c.k', 'f4nny', 'f_u_c_k', 'facial ', 'fack', 'fag', 'fagg', 'fagged', 'fagging', 'faggit', 'faggitt', 'faggot', 'faggs', 'fagot', 'fagots', 'fags', 'faig', 'faigt', 'fanny', 'fannybandit', 'fannyflaps', 'fannyfucker', 'fanyy', 'fart', 'fartknocker', 'fat', 'fatass', 'fcuk', 'fcuker', 'fcuking', 'feck', 'fecker', 'felch', 'felcher', 'felching', 'fellate', 'fellatio', 'feltch', 'feltcher', 'fingerfuck', 'fingerfucked', 'fingerfucker ', 'fingerfuckers', 'fingerfucking', 'fingerfucks', 'fisted', 'fistfuck', 'fistfucked', 'fistfucker ', 'fistfuckers', 'fistfucking', 'fistfuckings', 'fistfucks', 'fisting', 'fisty', 'flange', 'floozy', 'foad', 'fondle', 'foobar', 'fook', 'fooker', 'foreskin', 'freex', 'frigg', 'frigga', 'fubar', 'fuck', 'fuck ', 'fuck-ass ', 'fuck-bitch ', 'fuck-tard', 'fucka', 'fuckass', 'fucked', 'fucker', 'fuckers', 'fuckface', 'fuckhead', 'fuckheads', 'fuckin', 'fucking', 'fuckings', 'fuckingshitmotherfucker', 'fuckme', 'fuckmeat ', 'fucknugget', 'fucknut', 'fuckoff', 'fucks', 'fucktard', 'fucktoy ', 'fuckup', 'fuckwad', 'fuckwhit', 'fuckwit', 'fudgepacker', 'fuk', 'fuker', 'fukker', 'fukkin', 'fuks', 'fukwhit', 'fukwit', 'fux', 'fux0r', 'fvck', 'fxck', 'g-spot', 'gae', 'gai', 'gang-bang ', 'gangbang', 'gangbang ', 'gangbanged', 'gangbangs', 'ganja', 'gay', 'gaylord', 'gays', 'gaysex', 'gey', 'gfy', 'ghay', 'ghey', 'gigolo', 'glans', 'goatse', 'god', 'god-dam', 'god-damned', 'godamn', 'godamnit', 'goddam', 'goddammit', 'goddamn', 'goddamned', 'goldenshower', 'gonad', 'gonads', 'gook', 'gooks', 'gringo', 'gspot', 'gtfo', 'guido', 'h0m0', 'h0mo', 'handjob', 'hardcoresex', 'he11', 'hebe', 'heeb', 'hell', 'hemp', 'heroin', 'herp', 'herpes', 'herpy', 'heshe', 'hitler', 'hiv', 'hoar', 'hoare', 'hobag', 'hoer', 'hom0', 'homey', 'homo', 'homoerotic', 'homoey', 'honky', 'hooch', 'hookah', 'hooker', 'hoor', 'hootch', 'hooter', 'hooters']
      profane_words_2=['hore', 'horniest', 'horny', 'hotsex', 'hump', 'humped', 'humping', 'hymen', 'inbred', 'incest', 'injun', 'j3rk0ff', 'jack-off', 'jackass', 'jackhole', 'jackoff', 'jap', 'japs', 'jerk', 'jerk-off', 'jerk0ff', 'jerked', 'jerkoff', 'jism', 'jiz', 'jizm', 'jizz', 'jizzed', 'junkie', 'junky', 'kawk', 'kike', 'kikes', 'kill', 'kinky', 'kkk', 'klan', 'knob', 'knobead', 'knobed', 'knobend', 'knobhead', 'knobjocky', 'knobjokey', 'kock', 'kondum', 'kondums', 'kooch', 'kooches', 'kootch', 'kraut', 'kum', 'kummer', 'kumming', 'kums', 'kunilingus', 'kwif ', 'kyke', 'l3i+ch', 'l3itch', 'labia', 'lech', 'leper', 'lesbians', 'lesbo', 'lesbos', 'lez', 'lezbian', 'lezbians', 'lezbo', 'lezbos', 'lezzie', 'lezzies', 'lezzy', 'lmao', 'lmfao', 'loin', 'loins', 'lube', 'lust', 'lusting', 'lusty', 'm-fucking', 'm0f0', 'm0fo', 'm45terbate', 'ma5terb8', 'ma5terbate', 'mafugly ', 'mams', 'masochist', 'massa', 'master-bate', 'masterb8', 'masterbat*', 'masterbat3', 'masterbate', 'masterbating', 'masterbation', 'masterbations', 'masturbate', 'masturbating', 'masturbation', 'menses', 'menstruate', 'menstruation', 'meth', 'mo-fo', 'mof0', 'mofo', 'molest', 'moolie', 'moron', 'mothafuck', 'mothafucka', 'mothafuckas', 'mothafuckaz', 'mothafucked', 'mothafucker', 'mothafuckers', 'mothafuckin', 'mothafucking', 'mothafuckings', 'mothafucks', 'motherfuck', 'motherfucka', 'motherfucked', 'motherfucker', 'motherfuckers', 'motherfuckin', 'motherfucking', 'motherfuckings', 'motherfuckka', 'motherfucks', 'mtherfucker', 'mthrfucker', 'mthrfucking', 'muff', 'muffdiver', 'murder', 'mutha', 'muthafecker', 'muthafuckaz', 'muthafucker', 'muthafuckker', 'muther', 'mutherfucker', 'mutherfucking', 'muthrfucking', 'n1gga', 'n1gger', 'nad', 'nads', 'naked', 'napalm', 'nappy', 'nazi', 'nazism', 'negro', 'nigg3r', 'nigg4h', 'nigga', 'niggah', 'niggas', 'niggaz', 'nigger', 'niggers', 'niggle', 'niglet', 'nimrod', 'ninny', 'nipple', 'ngo', 'nob', 'nobhead', 'nobjocky', 'nobjokey', 'nooky', 'numbnuts', 'nutsack', 'nympho', 'omg', 'opiate', 'opium', 'oral', 'orally', 'organ', 'orgasim ', 'orgasims ', 'orgasm', 'orgasmic', 'orgasms ', 'orgies', 'orgy', 'ovary', 'ovum', 'ovums', 'p.u.s.s.y.', 'p0rn', 'paddy', 'paki', 'pantie', 'panties', 'panty', 'pastie', 'pasty', 'pawn', 'pcp', 'pecker', 'pedo', 'pedophile', 'pedophilia', 'pedophiliac', 'pee', 'peepee', 'penetrate', 'penetration', 'penial', 'penile', 'penis', 'penisfucker', 'perversion', 'peyote', 'phalli', 'phallic', 'phonesex', 'phuck', 'phuk', 'phuked', 'phuking', 'phukked', 'phukking', 'phuks', 'phuq', 'pigfucker', 'pillowbiter', 'pimp', 'pimpis', 'pinko', 'piss', 'piss-off', 'pissed', 'pisser', 'pissers', 'pisses ', 'pissflaps', 'pissin', 'pissing', 'pissoff', 'pissoff ', 'pms', 'polack', 'pollock', 'poon', 'poontang', 'poop', 'porn', 'porno', 'pornography', 'pornos', 'pot', 'potty', 'prick', 'pricks', 'prig', 'pron', 'prostitute', 'prude', 'pube', 'pubic', 'pubis', 'punkass', 'punky', 'puss', 'pusse', 'pussi', 'pussies', 'pussy', 'pussypounder', 'pussys', 'puto', 'queaf', 'queaf ', 'queef', 'queer', 'queero', 'queers', 'quicky', 'quim', 'r-tard', 'racy', 'rape', 'raped', 'raper', 'rapist', 'raunch', 'rectal', 'rectum', 'rectus', 'reefer', 'reetard', 'reich', 'retard', 'retarded', 'revue', 'rimjaw', 'rimjob', 'rimming', 'ritard', 'rtard', 'rum', 'rump', 'rumprammer', 'ruski', 's-h-1-t', 's-h-i-t', 's-o-b', 's.h.i.t.', 's.o.b.', 's0b', 's_h_i_t', 'sadism', 'sadist', 'sandbar ', 'scag', 'scantily', 'schizo', 'schlong', 'screw', 'screwed', 'screwing', 'scroat', 'scrog', 'scrot', 'scrote', 'scrotum', 'scrud', 'scum', 'seaman', 'seamen', 'seduce', 'semen', 'sex', 'sexual', 'sh!+', 'sh!t', 'sh1t', 'shag', 'shagger', 'shaggin', 'shagging', 'shamedame', 'shemale', 'shi+', 'shit', 'shitdick', 'shite', 'shiteater', 'shited', 'shitey', 'shitface', 'shitfuck', 'shitfull', 'shithead', 'shithole', 'shithouse', 'shiting', 'shitings', 'shits', 'shitt', 'shitted', 'shitter', 'shitters', 'shitting', 'shittings', 'shitty', 'shiz', 'sissy', 'skag', 'skank', 'slave', 'sleaze', 'sleazy', 'slope ', 'slut', 'slutdumper', 'slutkiss', 'sluts', 'smegma', 'smut', 'smutty', 'snatch', 'sniper', 'snuff', 'sodom', 'son-of-a-bitch', 'souse', 'soused', 'spac', 'sperm', 'spic', 'spick', 'spik', 'spiks', 'spooge', 'spunk', 'steamy', 'stfu', 'stiffy', 'stoned', 'strip', 'stroke', 'stupid', 'suck', 'sucked', 'sucking', 'sumofabiatch', 't1t', 't1tt1e5', 't1tties', 'tampon', 'tard', 'tawdry', 'teabagging', 'teat', 'teets', 'teez', 'terd', 'teste', 'testee', 'testes', 'testical', 'testicle', 'testis', 'thrust', 'thug', 'tinkle', 'tit', 'titfuck', 'titi', 'tits', 'titt', 'tittie5', 'tittiefucker', 'titties', 'titty', 'tittyfuck', 'tittyfucker', 'tittywank', 'titwank', 'toke', 'toots', 'tosser', 'tramp', 'transsexual', 'trashy', 'tubgirl', 'turd', 'tush', 'tw4t', 'twat', 'twathead', 'twats', 'twatty', 'twunt', 'twunter', 'ugly', 'undies', 'unwed', 'urinal', 'urine', 'uterus', 'uzi', 'v14gra', 'v1gra', 'vag', 'vagina', 'valium', 'viagra', 'virgin', 'vixen', 'vodka', 'vomit', 'voyeur', 'vulgar', 'vulva', 'w00se', 'wad', 'wank', 'wanker', 'wanky', 'wazoo', 'wedgie', 'weed', 'weenie', 'weewee', 'weiner', 'weirdo', 'wench', 'wetback', 'wh0re', 'wh0reface', 'whitey', 'whiz', 'whoar', 'whoralicious', 'whore', 'whorealicious', 'whored', 'whoreface', 'whorehopper', 'whorehouse', 'whores', 'whoring', 'wigger', 'womb', 'woody', 'wop', 'wtf', 'x-rated', 'xrated', 'xxx', 'yeasty', 'yobbo', 'zoophile']
      profane_words=profane_words_1 + profane_words_2

      return profane_words

<<<<<<< HEAD


=======
>>>>>>> 6322b458307784dfdeb11329a7343ff26c8cc206
  def get_name_cleanedup(self,tmp):

    """

    tmp          : is spark dataframe containing first and last names values stored in
                   columns self.first_nm_col and self.last_nm_col. See above constructor method.

    This method checks for valid names. Empty names, and any of 'test','notgiven','noname','valued customer',
    'customer','valued','anycart','albertsons','safeway','na','n/a' are considered invalid name.

    If numbers of special characters are present in the name string, these will be replaced with empty string.

    This method creates first_nm, and last_nm columns if not already present in the original data frame.
    This also keeps a copy of original names in first_nm_orig and last_nm_orig in dataframe.

    Handling multi string names:
      If first name contains multiple strings, only first string is considered as first name if its
      length is more than 3 characters. If first string of first name has fewer than 3 characters, and
      second string contains at least three characters, the second string is considered as first name.

      Same applies with last name, where last string of last name is given priority over other strings.

    """

    import pyspark.sql.functions as F
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType, FloatType, BooleanType, StringType
    import re, string

    def get_clean_name(name):
      import re
      pattern=r'[^A-Za-z\s]'
      repl=''
      return re.sub(pattern,repl,name).strip().replace('n/a','').replace('n a','').lower()

    def get_first_name(names):
        if names.lower() in (['test','notgiven','noname','valued customer','customer','valued','anycart','albertsons','safeway','na','n/a','', ' ']):
          return ''
        x=[y.strip() for y in names.split()]
        if len(x)==1:
          return x[0]
        if len(x[0])>len(x[1]):
          return x[0]
        else:
          if len(x[0])>=3:
            return x[0]
          else:
            return x[1]

    def get_last_name(names):
        if names.lower() in (['test','notgiven','noname','valued customer','customer','valued','anycart','albertsons','safeway','na','n/a','', ' ']):
          return ''
        x=[y.strip() for y in names.split()]
        if len(x)==1:
          return x[0]
        if len(x[1])>len(x[0]):
          return x[1]
        else:
          if len(x[1])>=3:
            return x[1]
          else:
            return x[0]

    tmp=tmp.select([F.col(c).cast("string") for c in tmp.columns])

    for c in tmp.columns:
      if 'last_update_ts' in c:
        pass
      if c in ['card_nbr','household_id','phone','first_nm','last_nm','first_nm_orig','last_nm_orig','first_used_store_id','last_used_store_id','most_used_store_id','first_used_zip_code', 'last_used_zip_code', 'most_used_zip_code']:
        if 'store' in c:
          tmp=tmp.fillna(value='9999',subset=[c])
        if 'zip' in c:
          tmp=tmp.fillna(value='99999',subset=[c])
        else:
          tmp=tmp.withColumn(c,F.lower(F.col(c)))
          tmp=tmp.fillna(value='',subset=[c])
      else:
        tmp=tmp.withColumn(c,F.lower(F.col(c)))
        tmp=tmp.withColumn(c,F.regexp_replace(c,'none',''))
        tmp=tmp.withColumn(c,F.regexp_replace(c,'nan',''))
        tmp=tmp.withColumn(c,F.trim(F.col(c)))
        tmp=tmp.fillna(value='',subset=[c])

    ### even if the names columns are not first_nm and last_nm, created these columns that will be used subsequently

    tmp = tmp.withColumn('first_nm',F.when(F.col(self.first_nm_col).isin(['valued customer','customer','valued','anycart','albertsons','safeway']),'').otherwise(F.col(self.first_nm_col)))
    tmp = tmp.withColumn('last_nm',F.when(F.col(self.last_nm_col).isin(['valued customer','customer','valued','anycart','albertsons','safeway']),'').otherwise(F.col(self.last_nm_col)))

    getFirstNameUDF=udf(lambda x: get_first_name(x),StringType())
    getLastNameUDF=udf(lambda x: get_last_name(x),StringType())
    getCleanNameUDF=udf(lambda x: get_clean_name(x),StringType())

    tmp=tmp.withColumn('first_nm',getCleanNameUDF(F.col('first_nm')))
    tmp=tmp.withColumn('last_nm',getCleanNameUDF(F.col('last_nm')))

    tmp=tmp.withColumn('first_nm_v1',getFirstNameUDF(F.col('first_nm')))
    tmp=tmp.withColumn('last_nm_v1',getLastNameUDF(F.col('last_nm')))

    tmp=tmp.withColumnRenamed('first_nm','first_nm_orig')
    tmp=tmp.withColumnRenamed('last_nm','last_nm_orig')
    tmp=tmp.withColumnRenamed('first_nm_v1','first_nm')
    tmp=tmp.withColumnRenamed('last_nm_v1','last_nm')

    return tmp

  #####################################################################################
  ################################## Email Cleanup ####################################
  #####################################################################################

  def get_cleanedup_email(self,tmp,df_profane_words):

    """

    tmp                   : is spark dataframe containing email address values stored in
                            columns self.email_address_col. See above constructor method.
    df_profane_words      : a spark or pandas dataframe that contains list of profane english language words.
                            If an email address string is one of the profane words, it will be formatted to empty email address.

    email address contains at least one alpha character.
    In addition, the following strings are considered invalid email address.
    'n/a', ' ', '', 'na','n a', 'no','none','noemail','email','albertsons','safeway','invalid'

    email address regex: r"([-!#-'*+/-9=?A-Z^-~]+(\.[-!#-'*+/-9=?A-Z^-~]+)*|\"([]!#-[^-~ \t]|(\\[\t -~]))+\")@([-!#-'*+/-9=?A-Z^-~]+(\.[-!#-'*+/-9=?A-Z^-~]+)*|\[[\t -Z^-~]*])")

    This method creates email_address, email_user (string before @) if not already present in the original data frame.
    This also keeps a copy of original email address email_address_old in the data frame.

    """

    import pyspark.sql.functions as F
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType, FloatType, BooleanType, StringType
    import pandas, string, re

    if not isinstance(df_profane_words,pandas.DataFrame):
      pdf_profane_words=df_profane_words.toPandas()
    else:
      pdf_profane_words=df_profane_words

    pdf_profane_words.columns=[x.lower() for x in df_profane_words.columns]

    if not 'invalid_name' in pdf_profane_words.columns:
      profane_words_dict={'na':1} ## just a placeholder
    else:
      profane_words_dict={x:1 for x in pdf_profane_words.invalid_name.tolist()}

    def get_len_string(x):
      return len(x)

    def get_len_string_component(x):
      return len(x.strip().split())

    def is_string_just_number(s):
      import re
      patt='[^0-9]'
      reg_patt=re.compile(patt)
      if "".join(reg_patt.findall(s)).replace(' ',''):
        return False
      else:
        return True

    def get_sep_num_string_address(s):
      import re
      return re.sub(' +',' '," ".join([" ".join(re.split(r'(\d+)', x)).strip() if not(x.isalnum() and x.isnumeric()) else x.strip() for x in s.split()]).strip())


    def get_email_user_name(email):
        if '@' in email:
          return email.split('@')[0]
        else:
          return ''

    def get_valid_email(email):
      import re
      regex = re.compile(r"([-!#-'*+/-9=?A-Z^-~]+(\.[-!#-'*+/-9=?A-Z^-~]+)*|\"([]!#-[^-~ \t]|(\\[\t -~]))+\")@([-!#-'*+/-9=?A-Z^-~]+(\.[-!#-'*+/-9=?A-Z^-~]+)*|\[[\t -Z^-~]*])")
      if email.split('@')[0] in profane_words_dict:
        return ''
      if re.fullmatch(regex, email.lower()):
        return email.lower().replace('n/a','').replace('na','').strip()
      else:
          return ''

    def get_email_user(x):
      result=x.split("@")[0] if "@" in x else ''
      return result.lower().replace('n/a','').replace('na','')

    getLenStringUDF=udf(lambda x: get_len_string(x), IntegerType())
    getLenStringComponentUDF=udf(lambda x: get_len_string_component(x), IntegerType())
    isStringJustNumberUDF=udf(lambda x: is_string_just_number(x), BooleanType())
    getSepNumStringAddressUDF=udf(lambda x: get_sep_num_string_address(x), StringType())
    getEmailUserNameUDF=udf(lambda x: get_email_user_name(x), StringType())
    getCleanEmailUDF=udf(lambda x: get_valid_email(x), StringType())
    getEmailUserUDF=udf(lambda z:get_email_user(z),StringType())


    invalid_email_user_names=['n/a', ' ', '', 'na','n a', 'no','none','noemail','email','albertsons','safeway','invalid']
    if self.email_address_col in tmp.columns:
      tmp=tmp.withColumn(self.email_address_col,F.lower(F.col(self.email_address_col)))
      tmp=tmp.withColumn('email_user',getEmailUserUDF(F.col(self.email_address_col)))
      tmp=tmp.withColumn('email_user',F.when(F.col('email_user').isin(invalid_email_user_names),'').otherwise(F.col('email_user')))
    else:
      tmp=tmp.withColumn('email_user',F.lit(''))

    tmp=tmp.withColumn('email_address',F.lower(self.email_address_col))
    tmp=tmp.withColumn('email_address',F.when(F.col('email_address').isNull(),'').otherwise(F.col('email_address')))
    tmp=tmp.withColumn('email_address',F.when(F.col('email_address').isNull(),'').otherwise(F.col('email_address')))
    tmp=tmp.withColumn('email_address',F.when(F.col('email_address').isin(['na','n/a']),'').otherwise(F.col('email_address')))
    tmp=tmp.withColumn('email_address_new',getCleanEmailUDF(F.col('email_address')))
    tmp=tmp.withColumnRenamed('email_address','email_address_old')
    tmp=tmp.withColumnRenamed('email_address_new','email_address')
    tmp=tmp.withColumn('email_address_old',F.when(F.col('email_address_old').isNull(),'').otherwise(F.col('email_address_old')))
    tmp=tmp.withColumn('email_address',F.when(F.col('email_address').isNull(),'').otherwise(F.col('email_address')))
    tmp=tmp.withColumn('email_address',F.when(tmp.email_user.isin(invalid_email_user_names),'').otherwise(F.col('email_address')))

    return tmp

  #####################################################################################
  ################################## Address Cleanup ##################################
  #####################################################################################

  def get_cleanedup_address(self,tmp,add_suff):

    """

    tmp                   : is spark dataframe containing postal address values stored in
                            columns self.address_line1_col and self.address_line2_col.

    add_suff              : a spark or pandas dataframe that contains list of address suffix names mapping such as st --> street
                            This table is used to normalize the address.

    address contains at least one alphanumeric character.
    In addition, the following strings are considered invalid address:
    'n/a', ' ', '', 'na','n a', 'no','none', 'no', 'albertsons', 'safeway', 'invalid'

    This method creates columns: address, address_old if not already present in the original data frame.

    """

    import pyspark.sql.functions as F
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType, FloatType, BooleanType, StringType
    import pandas, string, re

    ## create a dict of address suffix to get fill suffix name
    if not isinstance(add_suff,pandas.DataFrame):
      add_suff=add_suff.toPandas()
    D={}
    for _, item in add_suff.iterrows():
      k1=item[1]; k2=item[2]; v=item[0]
      D.update({k1:v})
      D.update({k2:v})


    def get_len_string(x):
      return len(x)

    def get_len_string_component(x):
      return len(x.strip().split())

    def is_string_just_number(s):
      import re
      patt='[^0-9]'
      reg_patt=re.compile(patt)
      if "".join(reg_patt.findall(s)).replace(' ',''):
        return False
      else:
        return True

    def get_sep_num_string_address(s):
      import re
      return re.sub(' +',' '," ".join([" ".join(re.split(r'(\d+)', x)).strip() if not(x.isalnum() and x.isnumeric()) else x.strip() for x in s.split()]).strip())

    def get_clean_address(address):
      import re
      if not address:
        return ''
      address=address.lower()
      pattern='[^0-9a-zA-Z]'
      repl=' '
      result=re.sub(pattern,repl,address)
      result=result.strip(string.punctuation)
      result=re.sub(' +',' '," ".join([" ".join(re.split(r'(\d+)', x)).strip().replace('apt',' apt')  if not(x.isalnum() and x.isnumeric()) and not any(y in x for y in ['th','nd','rd','st']) else x.strip() for x in result.split()]).strip())

      return result.strip()

    def get_full_street_name(x):
      return D[x] if x in D else x

    def get_full_address_name(x):
      tmp=x.lower().split()
      return " ".join(tmp[:2]+[get_full_street_name(y) for y in tmp[2:]])

    def get_clean_address(y):
      if y=='':
        return ''
      y=y.lower()
      pattern='[^0-9a-zA-Z]'
      repl=' '
      result=re.sub(pattern,repl,y)
      result=result.strip(string.punctuation)
      result=re.sub(' +',' '," ".join([" ".join(re.split(r'(\d+)', x)).strip() if not(x.isalnum() and x.isnumeric()) else x.strip() for x in result.split()]).strip())
      result=result.replace('n/a','').replace('na','').replace('n a','')
      return result.strip()


    getLenStringUDF=udf(lambda x: get_len_string(x), IntegerType())
    getLenStringComponentUDF=udf(lambda x: get_len_string_component(x), IntegerType())
    isStringJustNumberUDF=udf(lambda x: is_string_just_number(x), BooleanType())
    getSepNumStringAddressUDF=udf(lambda x: get_sep_num_string_address(x), StringType())
    getCleanAddressUDF=udf(lambda x: get_clean_address(x), StringType())
    getFullAddressNameUDF=udf(lambda x: get_full_address_name(x), StringType())
    getCleanAddressUDF=udf(lambda z:get_clean_address(z),StringType())

    if (self.address_line1_col in tmp.columns) and (self.address_line2_col in tmp.columns):
      tmp=tmp.withColumn('address',F.concat_ws(' ',F.col('address_line1'),F.col('address_line2')))
      tmp=tmp.withColumn('address',F.lower(F.col('address')))
      tmp=tmp.withColumn('address',getCleanAddressUDF(F.col('address')))

    if (self.address_line1_col in tmp.columns) and (not self.address_line2_col in tmp.columns):
      tmp=tmp.withColumn('address',F.col('address_line1'))
      tmp=tmp.withColumn('address',F.lower(F.col('address')))
      tmp=tmp.withColumn('address',getCleanAddressUDF(F.col('address')))

    if (not self.address_line1_col in tmp.columns) and (self.address_line2_col in tmp.columns):
      tmp=tmp.withColumn('address',F.col('address_line2'))
      tmp=tmp.withColumn('address',F.lower(F.col('address')))
      tmp=tmp.withColumn('address',getCleanAddressUDF(F.col('address')))

    if not 'address' in tmp.columns:
      tmp=tmp.withColumn('address',F.lit(''))

    invalid_addresses=['n/a', ' ', '', 'na','n a', 'no','none', 'no', 'albertsons', 'safeway', 'invalid']
    tmp=tmp.withColumn('address',F.lower(F.col('address')))
    tmp=tmp.withColumn('address',F.when(F.col('address').isin(invalid_addresses),'').otherwise(F.col('address')))
    tmp=tmp.withColumn('address',F.when(F.col('address').isNull(),'').otherwise(F.col('address')))
    tmp=tmp.withColumn('address',F.when(F.col('address').isin(['na','n/a']),'').otherwise(F.col('address')))
    tmp=tmp.withColumn('address',F.when(isStringJustNumberUDF(F.col('address')),'').otherwise(F.col('address')))
    tmp=tmp.withColumn('address_new',F.when(F.col('address')!='',getCleanAddressUDF(F.col('address'))).otherwise(F.col('address')))
    tmp=tmp.withColumn('address_new',F.when(F.col('address_new')!='',getFullAddressNameUDF(F.col('address_new'))).otherwise(F.col('address_new')))
    tmp=tmp.withColumn('len_add',getLenStringUDF(F.col('address_new')))
    tmp=tmp.withColumn('len_add_comp',F.when(F.col('address')!='',getLenStringComponentUDF(F.col('address_new'))).otherwise(F.lit("0").cast('int')))
    tmp=tmp.withColumnRenamed('address','address_old')
    tmp=tmp.withColumnRenamed('address_new','address')
    tmp=tmp.withColumn('address',F.when(F.col('address').contains('general delivery'),'').otherwise(F.col('address')))
    tmp=tmp.withColumn('address',F.when(F.col('address').startswith('po box'),'').otherwise(F.col('address')))

    return tmp

################################################################################################################
############################   Gender Preidtion Using names gender data and NB model ###########################
################################################################################################################


class genderPredictions():

  """

  This class implements mapping first names to gender based on names to gender mapping hash table.
  For names not found in hash table, a Machine Learning Algorithm is used to generate the gender given first name.

  """


  def __init__(self):
    import pyspark
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import udf

    self.spark = SparkSession.builder.master("local[1]").appName("MLIR").getOrCreate()


  def get_gender_prediction(self, tmp, df_names_us_ca='', df_names_row=''):

    """

    tmp                   : spark data frame contains pii such as first name.
    df_names_us_ca        : spark data frame that contains name and gender as two columns. This is for US, Canada based names. Each row is unique.
    df_names_row          : spark data frame that contains name and gender as two columns. This is for rest of world (excluding US and Canada) based names. Each row is unique.

    The method maps names and creates a new column in dataframe (inplace) as gender for all non empty first names.

    ML Algrotihm used is Naive Bayes.

    """

    import pyspark.sql.functions as F
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType, FloatType, BooleanType, StringType
    import pandas, string, re

    ## df_names_us_ca: US/CA names to gender map
    ## df_names_row: Rest of world names to gender map
    ## gender_model_nb: NB model with feature generated using below "gender_features()" function

    def gender_features(x):
      f={'first_letter':'','first_2letter':'','last_letter':'','last_2letter':'','last_3letter':''}
      if x=='':
        pass
      else:
        f.update({'first_letter':x[0].lower(),'first_2letter':x[:2].lower()})
        f.update({'last_letter':x[-1].lower(),'last_2letter':x[-2:].lower(),'last_3letter':x[-3:].lower()})
      return f

    def get_nb_classifier():

      import nltk
      import random
      nltk.download('names', quiet=True)
      from nltk.corpus import names

      # preparing a list of examples and corresponding class labels.

      labeled_names = ([(name.lower(), 'M') for name in names.words('male.txt')]+
                 [(name.lower(), 'F') for name in names.words('female.txt')])

      featuresets = [(gender_features(n), gender)
                     for (n, gender)in labeled_names]

      classifier = nltk.NaiveBayesClassifier.train(featuresets)

      return classifier


    def get_gender_prediction_using_nb_model(x):

      return model.value.classify(gender_features(x)) if x!='' else ''

    getGenderPredictionUsingNBModel=udf(lambda x: get_gender_prediction_using_nb_model(x), StringType());

    df_g=tmp.select('*')

    if df_names_us_ca=='':
      df_names_us_ca=self.spark.createDataFrame([('','')],('name','gender'))
    if df_names_row=='':
      df_names_row=self.spark.createDataFrame([('','')],('name','gender'))

    ### jon the gender table to get gender mapping column
    df_names_us_ca=df_names_us_ca.withColumnRenamed('name','first_nm').withColumnRenamed('gender','gender_us_ca')
    df_g=df_g.join(df_names_us_ca.dropDuplicates().select('first_nm','gender_us_ca').dropDuplicates(),on=['first_nm'],how='left')

    df_names_row=df_names_row.withColumnRenamed('name','first_nm').withColumnRenamed('gender','gender_row')
    df_g=df_g.join(df_names_row.dropDuplicates().select('first_nm','gender_row').dropDuplicates(),on=['first_nm'],how='left')

    ## map the gender
    df_g=df_g.withColumn('gender',F.lit(''))
    df_g=df_g.withColumn('gender',F.when(F.col('gender_us_ca').isNull(),F.col('gender_row')).otherwise(F.col('gender_us_ca')))

    ## convert the null gender values to empty string
    ## get the NB classifier, and broadcast it to use within UDF without loading repetitivly

    classifier=get_nb_classifier()
    model = self.spark.sparkContext.broadcast(classifier)

    df_g=df_g.withColumn('gender_us_ca',F.when(df_g.gender_us_ca.isNull(),'').otherwise(F.col('gender_us_ca')))
    df_g=df_g.withColumn('gender_row',F.when(df_g.gender_row.isNull(),'').otherwise(F.col('gender_row')))
    df_g=df_g.withColumn('gender',F.when(df_g.gender.isNull(),'').otherwise(F.col('gender')))

    df_g=df_g.withColumn('gender',F.when((df_g.gender!='') & (df_g.first_nm!=''),F.col('gender')).\
                         otherwise(getGenderPredictionUsingNBModel(df_g.first_nm)))

    df_g=df_g.withColumn('gender',F.when(df_g.gender.isNull(),'').otherwise(F.col('gender')))
    df_g=df_g.drop('gender_us_ca','gender_row')
    df_g=df_g.withColumn('gender',F.lower(F.col('gender')))

    return df_g


##########################################################################################
#####################    Generate record pairs, ##########################################
##########################################################################################

class generateRecordPairs(snowflakeConnector):

  """

  This class implements generating record pairs based on given blocking key such as phone.

  """


  def __init__(self, app_id, sf_dict, db_dict, pii_cols_updated):
    if app_id=='' or not app_id:
      app_id=''

    super().__init__(app_id)

    for k,v in sf_dict.items():
      self.set_sf(k,v)
    if db_dict:
      for k,v in db_dict.items():
        self.set_sf(k,v)

    import pyspark
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import udf

    self.spark = SparkSession.builder.master("local[1]").appName("MLIR").getOrCreate()


    """

    pii_cols_updated      : list of pii columns from given PII table and derived PII columns such as gender,
                            cleaned email, address columns, derrived first txn dte, first store, first zip code,
                            first upc etc.


    """
    self.pii_cols_updated=pii_cols_updated

    self.card_nbr_col=self.pii_cols_updated['card_nbr_col']
    self.household_col=self.pii_cols_updated['household_col']
    self.phone_col=self.pii_cols_updated['phone_col']
    self.first_nm_col=self.pii_cols_updated['first_nm_col']
    self.last_nm_col=self.pii_cols_updated['last_nm_col']
    self.address_col=self.pii_cols_updated['address_col']
    self.email_address_col=self.pii_cols_updated['email_address_col']
    self.gender_col=self.pii_cols_updated['gender_col']
    self.email_user_col=self.pii_cols_updated['email_user_col']
    self.first_txn_dte_col=self.pii_cols_updated['first_txn_dte_col']
    self.first_used_store_id_col=self.pii_cols_updated['first_used_store_id_col']
    self.first_upc_col=self.pii_cols_updated['first_upc_col']
    self.first_used_zip_code_col=self.pii_cols_updated['first_used_zip_code_col']

  def get_record_pairs_pii(self, tab_tc, tab_pii, key_col):

    """

    tab_tc                : the table to be created. This is concatenate of PII of record pairs given blocking key.
                            Full table specification with database and schema and table name.
    tab_pii               : is spark data frame containing pii columns. See constructor class. It must consiste a
                            blocking column and card numbers column. See below.
    key_col               : is blocking key column such as email, phone. This must be present in the tmp dataframe.

    This method generates card numbers record pairs given a blocking key along with respective PII columns.
    For instance if phone number p is associated with 3 card numbers A,B, and C then (A,B), (B,C), and
    (A,c) are three record pairs will be generated in the resulting output.

    Note: We focus on creating record pairs for those columns which has first name or email (valid) as PII avail else
          It does not makes sense to create record without any meaningful pii columns as there will not be much to
          predit using probabilistic model.


    """

    import pyspark.sql.functions as F
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType, FloatType, BooleanType, StringType
    import pandas, string
    from itertools import chain


    if self.first_nm_col and self.email_address_col:
      cond=f"and (c.{self.first_nm_col}!='' or c.{self.email_address_col}!='')"
    elif self.first_nm_col and (not self.email_address_col):
      cond=f"and c.{self.first_nm_col}!=''"
    elif (not self.first_nm_col) and (self.email_address_col):
      cond=f"and c.{self.email_address_col}!=''"
    else:
      cond=""

    tab_tc_tmp_rp_1=tab_tc+"_1"
    tab_tc_tmp_rp_2=tab_tc+"_2"

    ### why need two tables? to get the pii columns for card_nbr_1 and card_nbr_2 respectively and concat along axis=1

    query_rp_1=f"""create or replace table {tab_tc_tmp_rp_1} as
                  select distinct t0.*, c1c2 from {tab_pii} t0
                  inner join
                  (
                    select distinct concat(card_nbr_1,card_nbr_2) as c1c2, {key_col}, card_nbr_1, card_nbr_2,
                    row_number() over(order by card_nbr_1, card_nbr_2) as rn from
                    (
                        select distinct a.{key_col}, card_nbr_1, card_nbr_2 from
                        (
                          select distinct c.{key_col}, c.card_nbr card_nbr_1 from {tab_pii} c
                          inner join
                          (
                              select {key_col}, count(distinct card_nbr) cnt from  {tab_pii}
                              group by 1
                              having cnt<=10000 and cnt>1
                          ) d
                          on c.{key_col}=d.{key_col}
                          where c.{key_col} is not null and c.{key_col}!=''
                          {cond}
                        ) a
                        inner join
                        (
                          select distinct {key_col}, card_nbr card_nbr_2 from {tab_pii}
                          where {key_col} is not null and {key_col}!=''
                        ) b
                        on a.{key_col}=b.{key_col}
                        where a.card_nbr_1<b.card_nbr_2
                    )
                  ) t1
                  on t0.card_nbr=t1.card_nbr_1;"""


    query_rp_2=f"""create or replace table {tab_tc_tmp_rp_2} as
                  select distinct t0.*, c1c2 from {tab_pii} t0
                  inner join
                  (
                    select distinct concat(card_nbr_1,card_nbr_2) as c1c2, {key_col}, card_nbr_1, card_nbr_2,
                    row_number() over(order by card_nbr_1, card_nbr_2) as rn from
                    (
                        select distinct a.{key_col}, card_nbr_1, card_nbr_2 from
                        (
                          select distinct c.{key_col}, c.card_nbr card_nbr_1 from {tab_pii} c
                          inner join
                          (
                              select {key_col}, count(distinct card_nbr) cnt from  {tab_pii}
                              group by 1
                              having cnt<10000 and cnt>1
                          ) d
                          on c.{key_col}=d.{key_col}
                          where c.{key_col} is not null and c.{key_col}!=''
                          {cond}
                        ) a
                        inner join
                        (
                          select distinct {key_col}, card_nbr card_nbr_2 from {tab_pii}
                          where {key_col} is not null and {key_col}!=''
                        ) b
                        on a.{key_col}=b.{key_col}
                        where a.card_nbr_1<b.card_nbr_2
                    )
                  ) t1
                  on t0.card_nbr=t1.card_nbr_2;"""


    try:
      self.run_snowflake_query(query_rp_1, tab_tc_tmp_rp_1)
    except Exception:
      raise Exception(f'{tab_tc_tmp_rp_1} update Failed !!')

    try:
      self.run_snowflake_query(query_rp_2, tab_tc_tmp_rp_2)
    except Exception:
      raise Exception(f'{tab_tc_tmp_rp_2} update Failed !!')

    rp_cols_1=[x+"_1" for x in self.pii_cols_updated.values()]
    rp_as_cols_1=[x+" as "+y for x,y in zip(self.pii_cols_updated.values(),rp_cols_1)]
    rp_as_cols_1_str= "T1."+", T1.".join(rp_as_cols_1)

    rp_cols_2=[x+"_2" for x in self.pii_cols_updated.values()]
    rp_as_cols_2=[x+" as "+y for x,y in zip(self.pii_cols_updated.values(),rp_cols_2)]
    rp_as_cols_2_str= "T2."+", T2.".join(rp_as_cols_2)

    rp_cols_str_comp='T1.c1c2, '+ rp_as_cols_1_str +", " + rp_as_cols_2_str

    query_join_rp=f"""create or replace table {tab_tc} as
                      select {rp_cols_str_comp} from {tab_tc_tmp_rp_1} T1
                      inner join {tab_tc_tmp_rp_2} T2
                      on t1.c1c2=T2.c1c2;
    """

    try:
      self.run_snowflake_query(query_join_rp, tab_tc)
    except Exception:
      raise Exception(f'{tab_tc} table creation Failed !!')

  def get_union_record_pairs(self, tab_tc, tab_sf_blocking_keys):

    """

    tab_tc                  : table to be created with full table specification. This will be union of record pairs
                              tables for each blocking key.
    tab_sf_blocking_keys       : list of snowflake record pair table names saved for each blocking keys.

      This method outputs a snowflake table as combined recpord pairs from one or more blocking keys. This will be
      inputo probabilistic prediction algorithm.

    """

    if isinstance(tab_sf_blocking_keys,list):
      if len(tab_sf_blocking_keys)==1:
        query=f"""
                  create or replace table {tab_tc} as
                  select * from {tab_sf_blocking_keys[0]}
                """
        try:
          self.run_snowflake_query(query, tab_tc)
        except Exception:
          raise Exception(f'{tab_tc} table creation Failed !!')
      else:

        query_truncate=f"""
                  truncate table {tab_tc}
                """
        try:
          self.run_snowflake_query(query_truncate, tab_tc)
        except Exception:
          raise Exception(f'{tab_tc} truncation Failed !!')

        for t in tab_sf_blocking_keys:
          query_insert=f"""
                  insert into {tab_tc}
                  (
                    select A.* from {t} A
                  )
                """
          try:
            self.run_snowflake_query(query_insert, t)
          except Exception:
            raise Exception(f'{t} inserttion Failed !!')

    else:
      if self.table_existence_check(tab_sf_blocking_keys):
        query=f"""
                  create or replace table {tab_tc} as
                  select * from {tab_sf_blocking_keys}
                """
        try:
          self.run_snowflake_query(query, tab_tc)
        except Exception:
          raise Exception(f'{tab_tc} table creation Failed !!')
      else:
        raise Exception(f"""{tab_sf_blocking_keys} must be either snowflake table or list of snowflake tables.""")


#############################################################################################################
#################################### Deterministic Algorithm for Device Id pairs ############################
#############################################################################################################

class deterministicAlgorithm:

  """
      This class implements deterministic algorithm. The deterministic algorithm uses a blocking key,
      and some other PII info to map card pairs to same shopper id. We start with using mobile device id
      as blocking key, names, and emails as additional pii columns are also used in the current deterministic
      algorithm.

  """

  def __init__(self,cols_dev_pii={}):

    """

    cols_dev_pii            : a python dictionary of columns mapping in PII table. It must contain card nbr, mobile device,
                              and either or both of (first and last name) and email address.

    """

    if not cols_dev_pii or cols_dev_pii=={}:
      cols_dev_pii={'card_nbr_col':'card_nbr','mobile_device_id_col':'mobile_device_id', 'first_nm_col':'first_nm', 'last_nm_col':'last_nm', 'email_address_col':'email_address'}

    self.cols_dev_pii=cols_dev_pii

    ## mandatory columns
    self.card_nbr_col=cols_dev_pii['card_nbr_col']
    self.mobile_device_id_col=cols_dev_pii['mobile_device_id_col']

    ## either name, email or both should be in columns
    if 'first_nm_col' in self.cols_dev_pii:
      self.first_nm_col=cols_dev_pii['first_nm_col']
    if 'last_nm_col' in self.cols_dev_pii:
      self.last_nm_col=cols_dev_pii['last_nm_col']
    if 'email_address_col' in self.cols_dev_pii:
      self.email_address_col=cols_dev_pii['email_address_col']

  def get_dev_id_record_pairs_pii(self, df_dev):

    """

    df_dev              : is spaerk data frame that must contain card number, mobile device id and either or both of
                          (first, last names) or email address.

    This method returns record pairs i.e. card pair combination given blocking keys such as mobile dev id. The card pairs
    are input to deterministic algorithm which maps these card pairs based on other PII info to match/no match.


    """

    import pyspark.sql.functions as F
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType, FloatType, BooleanType, StringType
    import pandas, string

    df_dev=df_dev.select('*')

  #############################################################################################################
  ######### Create record pairs for each mob dev id: only matched cards that belong to same dev id ############
  #############################################################################################################

    ### df_dev contains dev_id and PII i.e. these clumns ['card_nbr', 'email_address', 'mobile_device_id', 'first_nm', 'last_nm']


    if self.card_nbr_col not in df_dev.columns:
      raise Exception(f'Missing column! :{self.card_nbr_col}!!!')
    if self.mobile_device_id_col not in df_dev.columns:
      raise Exception(f'Missing column! :{self.mobile_device_id_col}!!!')
    if (not (self.first_nm_col in df_dev.columns and self.last_nm_col in df_dev.columns)) or (not self.email_address_col in df_dev.columns):
      raise Exception(f'At least (first nm and last nm) or email_address should be present in dataframe!!!')

    if 'mobile_device_id' not in df_dev.columns:
      df_dev=df_dev.withColumn('mobile_device_id',F.col(self.mobile_device_id_col))
    if 'card_nbr' not in df_dev.columns:
      df_dev=df_dev.withColumn('card_nbr',F.col(self.card_nbr_col))

    df_dev.createOrReplaceTempView('df_dev_view')
    df_dev_cards=spark.sql("""select distinct mobile_device_id, card_nbr from df_dev_view """)

    if not 'cnt' in df_dev_cards.columns:
      df_dev_cards=df_dev_cards.join(df_dev_cards.groupby(F.col('mobile_device_id')).agg(F.countDistinct(F.col('card_nbr')).alias('cnt')),on=['mobile_device_id'])
    df_dev_cards=df_dev_cards.filter(F.col('cnt')>1)

    def get_card_combinations(cards):
      from itertools import combinations
      ll=list(combinations(cards.split(','),2))
      ll=[[min(x),max(x)] for x in ll]
      t=",".join(["-".join(sorted(x)) for x in ll])
      return t

    getCardCombinationsUDF=udf(lambda x:get_card_combinations(x),StringType())

    df_dev_cards_grouped=df_dev_cards.groupby('mobile_device_id').agg(F.concat_ws(",", F.collect_list(F.col('card_nbr'))).alias('card_nbrs'))
    df_dev_cards_grouped=df_dev_cards_grouped.withColumn('card_pairs_grouped',getCardCombinationsUDF(F.col('card_nbrs')))
    df_dev_cards_grouped=df_dev_cards_grouped.withColumn('card_pairs',F.explode(F.split(F.col('card_pairs_grouped'),',')))

    df_dev_cards_grouped=df_dev_cards_grouped.withColumn('card_nbr_1',F.split(F.col('card_pairs'),'-').getItem(0))
    df_dev_cards_grouped=df_dev_cards_grouped.withColumn('card_nbr_2',F.split(F.col('card_pairs'),'-').getItem(1))
    tmp=df_dev_cards_grouped.select('mobile_device_id','card_pairs','card_nbr_1','card_nbr_2')

    df_dev_card_pairs=tmp.select('*')
    df_dev_card_pairs=df_dev_card_pairs.withColumn('c1c2',F.concat(df_dev_card_pairs.card_nbr_1,df_dev_card_pairs.card_nbr_2))
    df_dev_card_pairs.createOrReplaceTempView('df_dev_card_pairs_view')

    df_dev_card_pairs_1=spark.sql("""select distinct A.*, c1c2 from df_dev_view A inner join df_dev_card_pairs_view B on a.card_nbr=B.card_nbr_1 and A.mobile_device_id=B.mobile_device_id""")
    df_dev_card_pairs_2=spark.sql("""select distinct A.*, c1c2 from df_dev_view A inner join df_dev_card_pairs_view B on a.card_nbr=B.card_nbr_2 and A.mobile_device_id=B.mobile_device_id""")

    for p in df_dev_card_pairs_1.columns:
      if p=="c1c2" or p=='mobile_device_id':
        pass
      else:
        df_dev_card_pairs_1=df_dev_card_pairs_1.withColumnRenamed(p,p+"_1")

    for p in df_dev_card_pairs_2.columns:
      if p=="c1c2" or p=='mobile_device_id':
        pass
      else:
        df_dev_card_pairs_2=df_dev_card_pairs_2.withColumnRenamed(p,p+"_2")

    df_dev_card_pairs_comb=df_dev_card_pairs_1.join(df_dev_card_pairs_2,on=['c1c2','mobile_device_id'])

    cols_to_output=['c1c2', 'mobile_device_id', 'card_nbr_1', 'card_nbr_2']
    if self.first_nm_col+'_1' in df_dev_card_pairs_comb.columns:
      cols_to_output+=[self.first_nm_col+'_1',self.first_nm_col+'_2']
    if self.last_nm_col+'_1' in df_dev_card_pairs_comb.columns:
      cols_to_output+=[self.last_nm_col+'_1',self.last_nm_col+'_2']
    if self.email_address_col+'_1' in df_dev_card_pairs_comb.columns:
      cols_to_output+=[self.email_address_col+'_1',self.email_address_col+'_2']

    df_dev_card_pairs_comb=df_dev_card_pairs_comb.select(*cols_to_output)

    return df_dev_card_pairs_comb

  #############################################################################################################
  ############################# Create null column ind and same column indicators    ##########################
  #############################################################################################################

  def get_features(self,tmp):

    """

    tmp              : is spaerk data frame of card numbers record pairs.
                       See method deterministicAlgorithm.get_dev_id_record_pairs_pii.

    This method creates additional columns indicator such as if a column values are null/missing, and if pair of PII
    columns have same/fuzzy values. For instance column same_email_address is an indicator column which is 1 if pair
    of email address for both records are same.

    """


    import pyspark.sql.functions as F
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType, FloatType, BooleanType, StringType
    import pandas, string

    tmp=tmp.select('*')

    ## generate features based on first name, last name, email address

    feat_cols=['mobile_device_id']
    if 'first_nm_col' in self.cols_dev_pii:
      feat_cols+=[self.first_nm_col]
    if 'last_nm_col' in self.cols_dev_pii:
      feat_cols+=[self.last_nm_col]
    if 'email_address_col' in self.cols_dev_pii:
      feat_cols+=[self.email_address_col]

    tmp=tmp.withColumn('mobile_device_id_1',F.col('mobile_device_id'))
    tmp=tmp.withColumn('mobile_device_id_2',F.col('mobile_device_id'))
    tmp=tmp.drop('mobile_device_id')


    for c in feat_cols:
      c1=c+"_1"; c2=c+"_2"; new_col='either_null_'+c
      tmp=tmp.withColumn(new_col,F.when((F.col(c1).isNull()) | (F.col(c1)=='') | (F.col(c2).isNull()) | (F.col(c2)==''),F.lit(1)).otherwise(F.lit(0)))

    for c in feat_cols:
      c1=c+"_1"; c2=c+"_2"; new_col='both_null_'+c
      tmp=tmp.withColumn(new_col,F.when((F.col(c1).isNull()) & (F.col(c1)=='') & (F.col(c2).isNull()) & (F.col(c2)==''),F.lit(1)).otherwise(F.lit(0)))

    if self.first_nm_col in feat_cols:
      tmp=tmp.withColumn('approx_fn',F.when((F.col('either_null_'+self.first_nm_col)==0) & (F.col(self.first_nm_col+'_1')==F.col(self.first_nm_col+'_2')),F.lit(1)).otherwise(F.lit(0)))

    if self.last_nm_col in feat_cols:
      tmp=tmp.withColumn('approx_ln',F.when((F.col('either_null_'+self.last_nm_col)==0) & (F.col(self.last_nm_col+'_1')==F.col(self.last_nm_col+'_2')),F.lit(1)).otherwise(F.lit(0)))

    if self.email_address_col in feat_cols:
      tmp=tmp.withColumn('same_email_address',F.when((F.col('either_null_'+self.email_address_col)==0) & (F.col(self.email_address_col+'_1')==F.col(self.email_address_col+'_2')),F.lit(1)).otherwise(F.lit(0)))

    return tmp

  ####### Deterministic Algo: matches if cards mapped to same dev id are matched as same shopper_id    ########

  def deterministic_algo(self,tmp):

    """

    tmp              : is spaerk data frame of card numbers record pairs and null and same value indicators columns.
                       See method deterministicAlgorithm.get_dev_id_record_pairs_pii and
                       deterministicAlgorithm.get_features.

    This method maps if record pairs corresponds to same shopper id. It creaes indicator column prediction=1 if same
    shopper id and 0 if not.

    """


    import pyspark.sql.functions as F
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType, FloatType, BooleanType, StringType
    import pandas, string

    tmp=tmp.select('*')

    feat_cols=['mobile_device_id']
    if 'first_nm_col' in self.cols_dev_pii:
      feat_cols+=[self.first_nm_col]
    if 'last_nm_col' in self.cols_dev_pii:
      feat_cols+=[self.last_nm_col]
    if 'email_address_col' in self.cols_dev_pii:
      feat_cols+=[self.email_address_col]

    if 'mobile_device_id' in feat_cols:
      tmp=tmp.withColumn('same_mobile_device_id',F.when((F.col('either_null_mobile_device_id')==0) & (F.col('mobile_device_id_1')==F.col('mobile_device_id_2')),F.lit(1)).otherwise(F.lit(0)))

  ### Next define condtions for match

    df_d_matched=tmp.select('*')
    df_d_matched=df_d_matched.withColumn("prediction", F.when(((F.col('same_mobile_device_id')==1) & (F.col('approx_fn')==1) & (F.col('approx_ln')==1)) | ((F.col('same_mobile_device_id')==1) & (F.col('same_email_address')==1)),  F.lit(1)).otherwise(F.lit(0)))

    return df_d_matched

##########################################################################################
#########################   Probabilistic Model ##########################################
##########################################################################################

class probabilisticAlgorithm(similarityFunctions):

  """

  This class implements probabilistic algorithm. This contains get_model_features, train_xgboost and
  get_prediction methods to create features used for model training and prediction, training a machine
  learning xgboost model on labeled data, and predictions using model and given features.

  """

  def __init__(self):
    super().__init__()

  ############################   Featurization #############################################

  def get_model_features(self, df, featureCols):

    """

    df                  : a spark data frame on PII columns, and derived feature columns. This is similar to feature columns, x columns
    featureCols         : a pythonc dictionary of PII columns. i.e. col_name: actual column name in pii data.

    This method creates feature columns such as similarity measure between pair of PII columns that corresponds to a record pair.
    Outputs dataframe containing original columns and additional feature columns.

    """


    import pyspark.sql.functions as F
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType, FloatType, BooleanType, StringType
    import pandas, string

    ### Input the record pairs dataframe df
    ### Output: get the featurized dataframe

    tmp=df.select('*')

    #####################   Get the either or both null columns ############################

    x_cols=featureCols.values()

    for c in x_cols:

      c1=c+"_1"; c2=c+"_2";

      new_col_either='either_null_'+c
      new_col_both_valid='both_valid_'+c

      if (not c1 in tmp.columns) or (not c2 in tmp.columns):
        raise Exception(f'Either or both of columns {c1,c2} not present in input dataframe !!!!')

      tmp=tmp.withColumn(new_col_either,F.when((F.col(c1).isNull()) | (F.col(c1)=='') | (F.col(c2).isNull()) | (F.col(c2)==''),F.lit(1)).otherwise(F.lit(0)))

      tmp=tmp.withColumn(new_col_both_valid,F.when((F.col(c1).isNotNull()) & (F.col(c1)!='') & (F.col(c2).isNotNull()) & (F.col(c2)!=''),F.lit(1)).otherwise(F.lit(0)))

    ################################ Define UDFs for features ##############################################

    def get_soundex(x):
      import jellyfish
      if x=='':
        return ''
      else:
        return jellyfish.soundex(x)

    def get_metaphone(x):
      import phonetics
      if x=='':
        return ''
      else:
        return phonetics.metaphone(x)

    def is_approx_names(x,y, z):
      return 1 if ((x in y) or (y in x)) and (z==0) else 0

    def get_generalized_jw_simi(x,y,z):
      return self.get_generalized_fuzz_jw_similarity(x,y) if z==0 else 0.1

    def get_generalized_lev_ratio(x,y,z):
      return self.get_generalized_fuzz_Levenshtein_ratio(x,y) if z==0 else 0.1

    def get_generalized_damerauLevenshtein_simi(x,y,z):
      return self.get_generalized_damerauLevenshtein_similarity(x,y) if z==0 else 0.1

    def get_name_in_email(x,y,z,t,e1,e2):
        f1=1 if ((x in e1 or y in e1) and z==0 and t==0) else 0
        f2=1 if ((x in e1 or y in e1) and z==0 and t==0) else 0
        return max(f1,f2)

    isApproxNameUDF=udf(lambda x,y,z: is_approx_names(x,y,z),IntegerType())
    getSoundexUDF=udf(lambda x: get_soundex(x),StringType())
    getMetaphoneUDF=udf(lambda x: get_metaphone(x),StringType())

    getGeneralizedJWSimilarityeUDF=udf(lambda x,y,z: get_generalized_jw_simi(x,y,z),FloatType())
    getGeneralizedLevRatioUDF=udf(lambda x,y,z: get_generalized_lev_ratio(x,y,z),FloatType())
    getGeneralizedLevDamerauLevSimiUDF=udf(lambda x,y,z: get_generalized_damerauLevenshtein_simi(x,y,z),FloatType())

    firstNameinEmailUDF=udf(lambda x,y,z,t,e1,e2: get_name_in_email(x,y,z,t,e1,e2),IntegerType())

    ######################### First Name based features  #########################

    if 'first_nm_col' in featureCols:
      first_nm_col=featureCols['first_nm_col']
      f_nm_1=first_nm_col+'_1'
      f_nm_2=first_nm_col+'_2'
      f_nm_eith='either_null_'+first_nm_col
      tmp=tmp.withColumn('first_nm_1_sdx',getSoundexUDF(F.col(f_nm_1)))
      tmp=tmp.withColumn('first_nm_2_sdx',getSoundexUDF(F.col(f_nm_2)))
      tmp=tmp.withColumn('first_nm_1_mp',getMetaphoneUDF(F.col(f_nm_1)))
      tmp=tmp.withColumn('first_nm_2_mp',getMetaphoneUDF(F.col(f_nm_2)))

      tmp=tmp.withColumn('same_fn',F.when((F.col(f_nm_eith)==0) & (F.col(f_nm_1)==F.col(f_nm_2)),F.lit(1)).otherwise(F.lit(0)))
      tmp=tmp.withColumn('approx_fn',isApproxNameUDF(F.col(f_nm_1),F.col(f_nm_2),F.col(f_nm_eith)))
      tmp=tmp.withColumn('same_first_nm_sdx',F.when((F.col(f_nm_eith)==0) & (F.col('first_nm_1_sdx')==F.col('first_nm_2_sdx')),F.lit(1)).otherwise(F.lit(0)))
      tmp=tmp.withColumn('same_first_nm_mp',F.when((F.col(f_nm_eith)==0) & (F.col('first_nm_1_mp')==F.col('first_nm_2_mp')),F.lit(1)).otherwise(F.lit(0)))
      tmp=tmp.withColumn('jw_simi_fn',getGeneralizedJWSimilarityeUDF(F.col(f_nm_1),F.col(f_nm_2),F.col(f_nm_eith)))
      tmp=tmp.withColumn('lev_ratio_fn',getGeneralizedLevRatioUDF(F.col(f_nm_1),F.col(f_nm_2),F.col(f_nm_eith)))
      tmp=tmp.withColumn('damerau_lev_simi_fn',getGeneralizedLevDamerauLevSimiUDF(F.col(f_nm_1),F.col(f_nm_2),F.col(f_nm_eith)))


    ########################## Last Name based features  #########################

    if 'last_nm_col' in featureCols:
      last_nm_col=featureCols['last_nm_col']
      l_nm_1=last_nm_col+'_1'
      l_nm_2=last_nm_col+'_2'
      l_nm_eith='either_null_'+last_nm_col
      tmp=tmp.withColumn('last_nm_1_sdx',getSoundexUDF(F.col(l_nm_1)))
      tmp=tmp.withColumn('last_nm_2_sdx',getSoundexUDF(F.col(l_nm_2)))
      tmp=tmp.withColumn('last_nm_1_mp',getMetaphoneUDF(F.col(l_nm_1)))
      tmp=tmp.withColumn('last_nm_2_mp',getMetaphoneUDF(F.col(l_nm_2)))

      tmp=tmp.withColumn('same_ln',F.when((F.col(l_nm_eith)==0) & (F.col(l_nm_1)==F.col(l_nm_2)),F.lit(1)).otherwise(F.lit(0)))
      tmp=tmp.withColumn('approx_ln',isApproxNameUDF(F.col(l_nm_1),F.col(l_nm_2),F.col(l_nm_eith)))
      tmp=tmp.withColumn('same_last_nm_sdx',F.when((F.col(l_nm_eith)==0) & (F.col('last_nm_1_sdx')==F.col('last_nm_2_sdx')),F.lit(1)).otherwise(F.lit(0)))
      tmp=tmp.withColumn('same_last_nm_mp',F.when((F.col(l_nm_eith)==0) & (F.col('last_nm_1_mp')==F.col('last_nm_2_mp')),F.lit(1)).otherwise(F.lit(0)))
      tmp=tmp.withColumn('jw_simi_ln',getGeneralizedJWSimilarityeUDF(F.col(l_nm_1),F.col(l_nm_2),F.col(l_nm_eith)))
      tmp=tmp.withColumn('lev_ratio_ln',getGeneralizedLevRatioUDF(F.col(l_nm_1),F.col(l_nm_2),F.col(l_nm_eith)))
      tmp=tmp.withColumn('damerau_lev_simi_ln',getGeneralizedLevDamerauLevSimiUDF(F.col(l_nm_1),F.col(l_nm_2),F.col(l_nm_eith)))

    ######################### Email based features  #########################

    if 'email_address_col' in featureCols:
      email_address_col=featureCols['email_address_col']
      tmp=tmp.withColumn('same_email_address',F.when((F.col('either_null_'+email_address_col)==0) & (F.col(email_address_col+'_1')==F.col(email_address_col+'_2')),F.lit(1)).otherwise(F.lit(0)))

    ## approx_email_address is based on email_user column
    if 'email_user_col' in featureCols:
      email_user_col=featureCols['email_user_col']
      tmp=tmp.withColumn('approx_email_address',F.when((F.col('either_null_'+email_user_col)==0) & (F.col(email_user_col+'_1')==F.col(email_user_col+'_2')),F.lit(1)).otherwise(F.lit(0)))

    ## feature if name in emails: if one of first name in second email_user col and both these are non null
    if ('email_user_col' in featureCols) and ('first_nm_col' in featureCols):
      email_user_col=featureCols['email_user_col']
      first_nm_col=featureCols['first_nm_col']
      tmp=tmp.withColumn('first_nm_in_email',firstNameinEmailUDF(F.col(first_nm_col+'_1'),F.col(first_nm_col+'_2'),F.col('either_null_'+first_nm_col),F.col('either_null_'+email_user_col), F.col(email_user_col+'_1'),F.col(email_user_col+'_2')))

    ######################### Address based features  #########################
    if 'address_col' in featureCols:
      address_col=featureCols['address_col']
      tmp=tmp.withColumn('same_address',F.when((F.col('either_null_'+address_col)==0) & (F.col(address_col+'_1')==F.col(address_col+'_2')),F.lit(1)).otherwise(F.lit(0)))
      tmp=tmp.withColumn('jw_simi_address',getGeneralizedJWSimilarityeUDF(F.col(address_col+'_1'),F.col(address_col+'_2'),F.col('either_null_'+address_col)))

    ######################### Gender based features  #########################
    if 'gender_col' in featureCols:
      gender_col=featureCols['gender_col']
      tmp=tmp.withColumn('same_gender',F.when((F.col('either_null_'+gender_col)==0) & (F.col(gender_col+'_1')==F.col(gender_col+'_2')),F.lit(1)).otherwise(F.lit(0)))

    return tmp

  ############################   Train method  #############################################

  @staticmethod
  def train_xgboost(inputData, x_cols, targetCol):

    """

    inputData                   : a spark data frame of model fetures. See method probabilisticAlgorithm.get_model_features
                                  This contains both predictors column and target columns.
    x_cols                      : list of feature columns to be included in model training.
    targetCol                   : target column/label/Y columns. This is binary column taking 1 and 0.

    This method does a one time training of input data using xgboost algorithgm. Given the input data, x cols and target cols, it
    fits the algorithm to the training data and returns a pipeline model object. (pyspark.ml.PipelineModel)

    """


    import pyspark.sql.functions as F
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType, FloatType, BooleanType, StringType
    import pandas, string
    from xgboost.spark import SparkXGBClassifier
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml import PipelineModel
    from pyspark.ml import Pipeline

    if 'target' not in inputData.columns:
      inputData=inputData.withColumn('target',F.col(targetCol))

    vectorAssembler = VectorAssembler().setInputCols(x_cols).setOutputCol("features")
    xgboost = SparkXGBClassifier(features_col="features", label_col="target")
    pipeline = Pipeline().setStages([vectorAssembler, xgboost])
    model = pipeline.fit(inputData)

    return model

  ############################   Prediction method  ########################################

  @staticmethod
  def get_predictions(model, inputData, threshold=0.5):

    """

    model                       : is Pipeline fitted model. See probabilisticAlgorithm.train_xgboostmethod.
    inputData                   : a spark data frame of model fetures. See method probabilisticAlgorithm.get_model_features
                                  This contains only predictors columns (x cols). This dataframe contains all feature columns
                                  used to train the model otherwise this will throw an error.
    threshold                   : a value between 0 and 1. This is used to map predicted probability to binary values.
                                  For instance if using the default of predicted probability is at least 0.5,
                                  the record pairs are predicted to match (1) otherwise no match (0).
                                  Experiments have shown that a higher threshold such as 0.75 performs better in terms of reducing
                                  false positives but same time its more conservative.

    This method creats a prediction columns such as prob_1, prob_0 and prediction. These are predicted probability of
    class 1, class 0 and mapped probability prdiction using given threshold.

    Returns a dataframe with original data frame columns and new prediction columns as mentioned above.

    """
    import pyspark.sql.functions as F
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType, FloatType, BooleanType, StringType
    import pandas, string

    def get_pred_1_prob(x):
      try:
        return float(x[1])
      except ValueError:
        None

    getPred_1_ProbUDF=udf(lambda x: get_pred_1_prob(x),FloatType())

    df_pred=model.transform(inputData)
    df_pred=df_pred.withColumn('prob_1',getPred_1_ProbUDF(F.col('probability')))
    df_pred=df_pred.withColumn('prob_0',1-getPred_1_ProbUDF(F.col('probability')))
    df_pred=df_pred.withColumn('prediction',F.when(F.col('prob_1')>threshold,F.lit(1)).otherwise(F.lit(0)))

    return df_pred


####################################################################################
#####################   Connected Components Class   ###############################
####################################################################################

class connectedComponents:

  """

  This class implements connected component algorithm. The algorithm maps connected record pairs to same group.
  For instance if A and B are connected, B and C are connected, then algorithm will map A,B and C to same group.

  This algorithm leverages a local installaton of graphframes library on databricks cluster.


  """


  def __init__(self):

    import pyspark
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import udf

    self.spark = SparkSession.builder.master("local[1]").appName("MLIR").getOrCreate()


  def get_connected_components(self, dataframes, checkpoint_path=''):
    if not checkpoint_path or checkpoint_path=='':
      checkpoint_path="/tmp/graphframes-example-connected-components"

    """
    dataframes          : is a single dataframe or list of dataframes. Each dataframe (if list) must have card_nbr_1, card_nbr_2,
                          and possibly a prediction column. If prediction column is missing, it is assumed that all record pairs
                          are match

    checkpoint_path     : this is path where connected components algorithms write and reads during iterations of algorithm. the running process
                          must have a read and write access to this path.

    The method runs the connected components algorithm. Here is reference to doc:
    https://graphframes.github.io/graphframes/docs/_site/user-guide.html

    Outputs a dataframe on id (for instance card number) and component (connected id have same component)

    """

    from graphframes import GraphFrame
    import pyspark.sql.functions as F
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType, FloatType, BooleanType, StringType
    import pandas, string


    df_edges = self.spark.createDataFrame([('','')], ('src','dst'))

    if not isinstance(dataframes,list):
      df_edges=dataframes
    elif isinstance(dataframes,list):
      if len(dataframes)==1:
        df_edges=dataframes[0]
      else:
        for d in dataframes:
          df_edges=df_edges.union(d)

    df_edges=df_edges.dropDuplicates()

    df_edges=df_edges.withColumn('match',F.lit('yes'))
    df_vertices=df_edges.select('src').union(df_edges.select('dst').withColumnRenamed('dst','src')).dropDuplicates()
    df_vertices=df_vertices.withColumnRenamed('src','id').dropDuplicates()

    self.spark.sparkContext.setCheckpointDir(checkpoint_path)

    g = GraphFrame(df_vertices, df_edges)
    df_conn_comp = g.connectedComponents()

    return df_conn_comp


##################################################################################################
############################ Geenerate Shopper ID  Class. ########################################
##################################################################################################

class generateShopperID(snowflakeConnector):

  """
  This class implements shopper id generation. It works even when  card number has no PII info availble.
  app_id          : app id (system id) or a user id with access to snowflake tables.
  sf_dict         : config parameters for snowflake connection.
  db_dict         : config parameters for snowflake databases, schema, warehouse, role
  tab_prefix      : database and schema
  """

  def __init__(self,app_id,sf_dict,db_dict,tab_prefix=''):

    """
        constructor class to set config parameters to snowflake.
    """

    if app_id=='' or not app_id:
      app_id=''
    super().__init__(app_id)

    for k,v in sf_dict.items():
      self.set_sf(k,v)
    if db_dict:
      for k,v in db_dict.items():
        self.set_sf(k,v)

    import pyspark
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import udf

    self.spark = SparkSession.builder.master("local[1]").appName("MLIR").getOrCreate()

  def get_shopper_ids(self, tab_sf_df_pii, tab_sf_df_conn='',cols_shopper_id=[]):

    """
    tab_sf_df_pii           : is snowflake table with pii columns.
    tab_sd_df_conn          : is snowflake table which is output of connnected component algorithm.
    This method outputs the spark data frame with new shopper id  column.
    """

    import pyspark.sql.functions as F
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType, FloatType, BooleanType, StringType
    import pandas, string
    from pyblake2 import blake2b

    def get_hash_twenty(s):
      h = blake2b(digest_size=20)
      h.update(str(s).encode('utf-8'))
      return h.hexdigest()

    getHashTwnetyUDF=udf(lambda z:get_hash_twenty(z),StringType())

    ### if tab_sf_df_conn is empty string, create an empty table in snowflake

    if (tab_sf_df_conn==''):
      df_pii=self.read_snowflake(tab_sf_df_pii)
      df_pii=df_pii.withColumn('shopper_id',getHashTwnetyUDF('card_nbr'))
      self.write_snowflake(df_pii,tab_tc)

    ### if snowflake table for conn comp exists but empty

    if self.table_existence_check(tab_sf_df_conn):
      if (self.read_snowflake(tab_sf_df_conn).count()==0):
        df_pii=self.read_snowflake(tab_sf_df_pii)
        df_pii=df_pii.withColumn('shopper_id',getHashTwnetyUDF('card_nbr'))
        self.write_snowflake(df_pii,tab_tc)

    ### Run snowflake query to get first txn dte, first store, first upc for conn comp cards

    tab_tmp_conn=tab_sf_df_conn+'_tmp'

    query_conn_sf=f"""create or replace table {tab_tmp_conn} as
                  select A.*, component from {tab_sf_df_pii} A
                  inner join {tab_sf_df_conn} B
                  on a.card_nbr=b.id;
                  """

    try:
      self.run_snowflake_query(query_conn_sf, tab_tmp_conn)
    except Exception:
      raise Exception(f'{tab_tmp_conn} update Failed !!')

    tab_tmp_conn_ftd=tab_tmp_conn+'_ftd'
    tab_tmp_conn_ftd_store=tab_tmp_conn+'_ftd_store'
    tab_tmp_conn_ftd_store_upc=tab_tmp_conn+'_ftd_store_upc'
    tab_tmp_conn_ftd_store_upc_card=tab_tmp_conn+'_ftd_store_upc_card'
    tab_tmp_conn_ftd_store_upc_all_card=tab_tmp_conn+'_ftd_store_upc_all_card'

    query_ftd=f"""create or replace table {tab_tmp_conn_ftd} as
                  select component, min(first_txn_dte) first_txn_dte from
                  {tab_tmp_conn} group by 1;"""

    query_ftd_store=f"""create or replace table {tab_tmp_conn_ftd_store} as
    select t0.component, t0.first_txn_dte, min(t0.first_used_store_id) as first_used_store_id from {tab_tmp_conn} t0
    inner join {tab_tmp_conn_ftd} t1
    on t0.component=t1.component and t0.first_txn_dte=t1.first_txn_dte
    group by 1,2;"""


    query_ftd_store_upc=f"""create or replace table {tab_tmp_conn_ftd_store_upc} as
    select t0.component, t0.first_txn_dte, t0.first_used_store_id, min(first_upc) as first_upc from {tab_tmp_conn} t0
    inner join {tab_tmp_conn_ftd_store} t1
    on t0.component=t1.component and t0.first_txn_dte=t1.first_txn_dte and t0.first_used_store_id=t1.first_used_store_id
    group by 1,2,3;"""

    query_ftd_store_upc_card=f"""create or replace table {tab_tmp_conn_ftd_store_upc_card} as
    select t0.component, t0.first_txn_dte, t0.first_used_store_id, t0.first_upc, min(card_nbr) as shopper_first_card_nbr
    from {tab_tmp_conn} t0
    inner join {tab_tmp_conn_ftd_store_upc} t1
    on t0.component=t1.component and t0.first_txn_dte=t1.first_txn_dte
    and t0.first_used_store_id=t1.first_used_store_id and t0.first_upc=t1.first_upc
    group by 1,2,3,4;"""

    query_ftd_store_upc_all_cards=f"""create or replace table {tab_tmp_conn_ftd_store_upc_all_card} as
                                      select t0.*, t1.shopper_first_card_nbr from {tab_tmp_conn} t0
                                      inner join {tab_tmp_conn_ftd_store_upc_card} t1
                                      on t0.component=t1.component
                                      """

    try:
      self.run_snowflake_query(query_ftd, tab_tmp_conn_ftd)
    except Exception:
      raise Exception(f'{tab_tmp_conn_ftd} update Failed !!')

    try:
      self.run_snowflake_query(query_ftd_store, tab_tmp_conn_ftd_store)
    except Exception:
      raise Exception(f'{tab_tmp_conn_ftd_store} update Failed !!')

    try:
      self.run_snowflake_query(query_ftd_store_upc, tab_tmp_conn_ftd_store_upc)
    except Exception:
      raise Exception(f'{tab_tmp_conn_ftd_store_upc} update Failed !!')

    try:
      self.run_snowflake_query(query_ftd_store_upc_card, tab_tmp_conn_ftd_store_upc_card)
    except Exception:
      raise Exception(f'{tab_tmp_conn_ftd_store_upc_card} update Failed !!')

    try:
      self.run_snowflake_query(query_ftd_store_upc_all_cards, tab_tmp_conn_ftd_store_upc_all_card)
    except Exception:
      raise Exception(f'{tab_tmp_conn_ftd_store_upc_all_card} update Failed !!')


    # #################################### Create shopper_id  ######################################################

    query_conn_cards=f"""select A.*, B.shopper_first_card_nbr from {tab_sf_df_pii} A
                         inner join {tab_tmp_conn_ftd_store_upc_all_card} B
                         on A.card_nbr=B.card_nbr
                         """
    query_not_conn_cards=f"""select A.*, A.card_nbr as shopper_first_card_nbr from {tab_sf_df_pii} A
                              where not exists
                              (
                                select card_nbr from {tab_tmp_conn} B where a.card_nbr=b.card_nbr
                              )
                              """

    X0=self.read_snowflake(query_conn_cards)
    X1=self.read_snowflake(query_not_conn_cards)

    X0=X0.withColumn('shopper_id',getHashTwnetyUDF('shopper_first_card_nbr'))
    X1=X1.withColumn('shopper_id',getHashTwnetyUDF('shopper_first_card_nbr'))

    if cols_shopper_id==[]:
      cols_shopper_id=['shopper_id', 'card_nbr', 'phone', 'household_id', 'shopper_first_card_nbr', 'first_nm', 'last_nm', 'address', 'email_address', 'gender',  'email_user', 'first_used_store_id', 'first_used_zip_code', 'first_txn_dte', 'first_upc','first_nm_orig', 'last_nm_orig', 'address_old', 'address_line1', 'address_line2',  'email_address_old', 'len_add', 'len_add_comp' ,'dw_last_update_ts']


    X0=X0.select(*cols_shopper_id)
    X1=X1.select(*cols_shopper_id)

    return X0.union(X1)

  def update_shopper_id_full_table(self, tab_sf_incr_shopper_table, tab_sf_full_shopper_table, cols_shopper_id=[]):

    """
    tab_sf_incr_shopper_table         : is full path of incremental shopper ids table in snowflake. This is output
                                        dataframe from generateShopperID.get_shopper_ids
    tab_sf_full_shopper_table         : is full path of full shopper ids table in snowflake.
    incremental_update                : boolean if snowflake shopper id table is full or incremental update. For full
                                        updates, it overwrites the full table.
    cols_shopper_id                   : list of columns to keep in full shopper id table.
    This method updates the full shopper id table and saves in snowflake.
    """
    if not cols_shopper_id or cols_shopper_id==[]:
      cols_shopper_id=['shopper_id', 'card_nbr', 'phone', 'household_id', 'shopper_first_card_nbr', 'first_nm', 'last_nm', 'address', 'email_address', 'gender',  'email_user', 'first_used_store_id', 'first_used_zip_code', 'first_txn_dte', 'first_upc','first_nm_orig', 'last_nm_orig', 'address_old', 'address_line1', 'address_line2',  'email_address_old', 'len_add', 'len_add_comp', 'dw_last_update_ts']

    cols_shopper_id_str='A.'+", A.".join(cols_shopper_id)

    query=f"""insert into {tab_sf_full_shopper_table}
            (
            select {cols_shopper_id_str} from {tab_sf_incr_shopper_table} A
            where not exists
              (
                select card_nbr from {tab_sf_full_shopper_table} B where a.card_nbr=b.card_nbr
              )
            )
        """

    try:
      self.run_snowflake_query(query, tab_sf_full_shopper_table)
    except Exception:
      raise Exception(f'{tab_sf_full_shopper_table} update Failed !!')


##################################################################################################
################################## Geenerate HHID  Class. ########################################
##################################################################################################

class generateHHIDs(snowflakeConnector):

  """

  This class implements generating record pairs based on given blocking key such as phone.

  """


  def __init__(self, app_id, sf_dict, db_dict, cols_hhid):
    if app_id=='' or not app_id:
      app_id=''

    super().__init__(app_id)

    for k,v in sf_dict.items():
      self.set_sf(k,v)
    if db_dict:
      for k,v in db_dict.items():
        self.set_sf(k,v)

    import pyspark
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import udf

    self.spark = SparkSession.builder.master("local[1]").appName("MLIR").getOrCreate()


    """

    cols_hhid      : dict of pii columns from given PII table and derived PII columns such as gender,
                            cleaned email, address columns, derrived first txn dte, first store, first zip code,
                            first upc etc.


    """
    self.cols_hhid=cols_hhid

    self.card_nbr_col=self.cols_hhid['card_nbr_col']
    self.household_col=self.cols_hhid['household_col']
    self.phone_col=self.cols_hhid['phone_col']
    self.first_nm_col=self.cols_hhid['first_nm_col']
    self.last_nm_col=self.cols_hhid['last_nm_col']
    self.address_col=self.cols_hhid['address_col']
    self.email_address_col=self.cols_hhid['email_address_col']
    self.gender_col=self.cols_hhid['gender_col']
    self.email_user_col=self.cols_hhid['email_user_col']
    self.first_txn_dte_col=self.cols_hhid['first_txn_dte_col']
    self.first_used_store_id_col=self.cols_hhid['first_used_store_id_col']
    self.first_upc_col=self.cols_hhid['first_upc_col']
    self.len_add_col=self.cols_hhid['len_add_col']
    self.len_add_comp_col=self.cols_hhid['len_add_comp_col']
    self.first_used_zip_code_col=self.cols_hhid['first_used_zip_code_col']

  def get_record_pairs(self, tab_tc, tab_shopper_id, key_col):

    """

    tab_tc                : the table to be created. This is concatenate of PII of record pairs given blocking key.
                            Full table specification with database and schema and table name.
    tab_shopper_id        : is spark data frame containing shopper id and pii columns. See constructor class. It must consiste a
                            blocking column and shopper id column. See below.
    key_col               : is blocking key column such as email, phone. This must be present in the tmp dataframe.

    This method generates card numbers record pairs given a blocking key along with respective PII columns.
    For instance if phone number p is associated with 3 card numbers A,B, and C then (A,B), (B,C), and
    (A,c) are three record pairs will be generated in the resulting output.

    Note that the record pair generatio is different from shopper id in terms of scope. In shopper id, record pairs are predicted using hybrid model. Whereas for HHID, there is no model and instead using various blokcing keys, record pairs are generated, then their union, and lastly run the connected algorithm on these record pairs, assuming the card numbers which are part of record pair belong to same household. This is intuitive yet strong assumption.

    """

    import pyspark.sql.functions as F
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType, FloatType, BooleanType, StringType
    import pandas, string
    from itertools import chain

    ### when key_col is phone, email or hhid, already used in where condition

    if key_col in ['shopper_id', 'phone', 'email_address', 'household_id' , 'multi_key']:
      pass
    else:
      raise Exception('key_col must be one of shopper_id, phone, email_address, household_id or multi_key')

    main_query=f"""create or replace table {tab_tc} as
              select distinct t0.{key_col} as group_col, card_nbr_1, card_nbr_2, paired_cards from {tab_shopper_id} t0
              inner join
              (
                select distinct concat(card_nbr_1, '-', card_nbr_2) as paired_cards, {key_col}, card_nbr_1, card_nbr_2 from
                (
                    select distinct a.{key_col}, card_nbr_1, card_nbr_2 from
                    (
                      select distinct c.{key_col}, c.card_nbr card_nbr_1 from {tab_shopper_id} c
                      inner join
                      (
                          select {key_col}, count(distinct card_nbr) cnt from  {tab_shopper_id}
                          group by 1
                          having cnt<=10000 and cnt>1 --  key_col shoppers max limit=10k
                      ) d
                      on c.{key_col}=d.{key_col}
                      where c.{key_col} is not null and c.{key_col}!=''
                    ) a
                    inner join
                    (
                      select distinct {key_col}, card_nbr card_nbr_2 from {tab_shopper_id}
                      where {key_col} is not null and {key_col}!=''
                    ) b
                    on a.{key_col}=b.{key_col}
                    where a.card_nbr_1<b.card_nbr_2
                )
              ) t1
              on t0.card_nbr=t1.card_nbr_1;"""

    if key_col=='multi_key':

      ### when multi_key, first create the _tmp table and then use the main_query run with _tmp table instead of shopper_id

      tab_tc_tmp=tab_tc+'_tmp'
      main_query=main_query.replace(tab_shopper_id,tab_tc_tmp)

      query_0=f"""
                create or replace table {tab_tc_tmp} as
                select distinct A.*, concat_ws('-',left({self.phone_col},6), {self.address_col}, {self.first_nm_col}, {self.last_nm_col}, {self.first_used_store_id_col}) as multi_key
                from {tab_shopper_id} A
                where {self.phone_col} is not null and trim({self.phone_col})!=''
                and {self.address_col} is not null and trim({self.address_col})!=''
                and {self.first_nm_col} is not null and trim({self.last_nm_col})!=''
                and {self.last_nm_col} is not null and trim({self.last_nm_col})!=''
                and {self.first_used_store_id_col} is not null and trim({self.first_used_store_id_col})!=''
                and {self.len_add_col}>=3
                and {self.len_add_comp_col}>=3
                and len({self.first_nm_col})>=2 and len({self.last_nm_col})>=2
              """

      ### run the multi_key dependent query

      try:
        self.run_snowflake_query(query_0, tab_tc_tmp)
      except Exception:
        raise Exception(f'{tab_tc_tmp} creation Failed !!')

    ### Run the main query
    try:
      self.run_snowflake_query(main_query, tab_tc)
    except Exception:
      raise Exception(f'{tab_tc} update Failed !!')

  ### Get union of record pairs acrodd multiple keys

  def get_union_record_pairs(self, tab_tc, tab_sf_blocking_keys):

    """

    tab_tc                  : table to be created with full table specification. This will be union of record pairs
                              tables for each blocking key.
    tab_sf_blocking_keys       : list of snowflake record pair table names saved for each blocking keys.

    This method outputs a snowflake table as combined record pairs from one or more blocking keys. This will be
    input to connected component  algorithm.

    """

    query_empty=f"""
                    create or replace table {tab_tc}
                    (
                      card_nbr_1 varchar(256),
                      card_nbr_2 varchar(256),
                      paired_cards varchar(256)
                    );
                """

    ### when running for first time or table tab_tc does not exists, need to run the above query to create such table

    if isinstance(tab_sf_blocking_keys,list):
      if len(tab_sf_blocking_keys)==1:
        query=f"""
                  create or replace table {tab_tc} as
                  select * from {tab_sf_blocking_keys[0]}
                """
        try:
          self.run_snowflake_query(query, tab_tc)
        except Exception:
          self.run_snowflake_query(query_empty, tab_tc)
      else:

        query_truncate=f"""
                  truncate table {tab_tc}
                """
        try:
          self.run_snowflake_query(query_truncate, tab_tc)
        except Exception:
          self.run_snowflake_query(query_empty, tab_tc)

        for t in tab_sf_blocking_keys:
          query_insert=f"""
                  insert into {tab_tc}
                  (
                    select distinct card_nbr_1, card_nbr_2, paired_cards from {t} A
                    where not exists
                    (
                      select distinct paired_cards from {tab_tc}
                      B where a.paired_cards=b.paired_cards
                    )
                  )
                """
          try:
            self.run_snowflake_query(query_insert, t)
          except Exception:
            raise Exception(f'{t} inserttion Failed !!')

    else:
      if self.table_existence_check(tab_sf_blocking_keys):
        query=f"""
                  create or replace table {tab_tc} as
                  select * from {tab_sf_blocking_keys}
                """
        try:
          self.run_snowflake_query(query, tab_tc)
        except Exception:
          self.run_snowflake_query(query_empty, tab_tc)
      else:
        raise Exception(f"""{tab_sf_blocking_keys} must be either snowflake table or list of snowflake tables.""")


  def get_hhid_connected_components(self, dataframes, checkpoint_path=''):

    print('\nRunning connected component algorihm for householding!!!\n')

    if not checkpoint_path or checkpoint_path=='':
      checkpoint_path="/tmp/graphframes-example-connected-components"

    """
    dataframes          : is a single dataframe or list of dataframes. Each dataframe (if list) must have card_nbr_1, card_nbr_2.

    checkpoint_path     : this is path where connected components algorithms write and reads during iterations of algorithm. the running process
                          must have a read and write access to this path.

    The method runs the connected components algorithm. Here is reference to doc:
    https://graphframes.github.io/graphframes/docs/_site/user-guide.html

    Outputs a dataframe on id (for instance card number) and component (connected id have same component)

    """

    from graphframes import GraphFrame
    import pyspark.sql.functions as F
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType, FloatType, BooleanType, StringType
    import pandas, string


    df_edges = self.spark.createDataFrame([('','')], ('src','dst'))

    if not isinstance(dataframes,list):
      df_edges=dataframes
    elif isinstance(dataframes,list):
      if len(dataframes)==1:
        df_edges=dataframes[0]
      else:
        for d in dataframes:
          df_edges=df_edges.union(d)

    df_edges=df_edges.dropDuplicates()
    df_edges=df_edges.filter(df_edges.src!='')

    df_edges=df_edges.withColumn('match',F.lit('yes'))
    df_vertices=df_edges.select('src').union(df_edges.select('dst').withColumnRenamed('dst','src')).dropDuplicates()
    df_vertices=df_vertices.withColumnRenamed('src','id').dropDuplicates()

    self.spark.sparkContext.setCheckpointDir(checkpoint_path)

    g = GraphFrame(df_vertices, df_edges)
    df_conn_comp = g.connectedComponents()

    return df_conn_comp


  def get_hhids(self, tab_sf_shopper_id, tab_sf_hhid_conn_comp='', ):

    """

    tab_sf_shopper_id       : is snowflake table with shopper id and txn pii  columns
    tab_sf_hhid_conn_comp   : is snowflake table which is output of connnected component algorithm.

    This method outputs the spark data frame with new hhid column.

    """

    import pyspark.sql.functions as F
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType, FloatType, BooleanType, StringType
    import pandas, string
    from pyblake2 import blake2b

    def get_hash_twenty(s):
      h = blake2b(digest_size=20)
      h.update(str(s).encode('utf-8'))
      return h.hexdigest()

    getHashTwnetyUDF=udf(lambda z:get_hash_twenty(z),StringType())

    ### if tab_sf_hhid_conn_comp is empty string, create an empty table in snowflake

    df_pii=self.read_snowflake(tab_sf_shopper_id)

    if (tab_sf_hhid_conn_comp==''):
      df_pii=self.read_snowflake(tab_sf_shopper_id)
      df_pii=df_pii.withColumn('hhid',getHashTwnetyUDF('card_nbr'))
      return df_pii

    ### if snowflake table for conn comp exists but empty

    if self.table_existence_check(tab_sf_hhid_conn_comp):
      if (self.read_snowflake(tab_sf_hhid_conn_comp).count()==0):
        df_pii=self.read_snowflake(tab_sf_shopper_id)
        df_pii=df_pii.withColumn('hhid',getHashTwnetyUDF('card_nbr'))
        return df_pii

    ### Run snowflake query to get first txn dte, first store, first upc for conn comp cards

    tab_tmp_conn=tab_sf_hhid_conn_comp+'_tmp'

    query_conn_sf=f"""create or replace table {tab_tmp_conn} as
                  select A.*, component from {tab_sf_shopper_id} A
                  inner join {tab_sf_hhid_conn_comp} B
                  on a.card_nbr=b.id;
                  """

    try:
      self.run_snowflake_query(query_conn_sf, tab_tmp_conn)
    except Exception:
      raise Exception(f'{tab_tmp_conn} update Failed !!')

    tab_tmp_conn_ftd=tab_tmp_conn+'_ftd'
    tab_tmp_conn_ftd_store=tab_tmp_conn+'_ftd_store'
    tab_tmp_conn_ftd_store_upc=tab_tmp_conn+'_ftd_store_upc'
    tab_tmp_conn_ftd_store_upc_card=tab_tmp_conn+'_ftd_store_upc_card'
    tab_tmp_conn_ftd_store_upc_all_card=tab_tmp_conn+'_ftd_store_upc_all_card'

    query_ftd=f"""create or replace table {tab_tmp_conn_ftd} as
                  select component, min(first_txn_dte) first_txn_dte from
                  {tab_tmp_conn} group by 1;"""

    query_ftd_store=f"""create or replace table {tab_tmp_conn_ftd_store} as
    select t0.component, t0.first_txn_dte, min(t0.first_used_store_id) as first_used_store_id from {tab_tmp_conn} t0
    inner join {tab_tmp_conn_ftd} t1
    on t0.component=t1.component and t0.first_txn_dte=t1.first_txn_dte
    group by 1,2;"""


    query_ftd_store_upc=f"""create or replace table {tab_tmp_conn_ftd_store_upc} as
    select t0.component, t0.first_txn_dte, t0.first_used_store_id, min(first_upc) as first_upc from {tab_tmp_conn} t0
    inner join {tab_tmp_conn_ftd_store} t1
    on t0.component=t1.component and t0.first_txn_dte=t1.first_txn_dte
    and t0.first_used_store_id=t1.first_used_store_id
    group by 1,2,3;"""

    query_ftd_store_upc_card=f"""create or replace table {tab_tmp_conn_ftd_store_upc_card} as
    select t0.component, t0.first_txn_dte, t0.first_used_store_id, t0.first_upc, min(shopper_first_card_nbr) as hhid_first_card_nbr
    from {tab_tmp_conn} t0
    inner join {tab_tmp_conn_ftd_store_upc} t1
    on t0.component=t1.component and t0.first_txn_dte=t1.first_txn_dte
    and t0.first_used_store_id=t1.first_used_store_id and t0.first_upc=t1.first_upc
    group by 1,2,3,4;"""

    query_ftd_store_upc_all_cards=f"""create or replace table {tab_tmp_conn_ftd_store_upc_all_card} as
                                      select t0.*, t1.hhid_first_card_nbr from {tab_tmp_conn} t0
                                      inner join {tab_tmp_conn_ftd_store_upc_card} t1
                                      on t0.component=t1.component
                                      """

    try:
      self.run_snowflake_query(query_ftd, tab_tmp_conn_ftd)
    except Exception:
      raise Exception(f'{tab_tmp_conn_ftd} creation Failed !!')

    try:
      self.run_snowflake_query(query_ftd_store, tab_tmp_conn_ftd_store)
    except Exception:
      raise Exception(f'{tab_tmp_conn_ftd_store} creation Failed !!')

    try:
      self.run_snowflake_query(query_ftd_store_upc, tab_tmp_conn_ftd_store_upc)
    except Exception:
      raise Exception(f'{tab_tmp_conn_ftd_store_upc} creation Failed !!')

    try:
      self.run_snowflake_query(query_ftd_store_upc_card, tab_tmp_conn_ftd_store_upc_card)
    except Exception:
      raise Exception(f'{tab_tmp_conn_ftd_store_upc_card} creation Failed !!')

    try:
      self.run_snowflake_query(query_ftd_store_upc_all_cards, tab_tmp_conn_ftd_store_upc_all_card)
    except Exception:
      raise Exception(f'{tab_tmp_conn_ftd_store_upc_all_card} creation Failed !!')

    # #################################### Create hhid  ######################################################

    query_conn_cards=f"""select A.*, B.hhid_first_card_nbr from {tab_sf_shopper_id} A
                         inner join {tab_tmp_conn_ftd_store_upc_all_card} B
                         on A.card_nbr=B.card_nbr
                         """
    query_not_conn_cards=f"""select A.*, A.card_nbr as hhid_first_card_nbr from {tab_sf_shopper_id} A
                              where not exists
                              (
                                select card_nbr from {tab_tmp_conn} B where a.card_nbr=b.card_nbr
                              )
                              """

    X0=self.read_snowflake(query_conn_cards)
    X1=self.read_snowflake(query_not_conn_cards)

    X0=X0.withColumn('hhid',getHashTwnetyUDF('hhid_first_card_nbr'))
    X1=X1.withColumn('hhid',getHashTwnetyUDF('hhid_first_card_nbr'))

    cols_hhid_list=list(cols_hhid.values())

    X0=X0.select(*cols_hhid_list)
    X1=X1.select(*cols_hhid_list)

    Z=X0.union(X1)

    return Z


  def update_hhid_full_table(self, tab_sf_incr_hhid_table, tab_sf_full_hhid_table, cols_hhid_final):

    """
    tab_sf_incr_hhid_table         : is full path of incremental shopper ids table in snowflake. This is output
                                        dataframe from generateShopperID.get_hhids
    tab_sf_full_hhid_table         : is full path of full shopper ids table in snowflake.

    cols_hhid_final                : dict of columns to keep in full shopper id table.

    This method updates the full hhid table and saves in snowflake.
    """

    cols_hhid_final_list=list(cols_hhid_final.values())

    cols_hhid_str='A.'+", A.".join(cols_hhid_final_list)

    query=f"""insert into {tab_sf_full_hhid_table}
            (
            select {cols_hhid_str} from {tab_sf_incr_hhid_table} A
            where not exists
              (
                select card_nbr from {tab_sf_full_hhid_table} B where a.card_nbr=b.card_nbr
              )
            )
        """

    try:
      self.run_snowflake_query(query, tab_sf_full_hhid_table)
    except Exception:
      raise Exception(f'{tab_sf_full_hhid_table} update Failed !!')
