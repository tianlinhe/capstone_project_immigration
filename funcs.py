import seaborn as sns
from pyspark.sql.functions import isnan, when, col, count

def show_nan_pd(df):
    """Given a pandas df, visualize the %nan in each column
    :param df: pandas dataframe
    :return:
    """
    # count % nan in each column
    dfnull=df.isnull().sum()/len(df)*100

    # show only columns containing nan
    dfnull=dfnull[dfnull>0]
    dfnull.name='missing values (%)'
    print (dfnull)

    # plot columns with missing values in hor. barplot
    sns.barplot(y=dfnull.index,x=dfnull)

def show_nan_spark(df):
    """show missing values in a spark dataframe in barplot
    :param df: spark dataframe
    """
    # create a dataframe with missing values count per column
    dfnull=df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns])

    # show only columns with missing values
    cols=[k for (k,v) in dfnull.collect()[0].asDict().items() if v>0]
    
    # calculate % of missing values
    dfnull=dfnull.select(cols).toPandas()/df.count()*100
    
    # assign x-axis name
    dfnull.rename({0:'missing values (%)'},inplace=True)

    # plot columns with missing values in hor. barplot
    sns.barplot(y=dfnull.columns,x=dfnull.iloc[0])

def clean_pd(df,dropna_mode,idx):
    """clean a dataframe by 
    removing cols with many missing values,
    and rows which contains missing values only
    and duplicated index rows
    :param df: pandas dataframe
    :param dropna_mode: 'any' or 'all'
    :param idx: a string, or a list of string
    """
    print ('df.shape before cleaning',df.shape)
    
    # remove cols with >50% missing values
    df=df[df.columns[df.isnull().sum()/len(df)<0.5]]
    print ('df.shape after dropping columns with >50% nan', df.shape)
    #print (df.columns)
    
    # remove empty rows
    # for df with few missing values, it is save to drop any
    # for df with a lot of missing values, we drop rows that are completely empty
    df=df.dropna(how=dropna_mode)
    print ('df.shape after dropping empty rows', df.shape)
    
    # drop duplicated rows with subset=idx
    df=df.drop_duplicates(subset=idx)
    print ('df.shape after dropping duplicated rows', df.shape)
    
    return df

def clean_spark(df,dropna_mode,idx):
    """clean a dataframe by 
    removing cols with many missing values,
    and rows which contains missing values only
    and duplicated index rows
    :param df: spark dataframe
    :param idx: a list of string, identifier
    """
        
    print (f'df.shape before cleaning ({df.count()},{len(df.columns)})')
    
    # create a dataframe with missing values count per column
    dfnull=df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns])
    
    # select cols with <50% missing balues
    cols=[k for (k,v) in dfnull.collect()[0].asDict().items() if v/df.count()<0.5]
    df=df.select(cols)
    print ('len(df.columns) after dropping columns with >50% nan', len(df.columns))

    # drop row with missing values ONLY
    df=df.dropna(how=dropna_mode)
    print ('df.count after dropping empty rows', df.count())
    
    # drop duplicated rows
    df=df.dropDuplicates(subset=idx)
    print ('df.count after dropping duplicated rows', df.count())
    
    return df