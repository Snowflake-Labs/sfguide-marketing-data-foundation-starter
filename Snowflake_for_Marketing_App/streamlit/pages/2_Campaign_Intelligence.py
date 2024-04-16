# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import numpy as np

from datetime import date
st.set_page_config(layout="wide")

## defining functions used in the app

@st.cache_data(ttl=2000)
def get_country_group(_sp_session, app_db, app_sch, table_name):
   df = _sp_session.sql("select geo_country, count(*) as counts from {0}.{1}.{2} group by geo_country having counts > 5000 order by counts desc ;".format(app_db, app_sch, table_name)).to_pandas()
   return df


@st.cache_data(ttl=2000)
def get_overall_users(_sp_session, app_db, app_sch, table_name):
   overall_user_counts = _sp_session.sql("select distinct count(user_id) from {0}.{1}.{2};".format(app_db, app_sch, table_name)).collect()[0][0]
   return overall_user_counts


@st.cache_data(ttl=2000)
def get_spend_by_year(_sp_session, app_db, app_sch, table_name):
   df_out = _sp_session.sql("with spend_data as (select date_part('YEAR', date_day) as year_data, platform, sum(spend) as overall_spend from {0}.{1}.{2} group by 1, 2 order by year_data) SELECT YEAR_DATA, \"'facebook_ads'\" as FACEBOOK_ADS, \"'linkedin_ads'\" as LINKEDIN_ADS  FROM spend_data PIVOT(SUM(overall_spend) FOR platform IN ('facebook_ads', 'linkedin_ads')) AS p ORDER BY year_data;".format(app_db, app_sch, table_name)).to_pandas()
   return df_out

@st.cache_data(ttl=2000)
def get_impressions_by_year(_sp_session, app_db, app_sch, table_name):
   df_out = _sp_session.sql("with spend_data as (select date_part('YEAR', date_day) as year_data, platform, sum(impressions) as overall_impressions from {0}.{1}.{2} group by 1, 2 order by year_data) SELECT YEAR_DATA, \"'facebook_ads'\" as FACEBOOK_ADS, \"'linkedin_ads'\" as LINKEDIN_ADS  FROM spend_data PIVOT(SUM(overall_impressions) FOR platform IN ('facebook_ads', 'linkedin_ads')) AS p ORDER BY year_data;".format(app_db, app_sch, table_name)).to_pandas()
   return df_out


@st.cache_data(ttl=2000)
def get_account_spend_by_year(_sp_session, app_db, app_sch, table_name):
   df_out = _sp_session.sql("with spend_data as (select date_part('YEAR', date_day) as year_data, ACCOUNT_NAME, sum(spend) as overall_spend from {0}.{1}.{2} group by 1, 2 order by year_data) SELECT YEAR_DATA, \"'ABC Corp NA'\" as ABC_CORP_NA, \"'ABC Corp APJ'\" as ABC_CORP_APJ, \"'ABC Corp Demand Gen'\" as ABC_CORP_DEMAND_GEN, \"'ABC Corp EMEA'\" as ABC_CORP_EMEA, \"'ABC Corp'\" as ABC_CORP_ROW, \"'ABC Corp Global'\" as ABC_CORP_GLOBAL  FROM spend_data PIVOT(SUM(overall_spend) FOR ACCOUNT_NAME IN ('ABC Corp NA', 'ABC Corp APJ', 'ABC Corp Demand Gen', 'ABC Corp EMEA', 'ABC Corp', 'ABC Corp Global')) AS p ORDER BY year_data;".format(app_db, app_sch, table_name)).to_pandas()
   return df_out


@st.cache_data(ttl=2000)
def get_total_spend(_sp_session, app_db, app_sch, table_name):
   total_spend = _sp_session.sql("select sum(spend) as overall_spend from {0}.{1}.{2}; ".format(app_db, app_sch, table_name)).collect()[0][0]
   return total_spend

@st.cache_data(ttl=2000)
def get_spend_per_day(_sp_session, app_db, app_sch, table_name):
   df_spend_per_day = _sp_session.sql("select to_date(date_day) as date, sum(spend) as spend from {0}.{1}.{2} group by 1 order by date;".format(app_db, app_sch, table_name)).to_pandas()
   return df_spend_per_day

@st.cache_data(ttl=2000)
def get_clicks_per_day(_sp_session, app_db, app_sch, table_name):
   df_clicks_per_day = _sp_session.sql("select to_date(date_day) as date, sum(clicks) as clicks from {0}.{1}.{2} group by 1 order by date;".format(app_db, app_sch, table_name)).to_pandas()
   return df_clicks_per_day


@st.cache_data(ttl=2000)
def get_spend_per_clicks(_sp_session, app_db, app_sch, table_name):
   df_spend_per_clicks = _sp_session.sql("select to_date(date_day) as date, round(sum(spend)/NULLIF(sum(clicks),0)) as SPEND_PER_CLICKS from {0}.{1}.{2} group by 1 order by date;".format(app_db, app_sch, table_name)).to_pandas()
   return df_spend_per_clicks


@st.cache_data(ttl=2000)
def get_top_5_spend_per_clicks(_sp_session, app_db, app_sch, table_name):
   df_top_5_spend_per_clicks = _sp_session.sql("select campaign_name, sum(spend)/NULLIF(sum(clicks),0) as SPEND_PER_CLICKS from {0}.{1}.{2} group by 1 order by SPEND_PER_CLICKS desc limit 5;".format(app_db, app_sch, table_name)).to_pandas()
   return df_top_5_spend_per_clicks


@st.cache_data(ttl=2000)
def get_top_5_ads_with_impressions(_sp_session, app_db, app_sch, table_name):
   df_top_5_ads_with_impressions = _sp_session.sql("select AD_NAME, sum(IMPRESSIONS) as IMPRESSIONS from {0}.{1}.{2} group by 1 order by IMPRESSIONS desc limit 5;".format(app_db, app_sch, table_name)).to_pandas()
   return df_top_5_ads_with_impressions


@st.cache_data(ttl=2000)
def get_top_5_ads_with_clicks(_sp_session, app_db, app_sch, table_name):
   df_top_5_ads_with_impressions = _sp_session.sql("select AD_NAME, sum(CLICKS) as CLICKS from {0}.{1}.{2} group by 1 order by CLICKS desc limit 5;".format(app_db, app_sch, table_name)).to_pandas()
   return df_top_5_ads_with_impressions


def format_num(num):
    if num > 1000000:
        if not num % 1000000:
            return f'{num // 1000000}M'
        return f'{round(num / 1000000, 1)}M'
    return f'{num // 1000}K'



# Get the current credentials -- App way
# UNCOMMENT FOR NATIVE APP STARTS ##
sp_session = get_active_session()
# UNCOMMENT FOR NATIVE APP ENDS ##

# COMMENT FOR NATIVE APP STARTS ##
# ### Get the current credentials -- Local Streamlit way
# import sys
# sys.path.append('solution/2-Streamlit/python/lutils')
# import sflk_base as L

# # Define the project home directory, this is used for locating the config.ini file
# PROJECT_HOME_DIR='.'
# def initialize_snowpark():
#     if "snowpark_session" not in st.session_state:
#         config = L.get_config(PROJECT_HOME_DIR)
#         sp_session = L.connect_to_snowflake(PROJECT_HOME_DIR)
#         sp_session.use_role(f'''{config['APP_DB']['role']}''')
#         sp_session.use_schema(f'''{config['APP_DB']['database']}.{config['APP_DB']['schema']}''')
#         sp_session.use_warehouse(f'''{config['APP_DB']['warehouse']}''')
#         st.session_state['snowpark_session'] = sp_session

#     else:
#         config = L.get_config(PROJECT_HOME_DIR)
#         sp_session = st.session_state['snowpark_session']
    
#     return (config, sp_session)

# COMMENT FOR NATIVE APP ENDS ##

# App
st.title("Campaign Intelligence Starter")
st.write(
   """
   Campaign Intelligence Starter demonstrates how organizations can easily unify their digital advertising data across popular advertising platforms in Snowflake and increase their digital advertising ROI.
   
   """
)

with st.container():
   # UNCOMMENT FOR NATIVE APP STARTS ##
    app_db = sp_session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
    app_sch = 'CRM'
    fun_db = app_db
    fun_sch = 'PYTHON_FUNCTIONS'
    app_flag = True
    # UNCOMMENT FOR NATIVE APP ENDS ##
    
    # COMMENT FOR NATIVE APP STARTS ##
    # config, sp_session = initialize_snowpark()
    # app_db = config['APP_DB']['database']
    # app_sch = config['APP_DB']['schema']
    # fun_db = app_db
    # fun_sch = app_sch
    # app_flag = False
    # COMMENT FOR NATIVE APP ENDS ##
    colxx, colxxx, colyy, colx, coly, colz = st.columns(6)
    with colz:
      if st.button('Refresh Page', use_container_width=False, key='refresh1'):
         st.experimental_rerun()
    st.header("Campaign Intelligence Starter Summary Stats")
    st.subheader(':blue[Summary Stats]')
   # with st.expander("CampaignIntelligence-insights"):
    st.write("In this section, we will see some generic charts.")
    base_table = 'campaign72_view'
    col1, col2, col3 = st.columns(3)
    overall_spend = get_total_spend(sp_session, app_db, app_sch,base_table)
    #user_counts = get_users_in_lastNdays(sp_session, app_db, app_sch,base_table, 10)
    #revenue = get_revenue_lastNdays(sp_session, app_db, app_sch,base_table, 10)
    with col1:
     df_spend_per_day = get_spend_per_day(sp_session, app_db, app_sch,base_table)
     st.line_chart(df_spend_per_day, x='DATE', y='SPEND', use_container_width=True)
     st.metric(":orange[Overall Spend]", "$ {0}".format(format_num(overall_spend)))
    with col2:
     df_clicks_per_day = get_clicks_per_day(sp_session, app_db, app_sch,base_table)
     st.line_chart(df_clicks_per_day, x='DATE', y='CLICKS', use_container_width=True)
     Total_clicks = df_clicks_per_day['CLICKS'].sum()
     st.metric(":orange[Overall Clicks]", format_num(Total_clicks))
    with col3:
     df_spend_per_clicks = get_spend_per_clicks(sp_session, app_db, app_sch,base_table)
     st.line_chart(df_spend_per_clicks, x='DATE', y='SPEND_PER_CLICKS', use_container_width=True)
     avg_spend_per_clicks = df_spend_per_clicks['SPEND_PER_CLICKS'].sum()/df_spend_per_clicks.shape[0]
     st.metric(":orange[Average Spend/Clicks]", "$ {0}".format(round(avg_spend_per_clicks, 3)))
    st.divider()
    col4, col5, col6 = st.columns(3)
    with col4:
     st.subheader("Platform Spend over Years")
     df_spend = get_spend_by_year(sp_session, app_db, app_sch,base_table)
     st.bar_chart(
              df_spend, x="YEAR_DATA", y=["FACEBOOK_ADS", "LINKEDIN_ADS"]  # Optional
                 )
     Total_Linkedin_spend = df_spend['LINKEDIN_ADS'].sum()
     Total_FB_spend = df_spend['FACEBOOK_ADS'].sum()
     st.metric(":orange[Linkedin_investment]", "$ {0}".format(format_num(Total_Linkedin_spend)))
     st.metric(":orange[Facebook_investment]", "$ {0}".format(format_num(Total_FB_spend)))
    with col5:
     st.subheader("Platform and impressions")
     df_impressions = get_impressions_by_year(sp_session, app_db, app_sch,base_table)
     st.bar_chart(
              df_impressions, x="YEAR_DATA", y=["FACEBOOK_ADS", "LINKEDIN_ADS"]  # Optional
                 )
     Total_Linkedin_impressions = df_impressions['LINKEDIN_ADS'].sum()
     Total_FB_impressions = df_impressions['FACEBOOK_ADS'].sum()
     st.metric(":orange[Linkedin_impressions]", "{0}".format(format_num(Total_Linkedin_impressions)))
     st.metric(":orange[Facebook_impressions]", "{0}".format(format_num(Total_FB_impressions)))
    with col6:
     st.subheader("Spend by Ad Account")
     df_account_spend_by_year = get_account_spend_by_year(sp_session, app_db, app_sch,base_table)
     st.bar_chart(
              df_account_spend_by_year, x="YEAR_DATA", y=["ABC_CORP_NA", "ABC_CORP_APJ", 
                                                  "ABC_CORP_DEMAND_GEN", "ABC_CORP_EMEA", "ABC_CORP_ROW", "ABC_CORP_GLOBAL"]  # Optional
                 )
     Total_ABC_CORP_NA = df_account_spend_by_year['ABC_CORP_NA'].sum()
     Total_ABC_CORP_APJ = df_account_spend_by_year['ABC_CORP_APJ'].sum()
     Total_ABC_CORP_DEMAND_GEN = df_account_spend_by_year['ABC_CORP_DEMAND_GEN'].sum()
     Total_ABC_CORP_EMEA = df_account_spend_by_year['ABC_CORP_EMEA'].sum()
     Total_ABC_CORP_ROW = df_account_spend_by_year['ABC_CORP_ROW'].sum()
     Total_ABC_CORP_GLOBAL = df_account_spend_by_year['ABC_CORP_GLOBAL'].sum()
     
     coli, colj, colk= st.columns(3) 
     coll, colm, coln = st.columns(3)
    
     coli.metric(":orange[NA Spend]", "$ {0}".format(format_num(Total_ABC_CORP_NA)))
     colj.metric(":orange[APJ Spend]", "$ {0}".format(format_num(Total_ABC_CORP_APJ)))
     colk.metric(":orange[DEMAND_GEN Spend]", "$ {0}".format(format_num(Total_ABC_CORP_DEMAND_GEN)))
     coll.metric(":orange[EMEA Spend]", "$ {0}".format(format_num(Total_ABC_CORP_EMEA)))
     colm.metric(":orange[ROW Spend]", "$ {0}".format(format_num(Total_ABC_CORP_ROW)))
     coln.metric(":orange[GLOBAL Spend]", "$ {0}".format(format_num(Total_ABC_CORP_GLOBAL)))
    
    st.divider()
    col7, col8, col9 = st.columns(3)
    with col7:
     st.subheader(":blue[Top 5 Campaign with highest spend/clicks]")
     df_top_5_spend_per_clicks = get_top_5_spend_per_clicks(sp_session, app_db, app_sch,base_table)
     st.dataframe(df_top_5_spend_per_clicks, use_container_width=True)
    with col8:
     st.subheader(":blue[Top 5 Ads with highest impressions]")
     df_top_5_ads_with_impressions = get_top_5_ads_with_impressions(sp_session, app_db, app_sch,base_table)
     st.dataframe(df_top_5_ads_with_impressions, use_container_width=True )
    with col9:
     st.subheader(":blue[Top 5 Ads with highest clicks]")
     df_top_5_ads_with_clicks = get_top_5_ads_with_clicks(sp_session, app_db, app_sch,base_table)
     st.dataframe(df_top_5_ads_with_clicks, use_container_width=True)