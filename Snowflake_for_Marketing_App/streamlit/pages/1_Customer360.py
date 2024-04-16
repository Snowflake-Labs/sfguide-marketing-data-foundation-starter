# Import python packages
import streamlit as st
# UNCOMMENT FOR NATIVE APP STARTS ##
from snowflake.snowpark.context import get_active_session
# UNCOMMENT FOR NATIVE APP ENDS ##
import pandas as pd
import numpy as np
import pydeck as pdk
import altair as alt

from datetime import date
st.set_page_config(layout="wide")

## defining functions used in the app

@st.cache_data(ttl=2000)
def get_country_group(_sp_session, app_db, app_sch, table_name):
   df = _sp_session.sql("select geo_country, count(*) as counts from {0}.{1}.{2} group by geo_country having counts > 5000 order by counts desc ;".format(app_db, app_sch, table_name)).to_pandas()
   return df


@st.cache_data(ttl=2000)
def get_country_lat(_sp_session, app_db, app_sch, table_name):
   df = _sp_session.sql("select LAT as LATITUDE, LNG  as longitude, count(*) as counts from {0}.{1}.{2} group by latitude, longitude having counts > 5000 order by counts desc ;".format(app_db, app_sch, table_name)).to_pandas()
   return df


@st.cache_data(ttl=2000)
def get_overall_users(_sp_session, app_db, app_sch, table_name):
   overall_user_counts = _sp_session.sql("WITH UserInfo AS ( \
    SELECT user_pseudo_id, MAX(iff(event_name IN ('first_visit', 'first_open'), 1, 0)) AS is_new_user FROM {0}.{1}.{2} \
    GROUP BY 1) SELECT COUNT(*) AS user_count, SUM(is_new_user) AS new_user_count FROM UserInfo;".format(app_db, app_sch, table_name)).collect()[0][0]
   return overall_user_counts


@st.cache_data(ttl=2000)
def get_users_in_lastNdays(_sp_session, app_db, app_sch, table_name, day_since):
   overall_user_counts = _sp_session.sql("WITH UserInfo AS ( \
    SELECT user_pseudo_id, MAX(iff(event_name IN ('first_visit', 'first_open'), 1, 0)) AS is_new_user FROM {0}.{1}.{2} \
    GROUP BY 1) SELECT COUNT(*) AS user_count, SUM(is_new_user) AS new_user_count FROM UserInfo;".format(app_db, app_sch, table_name)).collect()[0][1]
   return overall_user_counts


@st.cache_data(ttl=2000)
def get_revenue_lastNdays(_sp_session, app_db, app_sch, table_name, day_since):
   user_counts = _sp_session.sql("select sum(user_ltv_revenue) as revenue from {0}.{1}.{2} where event_date > dateadd(day, -{3}, current_date());".format(app_db, app_sch, table_name, day_since)).collect()[0][0]
   return user_counts

@st.cache_data(ttl=2000)
def get_session_count(_sp_session, app_db, app_sch, table_name):
   sess_count = _sp_session.sql("SELECT event_name, COUNT(*) AS event_count FROM {0}.{1}.{2} WHERE event_name IN ('session_start') GROUP BY 1".format(app_db, app_sch, table_name)).collect()[0][1]
   return sess_count

@st.cache_data(ttl=2000)
def get_tot_page_view(_sp_session, app_db, app_sch, table_name):
   tot_page_view = _sp_session.sql("WITH UserInfo AS ( SELECT user_pseudo_id, count_if(event_name = 'page_view') AS page_view_count, count_if(event_name IN ('user_engagement', 'purchase')) AS purchase_event_count FROM {0}.{1}.{2} GROUP BY 1) select SUM(page_view_count) AS total_page_views, SUM(page_view_count) / COUNT(*) AS avg_page_views FROM UserInfo;".format(app_db, app_sch, table_name)).collect()[0]
   return tot_page_view


@st.cache_data(ttl=2000)
def get_purchase_time(_sp_session, app_db, app_sch, table_name):
   df_out = _sp_session.sql("SELECT event_date, iff(avg(iff(COALESCE(v.value:value:int_value, v.value:value:float_value, v.value:value:double_value)::int = COALESCE(v.value:value:int_value, v.value:value:float_value, v.value:value:double_value), COALESCE(v.value:value:int_value, v.value:value:float_value, v.value:value:double_value), 0))/(60*60) > 60000, 60000, avg(iff(COALESCE(v.value:value:int_value, v.value:value:float_value, v.value:value:double_value)::int = COALESCE(v.value:value:int_value, v.value:value:float_value, v.value:value:double_value), COALESCE(v.value:value:int_value, v.value:value:float_value, v.value:value:double_value), 0))/(60*60)) AS AVG_TIME_SPENT FROM {0}.{1}.{2}, LATERAL FLATTEN( INPUT => EVENT_PARAMS_UTMS ) v WHERE event_name = 'user_engagement' group by 1 order by AVG_TIME_SPENT desc;".format(app_db, app_sch, table_name)).to_pandas()
   return df_out


@st.cache_data(ttl=2000)
def get_page_views(_sp_session, app_db, app_sch, table_name):
   view_count = _sp_session.sql("SELECT event_name, COUNT(*) AS event_count FROM {0}.{1}.{2} WHERE event_name IN ('page_view') GROUP BY 1".format(app_db, app_sch, table_name)).collect()[0][1]
   return view_count


@st.cache_data(ttl=2000)
def get_avg_session_per_user(_sp_session, app_db, app_sch, table_name):
   avg_session_per_user = _sp_session.sql("with data as (SELECT user_pseudo_id, EVENT_PARAMS_UTMS[0]:key::varchar as key, count(*) as counts_per_user FROM {0}.{1}.{2} WHERE key = 'ga_session_id' group by 1,2) select sum(counts_per_user)/count(*) as avg_session_per_user from data;".format(app_db, app_sch, table_name)).collect()[0][0]
   if isinstance(avg_session_per_user, int):
      return avg_session_per_user
   else:
      return 0

@st.cache_data(ttl=2000)
def get_page_view_per_user(_sp_session, app_db, app_sch, table_name):
   page_view_per_user = _sp_session.sql("with data as ( SELECT user_pseudo_id, event_name, count(*) as counts_per_user FROM {0}.{1}.{2} WHERE event_name = 'page_view' group by 1,2) select sum(counts_per_user)/count(*) as avg_page_view_per_user from data ;".format(app_db, app_sch, table_name)).collect()[0][0]
   return page_view_per_user


@st.cache_data(ttl=2000)
def get_device_type(_sp_session, app_db, app_sch, table_name):
   df_device_type = _sp_session.sql("with data as ( select event_date, DEVICE_CATEGORY, count(*) as counts from {0}.{1}.{2} group by 1, 2) SELECT EVENT_DATE, ifnull(\"'tablet'\", 0) AS TABLET,  ifnull(\"'mobile'\",0) AS MOBILE, ifnull(\"'desktop'\",0) AS DESKTOP, ifnull(\"'smart tv'\",0) AS SMART_TV FROM data PIVOT(SUM(counts) FOR DEVICE_CATEGORY IN ('tablet', 'mobile', 'desktop', 'smart tv')) AS p ORDER BY EVENT_DATE;".format(app_db, app_sch, table_name)).to_pandas()
   return df_device_type

@st.cache_data(ttl=2000)
def get_browser_type(_sp_session, app_db, app_sch, table_name):
   df_browser_type = _sp_session.sql("with data as ( select event_date, DEVICE_WEB_INFO_BROWSER, count(*) as counts from {0}.{1}.{2} group by 1, 2) SELECT EVENT_DATE, ifnull(\"'Chrome'\", 0) AS CHROME,  ifnull(\"'Edge'\",0) AS EDGE, ifnull(\"'Safari'\",0) AS SAFARI, ifnull(\"'Firefox'\",0) AS FIREFOX FROM data PIVOT(SUM(counts) FOR DEVICE_WEB_INFO_BROWSER IN ('Chrome', 'Edge', 'Safari', 'Firefox')) AS p ORDER BY EVENT_DATE;".format(app_db, app_sch, table_name)).to_pandas()
   return df_browser_type


@st.cache_data(ttl=2000)
def get_status_counts(_sp_session, app_db, app_sch, table_name):
   # df_status = _sp_session.sql("with data as (SELECT status, UTM_SOURCE, count(distinct salesforce_person_name) as counts FROM {0}.{1}.{2} group by status,UTM_SOURCE having status != 'NULL') select status, ifnull(\"'linkedin'\", 0) AS linkedin, ifnull(\"'facebook'\", 0) AS facebook FROM data PIVOT(SUM(counts) for UTM_SOURCE IN ('linkedin', 'facebook')) as p ORDER BY STATUS;".format(app_db, app_sch, table_name)).to_pandas()
   df_status = _sp_session.sql("with data as (SELECT status, UTM_SOURCE, count(distinct USER_PSEUDO_ID) as counts FROM {0}.{1}.{2} group by status,UTM_SOURCE having status != 'NULL') select * from data;".format(app_db, app_sch, table_name)).to_pandas()
   return df_status


@st.cache_data(ttl=2000)
def get_overall_engagements(_sp_session, app_db, app_sch, table_name):
   overall_engagements = _sp_session.sql("select count(distinct USER_PSEUDO_ID) as counts from {0}.{1}.{2} where event_name in ('session_start','user_engagement','click','page_view','Searched the Community','first_visit','view_search_results','scroll');".format(app_db, app_sch, table_name)).collect()[0][0]
   return overall_engagements


@st.cache_data(ttl=2000)
def get_user_attended(_sp_session, app_db, app_sch, table_name):
   user_attended = _sp_session.sql("select count(distinct USER_PSEUDO_ID) as counts from {0}.{1}.{2} where status in ('Attended', 'Attended On-demand');".format(app_db, app_sch, table_name)).collect()[0][0]
   return user_attended


@st.cache_data(ttl=2000)
def get_user_registered(_sp_session, app_db, app_sch, table_name):
   user_attended = _sp_session.sql("select count(distinct USER_PSEUDO_ID) as counts from {0}.{1}.{2} where status in ('Attended', 'Attended On-demand','Registered','No Show');".format(app_db, app_sch, table_name)).collect()[0][0]
   return user_attended



def format_num(num):
    if isinstance(num, int):
      if num > 1000000:
         if not num % 1000000:
               return f'{num // 1000000}M'
         return f'{round(num / 1000000, 1)}M'
      return f'{num // 1000}K'
    else:
      return 0


# Get the current credentials -- App way
# UNCOMMENT FOR NATIVE APP STARTS ##
sp_session = get_active_session()
# UNCOMMENT FOR NATIVE APP ENDS ##

# COMMENT FOR NATIVE APP STARTS ##
### Get the current credentials -- Local Streamlit way
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
st.title("Customer 360 Starter")
st.write(
   """
Customer 360 Starter enables organizations to combine their website and CRM data. This integration allows for a comprehensive analysis of customer interactions across different touchpoints."""
)
st.write("**Website Data Analysis:**")
st.write("- Traffic Insights: Tracks visitor traffic to the website, identifying which pages receive the most visits.")
st.write("- Page-Specific Engagement: Highlights traffic to specific pages, such as those hosting webinars, providing insights into the popularity of these pages.")
st.write("**CRM Data Utilization:**")
st.write("- Webinar Engagement Tracking: All webinar-related data, including sign-ups and attendee information, are captured in the CRM system.")
st.write("- Detailed Participant Profiles: Analyzes characteristics of individuals who sign up for webinars, offering insights into their preferences and behaviors.")
st.write("**Opportunity Pipeline Development:**")
st.write("- Conversion Analysis: Assesses how website interactions convert to webinar sign-ups and contribute to the opportunity pipeline, aiding in strategic business decisions.")
st.write("")
st.write("*This structured approach ensures a detailed understanding of how website traffic correlates with CRM activities, particularly webinar engagement, enhancing strategic planning and decision-making.*")

with st.container():
   # UNCOMMENT FOR NATIVE APP STARTS ##
    app_db = sp_session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
    app_sch = 'CRM'
    fun_db = app_db
    fun_sch = 'PYTHON_FUNCTIONS'
    app_flag = True
   # UNCOMMENT FOR NATIVE APP ENDS ##

   # COMMENT FOR NATIVE APP STARTS ##
   #  config, sp_session = initialize_snowpark()
   #  app_db = config['APP_DB']['database']
   #  app_sch = config['APP_DB']['schema']
   #  fun_db = app_db
   #  fun_sch = app_sch
   #  app_flag = False
   # COMMENT FOR NATIVE APP ENDS ##
    ga_table = 'C360_CLICKS_CRM_JOINED_VW'
    colxx, colxxx, colyy, colx, coly, colz = st.columns(6)
    with colz:
      if st.button('Refresh Page', use_container_width=False, key='refresh1'):
         st.experimental_rerun()
    
    
    st.header("C360 Starter Analytics")
    
    st.subheader(':blue[Conversion Status Summary]')
    col4, col5, col6 = st.columns(3)
    overall_engagements = get_overall_engagements(sp_session, app_db, app_sch,ga_table )
    user_attended = get_user_attended(sp_session, app_db, app_sch,ga_table)
    user_registered = get_user_registered(sp_session, app_db, app_sch,ga_table)
    col4.metric(":orange[**Website Visitors**]", overall_engagements)
    col5.metric(":orange[**Webinar Registrations**]", user_registered)
    col6.metric(":orange[**Webinar Attendees**]", user_attended)
    col7, col8, col9 = st.columns(3)
    col7.metric(":orange[**Website conversion:**] Webinar Registrations / Website Visitors",str(round((user_registered/overall_engagements)*100,2))+'%')
    col8.metric(":orange[**Attendance:**] Webinar Attendees / Webinar Registrations",str(round((user_attended/overall_engagements)*100,2))+'%')


    df_status = get_status_counts(sp_session, app_db, app_sch,ga_table )
    st.subheader(':blue[Registration Status Vs Platform]')
   
    status_chart = alt.Chart(df_status).mark_bar().encode(
                  x=alt.X('sum(COUNTS):Q'),
                  y=alt.Y('STATUS:N', sort=alt.EncodingSortField(field="-x", op="sum"), axis=alt.Axis(labelFontSize=11, tickSize=0)), #axis=alt.Axis(labelFontSize=10, tickSize=0) ),
                  color='STATUS:N',
                  row=alt.Y('UTM_SOURCE:N')
               ) 
  
    st.altair_chart(status_chart, use_container_width=True) 
    st.header("Customer Click Analytics")

   
    st.subheader(':blue[Customers Overview]')
    st.write("In this section, we will see some generic charts using click analytics data.")

    col1, col2, col3 = st.columns(3)
    overall_user_counts = get_overall_users(sp_session, app_db, app_sch,ga_table )
    user_counts = get_users_in_lastNdays(sp_session, app_db, app_sch,ga_table, 30)
    revenue = get_revenue_lastNdays(sp_session, app_db, app_sch,ga_table,500)

    col1.metric(":orange[Total Users]", format_num(overall_user_counts))
    col2.metric(":orange[New Users]", format_num(user_counts), delta="{0}".format(overall_user_counts-user_counts))
    col3.metric(":orange[Revenue]", format_num(revenue))

    col4, col5, col6 = st.columns(3)
    session_count = get_session_count(sp_session, app_db, app_sch, ga_table)
    total_page_view = get_tot_page_view(sp_session, app_db, app_sch, ga_table)
    col4.metric(":orange[Total Page View]",format_num(total_page_view[0]))
    col5.metric(":orange[Average Page View per user]", int(total_page_view[1]))
    col6.metric(":orange[Session Count]", format_num(int(session_count)))

    col7, col8, col9 = st.columns(3)
    page_view_count = get_page_views(sp_session, app_db, app_sch, ga_table)
    avg_session_per_user = get_avg_session_per_user(sp_session, app_db, app_sch, ga_table)
    avg_page_view_per_session = get_page_view_per_user(sp_session, app_db, app_sch, ga_table)
    col7.metric(":orange[Count of Page Views]", format_num(int(page_view_count)))
    col8.metric(":orange[Number of Sessions per user]", int(avg_session_per_user))
    col9.metric(":orange[Number of Page Views per session]", int(avg_page_view_per_session))
    df = get_country_group(sp_session, app_db, app_sch,ga_table )
    status_chart = alt.Chart(df).mark_bar().encode(
                  x=alt.X('sum(COUNTS):Q'),
                  y=alt.Y('GEO_COUNTRY:N', sort=alt.EncodingSortField(field="-x", op="sum", order='ascending')),
                  color='GEO_COUNTRY:N'
               )
  
    st.altair_chart(status_chart, use_container_width=True) 
    st.subheader(':blue[Map view of all your customers globally]')
    with st.container():
   
      chart_data = get_country_lat(sp_session, app_db, app_sch,ga_table )
      ## Create a sample DataFrame with latitude and longitude values

    
      layer = pdk.Layer(
               'HeatmapLayer',     # Change the `type` positional argument here
               data=chart_data,
               get_position='[LONGITUDE, LATITUDE ]',
               opacity=0.9,
               auto_highlight=True,
               get_weight="COUNTS", 
               get_color='[200, 30, 0, 160]',         
               # get_fill_color=[255, 'LONGITUDE > 0 ? 200 * LONGITUDE : -200 * LONGITUDE', 'LONGITUDE', 140],
               get_fill_color=[180, 0, 200, 140],  # Set an RGBA value for fill
               pickable=True)
      st.pydeck_chart(pdk.Deck(
         map_style=None,
         initial_view_state=pdk.ViewState(
            latitude=0,
            longitude=0,
            zoom=1.25,
            pitch=30,
         ), 
         layers=[layer],
      ))

    st.subheader(':blue[Detailed Statistics]')
    with st.container():
      df_purchase_time = get_purchase_time(sp_session, app_db, app_sch,ga_table )
      st.subheader("Time spent on website by users across time")
      st.line_chart(df_purchase_time, x='EVENT_DATE', y='AVG_TIME_SPENT', use_container_width=True)
      st.subheader("Users by Device Type")
      df_device_type = get_device_type(sp_session, app_db, app_sch,ga_table )
      st.line_chart(df_device_type, x='EVENT_DATE', y=['TABLET', 'MOBILE', 'DESKTOP', 'SMART_TV'], use_container_width=True)
      st.subheader("Users by Browser")
      df_browser_type = get_browser_type(sp_session, app_db, app_sch,ga_table )
      st.line_chart(df_browser_type, x='EVENT_DATE', y=['CHROME', 'EDGE', 'SAFARI', 'FIREFOX'], use_container_width=True)

    