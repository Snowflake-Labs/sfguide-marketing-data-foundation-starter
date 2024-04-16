# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import numpy as np
import os
import base64

from datetime import date
st.set_page_config(layout="wide")

sp_session = get_active_session()

col1, col2 = st.columns(2)
with col1:
   st.subheader(':orange[Campaign Intelligence]')
   campaign_starter = 'images/Campaign_Starter.png'
   campaign_mime_type = campaign_starter.split('.')[-1:][0].lower()        
   with open(campaign_starter, "rb") as f:
      compaign_content_bytes = f.read()
   compaign_content_b64encoded = base64.b64encode(compaign_content_bytes).decode()
   compaign_image_string = f'data:image/{campaign_mime_type};base64,{compaign_content_b64encoded}'
   st.image(compaign_image_string)
   st.write("Click to open the [app](https://app.snowflake.com/sfsenorthamerica/polaris1/#/streamlit-apps/CUSTOMER72_DB.DEMO.CAMPAIGN_72_APP)")
with col2:
   st.subheader(':orange[Customer 360]')
   image_name = 'images/Customer_Starter.png'
   mime_type = image_name.split('.')[-1:][0].lower()        
   with open(image_name, "rb") as f:
      content_bytes = f.read()
   content_b64encoded = base64.b64encode(content_bytes).decode()
   image_string = f'data:image/{mime_type};base64,{content_b64encoded}'
   st.image(image_string)
   st.write("Click to open the [app](https://app.snowflake.com/sfsenorthamerica/polaris1/#/streamlit-apps/CUSTOMER72_DB.DEMO.CAMPAIGN_72_APP)")
