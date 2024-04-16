# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import numpy as np
import os
import base64

from datetime import date
st.set_page_config(layout="wide")

st.title("To be build")


st.subheader(':orange[Catalog based on the sources selected]')
image_name = 'images/Data_Catalog.png'
mime_type = image_name.split('.')[-1:][0].lower()        
with open(image_name, "rb") as f:
    content_bytes = f.read()
content_b64encoded = base64.b64encode(content_bytes).decode()
image_string = f'data:image/{mime_type};base64,{content_b64encoded}'
st.image(image_string)
st.write("Click to open the [app](https://app.snowflake.com/sfsenorthamerica/polaris1/#/streamlit-apps/CUSTOMER72_DB.DEMO.CAMPAIGN_72_APP)")
