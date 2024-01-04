# streamlit_app.py

import streamlit as st
import snowflake.connector
import pandas as pd
import pytz
import datetime as dt
from datetime import datetime
from streamlit_autorefresh import st_autorefresh

# update every 5 mins
#st_autorefresh(interval=5 * 60 * 1000, key="datarefresh")

#Get Month and Year for app title
#today = datetime.now()
#month = today.strftime("%B")
#year = today.year

#Calculate next refresh time
#refresh_times = ["08:00", "10:00", "12:00", "14:00", "16:00", "18:00", "20:00", "22:00", "00:00", "02:00", "04:00", "06:00"]
#timezone = pytz.timezone('US/Pacific')

#current_time = dt.datetime.now(timezone).strftime("%H:%M")

#next_refresh_time = None
#for time in refresh_times:
#    if current_time < time:
 #       next_refresh_time = time
 #       break
        
#if next_refresh_time is None:
#    next_refresh_time = refresh_times[0] # if all refresh times have passed, the next refresh time will be the first one in the list

#delta = dt.datetime.strptime(next_refresh_time, "%H:%M") - dt.datetime.strptime(current_time, "%H:%M")

#hours = delta.seconds // 3600
#minutes = (delta.seconds // 60) % 60


# Initialize connection.
# Uses st.cache_resource to only run once.
@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        **st.secrets["snowflake"], client_session_keep_alive=True
    )

conn = init_connection()

# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_data(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()
    
#Queries
rows = run_query("select lead_created_date, sum(total_leads), sum(convertedleads), sum(verifiedleads) from CD_ANALYTICS_TESTDB.OMKAR.SPRING_ADS_DASHBOARD where lead_Created_date is not null group by 1  order by 1 desc limit 100;")
df=pd.DataFrame(rows)
df.columns += 1
#df.index = df.index + 1
#df.insert(0, "Rank", df.index)
df.columns = ["Lead_created_date","Total Leads", "Total Opps", "Verified Leads"]
#df['Funded'] = df['Funded'].astype(int)



hide_table_row_index = """
            <style>
            thead tr th:first-child {display:none}
            tbody th {display:none}
           </style>
            """

#df['Date'] =  pd.to_datetime(df['Date'])


# Concatenate the 'Date' columns from df and df2
#dates = pd.concat([df['Date'], df2['Date']])

# Convert the dates to datetime objects and format as 'Month Year'
#formatted_dates = pd.to_datetime(dates).dt.strftime('%B %Y').unique()

# Define a custom sorting key function
#def custom_sort(date):
#    month_order = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
 #   month, year = date.split()
 #   return month_order.index(month), int(year)

# Sort the formatted dates using the custom sorting key function
#sorted_dates = sorted(formatted_dates, key=custom_sort)

# Display the sorted dates in the selectbox
#month_filter = st.sidebar.radio('Month:', sorted_dates)


#month_filter = st.sidebar.selectbox(
#    'Month:',
#    pd.to_datetime(pd.concat([df['Date'], df2['Date']])).dt.strftime('%B %Y').sort_values().unique()
#)

#selected_month = pd.to_datetime(month_filter).strftime("%B")
#selected_year = pd.to_datetime(month_filter).year

# HTML string for the title
html_str = f"""
<h1 style='text-align: center; color: white;'>Ads Dashboard</h1>
"""
st.markdown(html_str, unsafe_allow_html=True)

#filtered_df_1 = df[df['Date'].dt.strftime('%B %Y') == month_filter]


#options = ["EFS", "Fundies", "CSR Declines", "Progressa & Lendful Funded","CCC & Evergreen Funded"]
#selected_option = st.selectbox("Select:", options) #label_visibility="collapsed"

with st.sidebar:
            st.write("Filters")
 
st.subheader('header')
st.table(df)
        
   
        
#Display next refresh time and logo    
#col7, col8, col9 = st.columns([1.5,0.25,0.365])

#with col7:
#  if hours == 0:
#    st.write(f"Next refresh in {minutes} minute{'s' if minutes != 1 else ''} ({next_refresh_time})")
#  else:
#    st.write(f"Next refresh in {hours} hour{'s' if hours != 1 else ''} {minutes} minute{'s' if minutes != 1 else ''} ({next_refresh_time})")
#with col8:
#    st.write(" ")
#with col9:
col1 =  st.image("logo.png")


css = '''
      <style>
      section.main > div:has(~ footer ) {
      padding-bottom: 5px;
      }
      </style>
      '''
st.markdown(css, unsafe_allow_html=True)
