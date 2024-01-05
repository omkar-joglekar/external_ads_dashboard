# streamlit_app.py

import snowflake.connector
import streamlit as st
import pandas as pd

# Function to fetch data from Snowflake
@st.cache(allow_output_mutation=True)
def init_connection():
    return snowflake.connector.connect(
        **st.secrets["snowflake"], client_session_keep_alive=True
    )

@st.cache(allow_output_mutation=True)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

# Streamlit app setup
conn = init_connection()

# Streamlit sidebar filters
lead_source_filter = st.sidebar.selectbox('Select Lead Source', options=['FACEBOOK', 'GOOGLE'])
start_date_filter = st.sidebar.date_input('Select Start Date')
end_date_filter = st.sidebar.date_input('Select End Date')

# Snowflake query with filters
query = f"""
    SELECT lead_source, lead_created_date, SUM(total_leads), SUM(convertedleads), SUM(verifiedleads)
    FROM CD_ANALYTICS_TESTDB.OMKAR.SPRING_ADS_DASHBOARD
    WHERE lead_Created_date IS NOT NULL
        AND lead_source = '{lead_source_filter}'
        AND lead_created_date BETWEEN '{start_date_filter}' AND '{end_date_filter}'
    GROUP BY 1, 2
    ORDER BY 1 DESC
    LIMIT 100;
"""

# Fetch data based on the query
rows = run_query(query)
df = pd.DataFrame(rows)
df.columns += 1
df.columns = ["Lead source", "Lead Created Date", "Total Leads", "Total Opps", "Verified Leads"]

# HTML string for the title
html_str = f"""
<h1 style='text-align: center; color: white;'>Ads Dashboard</h1>
"""
st.markdown(html_str, unsafe_allow_html=True)

# Display the filtered table
st.subheader('Filtered Table')
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
