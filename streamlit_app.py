# streamlit_app.py

import streamlit as st
import snowflake.connector
import pandas as pd
import pytz
import datetime as dt
from datetime import datetime
from streamlit_autorefresh import st_autorefresh


@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        **st.secrets["snowflake"], client_session_keep_alive=True
    )

conn = init_connection()


@st.cache_data(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()
    
#Queries
rows = run_query('''select CASE WHEN lead_source='SPRINGFACEBOOK' THEN 'FACEBOOK' ELSE lead_source END AS lead_source, lead_created_date, sum(total_leads), sum(convertedleads), sum(verifiedleads) 
                  from CD_ANALYTICS_TESTDB.OMKAR.SPRING_ADS_DASHBOARD where lead_Created_date is not null and lead_source in 
                  ('SPRINGFACEBOOK', 'FACEBOOKSPRING','GOOGLE', 'GOOGLE BRANDED', 'GOOGLEPMAX', 'TIKTOK') 
                   group by 1,2
                   order by 2;''')
                  
df=pd.DataFrame(rows)
df.columns += 1
df.columns = ["Lead source","Lead Created Date","Total Leads", "Total Opps", "Verified Leads"]

rows2 = run_query('''select CASE WHEN lead_source='SPRINGFACEBOOK' THEN 'FACEBOOK' ELSE lead_source END AS lead_source, sum(total_leads), sum(convertedleads), sum(verifiedleads) 
                  from CD_ANALYTICS_TESTDB.OMKAR.SPRING_ADS_DASHBOARD where lead_Created_date is not null and lead_source in 
                  ('SPRINGFACEBOOK', 'FACEBOOKSPRING','GOOGLE', 'GOOGLE BRANDED', 'GOOGLEPMAX', 'TIKTOK') 
                   group by 1
                   order by 1 desc;''')
                  
df2=pd.DataFrame(rows2)
df2.columns += 1
df2.columns = ["Lead source","Total Leads", "Total Opps", "Verified Leads"]




hide_table_row_index = """
                        <style>
                            thead tr th:first-child {display:none}
                            tbody th {display:none}
                            table {
                                text-align: center;
                            }
                        </style>
                    """

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
            lead_source_options = list(sorted(df["Lead source"].unique())) + ["ALL"] 
            lead_source_filter = st.radio("Select Lead Source:", lead_source_options, index=len(lead_source_options)-1)

            start_date = st.date_input("Select Start Date:")
            end_date = st.date_input("Select End Date:")


if lead_source_filter == "ALL":
    # Execute the SQL query to get data for all lead sources
    query_all_lead_sources = '''
                       select lead_created_date, sum(total_leads), sum(convertedleads), sum(verifiedleads) 
                       from CD_ANALYTICS_TESTDB.OMKAR.SPRING_ADS_DASHBOARD where lead_Created_date is not null and lead_source in 
                       ('SPRINGFACEBOOK', 'FACEBOOKSPRING','GOOGLE', 'GOOGLE BRANDED', 'GOOGLEPMAX', 'TIKTOK') 
                       group by 1
                       order by 1;
                       '''
    rows_all_lead_sources = run_query(query_all_lead_sources)
    filtered_df = pd.DataFrame(rows_all_lead_sources)
    filtered_df.columns += 1
    filtered_df.columns = ["Lead Created Date","Total Leads", "Total Opps", "Verified Leads"]
    filtered_df = filtered_df[(filtered_df["Lead Created Date"] >= start_date) & 
                     (filtered_df["Lead Created Date"] <= end_date)]
    #filtered_df = filtered_df.drop(columns=["Lead source"])
    query_all_lead_sources2 = '''
                       select CASE WHEN lead_source='SPRINGFACEBOOK' THEN 'FACEBOOK' ELSE lead_source END AS lead_source, sum(total_leads), sum(convertedleads), sum(verifiedleads) 
                       from CD_ANALYTICS_TESTDB.OMKAR.SPRING_ADS_DASHBOARD where lead_Created_date is not null and lead_source in 
                       ('SPRINGFACEBOOK', 'FACEBOOKSPRING','GOOGLE', 'GOOGLE BRANDED', 'GOOGLEPMAX', 'TIKTOK') 
                       group by 1,2
                       order by 1 desc;
                       '''
    rows_all_lead_sources2 = run_query(query_all_lead_sources2)
    filtered_df2 = pd.DataFrame(rows_all_lead_sources2)
    filtered_df2.columns += 1
    filtered_df2.columns = ["Lead Source", "Total Leads", "Total Opps", "Verified Leads"]
    filtered_df2 = filtered_df[(filtered_df["Lead Created Date"] >= start_date) & 
                     (filtered_df["Lead Created Date"] <= end_date)]
    #filtered_df2 = filtered_df2.drop(columns=["Lead Created Date"])

else:
    # Filter the existing DataFrame based on the date range and selected Lead source
    filtered_df = df[(df["Lead source"] == lead_source_filter) & 
                     (df["Lead Created Date"] >= start_date) & 
                     (df["Lead Created Date"] <= end_date)]
    filtered_df = filtered_df.drop(columns=["Lead source"])
    filtered_df2 = df2[(df2["Lead source"] == lead_source_filter) & 
                     (df["Lead Created Date"] >= start_date) & 
                     (df["Lead Created Date"] <= end_date)]
    #filtered_df2 = filtered_df2.drop(columns=["Lead Created Date"])

filtered_df["Lead Created Date"] = pd.to_datetime(filtered_df["Lead Created Date"]).dt.strftime('%B %e, %Y')
#filtered_df2["Lead Created Date"] = pd.to_datetime(filtered_df2["Lead Created Date"]).dt.strftime('%B %e, %Y')
# Calculate grand totals and append to the DataFrame
grand_totals = filtered_df.sum(numeric_only=True).to_frame().T
grand_totals["Lead Created Date"] = "Grand Total"
filtered_df = pd.concat([filtered_df, grand_totals], ignore_index=True)


# Display the filtered DataFrame
selected_lead_source = "All Lead Sources" if lead_source_filter == "ALL" else lead_source_filter
st.subheader(f"Lead Source: {selected_lead_source}")

st.table(filtered_df)
st.markdown(hide_table_row_index, unsafe_allow_html=True)

st.table(filtered_df2)
st.markdown(hide_table_row_index, unsafe_allow_html=True)



#st.subheader('header')
#st.table(df)
        
   
        
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
