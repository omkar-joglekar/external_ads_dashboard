# streamlit_app.py

import streamlit as st
import snowflake.connector
import pandas as pd
import pytz
import datetime as dt
import numpy as np
from datetime import datetime
#from datetime import timedelta
from streamlit_autorefresh import st_autorefresh

st.set_page_config(layout="wide")

@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        **st.secrets["snowflake"], client_session_keep_alive=True
    )

conn = init_connection()


@st.cache_data(ttl=600)
def run_query(query, params=None):
    with conn.cursor() as cur:
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
        return cur.fetchall()
    
#Queries
rows = run_query('''SELECT lead_source,
                    LEAD_CREATED_DATE,
                    ZEROIFNULL(TOTAL_LEADS),
                    ZEROIFNULL(VERIFIEDLEADS),
                    ZEROIFNULL(TOTAL_OPPS),
                    LEAD_TO_OPP,
                    ZEROIFNULL(TOTAL_FUNDED),
                    LEAD_TO_FUNDED,
                    OPP_TO_FUNDED,
                    TOTAL_SPEND,
                    CPLEAD,
                    CPVERIFIEDLEADS,
                    CPOPP,
                    CPFUNDED FROM CD_ANALYTICS_TESTDB.OMKAR.Streamlit_Ads_dashboard ORDER BY 2;''')
                  
df=pd.DataFrame(rows)
df.columns += 1
df.columns = ["Lead source","Lead Created Date","Total Leads", "Verified Leads", "Total Opps", "Lead to Opp %", "Total Funded", "Lead to Funded %","Opp to Funded %","Total Spend", "CPLead", "CP Verified Leads", "CPOpps", "CPFunded"]

hide_table_row_index = """
                        <style>
                            thead tr th:first-child {display:none}
                            tbody th {display:none}
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
            

            # Get the first day of the current month
            default_start_date = dt.datetime.today().replace(day=1)

            # Get today's date
            default_end_date = dt.datetime.today()

            # Display the date inputs with default values
            start_date = st.date_input("Select Start Date:", value=default_start_date)
            end_date = st.date_input("Select End Date:", value=default_end_date)
            lead_source_filter = st.radio("Select Lead Source:", lead_source_options, index=len(lead_source_options)-1)

if lead_source_filter == "ALL":
    # Execute the SQL query to get data for all lead sources
    query_all_lead_sources = '''
                        SELECT LEAD_CREATED_DATE,
                        TOTAL_LEADS,
                        VERIFIEDLEADS,
                        TOTAL_OPPS,
                        LEAD_TO_OPP,
                        TOTAL_FUNDED,
                        LEAD_TO_FUNDED,
                        OPP_TO_FUNDED,
                        TOTAL_SPEND,
                        CPLEAD,
                        CPVERIFIEDLEADS,
                        CPOPP,
                        CPFUNDED FROM CD_ANALYTICS_TESTDB.OMKAR.Streamlit_Ads_DASHBOARD_ALL ORDER BY 1;'''
    rows_all_lead_sources = run_query(query_all_lead_sources)
    filtered_df = pd.DataFrame(rows_all_lead_sources)
    filtered_df.columns += 1
    filtered_df.columns = ["Lead Created Date","Total Leads", "Verified Leads", "Total Opps","Lead to Opp %", "Total Funded", "Lead to Funded %","Opp to Funded %","Total Spend", "CPLead", "CP Verified Leads", "CPOpps", "CPFunded"]
    filtered_df = filtered_df[(filtered_df["Lead Created Date"] >= start_date) & 
                     (filtered_df["Lead Created Date"] <= end_date)]
    #filtered_df = filtered_df.drop(columns=["Lead source"])
    query_all_lead_sources2 = '''
                        select CASE WHEN lead_source = 'SPRINGFACEBOOK' THEN 'FACEBOOK' 
                        WHEN lead_source_f is null then lead_source 
                        else lead_source_f END AS lead_source2, sum(total_leads),  sum(verifiedleads)
                       , sum(convertedleads), NULLIF(SUM(convertedleads), 0) / NULLIF(SUM(total_leads), 0)  AS Lead_to_Opp,
                       sum(fundedleads),NULLIF(SUM(fundedleads), 0) / NULLIF(SUM(total_leads), 0)  AS Lead_to_Funded
                       ,NULLIF(SUM(fundedleads), 0) / NULLIF(SUM(convertedleads), 0)  AS Opp_to_Funded,
                       sum(cost),
                       NULLIF(SUM(cost), 0) / NULLIF(SUM(total_leads), 0)  AS CPLead,
                       NULLIF(SUM(cost), 0) / NULLIF(SUM(verifiedleads), 0)  AS CPVerifiedLeads,
                       NULLIF(SUM(cost), 0) / NULLIF(SUM(convertedleads), 0)  AS CPOpp,
                       NULLIF(SUM(cost), 0) / NULLIF(SUM(fundedleads), 0)  AS CPFunded
                      from CD_ANALYTICS_TESTDB.OMKAR.SPRING_ADS_DASHBOARD1 where lead_source2 in 
                      ('FACEBOOK','FACEBOOKSPRING','GOOGLE', 'SPRINGGOOGLEBRANDED', 'GOOGLEPMAX', 'TIKTOK') and DATE_2 BETWEEN %s AND %s
                       group by 1
                       order by 2 desc;
                        '''
    params = (start_date, end_date)
    rows_all_lead_sources2 = run_query(query_all_lead_sources2, params)
    filtered_df2 = pd.DataFrame(rows_all_lead_sources2)
    filtered_df2.columns += 1
    filtered_df2.columns = ["Lead Source", "Total Leads", "Verified Leads", "Total Opps", "Lead to Opp %","Total Funded", "Lead to Funded %","Opp to Funded %","Total Spend", "CPLead", "CP Verified Leads", "CPOpps", "CPFunded"]

else:
    # Filter the existing DataFrame based on the date range and selected Lead source
    filtered_df = df[(df["Lead source"] == lead_source_filter) & 
                     (df["Lead Created Date"] >= start_date) & 
                     (df["Lead Created Date"] <= end_date)]
    filtered_df = filtered_df.drop(columns=["Lead source"])

    query_all_lead_sources2 = '''
                        select CASE WHEN lead_source = 'SPRINGFACEBOOK' THEN 'FACEBOOK' 
                        WHEN lead_source_f is null then lead_source 
                        else lead_source_f END AS lead_source2, sum(total_leads),  sum(verifiedleads)
                       , sum(convertedleads), NULLIF(SUM(convertedleads), 0) / NULLIF(SUM(total_leads), 0)  AS Lead_to_Opp,
                       sum(fundedleads),NULLIF(SUM(fundedleads), 0) / NULLIF(SUM(total_leads), 0)  AS Lead_to_Funded
                       ,NULLIF(SUM(fundedleads), 0) / NULLIF(SUM(convertedleads), 0)  AS Opp_to_Funded,
                       sum(cost),
                       NULLIF(SUM(cost), 0) / NULLIF(SUM(total_leads), 0)  AS CPLead,
                       NULLIF(SUM(cost), 0) / NULLIF(SUM(verifiedleads), 0)  AS CPVerifiedLeads,
                       NULLIF(SUM(cost), 0) / NULLIF(SUM(convertedleads), 0)  AS CPOpp,
                       NULLIF(SUM(cost), 0) / NULLIF(SUM(fundedleads), 0)  AS CPFunded
                      from CD_ANALYTICS_TESTDB.OMKAR.SPRING_ADS_DASHBOARD1 where lead_source2 in 
                      ('FACEBOOK','FACEBOOKSPRING','GOOGLE', 'SPRINGGOOGLEBRANDED', 'GOOGLEPMAX', 'TIKTOK') and DATE_2 BETWEEN %s AND %s
                       group by 1
                       order by 2 desc;
                           '''
    params = (start_date, end_date)
    rows_all_lead_sources2 = run_query(query_all_lead_sources2, params)
    filtered_df2 = pd.DataFrame(rows_all_lead_sources2)
    filtered_df2.columns += 1
    filtered_df2.columns = ["Lead Source", "Total Leads", "Verified Leads", "Total Opps", "Lead to Opp %","Total Funded","Lead to Funded %","Opp to Funded %","Total Spend", "CPLead", "CP Verified Leads", "CPOpps", "CPFunded"]
    filtered_df2 = filtered_df2[(filtered_df2["Lead Source"] == lead_source_filter)]

filtered_df["Lead Created Date"] = pd.to_datetime(filtered_df["Lead Created Date"]).dt.strftime('%B %e, %Y')
#filtered_df2["Lead Created Date"] = pd.to_datetime(filtered_df2["Lead Created Date"]).dt.strftime('%B %e, %Y')
# Calculate grand totals and append to the DataFrame
grand_totals = pd.DataFrame({
    "Lead Created Date": ["Grand Total"],
    "Total Leads": filtered_df["Total Leads"].sum(),
    "Verified Leads": filtered_df["Verified Leads"].sum(),
    "Total Opps": filtered_df["Total Opps"].sum(),
    "Lead to Opp %": filtered_df["Total Opps"].sum() / filtered_df["Total Leads"].sum(),
    "Total Funded": filtered_df["Total Funded"].sum(),
    "Lead to Funded %": filtered_df["Total Funded"].sum() / filtered_df["Total Leads"].sum(),
    "Opp to Funded %": filtered_df["Total Funded"].sum() / filtered_df["Total Opps"].sum(),
    "Total Spend": filtered_df["Total Spend"].sum(),
    "CPLead": filtered_df["Total Spend"].sum() / filtered_df["Total Leads"].sum(),
    "CP Verified Leads": filtered_df["Total Spend"].sum() / filtered_df["Verified Leads"].sum(),
    "CPOpps": filtered_df["Total Spend"].sum() / filtered_df["Total Opps"].sum(),
    "CPFunded": filtered_df["Total Spend"].sum() / filtered_df["Total Funded"].sum(),
})

filtered_df = pd.concat([filtered_df, grand_totals], ignore_index=True)
# Replace NaN values with blanks in the "Cost" column
filtered_df["Total Spend"] = filtered_df["Total Spend"].fillna(0)
filtered_df["Lead to Opp %"] = filtered_df["Lead to Opp %"].fillna(0)
filtered_df["Lead to Funded %"] = filtered_df["Lead to Funded %"].fillna(0)
filtered_df["Opp to Funded %"] = filtered_df["Opp to Funded %"].fillna(0)
filtered_df["CPLead"] = filtered_df["CPLead"].fillna(0)
filtered_df["CP Verified Leads"] = filtered_df["CP Verified Leads"].fillna(0)
filtered_df["CPOpps"] = filtered_df["CPOpps"].fillna(0)
filtered_df["CPFunded"] = filtered_df["CPFunded"].fillna(0)
# Apply formatting to numeric columns
formatted_df = filtered_df.style.format({
    "Total Leads": "{:,.0f}",
    "Verified Leads": "{:,.0f}",
    "Total Opps": "{:,.0f}",
    "Lead to Opp %": '{:,.2%}',
    "Total Funded": "{:,.0f}",
    "Lead to Funded %": '{:,.2%}',
    "Opp to Funded %": '{:,.2%}',
    "Total Spend": "${:,.2f}",
    "CPLead": "${:,.2f}",
    "CP Verified Leads": "${:,.2f}",
    "CPOpps": "${:,.2f}",
    "CPFunded": "${:,.2f}",
})



grand_totals2 = pd.DataFrame({
    "Lead Source": ["Grand Total"],
    "Total Leads": filtered_df2["Total Leads"].sum(),
    "Verified Leads": filtered_df2["Verified Leads"].sum(),
    "Total Opps": filtered_df2["Total Opps"].sum(),
    "Lead to Opp %": filtered_df2["Total Opps"].sum() / filtered_df2["Total Leads"].sum(),
    "Total Funded": filtered_df2["Total Funded"].sum(),
    "Lead to Funded %": filtered_df2["Total Funded"].sum() / filtered_df2["Total Leads"].sum(),
    "Opp to Funded %": filtered_df2["Total Funded"].sum() / filtered_df2["Total Opps"].sum(),
    "Total Spend": filtered_df2["Total Spend"].sum(),
    "CPLead": filtered_df2["Total Spend"].sum() / filtered_df2["Total Leads"].sum(),
    "CP Verified Leads": filtered_df2["Total Spend"].sum() / filtered_df2["Verified Leads"].sum(),
    "CPOpps": filtered_df2["Total Spend"].sum() / filtered_df2["Total Opps"].sum(),
    "CPFunded": filtered_df2["Total Spend"].sum() / filtered_df2["Total Funded"].sum(),
})

filtered_df2 = pd.concat([filtered_df2, grand_totals2], ignore_index=True)
#filtered_df2["Total Spend"] = pd.to_numeric(filtered_df2["Total Spend"], errors='coerce')
filtered_df2["Total Spend"] = filtered_df2["Total Spend"].fillna(0)
filtered_df2["Lead to Opp %"] = filtered_df2["Lead to Opp %"].fillna(0)
filtered_df2["Lead to Funded %"] = filtered_df2["Lead to Funded %"].fillna(0)
filtered_df2["Opp to Funded %"] = filtered_df2["Opp to Funded %"].fillna(0)
filtered_df2["CPLead"] = filtered_df2["CPLead"].fillna(0)
filtered_df2["CP Verified Leads"] = filtered_df2["CP Verified Leads"].fillna(0)
filtered_df2["CPOpps"] = filtered_df2["CPOpps"].fillna(0)
filtered_df2["CPFunded"] = filtered_df2["CPFunded"].fillna(0)
formatted_df2 = filtered_df2.style.format({
    "Total Leads": "{:,.0f}",
    "Verified Leads": "{:,.0f}",
    "Total Opps": "{:,.0f}",
    "Lead to Opp %": '{:,.2%}',
    "Total Funded": "{:,.0f}",
    "Lead to Funded %": '{:,.2%}',
    "Opp to Funded %": '{:,.2%}',
    "Total Spend": "${:,.2f}",
    "CPLead": "${:,.2f}",
    "CP Verified Leads": "${:,.2f}",
    "CPOpps": "${:,.2f}",
    "CPFunded": "${:,.2f}",
})



# Display the filtered DataFrame
selected_lead_source = "All Lead Sources" if lead_source_filter == "ALL" else lead_source_filter
st.subheader(f"Lead Source: {selected_lead_source}")

st.table(formatted_df)
st.markdown(hide_table_row_index, unsafe_allow_html=True)

st.table(formatted_df2)
st.markdown(hide_table_row_index, unsafe_allow_html=True)


col1 =  st.image("logo.png")


css = '''
      <style>
      section.main > div:has(~ footer ) {
      padding-bottom: 5px;
      }
      </style>
      '''
st.markdown(css, unsafe_allow_html=True)
