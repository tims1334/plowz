import geopandas as gp
import pandas as pd
import redshift_connector
from query_list import first_orders, orders
import streamlit as st
import datetime
import numpy as np
import altair as alt

@st.cache
def convert_df(df_download):
    return df_download.to_csv().encode('utf-8')
# Land map and spending

dma_map = gp.read_file('FB_DMA.shp')
fb_spend = pd.read_csv("fb_spend_campaign.csv")
fb_spend['DMA Name'] = fb_spend['dma']
spend_map = dma_map.merge(fb_spend, on='DMA Name')

# Choose campaigns
st.write('Choose campaigns')
campaign_l = spend_map['campaign_name'].drop_duplicates()
campaign_list = campaign_l.to_list()
campaigns = st.sidebar.multiselect("Select campaigns", campaign_list, default='Static Retargeting | US | 6.22.22')
spend_map = spend_map.loc[spend_map['campaign_name'].isin(campaigns)]

passwords = st.sidebar.text_input('DB Password')

# Connect
conn = redshift_connector.connect(
        host=st.secrets["hosts"],
        database=st.secrets["databases"],
        user=st.secrets["users"],
        password=passwords
        )
cursor: redshift_connector.Cursor = conn.cursor()
    
# Load
cursor.execute(orders)
df: pd.DataFrame = cursor.fetch_dataframe()
cursor.execute(first_orders)
fo: pd.DataFrame = cursor.fetch_dataframe()
df = df.merge(fo,how="left",on='customer_id')

# Transform
df['latitude'] = df.latitude.astype('float64')
df['longitude'] = df.longitude.astype('float64')
df['1stjob'] = df['first_job'].dt.date
df['first_job'] = df['first_job'].astype('str')
df['new_customers'] = df.loc[(df['1stjob']) == df['created_at'].dt.date, "customer_id"]
df['recurring_jobs'] = df.loc[df['recurring']==True,"id"]
# GeoDataFrame to join jobs and map
df['geometry'] = gp.points_from_xy(df['longitude'],df['latitude'], crs="EPSG:4326")
gdf = gp.GeoDataFrame(df,geometry=df['geometry'], crs="EPSG:4326")
job_map = spend_map.sjoin(gdf,how='left',predicate='intersects')

# Transform dates
job_map['date_start'] = job_map['date_start'].astype('str')
job_map['created_at']= job_map['created_at'].dt.date
job_map['date_start']=job_map.apply(lambda x: datetime.datetime.strptime(x['date_start'], '%Y-%m-%d'), axis=1)
job_map['date_start'] = job_map['date_start'].dt.date

# Fix recurring count
job_map['recurring'] = job_map['recurring'].apply(lambda x: 1 if x==True else 0)

# Group only jobs by date
jobs_by_date = job_map.groupby(['created_at','dma']).agg({
                                                        'customer_id':'nunique',
                                                        'id':'nunique',
                                                        'recurring_jobs':'nunique',
                                                        'new_customers':'nunique'
                                                        }).reset_index()


# Transformations for final df
spend_by_date = spend_map[['date_start','dma','campaign_name','spend','impressions','clicks']]
spend_by_date['created_at'] = spend_by_date['date_start']

jobs_by_date['created_at'] = jobs_by_date['created_at'].astype('str')
spend_by_date['created_at'] = spend_map.apply(lambda x: datetime.datetime.strptime(x['date_start'], '%m/%d%y'), axis=1).astype('str')

# Join spend and jobs by dates/dmas (includes campaign names)
job_spend_by_date = spend_by_date.merge(jobs_by_date,on=["dma","created_at"],how='left')
job_spend_by_date = pd.DataFrame(job_spend_by_date.fillna(0))

# Group job data by dma and created date --- otherwise we're counting job info again for each campaign
final = job_spend_by_date.fillna(0)
final = job_spend_by_date.groupby(['date_start','dma']).agg({
                                                            'spend':'sum',
                                                            'impressions':'sum',
                                                            'clicks':'sum',
                                                            'customer_id':'mean',
                                                            'id':'mean',
                                                            'recurring_jobs':'mean',
                                                            'new_customers':'mean'}).reset_index()


# Transform dates so we can use date picker
final['date_start'] = final.apply(lambda x: datetime.datetime.strptime(x['date_start'],'%Y-%m-%d'),axis=1).dt.date
job_spend_by_date['date_start'] = job_spend_by_date.apply(lambda x: datetime.datetime.strptime(x['date_start'],'%Y-%m-%d'),axis=1).dt.date

# Change job info to integers
indict = {'customer_id':int,'id':int,'recurring_jobs':int,'new_customers':int}
final = final.astype(indict)
job_spend_by_date = job_spend_by_date.astype(indict)

# Total by dma
tdma = final.groupby('dma').agg({
                            'spend':'sum',
                            'impressions':'sum',
                            'clicks':'sum',
                            'customer_id':'sum',
                            'id':'sum',
                            'recurring_jobs':'sum',
                            'new_customers':'sum'
                            }).reset_index()

# Add cost per new customer to dfs
tdma['cost_per_new_customer'] = tdma['spend'].div(tdma['new_customers']).replace(np.inf,0)
job_spend_by_date['cost_per_new_customer'] = job_spend_by_date['spend'].div(job_spend_by_date['new_customers']).replace(np.inf, 0)
final['cost_per_new_customer'] = final['spend'].div(final['new_customers']).replace(np.inf,0)

# Start Showing Data
st.write('***FB ads spend data will be 1 day behind***')
st.write("Choose start and end dates for info by dma")
start = st.date_input('Start Date',datetime.date(2022,6,23))
end = st.date_input('End Date')

# Filter by selected dates
final = final.loc[(final['date_start']>=start)&(final['date_start']<=end)]
job_spend_by_date = job_spend_by_date.loc[(job_spend_by_date['date_start']>=start)&(job_spend_by_date['date_start']<=end)]

# Create and show metrics
new_customer_sum = final.new_customers.sum()
spend_total = job_spend_by_date.spend.sum()
st.metric('Total New Customers between {} and {}'.format(start,end),(new_customer_sum))
st.metric('Total spent on FB ads between {} and {}'.format(start,end),'${:,.2f}'.format(spend_total))
cost = spend_total/new_customer_sum
st.metric('Avg. Cost per New Customer (Blended)', '${:,.2f}'.format(cost))

# Map from job df
st.write('Map of new customers')
dfp = df.loc[(df['1stjob']>=start)&(df['1stjob']<=end)]
st.map(dfp.loc[~df['new_customers'].isna()])

# Show dataframes

st.header('Job and Spend Totals by dma')
st.dataframe(tdma.style.format({'spend':'${:,.2f}',
                                'cost_per_new_customer':'${:,.2f}'}))

st.header('Job and Spend info grouped by date and dma')
st.dataframe(final.style.format({'spend':'${:,.2f}',
                                'cost_per_new_customer':'${:,.2f}'}))
#csv3 = convert_df(final)
#st.download_button(label="Download Job and Spend info csv",
#                    data=csv3,
#                    mime='text/csv')

#st.header('Job Spend by Date, campaigns included')
#st.write("Customer and Job info is duplicated per campaign (job data isn't broken down by campaign in our backend)")
#st.dataframe(job_spend_by_date.style.format({'spend':'${:,.2f}',
#                                'cost_per_new_customer':'${:,.2f}'}))
#csv1 = convert_df(job_spend_by_date)
#st.download_button(label="Download Job Spend by Date w Campaigns csv",
#                    data=csv1,
#                    mime='text/csv')

#csv2 = convert_df(job_map)
#st.download_button(label="Download all job data with market - no spend - no filter",
#                    data=csv2,
#                    mime='text/csv')
