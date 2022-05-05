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

dma_map = gp.read_file('FB_DMA.shp')
fb_spend = pd.read_csv("fb_spend_campaign.csv")
fb_spend['DMA Name'] = fb_spend['dma']
spend_map = dma_map.merge(fb_spend, on='DMA Name')

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
df['1stjob'] = df['first_job']
df['first_job'] = df['first_job'].astype('str')
df['new_customers'] = df.loc[(df['1stjob'].dt.date) >= datetime.date(2022,4,11), "customer_id"]
df['recurring_jobs'] = df.loc[df['recurring']==True,"id"]
# GeoDataFrame to join jobs and map
df['geometry'] = gp.points_from_xy(df['longitude'],df['latitude'], crs="EPSG:4326")
gdf = gp.GeoDataFrame(df,geometry=df['geometry'], crs="EPSG:4326")
job_map = spend_map.sjoin(gdf,how='left',predicate='intersects')
# Transform dates
job_map['created_at']= job_map['created_at'].dt.date
job_map['date_start']=job_map.apply(lambda x: datetime.datetime.strptime(x['date_start'], '%m/%d/%y'), axis=1)
job_map['date_start'] = job_map['date_start'].dt.date

job_map['recurring'] = job_map['recurring'].apply(lambda x: 1 if x==True else 0)

### Group only jobs by date
jobs_by_date = job_map.groupby(['created_at','dma']).agg({
                                                        'customer_id':'nunique',
                                                        'id':'nunique',
                                                        'recurring_jobs':'nunique',
                                                        'new_customers':'nunique'
                                                        }).reset_index()


### Spend by market and dates
spend_by_date = job_map.groupby(['date_start','dma']).mean('spend').reset_index()

### Join spend and jobs by dates/dmas
job_spend_by_date = spend_by_date.merge(jobs_by_date, left_on=['dma','date_start'],right_on=['dma','created_at'])
job_spend_by_date = job_spend_by_date[['date_start','dma','impressions','clicks','spend','customer_id','id','recurring_jobs','new_customers']]
job_spend_by_date['cost_per_new_customer'] = job_spend_by_date['spend'].div(job_spend_by_date['new_customers']).replace(np.inf, 0)
st.write('***Currently only for Prospecting | new markets | 4.11.22 FB campaign***')
st.write('***FB ads spend data will be 1 day behind***')
st.write("Choose start and end dates for info by dma")
start = st.date_input('Start Date',datetime.date(2022,4,11))
end = st.date_input('End Date')

jobs_by_dma = job_spend_by_date.loc[(job_spend_by_date['date_start']>=start)&(job_spend_by_date['date_start']<=end)]
jobs_by_dma = jobs_by_dma.groupby('dma').sum()
jobs_by_dma['cost_per_new_customer'] = jobs_by_dma['spend'].div(jobs_by_dma['new_customers']).replace(np.inf, 0)

st.metric('Total New Customers between {} and {}'.format(start,end),jobs_by_dma.new_customers.sum())
st.metric('Total spent on FB ads between {} and {}'.format(start,end),'${:,.2f}'.format(jobs_by_dma.spend.sum()))
cost = (jobs_by_dma.spend.sum())/(jobs_by_dma.new_customers.sum())
st.metric('Avg. Cost per New Customer (Blended)', '${:,.2f}'.format(cost))
st.write('Map of new customers')
#dmas['dma'] = dmas['NAME']
dfp = df.loc[(df['1stjob']>=start)&(df['1stjob']<=end)]
st.map(dfp.loc[~df['new_customers'].isna()])


st.header('total jobs by dma')
st.dataframe(jobs_by_dma)

csv2 = convert_df(jobs_by_dma)
st.download_button(label="Download Jobs by DMA csv",
                    data=csv2,
                    mime='text/csv')

st.header('job spend by date (full dates)')
st.dataframe(job_spend_by_date)
csv1 = convert_df(job_spend_by_date)
st.download_button(label="Download Job Spend by Date csv",
                    data=csv1,
                    mime='text/csv')



