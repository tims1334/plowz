import geopandas as gp
import pandas as pd
import redshift_connector
from query_list import first_orders, orders
import streamlit as st
import datetime
import numpy as np
import altair as alt


dma_map = gp.read_file('FB_DMA.shp')
fb_spend = pd.read_csv("fb_spend.csv")
fb_spend['DMA Name'] = fb_spend['DMA region']
spend_map = dma_map.merge(fb_spend, on='DMA Name')

passwords = st.sidebar.text_input('DB Password')


conn = redshift_connector.connect(
        host=st.secrets["hosts"],
        database=st.secrets["databases"],
        user=st.secrets["users"],
        password=passwords
        )
cursor: redshift_connector.Cursor = conn.cursor()

cursor.execute(orders)
df: pd.DataFrame = cursor.fetch_dataframe()
cursor.execute(first_orders)
fo: pd.DataFrame = cursor.fetch_dataframe()
df = df.merge(fo,how="left",on='customer_id')

df['latitude'] = df.latitude.astype('float64')
df['longitude'] = df.longitude.astype('float64')
df['1stjob'] = df['first_job']
df['first_job'] = df['first_job'].astype('str')
df['new_customers'] = df.loc[(df['1stjob'].dt.date) >= datetime.date(2022,4,1), "customer_id"]

df['geometry'] = gp.points_from_xy(df['longitude'],df['latitude'], crs="EPSG:4326")
gdf = gp.GeoDataFrame(df,geometry=df['geometry'], crs="EPSG:4326")
job_map = spend_map.sjoin(gdf,how='left',predicate='intersects')


dmas = job_map.groupby(['NAME','Campaign name']).agg({
                                                    'Amount spent (USD)':'mean',
                                                    'Unique purchases':'mean',
                                                    'customer_id':'nunique',
                                                    'id':'nunique',
                                                    'recurring':'sum',
                                                    'new_customers':'nunique'
                                                    })

dmas['cost_per_new_customer'] = dmas['Amount spent (USD)'].div(dmas['new_customers']).replace(np.inf, 0)
#dmas['cost_per_new_customer'] = dmas.apply(lambda x: np.where(x['new_customers'], x['Amount spent (USD)']/x['new_customers']), axis=1)
st.dataframe(dmas)
st.map(df.loc[~df['new_customers'].isna()])

