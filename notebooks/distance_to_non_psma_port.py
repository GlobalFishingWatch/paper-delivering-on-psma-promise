#%%
import os
import numpy as np 
import pandas as pd 
from scgraph.geographs.marnet import marnet_geograph
from itertools import product
from multiprocessing import Pool
from multiprocessing import get_context
import pycountry
import matplotlib.pyplot as plt
import matplotlib
import geopandas as gpd
import seaborn as sns
from datetime import datetime, timedelta
import pandas_gbq
import pydata_google_auth
import tqdm

matplotlib.rcParams['pdf.fonttype'] = 42
plt.rcParams['font.family'] = 'Arial'


# %%
# get port data from World Port Index
port = pd.read_csv('data/World_Port_Index.csv')
port = port[['FID', 'COUNTRY', 'PORT_NAME' ,'LATITUDE', 'LONGITUDE']]

# clean up
port = port[~np.logical_and(port['LATITUDE']==0, port['LONGITUDE']==0)]
port.reset_index(inplace=True, drop=True)

port.loc[port['COUNTRY'].isnull(), 'COUNTRY'] = 'NA'

# recode
port.loc[port['PORT_NAME']=='JOHNSTON ATOLL', 'COUNTRY'] = 'UM'
port.loc[port['PORT_NAME']=='MIDWAY ISLAND', 'COUNTRY'] = 'UM'
port.loc[port['PORT_NAME']=='WAKE ISLAND', 'COUNTRY'] = 'UM'
port.loc[port['PORT_NAME']=='BARENTSBURG', 'COUNTRY'] = 'SJ'
port.loc[port['PORT_NAME']=='LONGYEARBYEN', 'COUNTRY'] = 'SJ'
port.loc[port['PORT_NAME']=='NY ALESUND', 'COUNTRY'] = 'SJ'


# add ISO3
port['state'] = [pycountry.countries.get(alpha_2=x).alpha_3 for x in port['COUNTRY']] 


#%%
'''
The shortest maritime routes are computed from a network of lines
covering the seas and following some of the most frequent martitime routes.
This maritime network is based on the Oak Ridge National Labs CTA
Transportation Network Group, Global Shipping Lane Network, World, 2000
(retrieved from geocommons.com or github), enriched with some additional lines
around the European coasts based on AIS data. Simplified versions of this network
have been produced for different resolutions (5km, 10km, 20km, 50km, 100km)
based on a shrinking of too short edges and a removal of similar edges.
For more detail on this generalisation algorithm, see the marnet module
based on (JGiscoTools)[https://github.com/eurostat/JGiscoTools].
'''


# points at sea
sea = pd.read_csv('data/5deg_fao_eez.csv')
sea = sea[sea['is_eez']==0]  # high seas only
sea.reset_index(inplace=True, drop=True)
sea['id'] = sea.index.values


# dictionary of FID and coordinates
fid2coord_port = dict(zip(port['FID'], zip(port['LONGITUDE'], port['LATITUDE'])))
fid2coord_sea = dict(zip(sea['id'], zip(sea['lon'], sea['lat'])))


# unique pairs
pair = list(product(sea['id'], port['FID']))


# Function to calculate distance for a pair
def get_distance(p):
    x, y = p
    output = marnet_geograph.get_shortest_path(
        origin_node={ "longitude": fid2coord_sea[x][0], "latitude": fid2coord_sea[x][1]}, 
        destination_node={"longitude":  fid2coord_port[y][0], "latitude": fid2coord_port[y][1]},
        output_units='km',
        node_addition_circuity=8)
    
    return [x, y, output['length']]


# parallel computing (it takes ~ 5 hours)
n_cpus = os.cpu_count()
p = get_context('fork').Pool(n_cpus)
results = p.map(get_distance, pair)
p.close()


# Convert the result to a DataFrame
out = pd.DataFrame(results, columns=['FID_SEA', 'FID_PORT', 'distance_km'])
out.to_parquet('data/port_distance_v20240618.parquet', engine='pyarrow')


#%%
#------------------------------------------
# distance from sea to port
out = pd.read_parquet('data/port_distance_v20240618.parquet')

# add port States
out = pd.merge(out, port[['FID', 'state']].rename(columns={'FID':'FID_PORT'}), how='left', on='FID_PORT')


# remove ports in UMI, IOT, ATA, ATF, HMD, BVT, CPT
remove_iso3 = ['UMI', 'IOT', 'ATA', 'ATF', 'HMD', 'BVT', 'CPT']
out = out[~out['state'].isin(remove_iso3)]



# %%
# PSMA
psma = pd.read_csv('../data/psma_ratifiers.csv', encoding='latin-1')
psma['Entry_into_force_date'] = pd.to_datetime(psma['Entry_into_force_date'], format='%m/%d/%y')


# territories
st = pd.read_csv('../data/sovereign_territory_pair.csv', encoding='latin1')

# remove french overseas departments
french_overseas_dept = ['GUF', 'GLP', 'MTQ', 'MYT', 'REU']
st = st[~st['territory_iso3'].isin(french_overseas_dept)]

# add entry date
eu = ['AUT', 'BEL', 'BGR', 'HRV', 'CYP', 'CZE', 'DNK', 'EST', 'FIN', 'FRA', 'DEU', 'GRC', 'HUN', 'IRL', 'ITA', 'LVA', 'LTU', 'LUX', 'MLT', 'NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'ESP', 'SWE']
def entry_date(x):
    if x in 'FRA':
        out = psma.loc[psma['iso3']==x, 'Entry_into_force_date'].values[0]
    elif x in 'DNK':
        out = psma.loc[psma['iso3']==x, 'Entry_into_force_date'].values[0]
    elif x in ['GBR', 'NLD']:
        out = np.nan
    elif x in eu:
        out = psma.loc[psma['iso3']=='EU', 'Entry_into_force_date'].values[0]
    else:
        out = psma.loc[psma['iso3']==x, 'Entry_into_force_date']
        if out.shape[0] > 0:
            out = out.values[0]
        else:
            out = np.nan
    
    return out
    
st['Entry_into_force_date'] = st['sovereign_iso3'].map(entry_date)
st = st[['territory_iso3', 'Entry_into_force_date']].dropna()
st.rename(columns={'territory_iso3':'iso3'}, inplace=True)

psma = pd.concat([psma, st])


# add EU (pre-brexit) + french overseas departments
psma_plus = pd.DataFrame({'iso3': eu + french_overseas_dept})
psma_plus['Entry_into_force_date'] = psma.loc[psma['iso3']=='EU', 'Entry_into_force_date'].values[0]

psma = pd.concat([psma, psma_plus])


# %%
# loop over date (it will take ~ 8 minutes)
min_date = psma['Entry_into_force_date'].min() + timedelta(days=-30)
summary = pd.DataFrame()
for current_date in pd.date_range(min_date, '2021-12-31'):
    
    psma_state = psma.loc[psma['Entry_into_force_date'] <= current_date, 'iso3'].to_list()

    # for each FID_SEA, get the distance to the closest non-PSMA port
    foo = out[~out['state'].isin(psma_state)].copy()
    idx = foo.groupby('FID_SEA')['distance_km'].idxmin()
    bar = foo.loc[idx]
    bar['date'] = current_date

    summary = pd.concat([summary, bar])

    if pd.Timestamp(current_date).day==1:
        print(current_date)


# add lat and lon
summary = pd.merge(summary, sea.rename(columns={'id': 'FID_SEA'}), how='left', on='FID_SEA')


# %%
#-------------------------
# fishing hours in 2021
#-------------------------
fishing_hours = """
with

fishing_ais as (
  select
    vessel_id,
    extract(year from event_start) as year,
    floor(lat_mean / 5) as lat_bin,
    floor(lon_mean / 5) as lon_bin,
    timestamp_diff(event_end, event_start, minute)/60 as fishing_h
  from `world-fishing-827.pipe_ais_v3_published.product_events_fishing_v*`
  where event_start between '2015-01-01' and '2021-01-31'
),

fishing_engine as (
    select * from fishing_ais
    left join (
        select vessel_id, ssvid
        from `world-fishing-827.pipe_ais_v3_published.vessel_info`
    )
    using (vessel_id)
    left join (
        select ssvid, year, best.best_engine_power_kw as engine_power_kw
        from `world-fishing-827.pipe_ais_v3_published.vi_ssvid_byyear_v`
    )
    using(ssvid, year)
),

fishig_gridded as (
  select 
    lat_bin * 5 as lat_bin,
    lon_bin * 5 as lon_bin,
    sum(fishing_h) as fishing_h,
    sum(fishing_h * engine_power_kw) as kw_fishing_h
  from fishing_engine
  group by lat_bin, lon_bin
)

select
  lat_bin,
  lon_bin,
  fishing_h / (cos(lat_bin * (acos(-1)/180)) * 111 * 111) as fishing_h_km2,
  kw_fishing_h / (cos(lat_bin * (acos(-1)/180)) * 111 * 111) as kw_fishing_h_km2
from fishig_gridded
"""


SCOPES = [
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/drive',
]

credentials = pydata_google_auth.get_user_credentials(
    SCOPES, auth_local_webserver = True)

pandas_gbq.context.credentials = credentials
pandas_gbq.context.project = 'gfwanalysis'


# run query
effort = pandas_gbq.read_gbq(fishing_hours)


effort['lat'] = effort['lat_bin'] + 2.5
effort['lon'] = effort['lon_bin'] + 2.5


# merge
summary = pd.merge(summary, effort[['lat', 'lon', 'kw_fishing_h_km2']], how='left', on=['lat', 'lon'])
summary = summary[summary['kw_fishing_h_km2'] > 0].reset_index()
summary['log_kw_fishing_h_km2'] = np.log(summary['kw_fishing_h_km2'])


summary.to_csv('data/summary.csv', index=False)


# %%
# average distance weighted by log kw fishing hour
summary = pd.read_csv('data/summary.csv')
result = summary.groupby('date')['distance_km'].apply(lambda x: np.average(x, weights=summary.loc[x.index, 'log_kw_fishing_h_km2']))
result = pd.DataFrame(result).reset_index()


# plot
fig, ax = plt.subplots(figsize=(9, 6))
sns.lineplot(x='date', y='distance_km', data=result)
plt.ylim(0, 3000)
plt.savefig('port_distance_v20240618.pdf')


# How much increase?
print('from', result['distance_km'].tolist()[0], 'to', result['distance_km'].tolist()[-1])
print(result['distance_km'].tolist()[-1]/result['distance_km'].tolist()[0])
## -> from 1549 km to 2736 km, increased by 77%


# %%
#-------------------------------------------------
# by FAO
summary_fao = summary[~summary['fao'].isnull()].copy()
result_fao = summary_fao.groupby(['date', 'fao'])['distance_km'].apply(lambda x: np.average(x, weights=summary.loc[x.index, 'kw_fishing_h_km2']))
result_fao = pd.DataFrame(result_fao).reset_index()
result_fao['fao'] = result_fao['fao'].astype(int)


# change
fao0 = result_fao[result_fao['date']=='2016-06-01'].copy()
fao1 = result_fao[result_fao['date']=='2021-12-31'].copy()

fao_change = pd.merge(fao0, fao1, how='left', on='fao')
fao_change['increase'] = fao_change['distance_km_y']/ fao_change['distance_km_x']
fao_change = fao_change.sort_values('increase')


# plot map
import matplotlib.colors as colors
fao_df = gpd.read_file('../data/fao')

fao_df = pd.merge(fao_df, fao_change, how='left', left_on='zone', right_on='fao')
fao_df['increase'] = fao_df['increase']

ax = fao_df.plot(column='increase', cmap='viridis', norm=colors.Normalize(vmin=1))
ax.set_axis_off()
cbar = plt.colorbar(ax.get_children()[0], ax=ax, orientation='horizontal', shrink=0.5, ticks=[1,3,5,7])
world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
world.plot(ax=ax, color='#222222', linewidth=0)

plt.savefig('plot/fao_map_v20240618.pdf')


# another plot
import matplotlib.lines as mlines

foo = fao_change.copy()
foo = foo.sort_values('distance_km_x')
foo['fao'] = foo['fao'].astype(str)

# Create a mapping for categorical values to numerical values
area_mapping = {area: i for i, area in enumerate(foo['fao'].unique())}

# Map categorical values to numerical values
foo['fao_num'] = foo['fao'].map(area_mapping)

fig, ax = plt.subplots()

scatter1 = ax.scatter(x=foo['distance_km_x'], y=foo['fao_num'], label='2016')
scatter2 = ax.scatter(x=foo['distance_km_y'], y=foo['fao_num'], label='2021')

# Connect points with line segments
for i in range(len(foo)):
    line = mlines.Line2D([foo['distance_km_x'].iloc[i], foo['distance_km_y'].iloc[i]],
                        [foo['fao_num'].iloc[i], foo['fao_num'].iloc[i]],
                        color='red')
    ax.add_line(line)

# Set labels and title
ax.set_xlabel('Distance (km)')
ax.set_ylabel('FAO')

# Relabel y-axis with original categorical values
ax.set_yticks(list(area_mapping.values()))
ax.set_yticklabels(list(area_mapping.keys()))

# Add legend
ax.legend()

# ad grid
ax.grid(True, linestyle='-', linewidth=0.2, color='gray', alpha=0.5)

ax.set_xlim(left=0)
plt.savefig('fao_change_v20240618.pdf')
plt.show()