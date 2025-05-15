#%%
import os
import numpy as np 
import pandas as pd 
from scgraph.geographs.marnet import marnet_geograph
from itertools import product
from multiprocessing import Pool
import pycountry
import matplotlib.pyplot as plt
import matplotlib
import geopandas as gpd
import seaborn as sns
from datetime import datetime, timedelta

matplotlib.rcParams['pdf.fonttype'] = 42
plt.rcParams['font.family'] = 'Arial'



# %%
# get port data from World Port Index
port = pd.read_csv('../data/World_Port_Index.csv')
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



# points at sea
sea = pd.read_csv('data/5deg_FAORegions.csv')
sea = sea[~sea['F_AREA'].isnull()]
sea.rename(columns={'FID_1':'FID'}, inplace=True)
sea = sea[['FID', 'LATITUDE', 'LONGITUDE' ,'F_AREA']]
sea['F_AREA'] = sea['F_AREA'].astype(int)
sea.reset_index(inplace=True, drop=True)



'''
#%%
#------------------------------------------
# all distance from sea to port
#------------------------------------------
# dictionary of FID and coordinates
fid2coord_port = dict(zip(port['FID'], zip(port['LONGITUDE'], port['LATITUDE'])))
fid2coord_sea = dict(zip(sea['FID'], zip(sea['LONGITUDE'], sea['LATITUDE'])))


# unique FID pairs
fid_pair = list(product(sea['FID'], port['FID']))


# Function to calculate distance for a pair
def get_distance(pair):
    x, y = pair
    output = marnet_geograph.get_shortest_path(
        origin_node={ "longitude": fid2coord_sea[x][0], "latitude": fid2coord_sea[x][1]}, 
        destination_node={"longitude":  fid2coord_port[y][0], "latitude": fid2coord_port[y][1]},
        output_units='km',
        node_addition_circuity=8)
    
    return [x, y, output['length']]


num_processes = os.cpu_count()


# Create a Pool
with Pool(processes=num_processes) as pool:
    # Parallelize distance calculations using map
    distance = pool.map(get_distance, fid_pair)


# Convert the result to a DataFrame
out = pd.DataFrame(distance, columns=['FID_SEA', 'FID_PORT', 'distance_km'])

out.to_parquet('data/port_distance_v20240225.parquet', engine='pyarrow')

'''


#%%
#------------------------------------------
# shortest distance from sea to port
#------------------------------------------
out = pd.read_parquet('data/port_distance_v20240130.parquet')

# add port States
out = pd.merge(out, port[['FID', 'state']].rename(columns={'FID':'FID_PORT'}), how='left', on='FID_PORT')


# port ID in UMI, IOT, ATA, ATF, HMD, BVT, CPT
remove_port1 = port.loc[port['state'].isin(['UMI', 'IOT', 'ATA', 'ATF', 'HMD', 'BVT', 'CPT']), 'FID'].to_list()
# KWAJALEIN in Marshall Islands
remove_port2 = port.loc[port['PORT_NAME']=='KWAJALEIN', 'FID'].to_list()

remove_port = remove_port1 + remove_port2

out = out[~out['FID_PORT'].isin(remove_port)]


# %%
# PSMA
psma = pd.read_csv('../data/psma_ratifier.csv', encoding='latin-1')
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
# loop over date
min_date = psma['Entry_into_force_date'].min() + timedelta(days=-30)
summary = pd.DataFrame()
for current_date in pd.date_range(min_date, '2023-12-31'):
    
    psma_state = psma.loc[psma['Entry_into_force_date'] <= current_date, 'iso3'].to_list()

    # for each FID_SEA, get the distance to the closest non-PSMA port
    foo = out[~out['state'].isin(psma_state)].copy()
    idx = foo.groupby('FID_SEA')['distance_km'].idxmin()
    bar = foo.loc[idx]
    bar['date'] = current_date

    summary = pd.concat([summary, bar])

    print(current_date)


summary.to_parquet('data/summary_v20250502.parquet', engine='pyarrow')


#%%
# add lat and lon
summary = pd.read_parquet('data/summary_v20250502.parquet')
summary = pd.merge(summary, sea.rename(columns={'FID': 'FID_SEA'}), how='left', on='FID_SEA')
summary['LATITUDE'] = [int(10 * x) for x in summary['LATITUDE']]
summary['LONGITUDE'] = [int(10 * x) for x in summary['LONGITUDE']]

summary = summary[summary['date'] < '2022-01-01']



# %%
#-------------------------
# fishing hours in 2021
#-------------------------
effort = pd.read_parquet('data/fishing_hour_gridded_2021.parquet')
effort.dropna(inplace=True)


effort['LATITUDE'] = effort['lat_bin'] + 2.5
effort['LONGITUDE'] = effort['lon_bin'] + 2.5

effort['fishing_hours_km2'] = [111 * 111 * np.cos(x * (np.pi/180)) * y for x, y in zip(effort['LATITUDE'], effort['fishing_hours'])]
effort['kw_fishing_hours_km2'] = [111 * 111 * np.cos(x * (np.pi/180)) * y for x, y in zip(effort['LATITUDE'], effort['kw_fishing_hours'])]

# merge
effort['LATITUDE'] = [int(10 * x) for x in effort['LATITUDE']]
effort['LONGITUDE'] = [int(10 * x) for x in effort['LONGITUDE']]
summary = pd.merge(summary, effort, how='left', on=['LATITUDE', 'LONGITUDE'])
summary = summary[summary['fishing_hours'] > 0]

# %%
# average distance weighted by kw fishing hour
summary['log_kw_fishing_hours_km2'] = np.log(summary['kw_fishing_hours_km2'])
result = summary.groupby('date')['distance_km'].apply(lambda x: np.average(x, weights=summary.loc[x.index, 'log_kw_fishing_hours_km2']))
result = pd.DataFrame(result).reset_index()


# plot
fig, ax = plt.subplots(figsize=(9, 6))
sns.lineplot(x='date', y='distance_km', data=result)
plt.ylim(0, 2500)
plt.savefig('port_distance_v20250502.pdf')

# How much increase?
print('from', result['distance_km'].tolist()[0], 'to', result['distance_km'].tolist()[-1])
print(result['distance_km'].tolist()[-1]/result['distance_km'].tolist()[0])
## -> from 1143 km to 2261 km, increased by 98%


# %%
#-------------------------------------------------
# by FAO
result_fao = summary.groupby(['date', 'F_AREA'])['distance_km'].apply(lambda x: np.average(x, weights=summary.loc[x.index, 'log_kw_fishing_hours_km2']))
result_fao = pd.DataFrame(result_fao).reset_index()
result_fao['F_AREA'] = result_fao['F_AREA'].astype(str)


# change
fao0 = result_fao[result_fao['date']=='2016-06-01'].copy()
fao1 = result_fao[result_fao['date']=='2021-12-31'].copy()

fao_change = pd.merge(fao0, fao1, how='left', on='F_AREA')
fao_change['increase'] = fao_change['distance_km_y']/ fao_change['distance_km_x']
fao_change = fao_change.sort_values('increase')
fao_change['F_AREA'] = fao_change['F_AREA'].astype(int)

# plot map
from matplotlib.colors import LogNorm

fao_df = gpd.read_file('../data/fao')

fao_df = pd.merge(fao_df, fao_change, how='left', left_on='zone', right_on='F_AREA')
fao_df['increase'] = fao_df['increase']

ax = fao_df.plot(column='increase', cmap='viridis', norm=LogNorm())
ax.set_axis_off()
cbar = plt.colorbar(ax.get_children()[0], ax=ax, orientation='horizontal', ticks=[1, 5, 10])
world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
world.plot(ax=ax, color='#222222', linewidth=0)

plt.savefig('fao_increase_v20250502.pdf')


# another plot
import matplotlib.lines as mlines

foo = fao_change.copy()
foo = foo.sort_values('distance_km_x')
foo['F_AREA'] = foo['F_AREA'].astype(str)

# Create a mapping for categorical values to numerical values
area_mapping = {area: i for i, area in enumerate(foo['F_AREA'].unique())}

# Map categorical values to numerical values
foo['F_AREA_num'] = foo['F_AREA'].map(area_mapping)

fig, ax = plt.subplots()

scatter1 = ax.scatter(x=foo['distance_km_x'], y=foo['F_AREA_num'], label='2016')
scatter2 = ax.scatter(x=foo['distance_km_y'], y=foo['F_AREA_num'], label='2021')

# Connect points with line segments
for i in range(len(foo)):
    line = mlines.Line2D([foo['distance_km_x'].iloc[i], foo['distance_km_y'].iloc[i]],
                        [foo['F_AREA_num'].iloc[i], foo['F_AREA_num'].iloc[i]],
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

plt.savefig('fao_change_v20250502.pdf')
plt.show()
