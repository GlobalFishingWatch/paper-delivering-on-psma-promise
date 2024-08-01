# %%
import numpy as np 
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
plt.rcParams['pdf.fonttype'] = 42

#%%
# output of port_visit_landed_fishing_effort_v20240627.sql
data = pd.read_parquet('data/landed_fishing_effort_v20240627.parquet')

data.rename(columns={'iso3': 'port'}, inplace=True)

# convert french overseas departments to france
french_overseas_dep = ['MTQ', 'GLP', 'REU', 'MYT', 'GUF']
data['flag'] = ['FRA' if x in french_overseas_dep else x for x in data['flag']]
data['port'] = ['FRA' if x in french_overseas_dep else x for x in data['port']]


# re-aggregate
data = data.groupby(['flag', 'port', 'year'])['landed_fishing_effort'].sum().reset_index()


# flag of interest
pair = pd.read_csv('data/sovereign_territory_pair.csv', encoding='latin1')
pair = pair[['sovereign_iso3', 'territory_iso3']]

st_list = list(set(pair['sovereign_iso3']) | set(pair['territory_iso3']))

foo = data[data['flag'].isin(st_list)].copy()

# remove non-PSMA ports
# 2015
# CHN, MAC, HKG
# GRL, FRO in 2016
condition_1 = foo['year'] <= 2015
condition_2 = foo['port'].isin(['CHN', 'HKG', 'MAC'])
condition_3 = (foo['port'].isin(['FRO', 'GRL'])) & (foo['year'] == 2016)

remove_conditions = condition_1 | condition_2 | condition_3

bar = foo[~remove_conditions].copy()



# find the case where either
# territory-flagged vessels landing in sovereign ports
# sovereign-flagged vessels landing in territory ports
# territory-flagged vessels landing in other territory-flagged ports with same sovereign

bar = pd.merge(bar, pair.rename(columns={'territory_iso3': 'flag', 'sovereign_iso3':'sovereign_flag'}), how='left', on='flag')
bar['sovereign_flag'] = bar['sovereign_flag'].fillna(bar['flag'])

bar = pd.merge(bar, pair.rename(columns={'territory_iso3': 'port', 'sovereign_iso3':'sovereign_port'}), how='left', on='port')
bar['sovereign_port'] = bar['sovereign_port'].fillna(bar['port'])

baz = bar[(bar['sovereign_flag']==bar['sovereign_port']) & (bar['flag']!=bar['port'])].copy()

summary = baz.groupby('year')['landed_fishing_effort'].sum().reset_index()



# plot
# line
x = summary['year']
y = summary['landed_fishing_effort']


plt.plot(x, y)
plt.scatter(x, y)


# Adding labels and title
plt.xlabel('Year')
plt.ylabel('Landed fishing effort')
plt.ylim(0, np.max(summary['landed_fishing_effort']) * 1.1) 


plt.savefig('plot/figS7_v3.pdf')



#
aa = baz[baz['year']==2021].groupby('sovereign_port')['landed_fishing_effort'].sum().reset_index()
aa['prop'] = aa['landed_fishing_effort']/aa['landed_fishing_effort'].sum()

# %%
# landed fishing effort in ports of PSMA parties
data2021 = data[data['year']==2021].copy()

# add sovereign 
data2021 = pd.merge(data2021, pair.rename(columns={'territory_iso3': 'flag', 'sovereign_iso3':'sovereign_flag'}), how='left', on='flag')
data2021['sovereign_flag'] = data2021['sovereign_flag'].fillna(data2021['flag'])

data2021 = pd.merge(data2021, pair.rename(columns={'territory_iso3': 'port', 'sovereign_iso3':'sovereign_port'}), how='left', on='port')
data2021['sovereign_port'] = data2021['sovereign_port'].fillna(data2021['port'])


psma = pd.read_csv('data/psma_ratifiers.csv')
psma2021 = list(psma.loc[psma['year'] <= 2021, 'iso3'])
psma2021.extend(['AUT', 'BEL', 'BGR', 'HRV', 'CYP', 'CZE', 'DNK', 'EST', 'FIN', 'FRA', 'DEU', 'GRC', 'HUN', 'IRL', 'ITA', 'LVA', 'LTU', 'LUX', 'MLT', 'NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'ESP', 'SWE', 'GBR'])


# psma port
foo = data2021[data2021['port'].isin(psma2021)].copy()


a = summary.loc[summary['year']==2021, 'landed_fishing_effort'].values[0]
b = foo['landed_fishing_effort'].sum()

a/b


