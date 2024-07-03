# %%
import numpy as np 
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
plt.rcParams['pdf.fonttype'] = 42


# output of port_visit_landed_fishing_effort_v20240627.sql
data = pd.read_parquet('data/landed_fishing_effort_v20240627.parquet')


# add sovereign flag & iso3
pair = pd.read_csv('data/sovereign_territory_pair.csv', encoding='latin1')
pair = pair[['sovereign_iso3', 'territory_iso3']]

data = pd.merge(data, pair.rename(columns={'territory_iso3': 'iso3'}), how='left', on='iso3')
data['sovereign_iso3'] = data['sovereign_iso3'].fillna(data['iso3'])

pair.rename(columns={'sovereign_iso3': 'sovereign_flag', 'territory_iso3': 'flag'}, inplace=True)
data = pd.merge(data, pair.rename(columns={'territory_iso3': 'flag'}), how='left', on='flag')

data['sovereign_flag'] = data['sovereign_flag'].fillna(data['flag'])


#------------------
# add psma
#------------------
psma = pd.read_csv('data/psma_ratifiers.csv')
psma = [[x,y] for x, y in zip(psma['iso3'], psma['year'])]


# add EU
eu = ['AUT', 'BEL', 'BGR', 'DNK', 'HRV', 'CYP', 'CZE', 'EST', 'FIN', 'FRA', 'DEU', 'GRC', 'HUN', 'IRL', 'ITA', 'LVA', 'LTU', 'LUX', 'MLT', 'NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'ESP', 'SWE']
psma.extend([[x, 2016] for x in eu + ['GBR']])
psma = pd.DataFrame(psma, columns=['flag', 'year']).drop_duplicates()


# remove Danish territories (different year)
mask = (psma['flag'] == 'DNK') & (psma['year'] == 2017)
psma = psma[~mask]


# Merge with psma
data = pd.merge(data, psma, left_on='sovereign_flag', right_on='flag', how='left', suffixes=('', '_psma'))


# change Faroe Islands and Greenland
data.loc[data['flag'].isin(['FRO', 'GRL']), 'year'] = 2017


# Create the is_psma column based on the year comparison
data['is_psma'] = data.apply(lambda row: row['year'] >= row['year_psma'] if pd.notnull(row['year_psma']) else False, axis=1)

# Drop the psma year column to clean up
data = data.drop(columns=['year_psma'])


#------------------
# add sovereign-territory
#------------------
# add sovereign-territory or not
st_list = list(set(pair['flag']) | set(pair['sovereign_flag']))

data['is_st_flag'] = [1 if x in st_list else 0 for x in data['sovereign_flag']]


#-------------------
# vessels flagged to PSMA
#-------------------
foo = data[data['is_psma']].copy()

bar = foo.groupby(['year', 'is_st_flag'])['landed_fishing_effort'].sum().reset_index()


# In 2021
bar2021 = bar[bar['year']==2021].copy()
bar2021.loc[bar2021['is_st_flag']==1, 'landed_fishing_effort']/bar2021['landed_fishing_effort'].sum()

## 5.2%

a = foo[(foo['year']==2021) & (foo['is_st_flag']==1)].copy()
b = a.groupby('sovereign_flag')['landed_fishing_effort'].sum().reset_index()
b = b.sort_values('landed_fishing_effort', ascending=False)
b['rop'] = b['landed_fishing_effort']/b['landed_fishing_effort'].sum()

b


#--------------------
# plot Fig. S7
#--------------------
bar = foo.groupby(['year', 'is_st_flag'])['landed_fishing_effort'].sum().reset_index()
total = foo.groupby('year')['landed_fishing_effort'].sum().to_frame('landed_fishing_effort_total').reset_index()

bar = pd.merge(bar, total, how='left', on='year')
bar['prop'] = bar['landed_fishing_effort']/bar['landed_fishing_effort_total'] * 100


# line
x = bar['year'].drop_duplicates().sort_values()
y = bar[bar['is_st_flag']==1].sort_values('year')['prop']


plt.plot(x, y)
plt.scatter(x, y)


# Adding labels and title
plt.xlabel('Year')
plt.ylabel('Fishing effort (%)')
plt.ylim(0, 100) 
plt.xlim(2016, 2021)

plt.savefig('figS7.pdf')


#%%
#----------------------------
# landed fishing effort by foreign vessels
#----------------------------
a = data.loc[(data['year']==2021) & (data['sovereign_iso3']!= data['sovereign_flag']), 'landed_fishing_effort'].sum().item()
b = data.loc[data['year']==2021, 'landed_fishing_effort'].sum().item()

a/b
## 57%

# domestic visits
a = data[(data['year']==2021) & (data['sovereign_iso3']== data['sovereign_flag'])].copy()
b = a.groupby('sovereign_iso3')['landed_fishing_effort'].sum().reset_index()
b = b.sort_values('landed_fishing_effort', ascending=False)
b['prop'] = b['landed_fishing_effort']/b['landed_fishing_effort'].sum()
b