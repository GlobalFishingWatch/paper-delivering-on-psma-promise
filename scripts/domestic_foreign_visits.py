# %%
import numpy as np
import pandas as pd
import pycountry
import matplotlib.pyplot as plt

pycountry.countries.get(alpha_3='TWN').name = 'Chinese Taipei'
pycountry.countries.get(alpha_3='IRN').name = 'Iran'
pycountry.countries.get(alpha_3='MDA').name = 'Moldova'
pycountry.countries.get(alpha_3='KOR').name = 'Republic of Korea'
pycountry.countries.get(alpha_3='PRK').name = 'North Korea'
pycountry.countries.get(alpha_3='RUS').name = 'Russia'
pycountry.countries.get(alpha_3='TZA').name = 'Tanzania'
pycountry.countries.get(alpha_3='COD').name = 'DR Congo'
pycountry.countries.get(alpha_3='BRN').name = 'Brunei'
pycountry.countries.get(alpha_3='FSM').name = 'Micronesia'
pycountry.countries.get(alpha_3='VEN').name = 'Venezuela'


# %%
#----------------
# territory vs. sovereign
#----------------
pair = pd.read_csv('data/sovereign_territory_pair.csv')
pair = pair[['territory_iso3', 'sovereign_iso3']].copy()

## remove Niue and Cook Islands from New Zealand
pair = pair[~pair.territory_iso3.isin(['NIU', 'COK'])].copy()

## add Isle of Man
pair = pair.append({'territory_iso3': 'IMN', 'sovereign_iso3': 'GBR'}, ignore_index=True)

## add Taiwan-China
# pair = pair.append({'territory_iso3': 'TWN', 'sovereign_iso3': 'CHN'}, ignore_index=True)

## remove French overseas departments
## French Guiana, Guadeloupe, Martinique, Mayotte, Réunion
#french_overseas = ['GUF', 'GLP', 'MTQ', 'MYT', 'REU']
#pair = pair[~pair['territory_iso3'].isin(french_overseas)]

# %%
#----------------------------------------
# Domestic & foreign visits
#----------------------------------------
year = 2019

data = pd.read_parquet('data/port_visit.parquet')   # output of 'port_visit.sql' saved in parquet

data['visit_date'] = pd.to_datetime(data['visit_date'], format='%Y-%m-%d')

# remove vessels with unknown flags
data = data[np.logical_and(data.flag != 'UNK', ~data.flag.isnull())].copy()

# all visits in this year
data = data[data['visit_date'].dt.year==year]

# remove Antarctica
data = data[data.iso3 != 'ATA']

# fishing vessels / support vessels
foo = data[data['vessel_class']=='fishing'].copy()
# foo = data[data['vessel_class'].isin(['carrier', 'bunker'])].copy()

summary = foo.groupby(['iso3', 'flag']).size().to_frame('n_visits').reset_index()


# lump EU
eu = ['AUT', 'BEL', 'BGR', 'HRV', 'CYP', 'CZE', 'DNK', 'EST', 'FIN', 'FRA', 'DEU', 'GRC', 'HUN', 'IRL', 'ITA', 'LVA', 'LTU', 'LUX', 'MLT', 'NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'ESP', 'SWE']
summary[['iso3', 'flag']] = [['EU', 'EU'] if x in eu and y in eu else [x, y] for x, y in zip(summary.iso3, summary.flag)]

# tally
summary = summary.groupby(['iso3', 'flag']).sum().reset_index()

# check if sovereign-territory
pair_dict = [{x, y} for x, y in zip(pair.territory_iso3, pair.sovereign_iso3)]
def is_sovereign_territory(a, b):
    if {a, b} in pair_dict:
        b = 1
    else:
        b = 0
    return b

summary['is_st'] = [is_sovereign_territory(x, y) for x, y in zip(summary.iso3, summary.flag)]


# %%
#----------
# domestic
domestic = summary[summary['iso3']==summary['flag']].copy()
domestic.sort_values('n_visits', ascending=False, inplace=True)
domestic['state'] = ['EU' if x=='EU' else pycountry.countries.get(alpha_3=x).name for x in domestic.iso3]
domestic['type'] = 'domestic'

# foreign
foreign = {'type': 'foreign', 'n_visits': summary[np.logical_and(summary['iso3']!=summary['flag'], summary['is_st']==0)].sum().n_visits}
foreign = pd.DataFrame(foreign, index=[0])

# domestic/foreign
domestic_or_foreign = {'type': 'domestic_or_foreign', 'n_visits': summary[summary['is_st']==1].sum().n_visits}
domestic_or_foreign = pd.DataFrame(domestic_or_foreign, index=[0])

# for Azote
foo = pd.concat([foreign, domestic_or_foreign, domestic], ignore_index=True)

foo['proportion'] = foo['n_visits']/foo['n_visits'].sum()

#foo[['type', 'state', 'n_visits', 'proportion']].to_csv('data/port_visits_fishing@4.csv', index=False)
foo[['type', 'state', 'n_visits', 'proportion']].to_csv('data/port_visits_fishing@5.csv', index=False)



bar = foo.groupby('type').sum()
bar['proportion'].round(3) * 100