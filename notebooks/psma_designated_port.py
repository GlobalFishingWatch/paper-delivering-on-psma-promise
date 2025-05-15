# %%
import numpy as np 
import pandas as pd 


# the output of summarize_vessel_visit_psma_port_v20250429.sql
data = pd.read_csv('data/summarize_vessel_visit_psma_port_v20250429.csv')


# %%
#-----------------------------------
# How many visits to designated ports
# for visits to PSMA parties?
#-----------------------------------

# foreign visits to PSMA Parties
a = data[(data['type']=='foreign') & (data['is_psma_state']==1)].copy()

b = a[a['is_psma_port']==1].copy()

aa = a['n_visits_fishing_vessel_hs'].sum() + a['n_visits_support_vessel_hs'].sum()
bb = b['n_visits_fishing_vessel_hs'].sum() + b['n_visits_support_vessel_hs'].sum()
bb/aa

# 82%



# %%
#---------------------------------
# who visited non-designated ports of PSMA Parties?
#---------------------------------

c = a[a['is_psma_port']==0].copy()

c['n_visits_hs'] = c['n_visits_fishing_vessel_hs'] + c['n_visits_support_vessel_hs']


# remove unknown flag
c = c[~c['flag'].isnull()]


# add PSMA status of the flag in 2023
psma = pd.read_csv('../data/psma_ratifier_full.csv')
psma['year'] = pd.to_datetime(psma['Entry_into_force_date'], format='%m/%d/%y').dt.year

psma2023 = psma[psma['year'] < 2024].copy()

c['is_psma_flag'] = [1 if x in list(psma2023['iso3']) else 0 for x in c['flag']]



foo = c.groupby('is_psma_flag')['n_visits_hs'].sum()
foo = foo.reset_index()

foo.loc[foo['is_psma_flag']==1, 'n_visits_hs']/foo['n_visits_hs'].sum()

# 70% is PSMA flag

