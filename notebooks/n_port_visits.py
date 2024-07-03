# %%
import numpy as np
import pandas as pd
import pycountry
import matplotlib
import matplotlib.pyplot as plt
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['font.sans-serif'] = "Arial"

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
#----------------------------------------
# territory vs. sovereign
#----------------------------------------
pair = pd.read_csv('data/sovereign_territory_pair.csv', encoding='latin1')
pair = pair[['territory_iso3', 'sovereign_iso3']].copy()


## remove French overseas departments
## French Guiana, Guadeloupe, Martinique, Mayotte, Réunion
french_overseas_dept = ['GUF', 'GLP', 'MTQ', 'MYT', 'REU']
pair = pair[~pair['territory_iso3'].isin(french_overseas_dept)]

# make a set
pair_set = [{x, y} for x, y in zip(pair.territory_iso3, pair.sovereign_iso3)]


# %%
#----------------------------------------
# Domestic & foreign visits
#----------------------------------------
# output of 'port_visit_fishing_v20240627.sql'
visit_fishing = pd.read_parquet('data/port_visit_fishing_v20240627.parquet')
visit_fishing['vessel_class'] = 'fishing'

# output of 'port_visit_support_v20240627.sql'
visit_support = pd.read_parquet('data/port_visit_support_v20240627.parquet')
visit_support['vessel_class'] = 'support'

visit = pd.concat([visit_fishing, visit_support])


# remove vessels with unknown flags
visit = visit[np.logical_and(visit['flag'] != 'UNK', ~visit['flag'].isnull())].copy()


# remove Antarctica
visit = visit[visit['iso3'] != 'ATA']


# convert French overseas department to France
foo = visit.copy()
foo.reset_index(inplace=True)
foo['iso3'] = ['FRA' if x in french_overseas_dept else x for x in foo['iso3']]
foo['flag'] = ['FRA' if x in french_overseas_dept else x for x in foo['flag']]


# Change iso3 and flag to EU when both are EU
# EU (post Brexit)
eu = ['AUT', 'BEL', 'BGR', 'HRV', 'CYP', 'CZE', 'DNK', 'EST', 'FIN', 'FRA', 'DEU', 'GRC', 'HUN', 'IRL', 'ITA', 'LVA', 'LTU', 'LUX', 'MLT', 'NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'ESP', 'SWE']

def is_eu(row):
    if (row['year'] < 2021) and (row['iso3'] in eu + ['GBR']) and (row['flag'] in eu + ['GBR']):
        out = ['EU' ,'EU']
    elif (row['iso3'] in eu) and (row['flag'] in eu):
        out = ['EU', 'EU']
    else:
        out = [row.iso3, row.flag]
    
    return out


new_iso3_flag = foo.apply(lambda row: is_eu(row), axis=1)
new_iso3_flag = pd.DataFrame([[iso3, flag] for iso3, flag in new_iso3_flag])
new_iso3_flag.columns = ['iso3', 'flag']

foo['iso3'] = new_iso3_flag['iso3']
foo['flag'] = new_iso3_flag['flag']


# group again for EU and France (fishing & support)
foo = foo.groupby(['year', 'iso3', 'flag', 'vessel_class'])['n_visits'].sum()
foo = pd.DataFrame(foo).reset_index()


# classify to domestic, foreign, sovereign_territory
def visit_type(row):
    if row.flag == row.iso3:
        out = 'domestic'
    elif {row.flag, row.iso3} in pair_set:
        out = 'sovereign_territory'
    else:
        out = 'foreign'
    
    return out


foo['type'] = foo.apply(lambda row: visit_type(row), axis=1)


a = foo.loc[(foo['year']==2021) & (foo['type']=='domestic'), 'n_visits'].sum()
b = foo.loc[foo['year']==2021, 'n_visits'].sum()
(a/b).item()
## 64%


#%%
#----------------------------------------
# plot donuts chart for 2021 (Fig. 6)
#----------------------------------------
def plot_donut(df, n):
    flag_to_keep = df[df['type']=='domestic'].sort_values('n_visits', ascending=False)['flag'][:n]

    new_flag = []
    for index, row in bar.iterrows():
        if (row.type=='domestic') and (row.flag in list(flag_to_keep)):
            new_flag.append(row.flag)
        else:
            new_flag.append('other')

    df['new_flag'] = new_flag


    # change sovereign-teritory to domestic
    df.loc[df['type']=='sovereign_territory', 'new_flag'] = 'sovereign_territory'
    df.loc[df['type']=='sovereign_territory', 'type'] = 'domestic'
    df.loc[df['type']=='foreign', 'new_flag'] = 'foreign'


    # inner donut
    inner = df.groupby('type')['n_visits'].sum()
    inner = pd.DataFrame(inner).reset_index()


    # outer donut
    outer = df.groupby(['type', 'new_flag'])['n_visits'].sum()
    outer  = pd.DataFrame(outer).reset_index()

    outer_level = list(flag_to_keep) + ['other', 'sovereign_territory', 'foreign']
    outer['new_flag'] = pd.Categorical(outer['new_flag'], categories=outer_level, ordered=True)
    outer = outer.sort_values('new_flag')

    return [inner, outer]


# plot
fig, axs = plt.subplots(nrows=1, ncols=2, figsize=(12, 6))
size = 0.4


# fishing vessels
bar = foo[(foo['vessel_class']=='fishing') & (foo['year']==2021)].copy()
bar.reset_index(inplace=True)
inner, outer = plot_donut(bar, 5)

ax = axs[0]

ax.pie(inner['n_visits'], radius=1-size,
    labels=inner['type'],
    autopct='%1.1f%%',
    wedgeprops=dict(width=0.1, edgecolor='w'),
    startangle=90,
    counterclock=False)

ax.pie(outer['n_visits'], radius=1, 
    labels = outer['new_flag'],
    wedgeprops=dict(width=size, edgecolor='w'),
    startangle=90,
    counterclock=False)

ax.set_title('A. Fishing vessels', fontsize=16, fontweight='bold', loc='left')


# support vessels
bar = foo[(foo['vessel_class']=='support') & (foo['year']==2021)].copy()
bar.reset_index(inplace=True)
inner, outer = plot_donut(bar, 5)

ax = axs[1]

ax.pie(inner['n_visits'], radius=1-size,
    labels=inner['type'],
    autopct='%1.1f%%',
    wedgeprops=dict(width=0.1, edgecolor='w'),
    startangle=90,
    counterclock=False)

ax.pie(outer['n_visits'], radius=1, 
    labels = outer['new_flag'],
    wedgeprops=dict(width=size, edgecolor='w'),
    startangle=90,
    counterclock=False)

ax.set_title('B. Support vessels', fontsize=16, fontweight='bold', loc='left')


plt.savefig('plot/fig6.pdf')
plt.show()


#%%
#----------------------------------------
# plot over year (Fig. S4b)
#----------------------------------------
# tally
summary = foo.groupby(['year', 'type', 'vessel_class'])['n_visits'].sum()
summary = pd.DataFrame(summary).reset_index()

total = summary.groupby(['year', 'vessel_class'])['n_visits'].sum()
total = pd.DataFrame(total).reset_index()
total.rename(columns={'n_visits':'n_visits_total'}, inplace=True)
summary = pd.merge(summary, total, how='left', on=['year', 'vessel_class'])

summary['percentage'] = summary['n_visits']/summary['n_visits_total'] * 100

# plot
fig, axs = plt.subplots(nrows=1, ncols=2, figsize=(12, 4))


# fishing
bar = summary[summary['vessel_class']=='fishing'].copy()
df_pivot = bar.pivot(index='year', columns='type', values='percentage').fillna(0)

ax = axs[0]
p0 = df_pivot.plot(ax=ax, kind='bar', stacked=True, legend=False)
ax.set_title('A. Fishing vessels', fontsize=16, fontweight='bold', loc='left', y=1.1)



# support
bar = summary[summary['vessel_class']=='support'].copy()
df_pivot = bar.pivot(index='year', columns='type', values='percentage').fillna(0)

ax = axs[1]
p1 = df_pivot.plot(ax=ax, kind='bar', stacked=True)
ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
ax.set_title('B. Support vessels', fontsize=16, fontweight='bold', loc='left', y=1.1)


plt.savefig('plot/figS4b.pdf')


# %%
#----------------------------------------
# Port visits vs coastline
#----------------------------------------
# coastline distance
# from world factbook (https://www.cia.gov/the-world-factbook/field/coastline/)
coast = pd.read_csv('data/coastline_distance.csv')
coast = coast[coast['length_km'] > 0]


# add iso3
coast.loc[coast['country']=='Russia', 'country'] = 'Russian Federation'
coast.loc[coast['country']=='United States of America', 'country'] = 'United States'
coast.loc[coast['country']=='United Kingdom of Great Britain and Northern Ireland', 'country'] = 'United Kingdom'
coast.loc[coast['country']=='Micronesia', 'country'] = 'Micronesia, Federated States of'
coast.loc[coast['country']=='Svalbard', 'country'] = 'Svalbard and Jan Mayen'
coast.loc[coast['country']=='Vietnam', 'country'] = 'Viet Nam'
coast.loc[coast['country']=='Venezuela', 'country'] = 'Venezuela, Bolivarian Republic of'
coast.loc[coast['country']=='Korea (North)', 'country'] = "Korea, Democratic People's Republic of"
coast.loc[coast['country']=='Iran', 'country'] = "Iran, Islamic Republic of"
coast.loc[coast['country']=='Korea (South)', 'country'] = "Korea, Republic of"
coast.loc[coast['country']=='Myanmar (Burma)', 'country'] = "Myanmar"
coast.loc[coast['country']=='Taiwan', 'country'] = 'Taiwan, Province of China'
coast.loc[coast['country']=='Tanzania', 'country'] = 'Tanzania, United Republic of'
coast.loc[coast['country']=='Falkland Islands', 'country'] = 'Falkland Islands (Malvinas)'
coast.loc[coast['country']=='Cape Verde', 'country'] = 'Cabo Verde'
coast.loc[coast['country']=='East Timor', 'country'] = 'Timor-Leste'
coast.loc[coast['country']=="Côte d'Ivoire (Ivory Coast)", 'country'] = "Côte d'Ivoire"
coast.loc[coast['country']=='Curacao', 'country'] = 'Curaçao'
coast.loc[coast['country']=='São Tomé and Príncipe', 'country'] = 'Sao Tome and Principe'
coast.loc[coast['country']=='Syria', 'country'] = 'Syrian Arab Republic'
coast.loc[coast['country']=='Virgin Islands of the U.S.', 'country'] = 'Virgin Islands, U.S.'
coast.loc[coast['country']=='Congo (Republic)', 'country'] = 'Congo'
coast.loc[coast['country']=='Brunei', 'country'] = 'Brunei Darussalam'
coast.loc[coast['country']=='Jan Mayen', 'country'] = 'Svalbard and Jan Mayen'
coast.loc[coast['country']=='British Virgin Islands', 'country'] = 'Virgin Islands, British'
coast.loc[coast['country']=='Macau', 'country'] = 'Macao'
coast.loc[coast['country']=='Palestinian Territories', 'country'] = 'Palestine, State of'
coast.loc[coast['country']=='Congo (Democratic Republic)', 'country'] = 'Congo, The Democratic Republic of the'
coast.loc[coast['country']=='Saint Martin', 'country'] = 'Saint Martin (French part)'
coast.loc[coast['country']=='Sint Maarten', 'country'] = 'Sint Maarten (Dutch part)'
coast.loc[coast['country']=='Bolivia', 'country'] = 'Bolivia, Plurinational State of'
coast.loc[coast['country']=='Turkey', 'country'] = 'Türkiye'


# drop some rows
coast = coast[coast['country']!='Antarctica']
coast = coast[coast['country']!='Coral Sea Islands']
coast = coast[coast['country']!='Ashmore and Cartier Islands']
coast = coast[coast['country']!='French Southern and Antarctic Lands']
coast = coast[coast['country']!='U.S. Pacific Island Wildlife Refuges']
coast = coast[coast['country']!='Bouvet Island']
coast = coast[coast['country']!='Wake Island']
coast = coast[coast['country']!='Clipperton Island']
coast = coast[coast['country']!='Heard Island and McDonald Islands']
coast = coast[coast['country']!='Navassa Island']
coast = coast[coast['country']!='South Georgia and the South Sandwich Islands']
coast = coast[coast['country']!='Spratly Islands']  # disputed
coast = coast[coast['country']!='Paracel Islands']  # disputed
coast.reset_index(inplace=True, drop=True)


# add iso3
coast['iso3'] = [pycountry.countries.get(name=x).alpha_3 for x in coast['country']]


# port visit in 2021
visit2021 = visit[visit['year']==2021].copy()
visit2021 = visit2021.groupby(['iso3', 'flag'])['n_visits'].sum().to_frame('n_visits')
visit2021.reset_index(inplace=True)


# change territory to sovereign
visit2021 = pd.merge(visit2021, pair, how='left', left_on='iso3', right_on='territory_iso3')
visit2021['iso3'] = visit2021['sovereign_iso3'].fillna(visit2021['iso3'])
visit2021.drop(['territory_iso3', 'sovereign_iso3'], axis=1, inplace=True)

visit2021 = pd.merge(visit2021, pair, how='left', left_on='flag', right_on='territory_iso3')
visit2021['flag'] = visit2021['sovereign_iso3'].fillna(visit2021['flag'])
visit2021.drop(['territory_iso3', 'sovereign_iso3'], axis=1, inplace=True)


# remove PSMA Parties in 2021
psma = pd.read_csv('data/psma_ratifiers.csv')
psma = psma[psma['year'] < 2022]
eu = ['AUT', 'BEL', 'BGR', 'HRV', 'CYP', 'CZE', 'DNK', 'EST', 'FIN', 'FRA', 'DEU', 'GRC', 'HUN', 'IRL', 'ITA', 'LVA', 'LTU', 'LUX', 'MLT', 'NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'ESP', 'SWE']

visit2021 = visit2021[~visit2021['iso3'].isin(psma['iso3'])]
visit2021 = visit2021[~visit2021['iso3'].isin(eu + ['GBR'])]


# add domestic or foreign
visit2021['type'] = ['domestic' if x==y else 'foreign' for x, y in zip(visit2021['iso3'], visit2021['flag'])]


# total visits
total = visit2021.groupby('iso3')['n_visits'].sum().reset_index()

# visits by foreign-flagged vessels
foreign = visit2021[visit2021['type']=='foreign'].groupby('iso3')['n_visits'].sum().reset_index()
foreign.rename(columns={'n_visits':'n_visits_foreign'}, inplace=True)

summary = pd.merge(total, foreign[['iso3', 'n_visits_foreign']], how='left', on='iso3')
summary.fillna(0, inplace=True)
summary['proportion'] = summary['n_visits_foreign']/summary['n_visits']


# add coastline
summary = pd.merge(summary, coast[['iso3', 'length_km']], how='left', on='iso3')


# remove Nigeria and Mexico
summary = summary[~summary['iso3'].isin(['NGA', 'MEX'])]

# add country name
summary['country_name'] = [pycountry.countries.get(alpha_3=x).name for x in summary['iso3']]


# plot
baz = summary[summary['n_visits_foreign'] > 3].copy()

fig, ax = plt.subplots(figsize=(8, 6))

# Adjusting zorder of gridlines
ax.grid(True, which="both", color='lightgrey', linewidth=0.2, zorder=0)

# Plotting scatter with higher zorder
scatter = ax.scatter(x=baz['n_visits_foreign'], y=baz['length_km'], c=baz['proportion'], cmap='viridis', zorder=2)

# Adding labels and colorbar
ax.set_xlabel('Number of Foreign Visits')
ax.set_ylabel('Coastline length (km)')

for i, txt in enumerate(baz['country_name']):
    ax.text(baz['n_visits_foreign'].iloc[i], baz['length_km'].iloc[i], txt, fontsize=8, color='black', zorder=3)

# Adding colorbar
cbar = plt.colorbar(scatter, orientation='horizontal', shrink=0.2)
cbar.set_label('Proportion')
cbar.mappable.set_clim(0, 1)

plt.savefig('plot/fig3.pdf')
plt.show()




# %%
#----------------------------------------
# Fig S6
#----------------------------------------

# output of 'port_visit_fishing_all_v20240627.sql'
visit_fishing_all = pd.read_parquet('data/port_visit_fishing_all_v20240627.parquet')
visit_fishing_all['vessel_class'] = 'fishing'

# output of 'port_visit_support_v20240627.sql'
visit_support_all = pd.read_parquet('data/port_visit_support_all_v20240627.parquet')
visit_support_all['vessel_class'] = 'support'

visit_all = pd.concat([visit_fishing_all, visit_support_all])


# remove vessels with unknown flags
visit_all = visit_all[np.logical_and(visit_all['flag'] != 'UNK', ~visit_all['flag'].isnull())].copy()


# remove Antarctica
visit_all = visit_all[visit_all['iso3'] != 'ATA']


# convert French overseas department to France
foo = visit_all.copy()
foo.reset_index(inplace=True)
foo['iso3'] = ['FRA' if x in french_overseas_dept else x for x in foo['iso3']]
foo['flag'] = ['FRA' if x in french_overseas_dept else x for x in foo['flag']]


# Change iso3 and flag to EU when both are EU
# EU (post Brexit)
eu = ['AUT', 'BEL', 'BGR', 'HRV', 'CYP', 'CZE', 'DNK', 'EST', 'FIN', 'FRA', 'DEU', 'GRC', 'HUN', 'IRL', 'ITA', 'LVA', 'LTU', 'LUX', 'MLT', 'NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'ESP', 'SWE']

def is_eu(row):
    if (row['year'] < 2021) and (row['iso3'] in eu + ['GBR']) and (row['flag'] in eu + ['GBR']):
        out = ['EU' ,'EU']
    elif (row['iso3'] in eu) and (row['flag'] in eu):
        out = ['EU', 'EU']
    else:
        out = [row.iso3, row.flag]
    
    return out


new_iso3_flag = foo.apply(lambda row: is_eu(row), axis=1)
new_iso3_flag = pd.DataFrame([[iso3, flag] for iso3, flag in new_iso3_flag])
new_iso3_flag.columns = ['iso3', 'flag']

foo['iso3'] = new_iso3_flag['iso3']
foo['flag'] = new_iso3_flag['flag']


# group again for EU and France (fishing & support)
foo = foo.groupby(['year', 'iso3', 'flag', 'vessel_class'])['n_visits'].sum()
foo = pd.DataFrame(foo).reset_index()


# classify to domestic, foreign, sovereign_territory
def visit_type(row):
    if row.flag == row.iso3:
        out = 'domestic'
    elif {row.flag, row.iso3} in pair_set:
        out = 'sovereign_territory'
    else:
        out = 'foreign'
    
    return out


foo['type'] = foo.apply(lambda row: visit_type(row), axis=1)



# plot
fig, axs = plt.subplots(nrows=1, ncols=2, figsize=(12, 6))
size = 0.4


# fishing vessels
bar = foo[foo['vessel_class']=='fishing'].copy()
bar.reset_index(inplace=True)
inner, outer = plot_donut(bar, 5)

ax = axs[0]

ax.pie(inner['n_visits'], radius=1-size,
    labels=inner['type'],
    autopct='%1.1f%%',
    wedgeprops=dict(width=0.1, edgecolor='w'),
    startangle=90,
    counterclock=False)

ax.pie(outer['n_visits'], radius=1, 
    labels = outer['new_flag'],
    wedgeprops=dict(width=size, edgecolor='w'),
    startangle=90,
    counterclock=False)

ax.set_title('A', fontsize=16, fontweight='bold', loc='left')


# support vessels
bar = foo[foo['vessel_class']=='support'].copy()
bar.reset_index(inplace=True)
inner, outer = plot_donut(bar, 5)

ax = axs[1]

ax.pie(inner['n_visits'], radius=1-size,
    labels=inner['type'],
    autopct='%1.1f%%',
    wedgeprops=dict(width=0.1, edgecolor='w'),
    startangle=90,
    counterclock=False)

ax.pie(outer['n_visits'], radius=1, 
    labels = outer['new_flag'],
    wedgeprops=dict(width=size, edgecolor='w'),
    startangle=90,
    counterclock=False)

ax.set_title('B', fontsize=16, fontweight='bold', loc='left')


plt.savefig('plot/figS6.pdf')
plt.show()


