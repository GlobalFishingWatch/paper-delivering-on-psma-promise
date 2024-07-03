# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: py39
#     language: python
#     name: py39
# ---

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
from mpl_toolkits.axes_grid1.inset_locator import inset_axes

# +
from matplotlib import cm
cmap = cm.RdYlBu

from matplotlib import colors, colorbar
# -

blue = "#204280"
red = "#d73b68"

q = """
SELECT *, timeline AS year
FROM `scratch_jaeyoon.landed_fishing_effort_yearly_v20240624`
WHERE timeline BETWEEN 2015 AND 2021
"""
psma_yearly = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# +
fig = plt.figure(figsize=(13, 7))#, dpi=200, facecolor='#f7f7f7')
ax1 = fig.add_subplot(121)
ax2 = fig.add_subplot(122)

width = 0.7
offset = 0.22
hatch = ['.....', '', '//']
label_psma = ['PSMA States to foreign PSMA State ports',
              'PSMA States to non-PSMA State ports',
              'PSMA States to their own domestic (PSMA) ports']
label_nonpsma = ['Non-PSMA States to foreign non-PSMA State ports',
                 'Non-PSMA States to PSMA State ports',
                 'Non-PSMA States to their own domestic (non-PSMA) ports']
    
# temp_0 = psma_yearly[psma_yearly.port_flag == psma_yearly.port_flag.unique()[0]]
prev = np.zeros(6)
prev_t = 0


for n, ind in enumerate(psma_yearly.psma.unique()):

    if ind:
        temp = psma_yearly[psma_yearly.psma == ind]
        cmap_colors = cmap([col for col in temp['frac_psma_group'].values])
        
        ax1.bar(temp.year, temp.frac_domestic, width=width,
               color='none', lw=2., edgecolor=blue, hatch=hatch[2], alpha=1, label=label_psma[2])
        ax1.bar(temp.year, 1 - temp.frac_psma_group, bottom=temp.frac_domestic, width=width,
               color='none', lw=2., edgecolor=blue, hatch=hatch[1], alpha=1, label=label_psma[1])
        ax1.bar(temp.year, temp.frac_psma_group - temp.frac_domestic, bottom=1 - temp.frac_psma_group + temp.frac_domestic, width=width,
               color='none', lw=2., edgecolor=blue, hatch=hatch[0], alpha=1, label=label_psma[0])
    else:
        temp = psma_yearly[(psma_yearly.psma == ind)]
        cmap_colors = cmap([col for col in temp['frac_psma_group'].values])
        
        ax2.bar(temp.year, temp.frac_domestic, width=width,
               color='none', lw=2., edgecolor=red, hatch=hatch[2], alpha=1, label=label_nonpsma[2])
        ax2.bar(temp.year, temp.frac_psma_group - temp.frac_domestic, bottom=temp.frac_domestic, width=width,
               color='none', lw=2., edgecolor=red, hatch=hatch[1], alpha=1, label=label_nonpsma[0])
        ax2.bar(temp.year, 1 - temp.frac_psma_group, bottom=temp.frac_psma_group, width=width,
               color='none', lw=2., edgecolor=red, hatch=hatch[0], alpha=1, label=label_nonpsma[1])
        
# , bbox_to_anchor=(0.95,1))    
handles, labels = ax1.get_legend_handles_labels()
reordered_handles = [handles[2], handles[1], handles[0]]
reordered_labels = [labels[2], labels[1], labels[0]]
ax1.legend(loc=1, handles=reordered_handles, labels=reordered_labels)

handles, labels = ax2.get_legend_handles_labels()
reordered_handles = [handles[2], handles[1], handles[0]]
reordered_labels = [labels[2], labels[1], labels[0]]
ax2.legend(loc=1, handles=reordered_handles, labels=reordered_labels)


years = range(2015, 2024, 1)
ax2.set_xticks(years)

ax1.set_xlabel('Year', fontsize=13)
ax2.set_xlabel('Year', fontsize=13)
ax1.set_ylabel('Landed fishing effort unit in fraction', fontsize=13)
plt.xlim (2014.3, 2021.7)
ax1.set_ylim (0, 1.19)
ax2.set_ylim (0, 1.19)

ax1.set_title('PSMA')
ax2.set_title('Non-PSMA')


plt.savefig('../outputs/figures/fig5.pdf', format='pdf')

plt.tight_layout()
plt.show()

# -


# ## Port visits by flag, domestic flag vs foreign flag 2016/2021

q = """
SELECT DISTINCT port_flag_eu, diff AS difference, total, is_psma
FROM `world-fishing-827.scratch_jaeyoon.psma_port_visits_by_reflagged_vessels_v20230715`
"""
df1 = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

flag_map = {
    'PAN': 'Panama',
    'CAN': 'Canada',
    'NOR': 'Norway',
    'EU': 'European Union',
    'CHL': 'Chile',
    'TUR': 'Türkiye',
    'KIR': 'Kiribati',
    'IDN': 'Indonesia',
    'JPN': 'Japan',
    'USA': 'United States',
    'TWN': 'Chinese Taipei',
    'KOR': 'Republic of Korea',
    'CHN': 'China',
    'RUS': 'Russia',
    'LBR': 'Liberia',
    'PNG': 'Papua New Guinea',
    'MHL': 'Republic of the Marshall Islands',
    'ZAF': 'South Africa',
    'ISL': 'Iceland',
    'FRO': 'Faroe Islands',
    'FSM': 'Federated States of Micronesia',
    'MRT': 'Mauritania',
    'SEN': 'Senegal',
    'NAM': 'Namibia',
    'NZL': 'New Zealand',
    'FLK': 'Falkland Islands (Islas Malvinas)',
    'BRA': 'Brazil',
    'URY': 'Uruguay'
}

# +
fig = plt.figure(figsize=(12, 9), dpi=300, facecolor='#f7f7f7')
ax = fig.add_subplot(111)

temp = df1[::-1]
ax.scatter(temp.difference, temp.port_flag_eu, s=temp.total.apply(lambda x: np.sqrt(x) * 5), #temp.total, 
           facecolor=[blue if psma else red for psma in temp.is_psma], 
           edgecolor='white', 
           zorder=2)
ax.barh(temp.port_flag_eu, temp.difference, height=0.1, linewidth=0.02, 
        color='grey', #[blue if c else red for c in temp.difference > 0], 
        alpha=0.9)
ax.vlines(0, ymin=-1, ymax=len(temp), color='grey', lw=1.5, linestyle='--')
for i, row in temp.iterrows():
    if row.difference >= 0:
        ax.text(row.difference + 0.035, len(temp) - i - 1.15, flag_map[row.port_flag_eu])
    else:
        ax.text(row.difference - 0.03, len(temp) - i - 1.15, flag_map[row.port_flag_eu], ha='right')

ax.grid(axis='x', linewidth=0.2, color='grey')
ax.set_xlim(-0.95, 0.95)
ax.get_yaxis().set_ticks([])
ax.get_xaxis().set_ticklabels([str(round(t, 2)) if t < 0 else '+' + str(round(t, 2)) for t in ax.get_xticks()])

ax.set_ylim(-1, len(temp))
# plt.title('Ratio Change of Port Visits by Domestic Flagged Vessels (before vs. after 2017-01-01)\n' +
#           'Vessels Reflagged to or from a Given Flag only (Excluding All Non-Reflagging Vessels)\n' +
#           'Port Flag in ISO-3 Country Code, Circle Size indicating the Number of Port Visits in Total')
ax.set_xlabel('Ratio change of domestic flag vessel visits', fontsize=13)

ax.scatter(0.6, 2, s=np.sqrt(10000) * 5, facecolor='grey', edgecolor='none', linewidth=1)
ax.scatter(0.6, 3, s=np.sqrt(1000) * 5, facecolor='grey', edgecolor='none', linewidth=1)
ax.scatter(0.6, 3.7, s=np.sqrt(100) * 5, facecolor='grey', edgecolor='none', linewidth=1)
ax.text(0.57, 4.1, 'Number of\nport visits')
ax.text(0.65, 1.8, '10,000')
ax.text(0.65, 2.7, '1,000')
ax.text(0.65, 3.5, '100')
# ax.scatter(0, 0, s=)


plt.savefig('../outputs/figures/fig7.pdf', format='pdf')

plt.show()
# -

# ## Closed loop analysis

q = """
WITH
  by_port AS (
    SELECT psma, port_flag_eu AS flag, 
      closed_loop_cnt AS cnt_port, total_cnt AS total_port, ratio_closed_loop AS ratio_port
    FROM `scratch_jaeyoon.psma_closed_loop_by_port_v20240624`
  ),

  by_carrier AS (
    SELECT psma, carrier_flag_eu AS flag, closed_loop_cnt AS cnt_carrier, 
      total_cnt AS total_carrier, ratio_closed_loop AS ratio_carrier
    FROM `scratch_jaeyoon.psma_closed_loop_by_carrier_v20240624`
  ),

  all_flags AS (
    SELECT psma, flag
    FROM by_port
    UNION DISTINCT
    SELECT psma, flag
    FROM by_carrier
  )

SELECT 
  psma, flag,
  IFNULL (cnt_port, 0) AS cnt_port,
  IFNULL (total_port, 0) AS total_port,
  IFNULL (ratio_port, 0) AS ratio_port,
  IFNULL (cnt_carrier, 0) AS cnt_carrier,
  IFNULL (total_carrier, 0) AS total_carrier,
  IFNULL (ratio_carrier, 0) AS ratio_carrier
#FROM all_flags
#LEFT JOIN by_port
#USING (psma, flag)
FROM by_carrier
LEFT JOIN by_port
USING (psma, flag)
WHERE total_carrier > 1000
  OR cnt_port + cnt_carrier > 0
ORDER BY total_port
"""
cl = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

cl['total_max'] = cl.apply(lambda x: (max(x.total_port, x.total_carrier)), axis=1)

flag_map = {
    'PAN': 'Panama',
    'CAN': 'Canada',
    'NOR': 'Norway',
    'EU': 'European Union',
    'CHL': 'Chile',
    'TUR': 'Türkiye',
    'KIR': 'Kiribati',
    'IDN': 'Indonesia',
    'JPN': 'Japan',
    'USA': 'United States',
    'TWN': 'Chinese Taipei',
    'KOR': 'Republic of Korea',
    'CHN': 'China',
    'RUS': 'Russia Federation',
    'LBR': 'Liberia'
}

# +
fig = plt.figure(figsize=(8, 8), dpi=200, facecolor='#f7f7f7')
ax = fig.add_subplot(111)
ax.scatter(cl.ratio_port, cl.ratio_carrier, 
           s=cl.total_max.apply(lambda x: x/10),
           facecolor='none', edgecolor=cl.psma.apply(lambda x: blue if x else red),
           lw=1.5)
yadj = [-0.0, 0.06, -0.02, 0.04, -0.03, 0.06, -0.07] #, 0.04, 0.038, 0.04, -0.04, -0.035, 0.1, 0.10]
xadj = [-0.04, 0.06, -0.02, 0, 0, 0, 0.22] #, 0.0, 0.10, 0.11, 0.12, 0.17, 0.06, 0.06]
for i, (x, y, t, p, c) in cl[['ratio_port', 'ratio_carrier', 'flag', 'psma', 'total_max']].iterrows():
    ax.text(x - 0.02 - xadj[i], y - yadj[i], flag_map[t] + " (" + str(int(c)) + ")")
    
ax.plot([[-0., -0.], [1., 1.]], lw=0.7, linestyle='--', color='grey')
ax.grid(linestyle=':', lw=0.5)
# ax.scatter(0.75, 0.1, s=1000, facecolor='white', edgecolor='grey', lw=1)
# ax.scatter(0.75, 0.075, s=100, facecolor='white', edgecolor='grey', lw=1)
# ax.text(0.8, 0.115, '10,000 port visits')
# ax.text(0.8, 0.065, '1,000 port visits')

plt.xlim(-0.13, 1.1)
plt.ylim(-0.13, 1.1)
plt.xlabel("Proportion of transshipment vessel visits in a closed loop" +
           "\nto total transshipment vessel visits to a given port State", fontsize=10)
plt.ylabel("Proportion of transshipment vessel visits in a closed loop" +
           "\nto total transshipment vessel visits by the same flag State", fontsize=10)
# plt.title("Significance of closed loop connection with regard to total port visits and total carrier fleet size")

plt.savefig('../outputs/figures/fig8.pdf', format='pdf')

# Display the plot (optional)
plt.show()

# Optionally close the plot
plt.close()
# -
# ## Landed fishing effort by flag

q = """
SELECT *, timeline AS year, SUM (fishing_effort_landed_total) OVER (PARTITION BY flag) AS total_effort
FROM `world-fishing-827.scratch_jaeyoon.landed_fishing_effort_yearly_byflag_v20240624`
WHERE timeline BETWEEN 2015 AND 2021
  AND flag NOT IN ('UNK') #, 'GEO', 'COM')
ORDER BY SUM (fishing_effort_landed_total) OVER (PARTITION BY flag) DESC, year
"""
psma_yearly_all_flag = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

psma_flags = psma_yearly_all_flag[psma_yearly_all_flag.psma].flag.unique()
non_psma_flags = psma_yearly_all_flag[psma_yearly_all_flag.apply(lambda x: x.flag not in psma_flags, axis=1)].flag.unique()

# +
fig = plt.figure(figsize=(20, 14))#, dpi=200, facecolor='#f7f7f7')
fig, axes = plt.subplots(5, 4, sharey=True, sharex=True, figsize=(20, 20))

width = 0.7
offset = 0.22
hatch = ['.....', '', '//']
label_psma = ['PSMA States to foreign PSMA State ports',
              'PSMA States to non-PSMA State ports',
              'PSMA States to their own domestic (PSMA) ports']
label_nonpsma = ['Non-PSMA States to foreign non-PSMA State ports',
                 'Non-PSMA States to PSMA State ports',
                 'Non-PSMA States to their own domestic (non-PSMA) ports']
    
# temp_0 = psma_yearly[psma_yearly.port_flag == psma_yearly.port_flag.unique()[0]]
prev = np.zeros(6)
prev_t = 0

for n, flag in enumerate(non_psma_flags):
    temp = psma_yearly_all_flag[psma_yearly_all_flag.flag == flag]
    cmap_colors = cmap([col for col in temp['frac_psma_group'].values])
    i = int (n / 4)
    j = n % 4
    axes[i][j].bar(temp.year, temp.frac_domestic, width=width,
           color='none', lw=2., edgecolor=red, hatch=hatch[2], alpha=1, label=label_nonpsma[2])
    axes[i][j].bar(temp.year, temp.frac_psma_group - temp.frac_domestic, bottom=temp.frac_domestic, width=width,
           color='none', lw=2., edgecolor=red, hatch=hatch[1], alpha=1, label=label_nonpsma[0])
    axes[i][j].bar(temp.year, 1 - temp.frac_psma_group, bottom=temp.frac_psma_group, width=width,
           color='none', lw=2., edgecolor=red, hatch=hatch[0], alpha=1, label=label_nonpsma[1])
    axes[i][j].set_title(flag + f' ({int(round(temp.total_effort.iloc[0] /1000000,-1))}M kilowatt hours)')
    
    if n > 15:
        axes[i][j].set_xlabel('Year', fontsize=10)
    if j == 0:
        axes[i][j].set_ylabel('Landed fishing effort unit in fraction', fontsize=10)
    if n == 19:
        break


handles, labels = axes[0][0].get_legend_handles_labels()
reordered_handles = [handles[2], handles[1], handles[0]]
reordered_labels = [labels[2], labels[1], labels[0]]
axes[0][0].legend(loc=1, handles=reordered_handles, labels=reordered_labels, bbox_to_anchor=(1, 1.4))


plt.savefig('../outputs/figures/figS5.pdf', format='pdf')

plt.tight_layout()
plt.show()
# -

# ## Landed fishing effort by foreign vs domestic

q = """
SELECT *
FROM `scratch_jaeyoon.landed_fishing_effort_yearly_foreign_vs_domestic_v20240624`
"""
foreign_domestic = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# +
fig = plt.figure(figsize=(8, 7), dpi=150, facecolor='#f7f7f7')
ax1 = fig.add_subplot(111)

width = 0.7
offset = 0.22
label_psma = ['Landed fishing effort by foreign flagged vessels',
              'Landed fishing effort by domestic flagged vessels']
    
# temp_0 = psma_yearly[psma_yearly.port_flag == psma_yearly.port_flag.unique()[0]]
# prev = np.zeros(6)
# prev_t = 0

ax1.bar(foreign_domestic.timeline, 1 - foreign_domestic.frac_domestic, width=width,
       color=red, lw=2., edgecolor='none', alpha=1, label=label_psma[0])
ax1.bar(foreign_domestic.timeline, foreign_domestic.frac_domestic, bottom=1 - foreign_domestic.frac_domestic, width=width,
       color=blue, lw=2., edgecolor='none', alpha=1, label=label_psma[1])

for ind, row in foreign_domestic[['timeline', 'frac_domestic']].iterrows():
    ax1.text(row.timeline, 1 - row.frac_domestic + 0.01, str(round(1 - row.frac_domestic, 2)),
             ha='center', color='white', fontsize=11)
# , bbox_to_anchor=(0.95,1))    
handles, labels = ax1.get_legend_handles_labels()
reordered_handles = [handles[1], handles[0]]
reordered_labels = [labels[1], labels[0]]
ax1.legend(loc=1, handles=reordered_handles, labels=reordered_labels)

ax1.set_xlabel('Year', fontsize=13)
ax1.set_ylabel('Landed fishing effort unit in fraction', fontsize=13)
ax1.set_ylim (0, 1.12)
plt.grid(axis='y', linewidth=0.7, linestyle=':')

plt.tight_layout()

plt.savefig('../outputs/figures/figS4.pdf', format='pdf')

# Display the plot (optional)
plt.show()

# Optionally close the plot
plt.close()

# -

# ## Upload PSMA ratification dates


df_dates = pd.read_csv('../data/psma_ratifiers.csv')
print(len(df_dates))
df_dates.head(1)

df_dates['date'] = pd.to_datetime(df_dates.Entry_into_force_date)

# +
from google.cloud import bigquery

table_id = "world-fishing-827.scratch_jaeyoon.psma_ratifiers_v20240318"
config = bigquery.LoadJobConfig(
    # Will overwrite any existing table.
    # Use "WRITE_APPEND" to add data to an existing table
    write_disposition="WRITE_TRUNCATE",
)

client = bigquery.Client()
client.load_table_from_dataframe(
    df_dates, table_id, job_config=config
)
# -


