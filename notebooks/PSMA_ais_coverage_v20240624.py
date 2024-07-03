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
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter

blue = "#204280"
red = "#d73b68"

# ## AIS coverage, PSMA vs. non-PSMA with China separated

q = """
SELECT *
FROM `scratch_jaeyoon.psma_ais_messages_total_hours_2012_2021_by_psma_china_separated_v20240624`
ORDER BY is_psma, is_china, date
"""
df_psma_china = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')


# Calculate the 7-day rolling average
rolling_avg_psma_china = df_psma_china['total_hours'].rolling(window=7).mean()

df_psma_china['year'] = pd.to_datetime(df_psma_china.date).dt.year

result = df_psma_china.groupby(['year', 'is_psma', 'is_china'])['total_hours'].sum()
result = result.reset_index()
result['diff'] = result.groupby(['is_psma', 'is_china'])['total_hours'].diff()
result['frac'] = result.groupby(['is_psma', 'is_china'])['total_hours'].pct_change()
# result.sort_values(['is_psma', 'is_china'])

# +
fig = plt.figure(figsize=(14, 5), dpi=200)
ax = fig.add_subplot(111)

temp = df_psma_china[df_psma_china.is_psma]
rolling_avg_psma_china = temp['total_hours'].rolling(window=7).mean()
ax.plot(temp.date.values, rolling_avg_psma_china.values, label='7-Day Rolling Average', linewidth=2, color=blue)
    
plt.xlabel('Date')
plt.ylabel('Total hours by vessels broadcasting AIS per day')
plt.legend(loc=2)
plt.grid(True, linewidth=0.5)
plt.title('AIS hours by vessels flagged to PSMA')

plt.savefig('../outputs/figures/figS1_1.pdf', format='pdf')

plt.show()

# +
fig = plt.figure(figsize=(14, 5), dpi=200)
ax = fig.add_subplot(111)

temp = df_psma_china[~(df_psma_china.is_psma) & ~(df_psma_china.is_china)]
rolling_avg_psma_china = temp['total_hours'].rolling(window=7).mean()
ax.plot(temp.date.values, rolling_avg_psma_china.values, label='7-Day Rolling Average', linewidth=2, color=red)
    
plt.xlabel('Date')
plt.ylabel('Total hours by vessels broadcasting AIS per day')
plt.legend(loc=2)
plt.grid(True, linewidth=0.5)
plt.title('AIS hours by vessels flagged to non-PSMA without China')

plt.savefig('../outputs/figures/figS1_2.pdf', format='pdf')

plt.show()

# +
fig = plt.figure(figsize=(14, 5), dpi=200)
ax = fig.add_subplot(111)

temp = df_psma_china[~(df_psma_china.is_psma) & (df_psma_china.is_china)]
rolling_avg_psma_china = temp['total_hours'].rolling(window=7).mean()
ax.plot(temp.date.values, rolling_avg_psma_china.values, label='7-Day Rolling Average', linewidth=2, color=red)
    
plt.xlabel('Date')
plt.ylabel('Total hours by vessels broadcasting AIS per day')
plt.legend(loc=2)
plt.grid(True, linewidth=0.5)
plt.title('AIS hours by vessels flagged to China (non-PSMA)')

plt.savefig('../outputs/figures/figS1_3.pdf', format='pdf')

plt.show()
# -


