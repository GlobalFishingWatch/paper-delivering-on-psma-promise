# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.4.1
#   kernelspec:
#     display_name: py37
#     language: python
#     name: py37
# ---

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe

blue = "#204280"
red = "#d73b68"

q = """
CREATE TEMP FUNCTION target_flag () AS ("NZL");

WITH
  core_data AS (
    SELECT * EXCEPT (event_month), DATE (event_month || "-15") AS event_month
    FROM `scratch_jaeyoon.psma_reflagging_port_visits_overtime_nzl_v20220701`
  ),
  
  manual_removal AS (
    SELECT *
    FROM core_data #dedup
    WHERE 
      vessel_record_id NOT IN (
        'AIS_based_Stitcher-512000089-510065000',
        'AIS_based_IMO-8131441',
        'CCSBT-FV06013|IMO-8729676',
        'CCSBT-FV05984|IMO-8834639|RUS-894679',
        'AUS-861507|CCAMLR-86929|IMO-9123219',
        'AIS_based_Stitcher-503568200-512082000',
        'AIS_based_IMO-4194304',
        'AUS-418781|AUS-860009|IMO-7901758')
      AND ssvid NOT IN ("666050104")
  ),
  
  ranking AS (
    SELECT 
      *, 
      RANK () OVER (PARTITION BY focus_port_flag ORDER BY start_mark DESC, end_mark DESC) AS rank_time,
    FROM (
      SELECT *,
        IF (domestic_reflagging, 
          FIRST_VALUE (first_timestamp) OVER (
            PARTITION BY vessel_record_id 
            ORDER BY flag_eu = target_flag() DESC, first_timestamp ASC),
          NULL) AS start_mark,
        IF (foreign_reflagging, 
          FIRST_VALUE (last_timestamp) OVER (
            PARTITION BY vessel_record_id 
            ORDER BY flag_eu = target_flag() DESC, last_timestamp DESC),
          NULL ) AS end_mark
      FROM manual_removal )
  )
  
SELECT *
FROM ranking
ORDER BY rank_time, vessel_record_id, event_month
"""
df = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

cntr = df[((df.domestic_reflagging) | (df.foreign_reflagging))]
cntr_focus = df[(df.port_flag_eu == 'NZL') & ((df.domestic_reflagging) | (df.foreign_reflagging))]
cntr_flag = cntr[['vessel_record_id', 'flag_eu', 'focus_port_flag', 
              'first_timestamp', 'last_timestamp']].drop_duplicates()

# +
fig = plt.figure(figsize=(8, 2.6), dpi=200, facecolor='#f7f7f7')
ax = fig.add_subplot(111)
ax.scatter(cntr.event_month, 
           cntr.vessel_record_id,
           facecolor='k',
           edgecolor='none',
           marker='.',
           s=5, label='Port Visits Outside Port State')
ax.scatter(cntr_focus.event_month,
           cntr_focus.vessel_record_id,
           facecolor=[blue if f == p else red for f, p in zip(cntr_focus.flag_eu, cntr_focus.focus_port_flag)],
           marker='s', s=5)
ax.barh(cntr_flag.vessel_record_id, 
        cntr_flag.last_timestamp - cntr_flag.first_timestamp, 
        left=cntr_flag.first_timestamp,
        edgecolor=[blue if f == p else red for f, p in zip(cntr_flag.flag_eu, cntr_flag.focus_port_flag)],
        fill=False, linewidth=0.5,
        alpha=0.8)
ax.barh(cntr_flag.vessel_record_id, 
        cntr_flag.last_timestamp - cntr_flag.first_timestamp, 
        left=cntr_flag.first_timestamp,
        color=[blue if f == p else red for f, p in zip(cntr_flag.flag_eu, cntr_flag.focus_port_flag)],
        alpha=0.05)


plt.xlim(pd.Timestamp("2015-06-15"), pd.Timestamp("2022-01-15"))
plt.xticks(fontsize=12)
plt.tick_params(
    axis='y',          # changes apply to the x-axis
    which='both',      # both major and minor ticks are affected
    left=False,      # ticks along the bottom edge are off
    right=False,         # ticks along the top edge are off
#     top=Ture,
    labelleft=False)
# plt.title('Vessels Reflagging and Visiting Ports of New Zealand (monthly summary)', fontsize=8)
plt.ylabel('New Zealand', fontsize=12)

lw, ls, c, alpha = 1, '--', 'grey', 0.5
plt.axvline(pd.Timestamp("2016-01-01"), lw=lw, linestyle=ls, c=c, alpha=alpha)
plt.axvline(pd.Timestamp("2017-01-01"), lw=lw, linestyle=ls, c=c, alpha=alpha)
plt.axvline(pd.Timestamp("2018-01-01"), lw=lw, linestyle=ls, c=c, alpha=alpha)
plt.axvline(pd.Timestamp("2019-01-01"), lw=lw, linestyle=ls, c=c, alpha=alpha)
plt.axvline(pd.Timestamp("2020-01-01"), lw=lw, linestyle=ls, c=c, alpha=alpha)
plt.axvline(pd.Timestamp("2021-01-01"), lw=lw, linestyle=ls, c=c, alpha=alpha)
plt.show()
# -

q = """
CREATE TEMP FUNCTION target_flag () AS ("NAM");

WITH
  core_data AS (
    SELECT * EXCEPT (event_month), DATE (event_month || "-15") AS event_month
    FROM `scratch_jaeyoon.psma_reflagging_port_visits_overtime_nam_v20220701`
  ),
  
  manual_removal AS (
    SELECT *
    FROM core_data #dedup
    WHERE 
      vessel_record_id NOT IN (
        'AIS_based_IMO-4194304',
        'ICCAT-AT000NAM00046|IMO-7902790|IOTC-16906',
        'IMO-6821028',
        'AIS_based_IMO-9259123',
        'IMO-9022881')
      AND ssvid NOT IN ( "273394760", "512447000")  #only for Namibia)
  ),
  
  ranking AS (
    SELECT 
      *, 
      RANK () OVER (PARTITION BY focus_port_flag ORDER BY start_mark DESC, end_mark DESC) AS rank_time,
    FROM (
      SELECT *,
        IF (domestic_reflagging, 
          FIRST_VALUE (first_timestamp) OVER (
            PARTITION BY vessel_record_id 
            ORDER BY flag_eu = target_flag() DESC, first_timestamp ASC),
          NULL) AS start_mark,
        IF (foreign_reflagging, 
          FIRST_VALUE (last_timestamp) OVER (
            PARTITION BY vessel_record_id 
            ORDER BY flag_eu = target_flag() DESC, last_timestamp DESC),
          NULL ) AS end_mark
      FROM manual_removal )
  )
  
SELECT *
FROM ranking
ORDER BY rank_time, vessel_record_id, event_month
"""
df = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

cntr = df[((df.domestic_reflagging) | (df.foreign_reflagging))]
cntr_focus = df[(df.port_flag_eu == 'NAM') & ((df.domestic_reflagging) | (df.foreign_reflagging))]
cntr_flag = cntr[['vessel_record_id', 'flag_eu', 'focus_port_flag', 
              'first_timestamp', 'last_timestamp']].drop_duplicates()

# +
fig = plt.figure(figsize=(8, 3.2), dpi=200, facecolor='#f7f7f7')
ax = fig.add_subplot(111)
ax.scatter(cntr.event_month, 
           cntr.vessel_record_id,
           facecolor='k',
           edgecolor='none',
           marker='.',
           s=5, label='Port Visits Outside Port State')
ax.scatter(cntr_focus.event_month,
           cntr_focus.vessel_record_id,
           facecolor=[blue if f == p else red for f, p in zip(cntr_focus.flag_eu, cntr_focus.focus_port_flag)],
           marker='s', s=5)
ax.barh(cntr_flag.vessel_record_id, 
        cntr_flag.last_timestamp - cntr_flag.first_timestamp, 
        left=cntr_flag.first_timestamp,
        edgecolor=[blue if f == p else red for f, p in zip(cntr_flag.flag_eu, cntr_flag.focus_port_flag)],
        fill=False, linewidth=0.5,
        alpha=0.8)
ax.barh(cntr_flag.vessel_record_id, 
        cntr_flag.last_timestamp - cntr_flag.first_timestamp, 
        left=cntr_flag.first_timestamp,
        color=[blue if f == p else red for f, p in zip(cntr_flag.flag_eu, cntr_flag.focus_port_flag)],
        alpha=0.05)


plt.xlim(pd.Timestamp("2015-06-15"), pd.Timestamp("2022-01-15"))
# plt.xticks(rotation=60)
ax.xaxis.set_ticklabels([])
plt.tick_params(
    axis='y',          # changes apply to the x-axis
    which='both',      # both major and minor ticks are affected
    left=False,      # ticks along the bottom edge are off
    right=False,         # ticks along the top edge are off
#     top=Ture,
    labelleft=False)
# plt.title('Vessels Reflagging and Visiting Ports of Namibia (monthly summary)', fontsize=8)
plt.ylabel('Namibia', fontsize=12)

lw, ls, c, alpha = 1, '--', 'grey', 0.5
plt.axvline(pd.Timestamp("2016-01-01"), lw=lw, linestyle=ls, c=c, alpha=alpha)
plt.axvline(pd.Timestamp("2017-01-01"), lw=lw, linestyle=ls, c=c, alpha=alpha)
plt.axvline(pd.Timestamp("2018-01-01"), lw=lw, linestyle=ls, c=c, alpha=alpha)
plt.axvline(pd.Timestamp("2019-01-01"), lw=lw, linestyle=ls, c=c, alpha=alpha)
plt.axvline(pd.Timestamp("2020-01-01"), lw=lw, linestyle=ls, c=c, alpha=alpha)
plt.axvline(pd.Timestamp("2021-01-01"), lw=lw, linestyle=ls, c=c, alpha=alpha)

plt.show()
# -

q = """
CREATE TEMP FUNCTION target_flag () AS ("CHL");

WITH
  core_data AS (
    SELECT * EXCEPT (event_month), DATE (event_month || "-15") AS event_month
    FROM `scratch_jaeyoon.psma_reflagging_port_visits_overtime_chl_v20220701`
  ),
  
  manual_removal AS (
    SELECT *
    FROM core_data #dedup
    WHERE 
      vessel_record_id NOT IN (
        'AIS_based_IMO-4194304',
        'AIS_based_IMO-1051264',
        'AIS_based_IMO-1579008',
        'AIS_based_IMO-2101248',
        'IMO-7903914',
        'IMO-9620853',
        'IMO-9229465',
        'IMO-9276963',
        'IMO-9201891',
        'IMO-9139543',
        'IATTC-16322|IMO-7311745',
        'IMO-9793985')
      AND ssvid NOT IN ('')
  ),
  
  ranking AS (
    SELECT 
      *, 
      RANK () OVER (PARTITION BY focus_port_flag ORDER BY start_mark DESC, end_mark DESC) AS rank_time,
    FROM (
      SELECT *,
        IF (domestic_reflagging, 
          FIRST_VALUE (first_timestamp) OVER (
            PARTITION BY vessel_record_id 
            ORDER BY flag_eu = target_flag() DESC, first_timestamp ASC),
          NULL) AS start_mark,
        IF (foreign_reflagging, 
          FIRST_VALUE (last_timestamp) OVER (
            PARTITION BY vessel_record_id 
            ORDER BY flag_eu = target_flag() DESC, last_timestamp DESC),
          NULL ) AS end_mark
      FROM manual_removal )
  )
  
SELECT *
FROM ranking
ORDER BY rank_time, vessel_record_id, event_month
"""
df = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

cntr = df[((df.domestic_reflagging) | (df.foreign_reflagging))]
cntr_focus = df[(df.port_flag_eu == 'CHL') & ((df.domestic_reflagging) | (df.foreign_reflagging))]
cntr_flag = cntr[['vessel_record_id', 'flag_eu', 'focus_port_flag', 
              'first_timestamp', 'last_timestamp']].drop_duplicates()

# +
fig = plt.figure(figsize=(8, 1.5), dpi=200, facecolor='#f7f7f7')
ax = fig.add_subplot(111)
ax.scatter(cntr.event_month, 
           cntr.vessel_record_id,
           facecolor='k',
           edgecolor='none',
           marker='.',
           s=5, label='Port Visits Outside Port State')
ax.scatter(cntr_focus.event_month,
           cntr_focus.vessel_record_id,
           facecolor=[blue if f == p else red for f, p in zip(cntr_focus.flag_eu, cntr_focus.focus_port_flag)],
           marker='s', s=5)
ax.barh(cntr_flag.vessel_record_id, 
        cntr_flag.last_timestamp - cntr_flag.first_timestamp, 
        left=cntr_flag.first_timestamp,
        edgecolor=[blue if f == p else red for f, p in zip(cntr_flag.flag_eu, cntr_flag.focus_port_flag)],
        fill=False, linewidth=0.5,
        alpha=0.8)
ax.barh(cntr_flag.vessel_record_id, 
        cntr_flag.last_timestamp - cntr_flag.first_timestamp, 
        left=cntr_flag.first_timestamp,
        color=[blue if f == p else red for f, p in zip(cntr_flag.flag_eu, cntr_flag.focus_port_flag)],
        alpha=0.05)


plt.xlim(pd.Timestamp("2015-06-15"), pd.Timestamp("2022-01-15"))
# plt.xticks(rotation=60)
ax.xaxis.set_ticklabels([])
plt.tick_params(
    axis='y',          # changes apply to the x-axis
    which='both',      # both major and minor ticks are affected
    left=False,      # ticks along the bottom edge are off
    right=False,         # ticks along the top edge are off
#     top=Ture,
    labelleft=False)
# plt.title('Vessels Reflagging and Visiting Ports of Chile (monthly summary)', fontsize=8)
plt.ylabel('Chile', fontsize=12)

lw, ls, c, alpha = 1, '--', 'grey', 0.5
plt.axvline(pd.Timestamp("2016-01-01"), lw=lw, linestyle=ls, c=c, alpha=alpha)
plt.axvline(pd.Timestamp("2017-01-01"), lw=lw, linestyle=ls, c=c, alpha=alpha)
plt.axvline(pd.Timestamp("2018-01-01"), lw=lw, linestyle=ls, c=c, alpha=alpha)
plt.axvline(pd.Timestamp("2019-01-01"), lw=lw, linestyle=ls, c=c, alpha=alpha)
plt.axvline(pd.Timestamp("2020-01-01"), lw=lw, linestyle=ls, c=c, alpha=alpha)
plt.axvline(pd.Timestamp("2021-01-01"), lw=lw, linestyle=ls, c=c, alpha=alpha)
plt.show()
# -

q = """
CREATE TEMP FUNCTION target_flag () AS ("SEN");

WITH
  core_data AS (
    SELECT * EXCEPT (event_month), DATE (event_month || "-15") AS event_month
    FROM `scratch_jaeyoon.psma_reflagging_port_visits_overtime_sen_v20220701`
  ),
  
  manual_removal AS (
    SELECT *
    FROM core_data #dedup
    WHERE 
      vessel_record_id NOT IN (
        'AIS_based_IMO-4194304',
        'AIS_based_IMO-2101248',
        'AIS_based_IMO-1579008',
        'AIS_based_IMO-1051264',
        'AIS_based_IMO-9208978',
        'AIS_based_Stitcher-224097970-663250000')
      AND ssvid NOT IN ('')
  ),
  
  ranking AS (
    SELECT 
      *, 
      RANK () OVER (PARTITION BY focus_port_flag ORDER BY start_mark DESC, end_mark DESC) AS rank_time,
    FROM (
      SELECT *,
        IF (domestic_reflagging, 
          FIRST_VALUE (first_timestamp) OVER (
            PARTITION BY vessel_record_id 
            ORDER BY flag_eu = target_flag() DESC, first_timestamp ASC),
          NULL) AS start_mark,
        IF (foreign_reflagging, 
          FIRST_VALUE (last_timestamp) OVER (
            PARTITION BY vessel_record_id 
            ORDER BY flag_eu = target_flag() DESC, last_timestamp DESC),
          NULL ) AS end_mark
      FROM manual_removal )
  )
  
SELECT *
FROM ranking
ORDER BY vessel_record_id = "IMO-9003342" DESC, rank_time, vessel_record_id, event_month
"""
df = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

cntr = df[((df.domestic_reflagging) | (df.foreign_reflagging))]
cntr_focus = df[(df.port_flag_eu == 'SEN') & ((df.domestic_reflagging) | (df.foreign_reflagging))]
cntr_flag = cntr[['vessel_record_id', 'flag_eu', 'focus_port_flag', 
              'first_timestamp', 'last_timestamp']].drop_duplicates()

# +
fig = plt.figure(figsize=(8, 1.8), dpi=200, facecolor='#f7f7f7')
ax = fig.add_subplot(111)
ax.scatter(cntr.event_month, 
           cntr.vessel_record_id,
           facecolor='k',
           edgecolor='none',
           marker='.',
           s=5, label='Port Visits Outside Port State')
ax.scatter(cntr_focus.event_month,
           cntr_focus.vessel_record_id,
           facecolor=[blue if f == p else red for f, p in zip(cntr_focus.flag_eu, cntr_focus.focus_port_flag)],
           marker='s', s=5)
ax.barh(cntr_flag.vessel_record_id, 
        cntr_flag.last_timestamp - cntr_flag.first_timestamp, 
        left=cntr_flag.first_timestamp,
        edgecolor=[blue if f == p else red for f, p in zip(cntr_flag.flag_eu, cntr_flag.focus_port_flag)],
        fill=False, linewidth=0.5,
        alpha=0.8)
ax.barh(cntr_flag.vessel_record_id, 
        cntr_flag.last_timestamp - cntr_flag.first_timestamp, 
        left=cntr_flag.first_timestamp,
        color=[blue if f == p else red for f, p in zip(cntr_flag.flag_eu, cntr_flag.focus_port_flag)],
        alpha=0.05)


plt.xlim(pd.Timestamp("2015-06-15"), pd.Timestamp("2022-01-15"))
# plt.xticks(rotation=60)
ax.xaxis.set_ticklabels([])
plt.tick_params(
    axis='y',          # changes apply to the x-axis
    which='both',      # both major and minor ticks are affected
    left=False,      # ticks along the bottom edge are off
    right=False,         # ticks along the top edge are off
#     top=Ture,
    labelleft=False)
# plt.title('Vessels Reflagging and Visiting Ports (monthly summary)', fontsize=12)
plt.ylabel('Senegal', fontsize=12)

lw, ls, c, alpha = 1, '--', 'grey', 0.5
plt.axvline(pd.Timestamp("2016-01-01"), lw=lw, linestyle=ls, c=c, alpha=alpha)
plt.axvline(pd.Timestamp("2017-01-01"), lw=lw, linestyle=ls, c=c, alpha=alpha)
plt.axvline(pd.Timestamp("2018-01-01"), lw=lw, linestyle=ls, c=c, alpha=alpha)
plt.axvline(pd.Timestamp("2019-01-01"), lw=lw, linestyle=ls, c=c, alpha=alpha)
plt.axvline(pd.Timestamp("2020-01-01"), lw=lw, linestyle=ls, c=c, alpha=alpha)
plt.axvline(pd.Timestamp("2021-01-01"), lw=lw, linestyle=ls, c=c, alpha=alpha)
plt.show()
# -



# ## Closed loop analysis

q = """
WITH
  by_port AS (
    SELECT psma, port_flag_eu AS flag, 
      closed_loop_cnt AS cnt_port, total_cnt AS total_port, ratio_closed_loop AS ratio_port
    FROM `scratch_jaeyoon.psma_closed_loop_by_port`
  ),

  by_carrier AS (
    SELECT psma, carrier_flag_eu AS flag, closed_loop_cnt AS cnt_carrier, 
      total_cnt AS total_carrier, ratio_closed_loop AS ratio_carrier
    FROM `scratch_jaeyoon.psma_closed_loop_by_carrier`
  )

SELECT 
  psma, flag, cnt_port, total_port, 
  IFNULL (ratio_port, 0) AS ratio_port, 
  IFNULL (cnt_carrier, 0) AS cnt_carrier, 
  IFNULL (total_carrier, 0) AS total_carrier, 
  IFNULL (ratio_carrier, 0) AS ratio_carrier
FROM by_port
LEFT JOIN by_carrier
USING (psma, flag)
ORDER BY total_port
"""
cl = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

cl['total_max'] = cl.apply(lambda x: int(max(x.total_port, x.total_carrier)), axis=1)

cl

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
    'KOR': 'Rep. of Korea',
    'CHN': 'China',
    'RUS': 'Russia'
}

# +
fig = plt.figure(figsize=(8, 8), dpi=200, facecolor='#f7f7f7')
ax = fig.add_subplot(111)
ax.scatter(cl.ratio_port, cl.ratio_carrier, 
           s=cl.total_max.apply(lambda x: x/10),
           facecolor='none', edgecolor=cl.psma.apply(lambda x: blue if x else red),
           lw=1.5)
yadj = [0.07, 0.03, -0.015, 0.035, -0.02, 0.035, -0.025, -0.02, 0.038, 0.04, 0.055, -0.035, 0.07, 0.10]
xadj = [0, 0.04, -0.02, 0, 0.1, 0.1, 0.05, 0.06, 0.01, 0.11, 0.09, 0.17, 0.06, 0.06]
for i, (x, y, t, p, c) in cl[['ratio_port', 'ratio_carrier', 'flag', 'psma', 'total_max']].iterrows():
    ax.text(x - 0.02 - xadj[i], y - yadj[i], flag_map[t] + "(" + str(c) + ")")
    
ax.plot([[-0., -0.], [1., 1.]], lw=0.7, linestyle='--', color='grey')
ax.grid(linestyle=':', lw=0.5)
# ax.scatter(0.8, 0.1, s=1000, facecolor='white', edgecolor='grey', lw=1)
# ax.scatter(0.8, 0.075, s=100, facecolor='white', edgecolor='grey', lw=1)
# ax.text(0.85, 0.115, '1,000 port visits')
# ax.text(0.85, 0.065, '100 port visits')

plt.xlim(-0.13, 1.1)
plt.ylim(-0.13, 1.1)
plt.xlabel("Proportion of transshipment vessel visits in a closed loop" +
           "\nto total transshipment vessel visits to a given port State", fontsize=12)
plt.ylabel("Proportion of transshipment vessel visits in a closed loop" +
           "\nto total transshipment vessel visits to the same flag State", fontsize=12)
# plt.title("Significance of closed loop connection with regard to total port visits and total carrier fleet size")
plt.show()

# +
fig = plt.figure(figsize=(8, 8), dpi=200, facecolor='#f7f7f7')
ax = fig.add_subplot(111)
ax.scatter(cl.ratio_port, cl.ratio_carrier, 
           s=cl.total_carrier.apply(lambda x: x/10),
           facecolor='none', edgecolor=cl.psma.apply(lambda x: blue if x else red),
           lw=1.5)
yadj = [0.07, 0.03, -0.015, 0.035, -0.02, 0.035, -0.025, -0.02, 0.038, 0.04, 0.055, -0.035, 0.07, 0.10]
xadj = [0, 0.04, -0.02, 0, 0.1, 0.1, 0.05, 0.06, 0.01, 0.11, 0.09, 0.17, 0.06, 0.06]
for i, (x, y, t, p, c) in cl[['ratio_port', 'ratio_carrier', 'flag', 'psma', 'total_carrier']].iterrows():
    ax.text(x - 0.02 - xadj[i], y - yadj[i], flag_map[t] + "(" + str(c) + ")")
    
ax.plot([[-0., -0.], [1., 1.]], lw=0.7, linestyle='--', color='grey')
ax.grid(linestyle=':', lw=0.5)
# ax.scatter(0.8, 0.1, s=1000, facecolor='white', edgecolor='grey', lw=1)
# ax.scatter(0.8, 0.075, s=100, facecolor='white', edgecolor='grey', lw=1)
# ax.text(0.85, 0.115, '1,000 port visits')
# ax.text(0.85, 0.065, '100 port visits')

plt.xlim(-0.13, 1.1)
plt.ylim(-0.13, 1.1)
plt.xlabel("Ratio of the number of carriers visiting ports with closed loop connection" +
           "\nto the total number of all carrier visits to a given port", fontsize=12)
plt.ylabel("Ratio of the number of carriers visiting ports with closed loop connection" +
           "\nto the total number of all carriers of the same flag", fontsize=12)
# plt.title("Significance of closed loop connection with regard to total port visits and total carrier fleet size")
plt.show()
# -

# ## Reflagging by countries / PSMA

q = """
SELECT *, timeline AS year
FROM `scratch_jaeyoon.psma_country_outside_eezs_24m_raw_yearly_summary_v6`
WHERE timeline BETWEEN 2016 AND 2021
  AND port_flag != "ALL"
"""
yearly = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

name_map = {
    'PER': 'Peru',
    'MUS': 'Mauritius',
    'ZAF': 'S. Africa',
    'MHL': 'Marshall Islands',
    'FJI': 'Fiji',
    'SEN': 'Senegal',
    'URY': 'Uruguay',
    'PYF': 'French Polynesia',
    'Others': 'Others',
    'NOR': 'Norway',
    'KOR': 'Rep. of Korea',
    'CHN': 'China',
    'TWN': 'Chinese Taipei',
    'FRO': 'Faroe Islands',
    'JPN': 'Japan',
    'EU': 'European Union'
}

yadjust = [-0.00, -0.0075, -0.016, -0.010, -0.0085, -0.011, -0.012, -0.0035, 
           0, -0.01, -0.01, -0.01, -0.01, -0.005, -0.005, -0.01]

# +
fig = plt.figure(figsize=(9, 9), dpi=200, facecolor='#f7f7f7')
ax = fig.add_subplot(111)

from matplotlib import cm
cmap = cm.RdYlBu #seismic #
# cmap.set_bad(alpha = 0.0)
from matplotlib import colors, colorbar

width = 0.4
    
temp_0 = yearly[yearly.port_flag == yearly.port_flag.unique()[0]]
prev = np.zeros(6)
prev_t = 0
for n, flag in enumerate(yearly.port_flag.unique()):
    temp = yearly[yearly.port_flag == flag]
    cmap_colors = cmap([col for col in temp['frac'].values])
    
    
    for i, year in enumerate(temp.year.values):
        if year < 2021:
            ax.plot([year + width, year + 1 - width],
                    [temp[temp.year == year].ratio.values[0] + prev[i], 
                     temp[temp.year == year+1].ratio.values[0] + prev[i+1]],
                    c='#b9b9b9', lw=0.5 )
        ax.plot([year - width, year + width],
                [temp[temp.year == year].ratio.values[0] + prev[i],
                 temp[temp.year == year].ratio.values[0] + prev[i]],
                c='#e6e6e6', lw=0.5)
            
#     ax.bar(temp.year, np.ones(7), bottom=prev, width=width * 2, color='white')
#     prev += np.ones(7)
    ax.bar(temp.year, temp.ratio, bottom=prev, width=width * 2, color=cmap_colors)
    prev += temp.ratio.values
    
    height = temp[temp.year == 2021].ratio.values[0]
    ax.text(2021.5, (height * 0.5 + prev_t + yadjust[n]), name_map[flag]) #,  transform=ax.transAxes)
    prev_t += height

plt.xlim(2015.4, 2022.8)
plt.xlabel('Year', fontsize=12)
plt.ylabel('Fishing hours landed to port states in fraction', fontsize=12)
# plt.title('[Option 2a] Fishing Hours Landed by Domestic vs. Foreign Flagged Vessels')
plt.xticks([2016, 2017, 2018, 2019, 2020, 2021])

#
# Add colorbar
norm = colors.Normalize(vmin = 0, vmax = 1)
ax1 = fig.add_axes([0.3, 0.04, 0.4, 0.01])
cb = colorbar.ColorbarBase(ax1, norm = norm, cmap = cmap, orientation="horizontal") # plt.get_cmap('Reds'))
cb.set_label('Fishing hours landed by domestic flagged vessels in fraction', 
             fontsize=11, labelpad=5, y=0, color = "#000000")

plt.show()
# -

q = """
SELECT *, timeline AS year
FROM `scratch_jaeyoon.psma_country_outside_eezs_24m_psma_yearly_summary_v6`
WHERE timeline BETWEEN 2016 AND 2021
  #AND port_flag != "ALL"
"""
psma_yearly = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

name_map = {
    'KOR': 'Rep. of Korea',
    'CHN': 'China',
    'TWN': 'Chinese Taipei',
    'EU': 'European Union',
    'ALL_BUT_TOPS_PSMA': 'All PSMA ratified\nport states\n(except EU &\nRep. of Korea)',
    'ALL_BUT_TOPS_NON_PSMA': 'All PSMA \nnon-ratified\nport states\n(except China &\nChinese Taipei)'
}

yadjust = [-0.01, -0.015, -0.065, -0.035, -0.01, -0.01]

# +
fig = plt.figure(figsize=(9, 9), dpi=200, facecolor='#f7f7f7')
ax = fig.add_subplot(111)

width = 0.4
    
temp_0 = psma_yearly[psma_yearly.port_flag == psma_yearly.port_flag.unique()[0]]
prev = np.zeros(6)
prev_t = 0
for n, flag in enumerate(psma_yearly.port_flag.unique()):
    temp = psma_yearly[psma_yearly.port_flag == flag]
    cmap_colors = cmap([col for col in temp['frac'].values])
    
    
    for i, year in enumerate(temp.year.values):
        if year < 2021:
            ax.plot([year + width, year + 1 - width],
                    [temp[temp.year == year].ratio.values[0] + prev[i], 
                     temp[temp.year == year+1].ratio.values[0] + prev[i+1]],
                    c='#b9b9b9', lw=0.5 )
            if n == 2:
                ax.plot([year + width, year + 1 - width],
                        [temp[temp.year == year].ratio.values[0] + prev[i], 
                         temp[temp.year == year+1].ratio.values[0] + prev[i+1]],
                        c='#b9b9b9', lw=4, ls='--' )
                
        ax.plot([year - width, year + width],
                [temp[temp.year == year].ratio.values[0] + prev[i],
                 temp[temp.year == year].ratio.values[0] + prev[i]],
                c='#e6e6e6', lw=0.5)
        if n == 2:
            if (year == 2016):
                width_add = 0.6
                ax.plot([2016 - width - width_add, 2016 - width],
                        [temp[temp.year == year].ratio.values[0] + prev[i],
                         temp[temp.year == year].ratio.values[0] + prev[i]],
                        c='#b9b9b9', lw=4, ls='--')
            if (year == 2021):
                ax.plot([2021 + width, 2021 + width + width_add],
                        [temp[temp.year == year].ratio.values[0] + prev[i],
                         temp[temp.year == year].ratio.values[0] + prev[i]],
                        c='#b9b9b9', lw=4, ls='--')
            ax.plot([year - width, year + width],
                    [temp[temp.year == year].ratio.values[0] + prev[i],
                     temp[temp.year == year].ratio.values[0] + prev[i]],
                    c='#b9b9b9', lw=4, ls='--')
            
#     ax.bar(temp.year, np.ones(7), bottom=prev, width=width * 2, color='white')
#     prev += np.ones(7)
    ax.bar(temp.year, temp.ratio, bottom=prev, width=width * 2, color=cmap_colors)
    prev += temp.ratio.values
    
    height = temp[temp.year == 2021].ratio.values[0]
    ax.text(2021.45, (height * 0.5 + prev_t + yadjust[n]), name_map[flag]) #,  transform=ax.transAxes)
    prev_t += height

    if n == 2:
        ax.text(2014.9, prev_t - 0.02, "↑\nPSMA\nrafitied")
        ax.text(2014.9, prev_t - 0.15, "PSMA\nnon-\nrafitied\n↓")
    
plt.xlabel('Year', fontsize=12)
plt.ylabel('Fishing hours landed to port states in fraction', fontsize=12)
# plt.title('[Option 2b] Fishing Hours Landed by Domestic vs. Foreign Flagged Vessels (Grouped by PSMA Ratified States)')
plt.xlim (2014.8, 2022.8)
plt.xticks([2016, 2017, 2018, 2019, 2020, 2021])

#
# Add colorbar
norm = colors.Normalize(vmin = 0, vmax = 1)
ax1 = fig.add_axes([0.3, 0.04, 0.4, 0.01])
cb = colorbar.ColorbarBase(ax1, norm = norm, cmap = cmap, orientation="horizontal") # plt.get_cmap('Reds'))
cb.set_label('Fishing hours landed by domestic flagged vessels in fraction', 
             fontsize=11, labelpad=5, y=0, color = "#000000")

plt.show()
# -

# ## Doubling of domestic landing in non-PSMA, mostly driven by China

q = """
SELECT timeline AS year, SUM(domestic_fishing_hours_landed) / SUM (total_fishing_hours_landed),
  SUM(IF (port_flag = 'CHN', domestic_fishing_hours_landed, 0)) / SUM (total_fishing_hours_landed),
  SUM(IF (port_flag != 'CHN', domestic_fishing_hours_landed, 0)) / SUM (total_fishing_hours_landed),
FROM `scratch_jaeyoon.psma_country_outside_eezs_24m_psma_yearly_summary_v6`
WHERE timeline BETWEEN 2016 AND 2021
  AND NOT psma
GROUP BY 1
ORDER BY 1
"""
pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')


