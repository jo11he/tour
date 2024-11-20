import os

PDS_RSS_ROOT = "https://atmos.nmsu.edu/pdsd/archive/data/co-ssa-rss-1-{}-v10"

NAIF_ROOT = "https://naif.jpl.nasa.gov/pub/naif/pds/data/co-s_j_e_v-spice-6-v1.0/cosp_1000/data/{}/"

df_column_keys = ['Volume', 'Type', 'URL DATA', 'Start Date', 'End Date']

ancillary_data_shorts = ['ckf', 'eop', 'ion', 'spk', 'tro']

#print(PDS_RSS_ROOT.format("tigr3"))