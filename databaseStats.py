import pandas as pd
import numpy as np
import sys
sys.path.append('/vol/research/mammo2/will/python/usefulFunctions')
import usefulFunctions as uf
#from usefulFunctions import *

# Get list of excel files
spreadsheet_paths = uf.getFiles('/vol/research/mammo2/will/data/batches/metadata',
                           '*IMAGE.xls')
print(len(spreadsheet_paths), ' spreadsheets found')

# Create stats dict
stats_template = {'ImageSOPIUID':0,
                  'StudyIUID':0,
                  'ROIs':0,
                  'Unique ROIs':0}
stats = {}
batch_numbers = [1, 3, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 16, 18, 19, 21, 22,
                 23, 30, 31, 32, 33, 40, 42, 43 , 44, 45, 46, 47, 48, 49, 50,
                 51]
for num in batch_numbers:
    stats.update({'batch ' + str(num): dict(stats_template)})

# Load stats into stats
for path, batch in zip(spreadsheet_paths, stats):
    sheet_0 = pd.ExcelFile(path).parse(0)
    sheet_1 = pd.ExcelFile(path).parse(1)

    # Get ImageSOPIUID
    stats[batch]['ImageSOPIUID'] = len(sheet_0['ImageSOPIUID'])

    # Get StudyIUID
    tmp_id = ''
    for id in sheet_0['StudyIUID'] :
        if id != tmp_id:
            stats[batch]['StudyIUID'] += 1
            tmp_id = id

    # Get ROIs
    stats[batch]['ROIs'] = len(sheet_1['ImageSOPIUID'])

    # Get Unique ROIs
    tmp_id = ''
    for id in sheet_1['StudyIUID']:
        if id != tmp_id:
            stats[batch]['Unique ROIs'] += 1
            tmp_id = id


# Total up the stats
stats_total = dict(stats_template)
for batch in stats:
    for single_stat in stats[batch]:
        stats_total[single_stat] += stats[batch][single_stat]

# Print results
for batch in stats:
    print(batch)
    for single_stat in stats[batch]:
        print('    ', single_stat, ': ', stats[batch][single_stat])
print('\nTotals')
for single_stat in stats_total:
    print('    ', single_stat, ': ', stats_total[single_stat])
print('Unique ROIs = ', '{:.2f}'.format(
    stats_total['Unique ROIs'] / stats_total['ImageSOPIUID'] * 100), '%')
