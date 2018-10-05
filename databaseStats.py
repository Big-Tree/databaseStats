import pandas as pd
import numpy as np
import multiprocessing as mp
from functools import partial
import itertools
import sys
sys.path.append('/vol/research/mammo2/will/python/usefulFunctions')
import usefulFunctions as uf


# Should be passed ImageSOPIUID of unique lesions only to avoid repeated
# matches.
# If unique contrilaterals is set to true then the function will only return a
# single contrilateral per Image_SOPIUID ensuring that there is at most one
# contrilateral per patient.
def cont_match(properties_to_match, sheet_0,
                            unique_contrilaterals, Image_SOPIUID):
    index = sheet_0['ImageSOPIUID']
    index = index[index == Image_SOPIUID].index[0]
    # Run through each image in the spreadsheet looking for contrilaterals
    tmp_properties = dict(properties_to_match)
    matches = []
    for index in range(len(sheet_0['ImageSOPIUID'])):
        for _ in tmp_properties:
            tmp_properties[_] = sheet_0[_][index]
        if tmp_properties == properties_to_match:
            if sheet_0['ImageSOPIUID'][index] != Image_SOPIUID:
                # Contrilateral found
                if unique_contrilaterals == True:
                    return [sheet_0['ImageSOPIUID'][index]]
                else:
                    matches.append(sheet_0['ImageSOPIUID'][index])
    return matches


# Write a funciton that finds all the contrilaterals for a given
# ImageSOPIUID
def mp_get_contralateral(sheet_0, one_per_study, items):
    # Get properties for image:
    # StudyIUID,ViewPosition, ImageLaterality, PresentationIntenetType
    # Compute properties of contralateral
    # Search for all contralateral images
    # Through in an if statement incase we want one cont per StudyIUID
    
    count, length, Image_SOPIUID = items

    #print('    ', count, '/', length)

    # Get properties of the image
    index = sheet_0['ImageSOPIUID']
    index = index[index == Image_SOPIUID].index[0]
    properties_template = {'StudyIUID': '',
                           'ViewPosition': '',
                           'ImageLaterality': '',
                           'PresentationIntentType': ''}
    lesion_properties = dict(properties_template)
    for item in lesion_properties:
        lesion_properties[item] = sheet_0[item][index]

    # Set contrilateral properties
    cont_properties = dict(lesion_properties)
    if cont_properties['ImageLaterality'] == 'R':
        cont_properties['ImageLaterality'] = 'L'
    else:
        cont_properties['ImageLaterality'] = 'R'

    # Search for images that match cont_properties
    return len(cont_match(
        cont_properties, sheet_0,
        one_per_study, Image_SOPIUID))



#____________MAIN__________

pool = mp.Pool()
# Get list of excel files
spreadsheet_paths = uf.getFiles('/vol/research/mammo2/will/data/batches/metadata',
                           '*IMAGE.xls')
sheet_0 = pd.ExcelFile(spreadsheet_paths[0]).parse(0)
sheet_1 = pd.ExcelFile(spreadsheet_paths[0]).parse(1)

print(len(spreadsheet_paths), ' spreadsheets found')

# Create stats dict
stats_template = {'ImageSOPIUID':0,
                  'StudyIUID':0,
                  'ROIs':0,
                  'Unique ROIs':0,
                  'Unique Contrilaterals':0}
stats = {}
batch_numbers = [1, 3, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 16, 18, 19, 21, 22,
                 23, 30, 31, 32, 33, 40, 42, 43 , 44, 45, 46, 47, 48, 49, 50,
                 51]
for num in batch_numbers:
    stats.update({'batch ' + str(num): dict(stats_template)})

# Load stats into stats
for path in spreadsheet_paths:
    batch = 'batch ' + path.split('_')[1]
    print('Batch: ', batch)
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
    unique_ROIs = []
    tmp_ImageSOPIUID = ''
    for index, ImageSOPIUID in enumerate(sheet_1['StudyIUID']):
        if ImageSOPIUID != tmp_ImageSOPIUID:
            unique_ROIs.append(sheet_1['ImageSOPIUID'][index])
            tmp_ImageSOPIUID = ImageSOPIUID
    stats[batch]['Unique ROIs'] = len(unique_ROIs)

    # Get contrilaterals
    # Make sure only unique ROIs are used as args
    func = partial(mp_get_contralateral, sheet_0, True)
    results = pool.map(
        func, list(zip(
            range(len(unique_ROIs)),
            itertools.repeat(len(unique_ROIs)),
            unique_ROIs)))
    stats[batch]['Contrilaterals'] = sum(results)

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
