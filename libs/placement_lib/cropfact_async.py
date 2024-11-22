import asyncio
import aiohttp
import pandas as pd
import numpy as np


"""
Information about packages to be installed:
    environment python_test has the correct packages
    aiohttp==3.8.1
    asyncio==3.4.3
    nest-asyncio==1.5.5

    nest-asyncio allows loops to be run in a python recipe setting
        nest_asyncio.apply() must be in the python recipe before calling
        loop = asyncio.get_event_loop() and loop.run_until_complete(function)

    nest_asyncio and asyncio need to be imported in the python recipe.
"""

"""
Note on output columns:

# growth conditions come in as codes, otherwise as names. This avoids a naming conflict with vapor pressure deficit
# rh = relative humidity
# prec = precipitation
# rad = shortwave radiation
# pari = photosynthetically active radiation intercepted
# et0 = reference evapotranspiration
# etp = crop evapotranspiration potential
# eta = crop evapotranspiration actual
# vpd = vapor pressure deficit
# ws = wind speed

# VaporPressureDeficit (as a column name) is from the stress table, not the growth condition table


"""


"""
Example Code to be used in a python recipe to call crop fact. This code outputs data from crop fact, which will need to be merged back
with original dataset to be useful. Merging can be done on inputs to cropfact 
        (year, x_longitude, y_latitude, plant_date, harvest_date, irrigation, GMSTP). 
Since some preprocessing is done, that will need to be repeated before merging.

The input data frame has the following columns (in addition to other data columns that are not used):
    ap_data_sector,  year, x_longitude, y_latitude, plant_date (not in correct format),
    harvest_date (not in correct format), irrigation ('DRY','LIRR', etc.), GMSTP and others 
    
    CODE to call crop fact asynchronously and merge data with original dataframe 
    
        # This calls crop facts asynchronously. This code can be run outside of a notebook becausae of nest_asyncio
        import dataiku
        import asyncio
        import nest_asyncio
        import cropfact_async
        import pandas as pd
        import numpy as np

        # to allow run_until_complete to work (called at end of notebook)
        nest_asyncio.apply()

        # Read recipe inputs
        ds = dataiku.Dataset("input_data_by_trait")
        df_all = ds.get_dataframe(limit=100)

        df_uniq_input = cropfact_async.prepareDataFrameForCropFact(df_all)

        # parse dataframe and build jsons
        jsons_all = cropfact_async.getCropFactJSONWrapper(df_uniq_input)

        my_api = "https://cropfact.syngentaaws.org/services/cropphysiology"
        my_header = {'x-api-key': 'N1sgY9MO7O2vky35RBSLL20napGp3qRH6LWOAdKT', 'Content-Type': 'application/json'}

        loop = asyncio.get_event_loop()

        max_tries = 20
        n_tries = 0
        n_error = 100 # initial value to start the while loop, just needs to be bigger than 0 to start
        batch_size = 20

        # so cropfact sometimes complains about 'too many requests' all at once. 
        # this code will retry the calls that returned 'too many requests' at a slightly smaller batch_size
        # until either the number of retries is exceeded or there are no more 'too many requests' errors
        df_cropfact = pd.DataFrame()

        jsons_use = jsons_all.copy()
        while n_tries < max_tries and n_error > 0:
            start_time=time.time()
            df_cropfact_temp = loop.run_until_complete(cropfact_async.getRecords(jsons_use, my_api, my_header, batch_size=batch_size));
            dur = time.time()-start_time

            n_calls = len(jsons_use)
            # get idx of 'too many requests' error
            error_idx = np.nonzero(df_cropfact_temp['error'].values == 'Too Many Requests')[0]
            jsons_use = list(np.array(jsons_use)[error_idx])

            # drop rows of df_cropfact_temp with errors, append to df_cropfact
            df_cropfact_temp = df_cropfact_temp[df_cropfact_temp['error'].isnull()]
            df_cropfact = pd.concat((df_cropfact, df_cropfact_temp),axis=0)

            # update while loop parameters
            n_error=error_idx.shape[0]
            n_tries = n_tries + 1

            # update batch_size
            batch_size = np.minimum(batch_size,np.ceil(n_error/5).astype(int))
            if batch_size <= 0:
                batch_size = 1


        print("done")

        # Write recipe outputs to dataiku dataset
        ds_out = dataiku.Dataset("cropfact_errors")
        ds_out.write_with_schema(df_cropfact)


        # only keep calls that returned data
        df_cropfact = df_cropfact[df_cropfact['error'].isnull()]

        # merge crop fact data back into original dataset, then output
        df = cropfact_async.prepareDataFrameForCropFact(df_all, \
                                                    to_dos=['convert_date_format','convert_irr_format', 'perform_rounding'])
        # replace plant_date with plant_date_as_date, same with harvest_date. This is consistent with df_cropfact format
        df = df.drop(columns=['plant_date','harvest_date'])
        df = df.rename(columns={'plant_date_as_date' : 'plant_date', 'harvest_date_as_date' : 'harvest_date'})

        # convert irrigation column to presence or absence
        df['irr_stat'] = 'Absence'
        df['irr_stat'][(df['irr_conv'] == 'IRR') | (df['irr_conv'] == 'LIRR')] = 'Presence'

        df['drain_stat'] = 'Absence'
        df['drain_stat'][df['drain_stat'] == 'TILE'] = 'Presence'

        # update df_crop's irrigation to the same presence/absence format
        df_cropfact['Irrigation'][~df_cropfact['Irrigation'].isnull()] = 'Presence'
        df_cropfact['Irrigation'][df_cropfact['Irrigation'].isnull()] = 'Absence'

        # rename columns in df_crop
        df_crop_mapper = {'Planting':'plant_date', 'Harvest':'harvest_date', 'Irrigation':'irr_stat',
                         'DrainageSystem':'drain_stat','GMSTP_input':'GMSTP'}
        df_crop_pre_merge = df_cropfact.rename(columns=df_crop_mapper)

        # do merge
        on_list=['x_longitude','y_latitude','plant_date','harvest_date','irr_stat','drain_stat','GMSTP']
        df_out = df.merge(right=df_crop_pre_merge, how='left', on=on_list)

        # drop rows if missing data
        # grain yield potential will be empty if no cropfact data
        df_out=df_out.dropna(subset=['YGHMN','YGSMN','GMSTP','GrainYieldPotential'])
        df_out=df_out.drop_duplicates()

        # Write recipe outputs to dataiku dataset
        ds_out = dataiku.Dataset("input_data_cropfact")
        ds_out.write_with_schema(df_out)
"""


"""
This function prepares an input dataframe for the cropfact pipeline. It assumes that the following columns are in the dataframe:
    'ap_data_sector','x_longitude','y_latitude','year' (necessary)
    'plant_date', 'harvest_date', 'GMSTP', 'irrigation' (not necessary, but helpful)
    
This function can do the following:
    Convert dates to the correct format (yyyy-MM-dd typically)
    Perform a first pass on the irrigation column to only leave 'IRR', 'LIRR', and 'TILE' options. 
        This code could be quickly adjusted to perform on a 'placement' column, though that has not been done yet
    Round the longitude and latitude columns to 6 digits (< 1 meter) and round the GMSTP column to 0 digits
        This is done to reduce the number of unique inputs. The number of digits to round is inputted as a dictionary
        Only perform rounding on columns in rounding_dict
    Extract only the columns necessary for crop fact 
    Drop duplicate rows in the dataframe
The to_dos list can be modified to remove any action, which is useful when merging the output of crop fact back with the original dataset
This function is intended to help users prepare their inputs for the rest of the pipeline, though can be avoided...

"""


def prepareDataFrameForCropFact(df, to_dos=['convert_date_format', 'convert_irr_format', 'extract_cols', 'perform_rounding', 'drop_duplicates'],
                                rounding_dict={'x_longitude': 6, 'y_latitude': 6, 'GMSTP': 0}):
    df_out = df.copy()
    if 'convert_date_format' in to_dos:
        if 'plant_date' in df_out.columns:
            df_out['plant_date_as_date'] = df_out['plant_date'].dt.date.astype(str)
        if 'harvest_date' in df_out.columns:
            df_out['harvest_date_as_date'] = df_out['harvest_date'].dt.date.astype(str)

    if 'convert_irr_format' in to_dos and 'irrigation' in df_out.columns:
        # convert df['irrigation'] to 3 different inputs before doing unique
        # conversion to irr_stat and irr_trigger are done in a different function, this just removes 'DRY' and 'RAIN'
        df_out['irr_conv'] = df_out['irrigation']
        df_out['irr_conv'][(df_out['irr_conv'] != 'IRR') &
                           (df_out['irr_conv'] != 'LIRR') &
                           (df_out['irr_conv'] != 'TILE')] = 'NONE'

    # extract a small number of inputs from data frame

    input_cols = ['ap_data_sector', 'x_longitude', 'y_latitude', 'year']  # required inputs.
    round_cols = ['x_longitude', 'y_latitude']

    # append extra inputs if available
    if 'plant_date_as_date' in df_out.columns:
        input_cols.append('plant_date_as_date')
    elif 'plant_date' in df_out.columns:  # this assumes that the date is in the correct format already....
        df_out.rename(columns={'plant_date': 'plant_date_as_date'})  # rename so naming is consistent across inputs
        input_cols.append('plant_date_as_date')

    if 'harvest_date_as_date' in df_out.columns:
        input_cols.append('harvest_date_as_date')
    elif 'harvest_date' in df_out.columns:  # assumes that the date is in the correct format already....
        df_out.rename(columns={'harvest_date': 'harvest_date_as_date'})  # rename so naming is consistent across inputs
        input_cols.append('harvest_date_as_date')

    if 'irr_conv' in df_out.columns:
        input_cols.append('irr_conv')
    elif 'irrigation' in df_out.columns:  # assumes that
        df_out.rename(columns={'irrigation': 'irr_conv'})  # rename so naming is consistent across inputs
        input_cols.append('irr_conv')

    if 'GMSTP' in df_out.columns:
        input_cols.append('GMSTP')
        round_cols.append('GMSTP')

    if 'extract_cols' in to_dos:
        df_cropfact_input = df_out[input_cols]
    else:
        df_cropfact_input = df_out

    # round to reduce number of unique inputs, 6 decimal places for longitude/latitude is ~ < 1 meter
    if 'perform_rounding' in to_dos:
        for i in range(len(round_cols)):
            if round_cols[i] in rounding_dict:
                round_digs = rounding_dict[round_cols[i]]
                df_cropfact_input[round_cols[i]] = np.round(df_cropfact_input[round_cols[i]], round_digs)

    if 'drop_duplicates' in to_dos:
        df_cropfact_input = df_cropfact_input.drop_duplicates()

    return df_cropfact_input


""" 
This function generates a list of jsons from a dataframe. getCropFactJSON accepts a single set of parameters at a time, so this function loops through rows of a dataframe
This function is meant to be called after the dataframe is prepared and duplicates are removed (possibly via prepareDataFrameForCropFact)
df_uniq_input needs to have the following columns:
    x_longitude : longitude values
    y_latitude : latitude values
    plant_date_as_date : date (yyyy, or yyyy-MM-dd), could be None
    harvest_date_as_date : data in format similar to plant_date_as_date. If this is provided, GMSTP also must be provided
    ap_data_sector : data sector used to get crop name, for example 'CORN_NA_SUMMER'
    GMSTP : grain moisture at harvest. For this to be used, harvest_date_as_date needs to be in yyyy-MM-dd format.
df_uniq_input can also have the following inputs:
    irr_conv : irrigation and drainage information ('IRR','LIRR','TILE',None). Code converts this to irr_status, irr_trigger_threshold, and drainage status
    
returns a list of jsons, which can be used to get data from crop fact.
"""


def getCropFactJSONWrapper(df_uniq_input):
    country = 'GLOBAL'  # global works, code doesn't allow for more specific country codes at this point
    jsons = []
    for index, row in df_uniq_input.iterrows():
        coord = [row['x_longitude'], row['y_latitude']]
        drain_stat = "Absence"
        irr_stat = "Absence"
        irr_trig_thresh = None
        grain_moist = None

        # get crop name
        crop = getCropFromDataSector(row['ap_data_sector'])

        # make sure grain mositure is within required range
        # if harvest date, then GMSTP is mandatory
        if 'GMSTP' in row.index and row['GMSTP'] is not None and np.isnan(row['GMSTP']) == False:
            # these are the values for corn grain
            min_gmstp = 5
            max_gmstp = 34.9
            if crop == 'Soybean':
                min_gmstp = 5
                max_gmstp = 29.9
            grain_moist = max(min_gmstp, min(max_gmstp, row['GMSTP']))

        # convert irr_conv to irr_stat and irr_trig_thresh, also drain_stat
        if 'irr_conv' in row.index:
            if row['irr_conv'] == 'IRR':
                irr_stat = "Presence"
                irr_trig_thresh = 0.85
            elif row['irr_conv'] == 'LIRR':
                irr_stat = "Presence"
                irr_trig_thresh = 0.65
            elif row['irr_conv'] == 'TILE':
                drain_stat = "Presence"

        # dates need to have day, month year to use grain_moist as input
        if grain_moist is not None and (row['year'] is None or len(row['harvest_date_as_date']) != 10):
            grain_moist = None

        # dates must be within specified ranges, these ranges differ for each crop
        # end dataframe will have resulting error
        jsons.append(getCropFactJSON(country=country, country_sub=None, season="1",
                                     coord=coord, crop=crop, crop_sub=None, plant_date=row['plant_date_as_date'],
                                     harvest_date=row['harvest_date_as_date'], grain_moisture=grain_moist,
                                     irrigation_status=irr_stat, irrigation_trigger=irr_trig_thresh,
                                     drainage_status=drain_stat))
    return jsons


"""
This function creates a json that will be used when calling cropfact. 
    country : country. 'US' for example, though 'GLOBAL' also works
    country_sub : sub division of county, like IL or NE for USA. Only for US, CA, FR
    coord : [longitude, latitude], longitude can be in [-180, 180], latitude [-90,90]
    crop : crop name. This can be automatically grabbed using getCropFromDataSector if the ap_data_sector is provided and in the predefined dictionary
    crop_sub: subdivision for crop, only available for NA Corn Grain and NA Soybean. Check CropFact API for more details
    season : growing season, "1" or "2" -- not really sure what this does, as providing "2" to cropfact throws an error
    plant_date : planting date. format: yyyy-MM-dd or yyyy. 
    harvest_date : harvest date. same format as plant_date
    graint_moisture: grain moisture at harvest. in percentage, [5-34.9] for corn, [5-29.9] for soybean. Harvest date must be provided as yyyy-MM-dd to use grain moisture as input
    irrigation_status : "Presence" or "Absence" of irrigation. 
    irrgation_trigger : Value of the ratio of the crop actual evapotranspiration to the crop potential evapotranspiration used to trigger irrigation events
        if IRR, 0.85. if LIRR, 0.65 apparently?
    irrigation_timestamp: timestamps of irrigation events, date format yyyy-MM-dd. 
    irrigation_value : amount of irrigation events (>=0)
    drainage_status : "Presence" or "Absence" of drainage tiles
    
    outputs information in the appropriately structured json dictionary style. This can be used to call cropfact
    
    This function does not apply constraints to the input, such as fixing the grain moisture limit, or preventing calls when the harvest date is not provided.
    The example (above) has some code to do this. I did not include that code in this function so that users would know/decide which constraints/modifications 
    are applied to their input data.
    
"""


def getCropFactJSON(country='GLOBAL', country_sub=None, coord=[-90, 40], crop='Corn Grain', crop_sub=None, season="1",
                    plant_date='2020', harvest_date='2020', grain_moisture=None,
                    irrigation_status="Absence", irrigation_trigger=None, irrigation_timestamp=None, irrigation_value=None,
                    drainage_status="Absence"):

    # predefine scenario
    scenario = {}

    # set country and location info, put in scenario
    geo = {'type': 'Point', 'coordinates': coord}
    cropping_area = {'country': country, 'countrySubDivision': country_sub, 'geometry': geo}
    scenario['croppingArea'] = cropping_area

    # set crop info
    scenario['genotype'] = {'crop': crop, 'cropSubDivision': crop_sub}

    # set management info
    operations_list = [{"name": "Planting", "timestamp": plant_date},
                       {"name": "Harvest", "timestamp": harvest_date},
                       {"name": "Irrigation", "status": irrigation_status, "triggerThreshold": irrigation_trigger, "timestamp": irrigation_timestamp, "value": irrigation_value}]

    equip_list = [{"name": "DrainageSystem", "status": drainage_status}]
    scenario['management'] = {'season': season, 'operations': operations_list, 'equipments': equip_list}

    # phenotype info.
    pheno = {}
    pheno['physiologicalTraits'] = [{"name": "GrainMoistureContent", "timestamp": "Harvest", "value": grain_moisture}]
    scenario['phenotype'] = pheno

    json_body = {}
    json_body['scenario'] = scenario

    return json_body


"""
helper function to extract data from the structure cropfact returns. This function hides a lot of the logic used to extract relevant information.
getRecords calls this function a few times. See getRecords for usage examples
"""


def parseCropFactData(curr_dict={}, crop_dict=None, name_var='name'):
    for var in crop_dict:
        if 'value' in var:
            if var['value'] is not None and isinstance(var['value'], list) and len(var['value']) > 1:
                for i_var in range(len(var['value'])):
                    # if var['name'] == 'Temperature' or var['name'] == 'RelativeHumidity':
                    #    curr_dict[var['code']+var['timestamp'][i_var]] = var['value'][i_var]
                    # else:
                    curr_dict[var[name_var]+var['timestamp'][i_var]] = var['value'][i_var]
            elif var['value'] is not None and isinstance(var['value'], list) and len(var['value']) == 1:
                curr_dict[var[name_var]] = var['value'][0]
            elif var['value'] is None:
                curr_dict[var[name_var]] = None
            else:
                curr_dict[var[name_var]] = var['value']
        else:  # phenological stages
            curr_dict['PhenologicalStage'+var['name']] = var['timestamp']
    return curr_dict


"""
Function to convert ap_data_sector to appropriate crop name for crop fact. 
Fill in dictionary with ap_data_sector : crop name if a specific ap_data_sector is missing.
See crop fact api for more details about crop names
"""


def getCropFromDataSector(sector):
    # map ap data sector to crop name. Need to fill in ap data sectors

    # crop can be 'Corn Grain', 'Corn Silage', 'Soybean', 'Sunflower', 'Winter Wheat', 'Winter Barley'
    # map ap data sector to crop name
    crop_mapper = {'CORN_NA_SUMMER': 'Corn Grain',
                   'SOY_NA_SUMMER': 'Soybean',
                   'SOY_BRAZIL_SUMMER': 'Soybean',
                   'CORN_BRAZIL_SUMMER': 'Corn Grain',
                   'CORN_BRAZIL_SAFRINHA': 'Corn Grain',
                   'SUNFLOWER_EAME_SUMMER': 'Sunflower',
                   'CORNGRAIN_APAC_1': 'Corn Grain',
                   'CORN_EAME_SUMMER': 'Corn Grain',
                   'CORNGRAIN_EAME_SUMMER': 'Corn Grain',
                   'CORNSILAGE_EAME_SUMMER': 'Corn Silage',  # cropFact may not support Corn Silage yet
                   'WHEAT_EAME_WINTER': 'Winter Wheat',
                   'BARLEY_EAME_WINTER': 'Winter Barley',
                   'CORN_INDONESIA_WET': 'Corn Grain',
                   'CORN_INDONESIA_DRY': 'Corn Grain'}

    return crop_mapper[sector]


"""
This function calls crop fact. It is an async function, enabling multiple calls simultaneously.
getRecords calls this function repeatedly, it should probably not be called outside of getRecords

my_api : "https://cropfact.syngentaaws.org/services/cropphysiology"
my_header = {'x-api-key': key, 'Content-Type': 'application/json'}

"""


async def callCropFact(session, json, my_api, my_header):
    async with session.post(url=my_api, headers=my_header, json=json) as resp:
        return await resp.json()


"""
Call this function to get crop fact data for a list of jsons. Calls are made asynchronously, allowing for many calls at once.
Calls are made in blocks of 5000, as making more calls simultaneoulsy resulted in an error.
    --- THIS MAY BE OLD. batch_size controls this, and it might be that a maximum of 20 can be used. This wasn't the case when the code was developed

There is a maximum of 100,000 calls in a day for the same api-key



    jsons_all : list of jsons generated by repeated calls to getCropFactJSON
    my_api : "https://cropfact.syngentaaws.org/services/cropphysiology"
    my_header = {'x-api-key': key, 'Content-Type': 'application/json'}


returns dataframe with relevant information extracted from crop fact output
"""


async def getRecords(jsons_all, my_api, my_header, batch_size=20):
    # extract data from crop fact calls
    data = []

    # print("started calling crop fact")
    for i in range(0, len(jsons_all), batch_size):
        # if i%100 == 0:
        #    print(i/len(jsons_all))
        # split jsons_all
        if i+batch_size > len(jsons_all):
            jsons = jsons_all[i:]
        else:
            jsons = jsons_all[i:i+batch_size]
        conn = aiohttp.TCPConnector(ssl=False)
        # generate list of tasks
        async with aiohttp.ClientSession(connector=conn) as session:
            # check if we have a list of jsons or just a single one
            if isinstance(jsons, list):
                tasks = [callCropFact(session, json, my_api, my_header) for json in jsons]
            else:
                tasks = [callCropFact(session, jsons, my_api, my_header)]
            # asynchronously perform tasks, which are different calls to crop fact
            out = await asyncio.gather(*tasks)
        # done calling crop fact, now packaging outputs
        for j in range(len(out)):
            cd = out[j]
            if 'error' not in cd.keys():
                try:  # mostly to prevent code from stopping if there is an error. Errors should be stored in output dataframe
                    data.append({})
                    # some meta information
                    data[-1]['error'] = None
                    data[-1]['country'] = cd['scenario']['croppingArea']['country']
                    data[-1]['countrySubDivision'] = cd['scenario']['croppingArea']['countrySubDivision']
                    data[-1]['x_longitude'] = cd['scenario']['croppingArea']['geometry']['coordinates'][0]
                    data[-1]['y_latitude'] = cd['scenario']['croppingArea']['geometry']['coordinates'][1]
                    data[-1]['crop'] = cd['scenario']['genotype']['crop']

                    # get grain moisture content input, useful for merging later
                    phys_dict = cd['scenario']['phenotype']['physiologicalTraits']
                    for d in phys_dict:
                        if "name" in d.keys() and d["name"] == 'GrainMoistureContent':
                            if isinstance(d['value'], list):
                                data[-1]['GMSTP_input'] = d['value'][0]
                            else:
                                data[-1]['GMSTP_input'] = d['value']

                    for op in cd['scenario']['management']['operations']:
                        data[-1][op['name']] = op['timestamp']

                    for eq in cd['scenario']['management']['equipments']:
                        data[-1][eq['name']] = eq['status']

                    # phenological stages: get timestamp
                    data[-1] = parseCropFactData(curr_dict=data[-1].copy(), crop_dict=cd['scenario']['phenotype']['phenologicalStages'], name_var='name')

                    # physiological traits, split by stage if trait is defined for each stage
                    data[-1] = parseCropFactData(curr_dict=data[-1].copy(), crop_dict=cd['scenario']['phenotype']['physiologicalTraits'], name_var='name')

                    # environment variables -- Soil properties
                    data[-1] = parseCropFactData(curr_dict=data[-1].copy(), crop_dict=cd['scenario']['environment']['soilProperties'], name_var='name')

                    # environment vars -- growth conditions
                    data[-1] = parseCropFactData(curr_dict=data[-1].copy(), crop_dict=cd['scenario']['environment']['growthConditions'], name_var='code')

                    # environment vars -- stresses
                    data[-1] = parseCropFactData(curr_dict=data[-1].copy(), crop_dict=cd['scenario']['environment']['stresses'], name_var='name')
                except Exception as e:
                    if 'message' in cd:
                        data[-1]['error'] = cd['message']
                    else:
                        data[-1]['error'] = e
            else:  # append error message, leave rest of dataframe empty
                data.append({})
                data[-1]['error'] = cd['error']['message']

    # make data frame from crop fact outputs
    df_cropfact = pd.DataFrame.from_dict(data, orient='columns')

    return df_cropfact
