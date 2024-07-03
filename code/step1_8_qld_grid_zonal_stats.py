#!/usr/bin/env python

from __future__ import print_function, division
import fiona
import rasterio
import pandas as pd
from rasterstats import zonal_stats
import geopandas as gpd
import warnings
import os
import numpy as np

warnings.filterwarnings("ignore")

'''
step1_7_monthly_max_temp_zonal_stats.py
============================

Read in max_temp raster images from QLD silo and a polygon shapefile and perform zonal statistic analysis on a list of 
imagery. It returns a csv file containing the statistics for the input zones.

Author: Grant Staben
email: grant.staben@nt.gov.au
Date: 21/09/2020
version: 1.0

Modified: Rob McGregor
email: robert.mcgregor@nt.gov.au
Date: 2/11/2020
version 2.0


###############################################################################################

MIT License

Copyright (c) 2020 Rob McGregor

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the 'Software'), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.


THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

##################################################################################################

========================================================================================================
'''

def met_correction_fn(output_zonal_stats, var_, variable_values):
    """ Replace specific 0 values with Null values and correct b1, b2 and b3 calculations
    (refer to Fractional Cover metadata)

    @param output_zonal_stats: dataframe object containing the Landsat tile Fractional Cover zonal stats.
    @return: processed dataframe object containing the Landsat tile Fractional Cover zonal stats and
    updated values.
    """
    print(variable_values)

    output_zonal_stats['{0}_min'.format(var_)] = output_zonal_stats['{0}_min'.format(var_)].replace(0, np.nan)
    #
    output_zonal_stats['{0}_min'.format(var_)] = (output_zonal_stats['{0}_min'.format(var_)] * variable_values[2]) +  variable_values[4]
    output_zonal_stats['{0}_max'.format(var_)] = (output_zonal_stats['{0}_max'.format(var_)] * variable_values[2]) +  variable_values[4]
    output_zonal_stats['{0}_mean'.format(var_)] = (output_zonal_stats['{0}_mean'.format(var_)] * variable_values[2]) +  variable_values[4]
    output_zonal_stats['{0}_med'.format(var_)] = (output_zonal_stats['{0}_med'.format(var_)] * variable_values[2]) +  variable_values[4]
    output_zonal_stats['{0}_p25'.format(var_)] = (output_zonal_stats['{0}_p25'.format(var_)] * variable_values[2]) +  variable_values[4]
    output_zonal_stats['{0}_p50'.format(var_)] = (output_zonal_stats['{0}_p50'.format(var_)] * variable_values[2]) +  variable_values[4]
    output_zonal_stats['{0}_p75'.format(var_)] = (output_zonal_stats['{0}_p75'.format(var_)] * variable_values[2]) +  variable_values[4]
    output_zonal_stats['{0}_p95'.format(var_)] = (output_zonal_stats['{0}_p95'.format(var_)] * variable_values[2]) +  variable_values[4]
    output_zonal_stats['{0}_p99'.format(var_)] = (output_zonal_stats['{0}_p99'.format(var_)] * variable_values[2]) +  variable_values[4]
    output_zonal_stats['{0}_range'.format(var_)] = (output_zonal_stats['{0}_range'.format(var_)] * variable_values[2]) +  variable_values[4]


def project_shapefile_gcs_wgs84_fn(gcs_wgs84_dir, geo_df):
    """ Re-project a shapefile to 'GCSWGS84' to match the projection of the max_temp data.
    @param complete_tile: string object containing the Landsat tile name that was used to produce the 1ha plots.
    @param zonal_stats_ready_dir: zonal_stats_ready_dir: string object containing the path to a temporary sub-directory
    prime_temp_grid_dir\zonal_stats_ready\crs_name.
    @param gcs_wgs84_dir: string object containing the path to the subdirectory located in the temporary_dir\gcs_wgs84
    @return:
    """

    # read in shp as a geoDataFrame.
    #df = gpd.read_file(zonal_stats_ready_dir + '\\' + complete_tile + '_by_tile.shp')

    # project to GCSWGS84
    cgs_df = geo_df.to_crs(epsg=4326)

    # define crs file/path name variable.
    crs_name = 'GCSWGS84'

    # Export re-projected shapefiles.
    projected_shape_path = gcs_wgs84_dir + '\\' + 'geo_df_' + str(crs_name) + '.shp'

    # Export re-projected shapefiles.
    cgs_df.to_file(projected_shape_path)

    return cgs_df, projected_shape_path

# def no_data_fn(img_path):
#     from rios import applier, fileinfo
#     """ Get the files no data value"""
#
#     # create the list of  rainfall grids to calculate the total rainfall
#     # flat_list = readlist(imglist)
#     # print("image list: ", flat_list)
#
#     # Set up rios to apply images
#     controls = applier.ApplierControls()
#     infiles = applier.FilenameAssociations()
#     #outfiles = applier.FilenameAssociations()
#
#     # read in the list of imagery to calculate the mean
#     infiles.images = img_path
#     otherargs = applier.OtherInputs()
#     imginfo = fileinfo.ImageInfo(infiles.images[0])
#     # set up the nodata values
#     otherargs.coverNull = imginfo.nodataval[0]
#     print(f"Null values are: {otherargs.coverNull}")
#
#     import sys
#     sys.exit()
#
#     return

def apply_zonal_stats_fn(image_s, projected_shape_path, uid, qld_dict, variable):
    """
    Derive zonal stats for a list of Landsat imagery.

    @param image_s: string object containing the file path to the current max_temp tiff.
    @param projected_shape_path: string object containing the path to the current 1ha shapefile path.
    @param uid: ODK 1ha dataframe feature (unique numeric identifier)
    @return final_results: list object containing the specified zonal statistic values.
    """
    # create empty lists to write in  zonal stats results 

    zone_stats_list = []
    site_id_list = []
    image_name_list = []

    #no_data_value = no_data_fn(image_s)
    #print("Working on variable: ", variable)
    #print(qld_dict)
    #variable_values = qld_dict.get(variable)

    #print("variable_values: ", variable_values)
    no_data = -1 #variable_values[3]  # the no_data value for the silo max_temp raster imagery


    print("*"*50)
    print("no data: ", no_data)
    with rasterio.open(image_s, nodata=no_data) as srci:

        affine = srci.transform
        array = srci.read(1)


        #print("array: ", array)
        #print("Array shape: ", array.shape)
        #array = array + 3276.5

        #print("updated array: ", array)
        #print("Updated array shape: ", array.shape)
        #print("Scaled updated array: ", array)
        #print("Scaled Updated array shape: ", array.shape)

        # open the 'GCSWGS84' projected shapefile (1ha sites)
        with fiona.open(projected_shape_path) as src:

            zs = zonal_stats(src, array, affine=affine, nodata=no_data,
                             stats=['count', 'min', 'max', 'mean', 'median', 'std', 'percentile_25', 'percentile_50',
                                    'percentile_75', 'percentile_95', 'percentile_99', 'range'], all_touched=True)

            #https://gis.stackexchange.com/questions/393413/rasterstats-zonal-statistics-does-not-ignore-nodata
            #print(zs)
            # using "all_touched=True" will increase the number of pixels used to produce the stats "False" reduces
            # the number extract the image name from the opened file from the input file read in by rasterio
            print(zs)
            list_a = str(srci).rsplit('\\')
            #print("list_a: ", list_a)
            file_name = list_a[-1]
            print("file_name: ", file_name)
            list_b = file_name.rsplit("'")
            file_name_final = list_b[0]
            img_date = file_name_final[-23:-17]
            print("img date: ", img_date)
            print("ZS: ", zs)

            for zone in zs:
                zone_stats = zone
                # count = zone_stats["count"]
                mean = zone_stats["mean"]
                # minimum = zone_stats["min"]
                # maximum = zone_stats['max']
                # med = zone_stats['median']
                # std = zone_stats['std']
                # percentile_25 = zone_stats["percentile_25"]
                # percentile_50 = zone_stats["percentile_50"]
                # percentile_75 = zone_stats['percentile_75']
                # percentile_95 = zone_stats['percentile_95']
                # percentile_99 = zone_stats['percentile_99']
                # range_ = zone_stats['range']




                # put the individual results in a list and append them to the zone_stats list
                result = [mean, ] #std, med, minimum, maximum, count, percentile_25, percentile_50,
                         # percentile_75, percentile_95, percentile_99, range_]

                print("Result: ", result)
                zone_stats_list.append(result)

            # extract out the site number for the polygon
            for i in src:
                table_attributes = i['properties']  # reads in the attribute table for each record

                ident = table_attributes[
                    uid]  # reads in the id field from the attribute table and prints out the selected record
                site = table_attributes['site_name']

                details = [ident, site, img_date]
                #print("details: ", details)

                site_id_list.append(details)
                image_used = [file_name_final]
                image_name_list.append(image_used)

        # join the elements in each of the lists row by row
        final_results = [siteid + zoneR + imU for siteid, zoneR, imU in
                         zip(site_id_list, zone_stats_list, image_name_list)]

        # close the vector and raster file 
        src.close()
        srci.close()

    return final_results


def clean_data_frame_fn(output_list, max_temp_output_dir, data_type): #variable, var_, qld_dict):
    """ Create dataframe from output list, clean and export dataframe to a csv to export directory/max_temp sub-directory.

    @param output_list: list object created by appending the final results list elements.
    @param max_temp_output_dir: string object containing the path to the export directory/max_temp sub-directory .
    @param complete_tile: string object containing the current Landsat tile information.
    @return output_max_temp: dataframe object containing all max_temp zonal stats based on the ODK 1ha plots created
    based on the current Landsat tile.
    """

    # convert the list to a pandas dataframe with a headers
    headers = ['ident', 'site', 'im_date', 'mean', 'im_name']#var_ + '_std', var_ + '_med', var_ + '_min',
               #var_ + '_max', var_ + '_count', var_ + "_p25", var_ + "_p50", var_ + "_p75", var_ + "_p95",
               #var_ + "_p99", var_ + "_range", 'im_name']

    df1 = pd.DataFrame.from_records(output_list)
    print(df1)
    output_df = pd.DataFrame.from_records(output_list, columns=headers)
    # print('output_max_temp: ', output_max_temp)
    #variable_values = qld_dict.get(variable)
    #met_correction_fn(output_df, var_, variable_values)

    variable = data_type
    output_df["d_type"] = variable
    site = output_df['site'].unique()

    print(list(output_df.columns))
    #print('list of site names: ', site)

    # import sys
    # sys.exit()

    print("length of site list: ", len(site))
    if len(site) >= 1:
        for i in site:
            out_df = output_df[output_df['site'] == i]

            out_path = os.path.join(max_temp_output_dir, "{0}_{1}_zonal_stats.csv".format(
                str(i), variable))
            # export the pandas df to a csv file
            out_df.to_csv(out_path, index=False)


    else:
        out_path = os.path.join(max_temp_output_dir, "{0}_{1}_zonal_stats.csv".format(
            str(site), variable))
        # export the pandas df to a csv file
        output_df.to_csv(out_path, index=False)

    return output_df


def main_routine(in_dir, out_dir, csv_list, data_type, temp_dir_path, qld_dict, geo_df, dict_, met_ver, shapefile_path):
    """ Calculate the zonal statistics for each 1ha site per QLD monthly max_temp image (single band).
    Concatenate and clean final output DataFrame and export to the Export directory/zonal stats.

    xport_dir_path, zonal_stats_ready_dir, fpc_output_zonal_stats, fpc_complete_tile, i, csv_file, temp_dir_path, qld_dict"""


    uid = 'uid'
    output_list = []
    print("out_dir: ", out_dir)
    variable_values = qld_dict.get(met_ver)
    print(variable_values)
    var_ = variable_values[-1]
    print('Var_', var_)
    #print("geo df: ", geo_df)

    # import sys
    # sys.exit()

    # define the GCSWGS84 directory pathway
    #gcs_wgs84_dir = (temp_dir_path + '\\gcs_wgs84')

    # define the max_tempOutput directory pathway
    #output_dir = (os.path.join(export_dir_path, variable)) # + '\\max_temp')

    # call the project_shapefile_gcs_wgs84_fn function
    #cgs_df, projected_shape_path = project_shapefile_gcs_wgs84_fn(gcs_wgs84_dir, geo_df)

    print("csv_file: ", csv_list)


    # open the list of imagery and read it into memory and call the apply_zonal_stats_fn function
    with open(csv_list, 'r') as imagery_list:

        # loop through the list of imagery and input the image into the raster zonal_stats function
        for image in imagery_list:
            print('image: ', image)

            image_s = image.rstrip()
            print("image_s: ", image_s)



            final_results = apply_zonal_stats_fn(image_s, shapefile_path, uid, qld_dict, data_type)  # cgs_df,projected_shape_path,
            print("final results: ", final_results)

            print("1_8 line 295")
            # import sys
            # sys.exit()
            for i in final_results:
                output_list.append(i)


    #call the clean_data_frame_fn function
    clean_output_temp = clean_data_frame_fn(output_list, out_dir, data_type) #variable, var_, dict_)

    print("18 - 354")
    # import sys
    # sys.exit()


if __name__ == "__main__":
    main_routine()
