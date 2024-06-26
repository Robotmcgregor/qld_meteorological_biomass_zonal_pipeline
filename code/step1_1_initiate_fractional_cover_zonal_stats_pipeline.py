#!/usr/bin/env python

"""
Fractional cover zonal statistics pipeline
==========================================

Description: This pipeline comprises of 12 scripts which read in the Rangeland Monitoring Branch odk instances
{instance names, odk_output.csv and ras_odk_output.csv: format, .csv: location, directory}
Outputs are files to a temporary directory located in your working directory (deleted at script completion),
or to an export directory located a the location specified by command argument (--export_dir).
Final outputs are files to their respective property sub-directories within the Pastoral_Districts directory located in
the Rangeland Working Directory.


step1_1_initiate_fractional_cover_zonal_stats_pipeline.py
===============================
Description: This script initiates the Fractional cover zonal statistics pipeline.
This script:

1. Imports and passes the command line arguments.

2. Creates two directories named: user_YYYYMMDD_HHMM. If either of the directories exist, they WILL BE DELETED.

3. Controls the workflow of the pipeline.

4. deletes the temporary directory and its contents once the pipeline has completed.


Author: Rob McGregor
email: Robert.Mcgregor@nt.gov.au
Date: 27/10/2020
Version: 1.0

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

===================================================================================================

Command arguments:
------------------

--tile_grid: str
string object containing the path to the Landsat tile grid shapefile.

--directory_odk: str
string object containing the path to the directory housing the odk files to be processed - the directory can contain 1
to infinity odk outputs.
Note: output runtime is approximately 1 hour using the remote desktop or approximately  3 hours using your laptop
(800 FractionalCover images).

--export_dir: str
string object containing the location of the destination output folder and contents(i.e. 'C:Desktop/odk/YEAR')
NOTE1: the script will search for a duplicate folder and delete it if found.
NOTE2: the folder created is titled (YYYYMMDD_TIME) to avoid the accidental deletion of data due to poor naming
conventions.

--image_count
integer object that contains the minimum number of Landsat images (per tile) required for the fractional cover
zonal stats -- default value set to 800.

--landsat_dir: str
string object containing the path to the Landsat Directory -- default value set to r'Z:\Landsat\wrs2'.

--no_data: int
ineger object containing the Landsat Fractional Cover no data value -- default set to 0.

--rainfall_dir: str
string object containing the pathway to the rainfall image directory -- default set to r'Z:\Scratch\mcintyred\Rainfall'.

--search_criteria1: str
string object containing the end part of the filename search criteria for the Fractional Cover Landsat images.
-- default set to 'dilm2_zstdmask.img'

--search_criteria2: str
string object from the concatenation of the end part of the filename search criteria for the Fractional Cover
Landsat images. -- default set to 'dilm3_zstdmask.img'

--search_criteria3: str
string object from the concatenation of the end part of the filename search criteria for the QLD Rainfall images.
-- default set to '.img'

======================================================================================================

"""

# Import modules
from __future__ import print_function, division
import os
from datetime import datetime
import argparse
import shutil
import sys
import warnings
import glob
import pandas as pd
import geopandas

warnings.filterwarnings("ignore")


def get_cmd_args_fn():
    p = argparse.ArgumentParser(
        description='''Input a single or multi-band raster to extracts the values from the input shapefile. ''')


    p.add_argument('-d', '--data', help='The directory the site points csv file.')

    p.add_argument('-x', '--export_dir',
                   help='Enter the export directory for all of the final outputs.',
                   default=r'U:\scratch\rob\pipelines\outputs')

    p.add_argument('-i', '--image_count', type=int,
                   help='Enter the minimum amount of Landsat images required per tile as an integer (i.e. 950).',
                   default=100)

    p.add_argument('-l', '--qld_dir', help="The qld meteorological data directory path",
                   default=r"U:\scratch\rob\qld_grid_data")

    p.add_argument('-n', '--no_data', help="Enter the Landsat Fractional Cover no data value (i.e. 0)",
                   default=0)

    cmd_args = p.parse_args()

    if cmd_args.data is None:
        p.print_help()

        sys.exit()

    return cmd_args


def temporary_dir_fn():
    """ Create a temporary directory 'user_YYYMMDD_HHMM'.

    @return temp_dir_path: string object containing the newly created directory path.
    @return final_user: string object containing the user id or the operator.
    """

    # extract user name
    home_dir = os.path.expanduser("~")
    _, user = home_dir.rsplit('\\', 1)
    final_user = user[3:]

    # create file name based on date and time.
    date_time_replace = str(datetime.now()).replace('-', '')
    date_time_list = date_time_replace.split(' ')
    date_time_list_split = date_time_list[1].split(':')
    temp_dir_path = '\\' + str(final_user) + '_' + str(date_time_list[0]) + '_' + str(
        date_time_list_split[0]) + str(date_time_list_split[1])

    # check if the folder already exists - if False = create directory, if True = return error message zzzz.
    try:
        shutil.rmtree(temp_dir_path)

    except:
        print('The following temporary directory will be created: ', temp_dir_path)
        pass
    # create folder a temporary folder titled (titled 'tempFolder'
    os.makedirs(temp_dir_path)

    return temp_dir_path, final_user


def temp_dir_folders_fn(temp_dir_path):
    """ Create folders within the temp_dir directory.

    @param temp_dir_path: string object containing the newly created directory path.
    @return prime_temp_grid_dir: string object containing the newly created folder (temp_tile_grid) within the
    temporary directory.
    @return prime_temp_buffer_dir: string object containing the newly created folder (temp_1ha_buffer)within the
    temporary directory.

    """

    prime_temp_grid_dir = temp_dir_path + '\\temp_tile_grid'
    os.mkdir(prime_temp_grid_dir)

    zonal_stats_ready_dir = prime_temp_grid_dir + '\\zonal_stats_ready'
    os.makedirs(zonal_stats_ready_dir)

    proj_tile_grid_sep_dir = prime_temp_grid_dir + '\\separation'
    os.makedirs(proj_tile_grid_sep_dir)

    prime_temp_buffer_dir = temp_dir_path + '\\temp_1ha_buffer'
    os.mkdir(prime_temp_buffer_dir)

    gcs_wgs84_dir = (temp_dir_path + '\\gcs_wgs84')
    os.mkdir(gcs_wgs84_dir)

    albers_dir = (temp_dir_path + '\\albers')
    os.mkdir(albers_dir)

    return prime_temp_grid_dir, prime_temp_buffer_dir


def export_file_path_fn(export_dir, final_user):
    """ Create an export directory 'user_YYYMMDD_HHMM' at the location specified in command argument export_dir.

    @param final_user: string object containing the user id or the operator.
    @param export_dir: string object containing the path to the export directory (command argument).
    @return export_dir_path: string object containing the newly created directory path for all retained exports.
    """

    # create string object from final_user and datetime.
    date_time_replace = str(datetime.now()).replace('-', '')
    date_time_list = date_time_replace.split(' ')
    date_time_list_split = date_time_list[1].split(':')
    export_dir_path = export_dir + '\\' + final_user + '_meteorological_' + str(date_time_list[0]) + '_' + str(
        date_time_list_split[0]) + str(
        date_time_list_split[1])

    # check if the folder already exists - if False = create directory, if True = return error message.
    try:
        shutil.rmtree(export_dir_path)

    except:
        print('The following export directory will be created: ', export_dir_path)
        pass

    # create folder.
    os.makedirs(export_dir_path)

    return export_dir_path


def export_dir_folders_fn(export_dir_path, sub_dir_list):
    """ Create sub-folders within the export directory.

    @param export_dir_path: string object containing the newly created export directory path.
    @return tile_status_dir: string object containing the newly created folder (tile_status) with three sub-folders:
    for_processing, insufficient_files and tile_status_lists.
    @return tile_status_dir:
    @return plot_dir:
    @return zonal_stats_output_dir:
    @return rainfall_output_dir:
    """

    # rainfall_output_dir = (export_dir_path + '\\rainfall')
    # os.mkdir(rainfall_output_dir)

    for i in sub_dir_list:
        i_output_dir = (os.path.join(export_dir_path, i))
        os.mkdir(i_output_dir)

    # fpc_tile_status_dir = (export_dir_path + '\\fpc_tile_status')
    # os.mkdir(fpc_tile_status_dir)
    #
    # ref_tile_status_dir = (export_dir_path + '\\ref_tile_status')
    # os.mkdir(ref_tile_status_dir)
    #
    # th_tile_status_dir = (export_dir_path + '\\th_tile_status')
    # os.mkdir(th_tile_status_dir)
    #
    # pg_tile_status_dir = (export_dir_path + '\\pg_tile_status')
    # os.mkdir(pg_tile_status_dir)
    #
    # dp0_tile_status_dir = (export_dir_path + '\\dp0_tile_status')
    # os.mkdir(dp0_tile_status_dir)


    # # ----------------------------------------------------------------------
    #
    # fpc_tile_for_processing_dir = (fpc_tile_status_dir + '\\fpc_for_processing')
    # os.mkdir(fpc_tile_for_processing_dir)
    #
    # ref_tile_for_processing_dir = (ref_tile_status_dir + '\\ref_for_processing')
    # os.mkdir(ref_tile_for_processing_dir)
    #
    # th_tile_for_processing_dir = (th_tile_status_dir + '\\th_for_processing')
    # os.mkdir(th_tile_for_processing_dir)
    #
    # pg_tile_for_processing_dir = (pg_tile_status_dir + '\\pg_for_processing')
    # os.mkdir(pg_tile_for_processing_dir)
    #
    # dp0_tile_for_processing_dir = (dp0_tile_status_dir + '\\dp0_for_processing')
    # os.mkdir(dp0_tile_for_processing_dir)
    #
    # # -----------------------------------------------------------------------
    #
    # fpc_insuf_files_dir = (fpc_tile_status_dir + '\\fpc_insufficient_files')
    # os.mkdir(fpc_insuf_files_dir)
    #
    # ref_insuf_files_dir = (ref_tile_status_dir + '\\ref_insufficient_files')
    # os.mkdir(ref_insuf_files_dir)
    #
    # th_insuf_files_dir = (th_tile_status_dir + '\\th_insufficient_files')
    # os.mkdir(th_insuf_files_dir)
    #
    # pg_insuf_files_dir = (pg_tile_status_dir + '\\pg_insufficient_files')
    # os.mkdir(pg_insuf_files_dir)
    #
    # dp0_insuf_files_dir = (dp0_tile_status_dir + '\\dp0_insufficient_files')
    # os.mkdir(dp0_insuf_files_dir)
    #
    # # ------------------------------------------------------------------------
    #
    # fpc_stat_list_dir = fpc_tile_status_dir + '\\fpc_tile_status_lists'
    # os.mkdir(fpc_stat_list_dir)
    #
    # ref_stat_list_dir = ref_tile_status_dir + '\\ref_tile_status_lists'
    # os.mkdir(ref_stat_list_dir)
    #
    # th_stat_list_dir = th_tile_status_dir + '\\th_tile_status_lists'
    # os.mkdir(th_stat_list_dir)
    #
    # pg_stat_list_dir = pg_tile_status_dir + '\\pg_tile_status_lists'
    # os.mkdir(pg_stat_list_dir)
    #
    # dp0_stat_list_dir = dp0_tile_status_dir + '\\dp0_tile_status_lists'
    # os.mkdir(dp0_stat_list_dir)
    #
    # # -------------------------------------------------------------------------
    #
    # plot_dir = export_dir_path + '\\plots'
    # os.mkdir(plot_dir)
    #
    # interactive_outputs = plot_dir + '\\interactive'
    # os.mkdir(interactive_outputs)
    #
    # final_plot_outputs = export_dir_path + '\\final_plots'
    # os.mkdir(final_plot_outputs)
    #
    # final_interactive_outputs = export_dir_path + '\\final_interactive'
    # os.mkdir(final_interactive_outputs)
    #
    # fpc_zonal_stats_output_dir = (export_dir_path + '\\fpc_zonal_stats')
    # os.mkdir(fpc_zonal_stats_output_dir)
    #
    # ref_zonal_stats_output_dir = (export_dir_path + '\\ref_zonal_stats')
    # os.mkdir(ref_zonal_stats_output_dir)
    #
    # th_zonal_stats_output_dir = (export_dir_path + '\\th_zonal_stats')
    # os.mkdir(th_zonal_stats_output_dir)
    #
    # pg_zonal_stats_output_dir = (export_dir_path + '\\pg_zonal_stats')
    # os.mkdir(pg_zonal_stats_output_dir)
    #
    # h99a2_zonal_stats_output_dir = (export_dir_path + '\\h99a2_zonal_stats')
    # os.mkdir(h99a2_zonal_stats_output_dir)
    #
    # fpca2_zonal_stats_output_dir = (export_dir_path + '\\fpca2_zonal_stats')
    # os.mkdir(fpca2_zonal_stats_output_dir)
    #
    # dp0_zonal_stats_output_dir = (export_dir_path + '\\dp0_zonal_stats')
    # os.mkdir(dp0_zonal_stats_output_dir)
    #
    # dbi_zonal_stats_output_dir = (export_dir_path + '\\dbi_zonal_stats')
    # os.mkdir(dbi_zonal_stats_output_dir)
    #
    # dim_zonal_stats_output_dir = (export_dir_path + '\\dim_zonal_stats')
    # os.mkdir(dim_zonal_stats_output_dir)
    #
    # dis_zonal_stats_output_dir = (export_dir_path + '\\dis_zonal_stats')
    # os.mkdir(dis_zonal_stats_output_dir)
    #
    # dja_zonal_stats_output_dir = (export_dir_path + '\\dja_zonal_stats')
    # os.mkdir(dja_zonal_stats_output_dir)
    #
    # dka_zonal_stats_output_dir = (export_dir_path + '\\dka_zonal_stats')
    # os.mkdir(dka_zonal_stats_output_dir)
    #
    # stc_zonal_stats_output_dir = (export_dir_path + '\\stc_zonal_stats')
    # os.mkdir(stc_zonal_stats_output_dir)
    #
    # return fpc_tile_status_dir, ref_tile_status_dir


def main_routine():
    """" Description: This script determines which Landsat tile had the most non null zonal statistics records per site
    and files those plots (bare ground, all bands and interactive) into final output folders. """

    # print('fcZonalStatsPipeline.py INITIATED.')
    # read in the command arguments
    cmd_args = get_cmd_args_fn()
    data = cmd_args.data
    # tile_grid = cmd_args.tile_grid
    export_dir = cmd_args.export_dir
    qld_grid_dir = cmd_args.qld_dir
    no_data = int(cmd_args.no_data)
    # rainfall_dir = cmd_args.rainfall_dir
    image_count = int(cmd_args.image_count)
    # image_search_criteria1 = cmd_args.search_criteria1
    # image_search_criteria2 = cmd_args.search_criteria2
    # end_file_name = cmd_args.search_criteria3
    # rolling_mean = cmd_args.rolling_mean
    # end_date = cmd_args.end_date
    # path = cmd_args.path
    # row = cmd_args.row


    #qld_grid_dir = r"Z:\Scratch\Rob\test_grid_data"

    # dictionary {varable: [unit, variable, scale, null_data, add_offset]
    qld_dict = {"rh_tmax": ["%", "rh_tmax", 0.1, -32767.0, 3276.5],
                "rh_tmin": ["%", "rh_tmin", 0.1, -32767.0, 3276.5],
                "daily_rain": ["mm", "rain_d", 0.1, -32767.0, 3276.5],
                "et_morton_actual": ["mm", "et_ma", 0.1, -32767.0, 0.0],
                "et_morton_potential": ["mm", "et_mp", 0.1, -32767.0, 0.0],
                "et_morton_wet": ["mm", "et_mw", 0.1, -32767.0, 0.0],
                "et_short_crop": ["mm", "et_sc", 0.1, -32767.0, 0.0],
                "et_tall_crop": ["mm", "et_tc", 0.1, -32767.0, 0.0],
                "evap_morton_lake": ["mm", "evp_ml", 0.1, -32767.0, 0.0],
                "evap_pan": ["mm", "evp_s", 0.1, -32767.0, 0.0],
                "evap_syn": ["mm", "evp_s", 0.1, -32767.0, 0.0],
                "max_temp": ["C", "tmax", 0.1, -32767.0, 0.0],
                "min_temp": ["C", "tmin", 0.1, -32767.0, 0.0],
                "monthly_rain": ["mm", "rain_m", 0.1, -32767.0, 3276.5],
                "mslp": ["hPa", "mslp", 0.1, -32767.0, 0.0],
                "radiation": ["MJ/m2", "rad", 0.1, -32767.0, 0.0],
                "vp": ["hPa", "vp", 0.1, -32767.0, 0.0],
                "vp_deficit": ["hPa", "vp_d", 0.1, -32767.0, 0.0]}

    # call the temporaryDir function.
    temp_dir_path, final_user = temporary_dir_fn()
    # call the tempDirFolders function.
    prime_temp_grid_dir, prime_temp_buffer_dir = temp_dir_folders_fn(temp_dir_path)
    # call the exportFilepath function.
    export_dir_path = export_file_path_fn(export_dir, final_user)

    # # create a list of variable sub directories
    sub_dir_list = next(os.walk(qld_grid_dir))[1]
    #
    # call the exportDirFolders function.
    # fpc_tile_status_dir, ref_tile_status_dir = export_dir_folders_fn(export_dir_path, sub_dir_list)
    export_dir_folders_fn(export_dir_path, sub_dir_list)

    prop_of_interest = "None"

    print(sub_dir_list)
    # select_sub_list = [sub_dir_list[9], sub_dir_list[10],
    #                    sub_dir_list[14], sub_dir_list[15]] #[sub_dir_list[0], sub_dir_list[1],
    #
    select_sub_list = sub_dir_list
    print(select_sub_list)

    sub_dir_list_csv = []
    #
    for i in select_sub_list:
        print(i)
        type_dir = os.path.join(qld_grid_dir, i)

        import step1_2_list_of_qld_grid_images
        export_csv = step1_2_list_of_qld_grid_images.main_routine(
            export_dir_path, type_dir, ".tif", str(i), sub_dir_list, qld_grid_dir)

        sub_dir_list_csv.append(export_csv)


    print(data)
    import step1_3_project_buffer
    geo_df2, crs_name = step1_3_project_buffer.main_routine(data, export_dir_path, prime_temp_buffer_dir)

    # import step1_4_landsat_tile_grid_identify2
    # comp_geo_df, zonal_stats_ready_dir = step1_4_landsat_tile_grid_identify2.main_routine(
    #     tile_grid, geo_df2, data, zone, export_dir_path, prime_temp_grid_dir)

    geo_df2.reset_index(drop=True, inplace=True)
    geo_df2['uid'] = geo_df2.index + 1
    #geo_df2.to_file(os.path.join(export_dir_path, "biomass_1ha.shp"))

    shapefile_path = os.path.join(export_dir_path, "biomass_1ha_all_sites.shp")
    geo_df2.to_file(os.path.join(shapefile_path),
                    driver="ESRI Shapefile")

    print("Exported shapefile: ", shapefile_path)


    print("step1 8 QLD grid")
    for i, csv_file in zip(select_sub_list, sub_dir_list_csv):
        print("*"*50)
        print("i: ", i)
        print("csv file: ", csv_file)
        import step1_8_qld_grid_zonal_stats
        step1_8_qld_grid_zonal_stats.main_routine(
            export_dir_path, i, csv_file, temp_dir_path, qld_dict, geo_df2, qld_dict)

    # ---------------------------------------------------- Clean up ----------------------------------------------------

    shutil.rmtree(temp_dir_path)
    print('Temporary directory and its contents has been deleted from your working drive.')
    print(' - ', temp_dir_path)
    print('fractional cover zonal stats pipeline is complete.')
    print('goodbye.')


if __name__ == '__main__':
    main_routine()
