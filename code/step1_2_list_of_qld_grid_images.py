#!/usr/bin/env python

"""
step1_2_list_of_rainfall_images.py
=======================
Description: This script creates a csv containing the paths for all QLR rainfall raster images that meet the specified
search criteria, and create the two variables: rain_start_date, rain_finish_date which contain the date for the first
and last available images.


Author: Rob McGregor
email: robert.mcgreogr@nt.gov.au
Date: 27/11/2020
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

"""

# import modules
import os
import csv
import warnings

warnings.filterwarnings("ignore")


def list_dir_fn(rainfall_dir, end_file_name):
    """ Return a list of the rainfall raster images in a directory for the given file extension.

    @param rainfall_dir: string object containing the path to the directory containing the rainfall tif files
    (command argument --rainfall_dir).
    @param end_file_name: string object containing the ends with search criteria (command argument --search_criteria3).
    @return list image: list object containing the path to all rainfall images within the rainfall directory that meet
    the search criteria.
    """
    list_image = []

    for root, dirs, files in os.walk(rainfall_dir):

        for file in files:
            if file.endswith(end_file_name):
                img = (os.path.join(root, file))
                list_image.append(img)

    return list_image


def output_csv_fn(list_image, temp_dir_path, variable):
    """ Return a csv containing each file paths stored in the list_image variable (1 path per line).

    @param list_image: list object containing the path to all rainfall images within the rainfall directory that meet
    the search criteria - created under the list_dir_fn function.
    @param export_dir_path: string object containing the path to the export directory.
    @return export_rainfall: string object containing the path to the populated csv.
    """

    # assumes that file_list is a flat list, it adds a new path in a new row, producing multiple observations.
    export_var = os.path.join(temp_dir_path, '{0}_image_list.csv'.format(variable))

    with open(export_var, "w") as output:
        writer = csv.writer(output, lineterminator='\n')
        for file in list_image:
            writer.writerow([file])

    return export_var


def main_routine(variable_dir, o, d, end_file_name, temp_dir_path):
    # call the list_dir_fn function to return a list of the rainfall raster images.
    list_image = list_dir_fn(variable_dir, end_file_name)
    print("list_image", list_image)

    export_csv = output_csv_fn(list_image, temp_dir_path, d)

    return export_csv


if __name__ == "__main__":
    main_routine()
