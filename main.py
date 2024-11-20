# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import csv
import numpy as np
import pandas as pd
import urllib.error as url_error

from datetime import datetime

import urllib.request as request
from URL_ROOTS import PDS_RSS_ROOT, NAIF_ROOT, df_column_keys, ancillary_data_shorts


class CustomException(Exception):
    def __init__(self, msg):
        self.msg = msg
        print('custom exception occurred')


def load_tour_csv(tour_csv="titan_passes_operational.csv"):
    # load manually collected flyby schedule and naming data
    df = pd.read_csv(tour_csv, sep=", ")
    df = df.set_index("Flyby ID")

    # iterate over entries and adjust PDS Volume ID format to list with potentially multiple entries
    for i, value in df["PDS Volume ID"].items():
        df.at[i, "PDS Volume ID"] = value.split(' ')

    return df


def read_index_file(flyby_id, df):
    # assemble url to request
    experiment_id = df.at[flyby_id, "Experiment ID"]
    pds_volume_ids = df.at[flyby_id, "PDS Volume ID"]

    index_urls = []
    backup_urls = []
    for volume_id in pds_volume_ids:
        index_urls.append(PDS_RSS_ROOT.format(experiment_id) + f"/{volume_id}/INDEX/INDEX.TAB")
        backup_urls.append(PDS_RSS_ROOT.format(experiment_id) + f"/{volume_id}/index/index.tab")

    index_dict = {}
    for i, index_url in enumerate(index_urls):
        try:
            data = request.urlopen(index_url).read(20000)
            print(index_url)
            content = data.decode("utf-8")
            #data.raise_for_status()

        except url_error.HTTPError as err:
            if err.code == 404:
                print(f"404 Error on experiment_id {experiment_id} - trying alternative syntax...")
            else:
                raise (err)
            try:
                data = request.urlopen(backup_urls[i]).read(20000)
                print(index_url)
                content = data.decode("utf-8")
            except url_error.HTTPError as err:
                raise (err)
            else:
                print("...success!")

        index_dict[pds_volume_ids[i]] = content

    return index_dict


def create_entries_from_index_lines(lines, filter_on_extension=None):
    df_rows = []

    if filter_on_extension is None:
        filter_on_extension = ['.']  # mode in which no real filtering is going on
    else:
        assert type(filter_on_extension) == list, (f"Please provide extensions to be filtered as type list,"
                                                   f" now given as type {type(filter_on_extension)}")

    if len(lines) > 0:

        for line in lines:

            if any(ext in line.lower() for ext in filter_on_extension):
                # massage line format
                line = line.replace('"', ' ').replace(' ', '').split(',')

                # adopt set name into df entry
                df_row = [line[0].lower()]

                # construct data url for df entry
                data_url_root = line[1].rsplit('/', 1)[0]
                data_type = data_url_root.rsplit('/', 1)[-1]
                data_url = f'{data_url_root}/{line[2]}'.lower()
                df_row.append(data_type)
                df_row.append(data_url)

                # add timestamps to df entry
                df_row.append(line[-3])  # start [YYYY-DOY"T"hh:mm:ss.0]
                df_row.append(line[-2])  # end   [YYYY-DOY"T"hh:mm:ss.0]

                df_rows.append(df_row)

        return df_rows

    else:
        return df_rows


def check_df_content(df, key="Type"):
    odf = 0
    anc = 0

    if "odf" in list(df[key]):
        odf = 1

    if any(ext in list(df[key]) for ext in ["ion", "tro", "spk", "ckf", "eop"]):
        anc = 1

    return [odf, anc]


def add_to_df_from_index(index, df, filter_for_ancillaries=None, primary_odf_shorts=None):
    if filter_for_ancillaries is None:
        filter_for_ancillaries = ancillary_data_shorts

    if primary_odf_shorts is None:
        primary_odf_shorts = ["tigm", "tigf"]

    bools = check_df_content(df)

    if not bools[0]:  # odf missing

        # initial screening for primary odf contents
        odf_lines = []
        for line in index.splitlines():

            if (".odf" in line.lower() and primary_odf_shorts[0] in line.lower()
                    or ".odf" in line.lower() and primary_odf_shorts[1] in line.lower()):
                odf_lines.append(line)
            else:
                pass

        odf_df_rows = create_entries_from_index_lines(odf_lines)
        if len(odf_df_rows) > 0:
            new_odf_dat = pd.DataFrame(data=odf_df_rows, columns=df_column_keys)
            df = pd.concat([df, new_odf_dat], ignore_index=True)
            print("Added ODF")
            bools[0] == 1

    if not bools[1]:  # anc missing

        anc_df_rows = create_entries_from_index_lines(index.splitlines(), filter_for_ancillaries)
        if len(anc_df_rows) > 0:
            new_anc_dat = pd.DataFrame(data=anc_df_rows, columns=df_column_keys)
            df = pd.concat([df, new_anc_dat], ignore_index=True)
            print("Added ANC")
            bools[1] == 1

    return df


def interval_testing_w_datetime(start_0, end_0, ancillary_dates_list):
    # index 1, 2 are the bracketing intervals,
    # index 0 needs to be accomodated in brackets

    if len(ancillary_dates_list) == 0:
        raise ValueError(f"No ancillary time bracket given to test accommodation of odf coverage interval")

    elif len(ancillary_dates_list) == 2:
        return ancillary_dates_list

    elif len(ancillary_dates_list) == 4:

        start_1 = ancillary_dates_list[0]
        end_1 = ancillary_dates_list[1]
        start_2 = ancillary_dates_list[2]
        end_2 = ancillary_dates_list[3]

        if start_0 > start_1 and end_0 < end_1:
            return ancillary_dates_list[:2]

        elif start_0 > start_2 and end_0 < end_2:
            return ancillary_dates_list[2:]

        elif start_0 > start_1 and end_0 < end_2:
            return ancillary_dates_list

        else:
            raise ValueError(f"ODF coverage (dates {start_0} - {end_0} "
                             f"cannot be accomodated in available ancillary coverage brackets")

    else:
        raise ValueError(f"Ancillary time bracket given to test accommodation of odf coverage interval "
                         f"has invalid number of entries ({len(ancillary_dates_list)}) - "
                         f"permissible number of entries (2) and (4) (corresponding to start+end for max 2 intervals)")


def safe_dt_conversion(dt_str):

    try:
        dt = datetime.strptime(dt_str, '%Y-%jT%H:%M:%S')
    except ValueError:
        dt = datetime.strptime(dt_str, '%Y_%jT%H:%M:%S')

    return dt


def tailor_ancillary_contents(df):
    # for now assuming that per ancillary type we have maximum 2 files

    # get datetimes of odf coverage
    odf_data_field_row = df[df['Type'].str.lower() == 'odf']

    # massage datetime string for compatibility
    odf_start_str = odf_data_field_row["Start Date"].iloc[0].split('.')[0]
    odf_end_str = odf_data_field_row["End Date"].iloc[0].split('.')[0]

    odf_start_datetime = safe_dt_conversion(odf_start_str)
    odf_end_datetime = safe_dt_conversion(odf_end_str)


    for ancillary_short in ancillary_data_shorts:

        ancillary_dates_list = []
        ancillary_type_subdf = df[df['Type'].str.lower() == ancillary_short]

        # iterate over rows of given ancillary type
        for i, row in ancillary_type_subdf.iterrows():
            # massage datetime string for compatibility
            anc_start_str = row["Start Date"].split('.')[0]
            anc_end_str = row["End Date"].split('.')[0]

            print(ancillary_type_subdf)
            print(ancillary_short)
            print(anc_start_str)
            anc_start_datetime = safe_dt_conversion(anc_start_str)
            anc_end_datetime = safe_dt_conversion(anc_end_str)

            ancillary_dates_list.append(anc_start_datetime)
            ancillary_dates_list.append(anc_end_datetime)

        ancillary_bracket = interval_testing_w_datetime(odf_start_datetime, odf_end_datetime, ancillary_dates_list)

        if ancillary_bracket == ancillary_dates_list:
            pass
        elif ancillary_bracket == ancillary_dates_list[:2]:
            df.drop(ancillary_type_subdf.index[1], axis='index', inplace=True)
        elif ancillary_bracket == ancillary_dates_list[2:]:
            df.drop(ancillary_type_subdf.index[0], axis='index', inplace=True)

    return df


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    # 1. load passes csv into a df
    tour_df = load_tour_csv()  # Press ⌘F8 to toggle the breakpoint.

    #flyby_ids = tour_df.index[[0, 5]]
    flyby_ids = ["T011", "T074"]
    print(f"User defined list of flybys considered: {flyby_ids}")
    experiment_file_index = dict()


    # 2. iterate over the flybys and find index, get main gravity science pass, ancillary and start/end times
    for flyby_id in flyby_ids:
        print(flyby_id)
        index_df = pd.DataFrame(data=[], columns=df_column_keys)
        index_dict = read_index_file(flyby_id, tour_df)

        for volume_id in list(index_dict):
            print(f"Searching index of repo {volume_id}...")
            index_df = add_to_df_from_index(index_dict[volume_id], index_df)

        index_df = tailor_ancillary_contents(index_df)

        print(index_df["Type"])
        experiment_file_index[flyby_id] = index_df


    ######## experiment_file_index ######
    # dict of keys 'flyby_id'
    # mapping to pd DataFrame objects with keys ['Volume', 'Type', 'URL DATA', 'Start Date', 'End Date']
    # where each item (by rows) is a data file (odf and ancillary) that is required in the context of main GS data around given flyby

    ######## Next up: Conceive mirroring data structure on local.

    print("yeah dawg")


    # 3. iterate over relevant file types (needs definition in txt) and download them over relevant timeframe
    # @LUIGI

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
