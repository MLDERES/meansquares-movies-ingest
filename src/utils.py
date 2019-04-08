import datetime as dt
import logging
from pathlib import Path
import pandas as pd
from IPython.core.display import display
from constants import  *
import xlsxwriter, xlrd
from openpyxl import load_workbook, Workbook, worksheet


TODAY = dt.datetime.today()
NOW = dt.datetime.now()
_DEBUG = False


def start_logging(debug_to_console=False, support_for_mp=True):
    logging.basicConfig(level=logging.DEBUG,
                        datefmt='%m-%d %H:%M',
                        filename=Path(ROOT / 'meansquares-movies.log'),
                        filemode='a', format='%(asctime)s: %(levelname)-8s %(message)s')
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    console.setLevel(logging.INFO if not debug_to_console else logging.DEBUG)
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)


def write_excel(data, filename='combined', data_version=False, folder='', with_ts=True, **kwargs):
    """
    Write multiple data items to a single Excel file.  Where the data is a dictionary of
    datasources and dataframes
    :param data: dictionary of sheet names and dataframes
    :param filename: the name of the excel file to save
    :param folder: folder to store the excel file
    :param with_ts: if true, add a timestamp to the filename
    :param kwargs: other arguments to be passed to the pandas to_excel function
    :return: the filename of the excel file that was written
    """
    logger = logging.getLogger(__name__)
    logger.info(f"writing {len(data)} to excel... {folder}")
    fn = make_ts_filename(DATA_PATH / folder, filename, suffix='.xlsx', with_ts=with_ts)

    if 'float_format' not in kwargs.keys():
        kwargs['float_format'] = '%.3f'
    if type(data_version) is bool:
        data_version = f'_{TODAY.month:02d}{TODAY.day:02d}' if data_version else ''

    with pd.ExcelWriter(fn) as writer:
        for datasource, df in data.items():
            if type(df) is not pd.DataFrame:
                continue
            df.to_excel(writer, sheet_name=f'{datasource}{data_version}', **kwargs)
    logger.info(f"finished writing df to file... {filename}")
    return filename


def make_ts_filename(dir_name, src_name, suffix, with_ts=True):
    """
    Get a path with the filename specified by src_name with or without a timestamp, in the appropriate directory
    :param dir_name:
    :param src_name:
    :param suffix:
    :param with_ts:
    :return:
    """
    NOW = dt.datetime.now()
    filename_suffix = f'{TODAY.month:02d}{TODAY.day:02d}_{NOW.hour:02d}{NOW.minute:02}{NOW.second:02d}' \
        if with_ts else "latest"
    fn = f'{src_name}_{filename_suffix}'
    suffix = suffix if '.' in suffix else f'.{suffix}'
    filename = (dir_name / fn).with_suffix(suffix)
    return filename


# TODO: Work out Enums for datasource and data state
#  So an example would be a file that is cleaned and combines two sources would have an output name
#   of source1_source2_clean or src1_src2_features
def write_data(df, datasource_name, folder='interim', with_ts=True, **kwargs):
    """
    Export the dataset to a file
    :param df: the dataset to write
    :param datasource_name: the basefilename to write
    :param folder: the data subpath (one of 'interim', 'processed', 'external'
    :param with_ts: if True, then append the year, month, day and hour to the filename to be written
                    else append the suffix 'latest' to the basename
    :param idx: the name of the index or the column number
    :return: the name of the file written
    """
    NOW = dt.datetime.now()
    logger = logging.getLogger(__name__)
    logger.info(f"writing df to file... {datasource_name} {folder}")
    fn = make_ts_filename(DATA_PATH / folder, src_name=datasource_name, suffix='.csv')

    if 'float_format' not in kwargs.keys():
        kwargs['float_format'] = '%.3f'
    df.to_csv(fn, **kwargs)
    logger.info(f"finished writing df to file... {fn}")
    return fn


def read_latest(datasource_name, folder='interim', **kwargs):
    """
    Get the most recent version of a file with the neam
    :param datasource_name: name of the file to get the data from (one of KAGGLE, IMDB, TNUMBERS, K_AND_IMDB, COMBINED)
    :param folder: the subpath to the data, likely interim or processed
    :return:
    """

    read_path = DATA_PATH / folder
    fname = get_latest_data_filename(datasource_name, folder)
    logging.info(f"read from {fname}")
    return pd.read_csv(read_path / fname, index_col=0, infer_datetime_format=True, true_values=TRUE_VALUES,
                       false_values=FALSE_VALUES, **kwargs)


def read_latest_from_worksheet(filename, datasource_name='all', folder='interim', **kwargs):
    """
    Get the most recent version of the cleaned dataset
    :param datasource_name: name of the worksheet to get the data from (one of KAGGLE, IMDB, TNUMBERS, K_AND_IMDB,
            COMBINED, etc) or 'all'.  If 'all' then returns a dictionary of datasets keyed from datasource_name
    :param folder: the subpath to the data, likely interim or processed
    :param filename:
    :return:
    """
    assert folder in ['interim', 'processed', 'external', 'raw', 'production'], \
        f"Invalid folder to read '{folder}'"
    logger = logging.getLogger(__name__)
    read_path = DATA_PATH / folder
    fname = get_latest_data_filename(filename, folder, file_ext='.xlsx')
    logger.info(f"read {datasource_name} from {fname}")
    if datasource_name == 'all':
        ret_val = pd.read_excel(read_path / fname, sheet_name=None, index_col=0, infer_datetime_format=True, **kwargs)
    else:
        # TODO: Get a single sheet with the closest datasource name or a list of sheets
        ret_val = pd.read_excel(read_path / fname, sheet_name=datasource_name, index_col=0,
                                infer_datetime_format=True, **kwargs)
    return ret_val


def get_latest_data_filename(datasource_name, folder, file_ext='.csv'):
    """
    Determine the filename of the latest version of this file source
    :param folder:
    :param datasource_name:
    :return:
    """
    return get_latest_file(DATA_PATH / folder, datasource_name, file_ext)

def get_latest_file(file_path, filename_like, file_ext):
    """
    Find absolute path to the file with the latest timestamp given the datasource name and file extension in the path
    :param path: where to look for the file
    :param datasource: stem name of the file
    :param file_ext:
    :return:
    """
    file_ext = file_ext if '.' in file_ext else f'.{file_ext}'
    all_files = [f for f in file_path.glob(f'{filename_like}*{file_ext}',)]
    assert len(all_files) > 0, f'Unable to find any files like {file_path / filename_like}{file_ext}'
    fname = max(all_files, key=lambda x: x.stat().st_mtime).name
    return fname


def get_latest_dataset_label(datasource_name, folder):
    fn = get_latest_data_filename(datasource_name, folder)
    return fn.rsplit('_', 1)[0]

def get_file_version_from_name(fn):
    return fn.split('_')[1]

def display_all(df):
    with pd.option_context("display.max_rows", 1000, "display.max_columns", 1000):
        display(df)




if __name__ == "__main__":
    d = {'a':[1,2,3,4,5]}
    f = write_model(d,'dictionary')
    d2 = read_latest_model('dictionary')
    print(d2)