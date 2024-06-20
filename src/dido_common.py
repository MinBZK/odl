"""
did0_common.py is a library with common routines for DiDi.
"""

import os
import sys
import yaml
import psutil
import logging
import argparse
import sqlalchemy

import pandas as pd

from datetime import datetime
from dotenv import load_dotenv
from os.path import join, splitext, dirname, basename, exists
# from common import create_log, change_column_name, change_column_name, split_filename

import simple_table as st

logger = logging.getLogger()

# define constants
# set defaults
# SCHEMA_TEMPLATE = 'bronbestand_attribuutmeta_description'
# META_TEMPLATE   = 'bronbestand_bestandmeta_description'
# EXTRA_TEMPLATE  = 'bronbestand_attribuutextra_description'

# TAG_TABLE refer to TABLES indices in the confif.yaml file
TAG_TABLE_SCHEMA   = 'schema'
TAG_TABLE_META     = 'meta'
TAG_TABLE_EXTRA    = 'extra'
TAG_TABLE_DELIVERY = 'levering'
TAG_TABLE_QUALITY  = 'datakwaliteit'

# Tags refer to dictionary indices of each table
TAG_TABLES = 'tables'
TAG_PREFIX = 'prefix'
TAG_SUFFIX = 'suffix'
TAG_SCHEMA = 'schema'
TAG_DATA   = 'data'

# Required ODL columns to add to schema
ODL_RECORDNO         = 'bronbestand_recordnummer'
ODL_CODE_BRONBESTAND = 'code_bronbestand'
ODL_LEVERING_FREK    = 'levering_rapportageperiode'
ODL_DELIVERY_DATE    = 'levering_leveringsdatum'
ODL_DATUM_BEGIN      = 'record_datum_begin'
ODL_DATUM_EINDE      = 'record_datum_einde'
ODL_SYSDATUM         = 'sysdatum'

# special column names that require action
COL_CREATED_BY = 'created_by'

# 1,2,3,6,8 controls in code
# betekenis van datakwaliteitcodes
VALUE_OK = 0 # "Valide waarde"
VALUE_NOT_IN_LIST = 1  # "Domein - Waarde niet in lijst"
VALUE_MANDATORY_NOT_SPECIFIED = 2 #"Constraint - Geen waarde, wel verplicht"
VALUE_NOT_BETWEEN_MINMAX = 3 #"Domein - Waarde ligt niet tussen minimum en maximum"
VALUE_OUT_OF_REACH = 4  #"Waarde buiten bereik"
VALUE_IMPROBABLE = 5 #"Waarde niet waarschijnlijk"
VALUE_WRONG_DATATYPE = 6 #"Datatype - Waarde geen juist datatype"
VALUE_HAS_WRONG_FORMAT = 7 #"Waarde geen juist formaat"
VALUE_NOT_CONFORM_RE = 8 #"Domein - Waarde voldoet niet aan reguliere expressie"

# Allowed column names
ALLOWED_COLUMN_NAMES = ['kolomnaam', 'datatype', 'leverancier_kolomnaam',
                   'leverancier_kolomtype', 'gebruiker_info', 'beschrijving']
# Directory constants
DIR_SCHEMAS = 'schemas'
DIR_DOCS    = 'docs'
DIR_DONE    = 'done'
DIR_TODO    = 'todo'
DIR_SQL     = 'sql'

VAL_DIDO_GEN = '(dido generated)'

# Version labels
ODL_VERSION  = 'odl_version'
ODL_VERSION_DATE  = 'odl_version_date'
DIDO_VERSION = 'dido_version'
DIDO_VERSION_DATE = 'dido_version_date'

# Miscellaneous
DATE_FORMAT = '%Y-%m-%d'
TIME_FORMAT = '%H:%M:%S'
DATETIME_FORMAT = f'{DATE_FORMAT} {TIME_FORMAT}'


class DiDoError(Exception):
    """ To be raised for DiDo exceptions

    Args:
        Exception (str): Exception to be raised
    """
    def __init__(self, message):
        self.message = 'DiDo Fatal Error: ' + message
        logger.critical(self.message)
        super().__init__(self.message)

    ### __init__ ###

### Class: DiDoError ###


"""
Module contains common functions for project
"""
import os
import re
import random
import logging

import pandas as pd
from datetime import datetime
from logging.config import dictConfig # required to import logging


def create_log(filename: str, level: int = logging.INFO, reset: bool = True) -> object:
    """ Returns a log configuration

    The log configuration returned displays a normal message at the console
    and an extended version (including level, data, etc) to the log file.

    Get a logger as follows:

    logger = common.create_log(log_file, level = loggin.DEBUG)
    logger.info(f'log_file is {log_file}') # writes a message at level info
    logger.error('System crash, quitting') # writes message at level error

    Args:
        filename -- which need to be logged/monitored
        level -- treshold level only log messages above level

    Returns:
        logger object
    """
    if reset:
        file_mode = 'w'
    else:
        file_mode = 'a'

    logging_configuration = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': '%(asctime)s [%(levelname)s] %(module)s: %(message)s'
            },
            'brief': {
                'format': '%(message)s'
            },
        },
        'handlers': {
            'console': {
                'level': logging.INFO,
                'formatter': 'brief',
                'class': 'logging.StreamHandler',
                'stream': 'ext://sys.stdout',  # default stderr
            },
            'file': {
                'level': logging.DEBUG,
                'formatter': 'standard',
                'class': 'logging.FileHandler',
                'filename': filename,
                'mode': file_mode,
            },
        },
        'loggers': {
            '': {
                'level': level,
                'handlers': ['console', 'file']
            },
        },
    }

    log = logging.getLogger()
    log.setLevel(logging.DEBUG)

    try:
        dictConfig(logging_configuration)
    except:
        print('*** Error while initializing logger.')
        print(f'*** Either the path to the log file does not exist: {filename}')
        print('*** or the "logs" directory does not exist in that path')
        raise DiDoError('*** Aboring execution')

    # try..except

    return log

### create_log ###


def iso_cet_date(datum: datetime, tz: str = ''):
    result = datum.strftime(f"%Y-%m-%d %H:%M:%S {tz}")

    return result

### iso_cet_date ###


def split_filename(filename: str) -> tuple:
    """ Splits filename into dirpath, filename and .extension

    Args:
        filename (str): filename to split

    Returns:
        directory, filename without extension, .extension (including period)
    """
    filepath_without_extension, extension = os.path.splitext(filename)
    dirpath = os.path.dirname(filepath_without_extension)
    filebase = os.path.basename(filepath_without_extension)

    return dirpath, filebase, extension


def change_column_name(col_name: str, empty: str = 'kolom_') -> str:
    """align to snake_case; only alfanum and underscores, multiple underscores reduced to one

    Args:
        col_name -- name to be changed
        empty -- prefix for random name

    Returns:
        adjusted columnname
    """
    # remove outer whitespace
    col_name = col_name.strip().lower()

    # non-alfanums to underscore, multiple underscore to one
    col_name = re.sub('[^0-9a-zA-Z_]+', '_', col_name)

    # remove _ at beginning and end
    col_name = col_name.strip('_')

    # return if column starts with letter
    for i, letter in enumerate(col_name):
        if letter.isalpha():
            return col_name[i:]

    # if no letter return random name
    return f'{empty}{random.randint(1000, 9999)}'


def get_headers_and_types(schema: pd.DataFrame) -> tuple:
    """ of provided df

    Args:
        schema(pd.DataFrame): to get headers and types from

    Returns:
        headers in list, datatypes in dict {header:datatype}
    """
    headers = []
    datatypes = {}

    for row in schema.itertuples():
        header = row.kolomnaam
        if header not in ['*', 'type']:
            dtype = row.pandastype
            headers.append(header)
            datatypes[header] = dtype

    return headers, datatypes

### get_headers_and_types ###


def read_schema_file(filename: str) -> pd.DataFrame:
    """ mutation schema file

    Args:
        filename -- csv schema file

    Returns:
        contents
    """
    schema = pd.read_csv(filename, sep = ';', quotechar = '"', keep_default_na = False, encoding = 'UTF-8')

    # Postgres surrounds text by "'"; use double single quotes to have them accepted
    schema['beschrijving'] = schema['beschrijving'].str.replace("'", "''")

    # Two spaces at line end ensures newline in markdown
    schema['beschrijving'] = schema['beschrijving'].str.replace("\n", "  \n")

    return schema

### read_schema_file ###


def report_ram(message: str):
    logger = logging.getLogger()
    mem = psutil.virtual_memory()

    logger.info('')
    logger.info(message)
    logger.info(f'   Total memory:    {mem.total:,}')
    logger.info(f'   Memory used:     {mem.used:,}')
    logger.info(f'   Memory available {mem.available:,}')

    return

### report_ram ###


def get_par(config: dict, key: str, default = None):
    """ Gets the value of a key from a dictionary or DataFrame index

    when config is a dictionary its key is fetched, when it is a dataframe
    the key is looked up from the index.

    In both cases applies that when a key is not found the default is returned.

    Args:
        config (dict): dictuionary or DataFrame to fetch the value from
        key (str): value to find in dictionary
        default (_type_, optional): default when value is not in dict. Defaults to None.
    """
    if isinstance(config, pd.DataFrame):
        # config is DataFrame, look for key in index
        if key in config.index:
            return str(config.loc[key])

        else:
            return default

    else:
        # dictionary, return config[key] when present
        if key in config:
            return config[key]

        else:
            return default

### get_par ###


def get_par_par(config: dict, key_1: str, key_2: str, default = None):
    """ Gets the value of a key from a dictionary from which the result is a dictionary

   Key_1 points to a dictionary inside the dictionary, key_2 points to a
   value inside that dictionary. When either is not found default is returned

    Args:
        config (dict): dictuionary or DataFrame to fetch the value from
        key_1 (str): value to find in config
        key_2 (str): value to find in config[key_1]
        default (_type_, optional): default when value is not in dict. Defaults to None.
    """

    # dictionary, return config[key] when present
    if key_1 in config:
        sub_config = config[key_1]

        if key_2 in sub_config:
            return sub_config[key_2]

        else:
            return default

    else:
        return default

### get_par ###


def read_cli():
    """ Read command line arguments

    Returns:
        str: path to the application
        object: argparse arguments
    """
    pad, fn, ext = split_filename(sys.argv[0])
    app_path = {'path': pad, 'name': fn, 'ext': ext}

    argParser = argparse.ArgumentParser()
    argParser.add_argument("-c", "--compare", help="Compare action: 'dump' or 'compare'",
                           choices=['compare', 'dump'],
                           default='compare', const='compare', nargs='?')
    argParser.add_argument("-d", "--delivery", help="Name of delivery file in root/data directory")
    argParser.add_argument("--date", help="Date to take snapshot of database ")
    argParser.add_argument("-p", "--project", help="Path to project directory")
    argParser.add_argument("-r", "--reset", help="Empties the logfile before writing it",
                           action='store_const', const='reset')
    argParser.add_argument("-s", "--supplier", help="Name of supplier")
    argParser.add_argument("-t", "--target", help="File name to read target data from")
    argParser.add_argument("-v", "--view", help="View database table in dido_compare",
                           action='store_const', const='view' )
    argParser.add_argument("--Yes",  help="Answer Yes to all questions",
                           action='store_const', const='Ja')

    args = argParser.parse_args()

    return  app_path, args

### read_cli ###


def load_parameters() -> dict:
    """ Laadt parameters uit config file

    Returns:
        dict: dictionary with parameters from file
    """
    with open('config/dido.yaml', encoding = 'utf8', mode = "r") as infile:
        parameters = yaml.safe_load(infile)

    return parameters

### load_parameters ###


def create_data_types():
    parameters = load_parameters()
    basis_types = parameters['BASIC_TYPES']
    sub_types = parameters['SUB_TYPES']
    data_types = {}

    # enumerate over all sub_types
    for key in sub_types.keys():
        type_list = sub_types[key]

        # enumerate over each sub_type in the argument list
        for sub_type in type_list:
            # Assign type definition from basic type to subtype
            data_types[sub_type] = basis_types[key]

        # for
    # for

    logger.debug(f'Data types:\n {data_types}')

    return data_types, sub_types

### create_data_types ###


def load_sql() -> str:
    """ read the SQL support functions if available

    Returns:
        str: SQL statements as a string
    """
    sql = ''
    with open('config/dido_functions.sql', encoding = 'utf8', mode = "r") as infile:
        sql = infile.read().strip()

    return sql

### load_sql ###


def load_pgpass(host: str, port: str, db: str, user: str = ''):
    """ Searches for a password in .pgpass

    Function implements algorithm as described in:
    https://www.postgresql.org/docs/current/libpq-pgpass.html
    An field in an entry may be * in which case no selection is made for that field.

    Args:
        host (str, optional): hostname or IP.
        port (str, optional): port.
        db (str, optional): database name.
        user (str, optional): user name . Defaults to ''.

    Returns:
        None: if .pgpass is not found in the user root
        (None, None): if no entries match the criteria
        (str, str): (user name, password) of the first matching entry

    Example:
        credentials = load_pgpass(host = '10.10.12.12', db = '*')
        if credentials is None:
            print('No .pgpass file found')

        else:
            (user, pw) = credentials
            if user is None or pw is None:
                print('No matching entry in .pgpass found')

            else: # use and pw contain selected user and password in .pgpass
                print(user) # never print passwords
    """

    # check if a .pgpass exists, if not: return None
    pgpass_filename = os.path.expanduser('~/.pgpass')
    if os.path.isfile(pgpass_filename):
        pgpass = pd.read_csv(
            pgpass_filename,
            sep = ':',
            dtype = str,
            keep_default_na = False,
            header = None,
            index_col = None,
            engine = 'python',
            encoding = 'utf8',
        )

    else:
        return None

    # if host is specified select for host
    pgpass_host = pgpass.iloc[:, 0]
    pgpass = pgpass.loc[(pgpass_host == host) | (pgpass_host == '*')]
    logger.debug(f'Host - host: {host}, port: {port}, database: {db}, user: {user}: {len(pgpass)}')

    # if port is specified select for port
    port = str(port) # force the port to be an integer
    pgpass_port = pgpass.iloc[:, 1]
    pgpass = pgpass.loc[(pgpass_port == port) | (pgpass_port == '*')]
    logger.debug(f'Port - host: {host}, port: {port} and database: {db} user: {user}: {len(pgpass)}')

    # if database is specified select for database
    pgpass_db = pgpass.iloc[:, 2]
    pgpass = pgpass.loc[(pgpass_db == db) | (pgpass_db == '*')]
    logger.debug(f'Db - host: {host}, port: {port} and database: {db} user: {user}: {len(pgpass)}')

    # if user is specified select for user
    if len(user) > 0:
        pgpass_user = pgpass.iloc[:, 3]
        pgpass = pgpass.loc[(pgpass_user == user) | (pgpass_user == '*')]
        logger.debug(f'User - host: {host}, port: {port} and database: {db} user: {user}: {len(pgpass)}')

    # when no match is found, return (None, None)
    if len(pgpass) < 1:
        logger.warning(f'No candidate left in .pgpass after applying host: {host}, '
                        f'port: {port}, db: {db} and user: {user}')
        return (None, None)

    logger.debug(f'{len(pgpass)} candidates left in .pgpass after applying host: {host}, '
                 f'port: {port}, db: {db} and user: {user}. First picked')

    return (pgpass.iloc[0, 3], pgpass.iloc[0, 4])

### load_pgpass ###


def read_config(project_dir: str) -> dict:
    """ Reads config and environment file and initializes variables.

    The config file is read from the <project_dir>/config directory, just like
    the .env file. The config file is read as a dictionary into the config variable.
    The servers declared in the SERVER_CONFIGS section are updated with the
    credentials of .env.

    Next, program wide parameters are read from the ODL database: odl_parameters
    and odl_rapportageperiodes. These are assigned to the config dictionary.

    Args:
        project_dir (str): directory path pointing to the root of the
            project directory. Subdirectories are at least: config, root and work.

    Returns:
        dict: the config dictionary enriched with additional information
    """
    # read the configfile
    configfile = os.path.join(project_dir, 'config', 'config.yaml')
    logger.info(f'[Bootstrap: {configfile}]')
    with open(configfile, encoding = 'utf8', mode = "r") as infile:
        config = yaml.safe_load(infile)

    sql = load_sql()

    item_names = ['ROOT_DIR', 'WORK_DIR', 'PROJECT_NAME', 'HOST',
                  'SERVER_CONFIGS']
    errors = False
    for item in item_names:
        if item not in config.keys():
            errors = True
            logger.critical(f'{item} not specified in config.yaml')

    if errors:
        raise DiDoError('Er ontbreken elementen in config.yaml, zie hierboven')

    config['ROOT_DIR'] = os.path.join(project_dir, config['ROOT_DIR'])
    config['WORK_DIR'] = os.path.join(project_dir, config['WORK_DIR'])
    config['SQL_SUPPORT'] = sql

    # load dido.yaml
    parameters = load_parameters()

    # get the server variable
    servers = config['SERVER_CONFIGS'].keys()

    # check if the server to use exists
    host = config['HOST'].lower().strip()
    if host not in parameters['SERVERS'].keys():
        for server in parameters.keys():
            logger.info(f' - {server}')

        raise DiDoError(f'*** Server to use in config file {host} is not among the allowed servers')

    # find IP belonging to host
    host_ip = parameters['SERVERS'][host]

    logger.debug(config['SERVER_CONFIGS'])

    # Credentials
    env = load_credentials(project_dir)
    if 'POSTGRES_USER' in env.keys():
        user = env['POSTGRES_USER']
    else:
        user = ''

    # assign env credentials and server to the server_configs in the config file
    for server in config['SERVER_CONFIGS']:
        config['SERVER_CONFIGS'][server]['POSTGRES_HOST'] = host_ip
        port = config['SERVER_CONFIGS'][server]['POSTGRES_PORT']
        db = config['SERVER_CONFIGS'][server]['POSTGRES_DB']

        # check for user name and pw in .pgpass resp. .env
        creds = load_pgpass(host = host_ip, port = port, db = db, user = user)

        # if .pgpass not found or entries in .pgpass not found
        if creds is None or creds == (None, None):
            try:
                config['SERVER_CONFIGS'][server]['POSTGRES_USER'] = user
                config['SERVER_CONFIGS'][server]['POSTGRES_PW'] = env['POSTGRES_PASSWORD']
            except:
                raise DiDoError('No valid credentials found in .pgpass or config/.env')

        # .pgpass found with correct entries
        else:
            config['SERVER_CONFIGS'][server]['POSTGRES_USER'] = creds[0]
            config['SERVER_CONFIGS'][server]['POSTGRES_PW'] = creds[1]

        # if
    # for

    # store other settings of .env in config
    for key in env.keys():
        if key not in ['POSTGRES_USER', 'POSTGRES_PASSWORD']:
            config[key] = env[key]

    # fetch rapportage leveringsperiodes from odl
    odl_server = config['SERVER_CONFIGS']['ODL_SERVER_CONFIG']
    #odl_server['POSTGRES_HOST'] = '10.10.12.6'

    # when initializing a new database odl_)rapportageperiodes does not exist, assign None
    try:
        rapportage_periodes = load_odl_table('odl_rapportageperiodes_description', odl_server)
        config['REPORT_PERIODS'] = rapportage_periodes

    except:
        logger.warning('!!! No odl_rapportageperiodes_description present. You are creating a new database?')

    # assign these tot the config file
    config['PARAMETERS'] = parameters

    return config

### read_config ###


def read_delivery_config(project_path: str, delivery_filename: str):

    delivery_filename = os.path.join(project_path, 'data', delivery_filename)
    with open(delivery_filename, encoding = 'utf8', mode = "r") as infile:
        delivery = yaml.safe_load(infile)

    return delivery

### read_delivery_config ###


def load_credentials(project_dir: str) -> dict:
    """ Reads the .env file as dict, returns empty dict when no file is found

    Args:
        project_dir (str): project directory of the expected config/.env file

    Returns:
        dict: the .env file as dict or an empty dict when config/.env is not found
    """
    env_filename = '.env' # config['ENV']
    env_filename = os.path.join(project_dir, os.path.join('config', env_filename))

    if os.path.isfile(env_filename):
        with open(env_filename, encoding = 'utf8', mode = "r") as envfile:
            env = yaml.safe_load(envfile)

        logger.info('Credentials read from config/.env')

    else:
        # initialize empty env dictionary
        logger.info('No credentials file found: config/.env')

        env = {}

    # if

    return env

### load_credentials ###


def display_dido_header(text: str = None):
    if text is None:
        return

    dido = ' DiDo - Document Data Definition Generator '
    if len(text) > len(dido):
        text = text[0:len(dido)]

    delta = len(dido) - len(text)
    delta_2 = int(delta / 2)
    front = delta_2
    back = delta - delta_2
    text = '*' + front * ' ' + text + back * ' ' + '*'
    dido = '*' + dido + '*'
    asterisks = len(dido) * '*'
    between = '*' + (len(dido) - 2) * ' ' + '*'

    logger.info('')
    logger.info(asterisks)
    logger.info(between)
    logger.info(dido)
    logger.info(between)
    logger.info(text)
    logger.info(between)
    logger.info(asterisks)
    logger.info('')

    return


def get_limits(config: dict):
    """ Read LIMITS

    Args:
        config (dict): configuration containing the limits

    Returns:
        (tuple): the limited variables
    """
    max_rows = None
    max_errors = None
    data_test_fraction = 0 # limitations['data_test_fraction']

    if 'LIMITS' in config:
        limitations = config['LIMITS']
        max_rows = limitations['max_rows']
        max_errors = limitations['max_errors']

        if max_rows < 1:
            max_rows = None

    return max_rows, data_test_fraction, max_errors

### get_limits ###


def load_odl_table(table_name: str, server_config: dict) -> pd.DataFrame:
    """  Load ODL from PostgreSQL

    Currently only used to load data_kwaliteit_codes. This is stored centrally
    in the techniek/odl database.

    Args:
        table_name (str): name of the postgres table, schema is predefined
        odl_server_config (dict): contains postgres access data of the odl server

    Returns:
        pd.DataFrame: Operationele Data Laag
    """
    result = st.sql_select(
        table_name = table_name,
        columns = '*',
        sql_server_config = server_config,
        verbose = False,
    ).fillna('')

    return result

### load_odl_table ###


def load_schema(table_name: str, server_config: dict) -> pd.DataFrame:
    """ Load a table from a schema and database specified in server_config

    Args:
        table_name (str): name of the table to load
        server_config (dict): dictionary containing postgres parameters

    Returns:
        pd.DataFrame: SQL table loaded from postgres
    """
    return st.sql_select(
        table_name = table_name,
        columns = '*',
        sql_server_config = server_config,
        verbose = False,
    ).fillna('')

### load_schema ###


def get_server(config: dict, index: str, username: str, password: str) -> dict:
    server_config = config[index]
    server_config['POSTGRES_USER'] = username
    server_config['POSTGRES_PW'] = password

    # del env

    return server_config

### get_server ###


def get_table_name(project_name: str, supplier: str, table_info: str, postfix: str):
    table_name = f'{supplier}_{project_name}_{table_info}_{postfix}'

    return table_name

### get_table_name ###


def get_table_names(project_name: str, supplier: str, postfix: str = 'data') -> dict:
    """ Get table names of project for specific supplier.

    Args:
        project_name (str): Name of project
        leverancier (str): Name of leverancier
        postfix (str): either 'data' or 'description'

    Returns:
        dict: dictionary with all data table names
    """
    tables_name: dict = {TAG_TABLE_SCHEMA: get_table_name(project_name, supplier, TAG_TABLE_SCHEMA, postfix),
                         TAG_TABLE_META: get_table_name(project_name, supplier, TAG_TABLE_META, postfix),
                         TAG_TABLE_DELIVERY: get_table_name(project_name, supplier, TAG_TABLE_DELIVERY, postfix),
                         TAG_TABLE_QUALITY: get_table_name(project_name, supplier, TAG_TABLE_QUALITY, postfix),
                        }

    return tables_name

### get_table_names ###


def get_supplier_dict(config: dict, supplier: str, delivery):
    """ Returns supplier s info from supplier.

        The relevant delivery info is copied and all info about deliveries
        is deleted.

    Args:
        suppliers (dict): dictionary of suppliers
        supplier (str): name of the supplier to select
        delivery (int ro str): sequence number of delivery to apply

    Returns:
        dict: dictionary of supplier addjusted with correct delivery
    """
    flag = False
    suppliers = config['SUPPLIERS']
    leverancier = suppliers[supplier].copy()
    leverancier['config'] = config
    leverancier['supplier_id'] = supplier
    indices = []
    chosen = '*'
    delivery_configs = {}
    deliveries = []

    # find all deliveries in the config dictionary
    # store in delivery_configs
    for key in leverancier.keys():
        # delivery is identified by 'delivery' in the key
        if 'delivery' in key:
            indices.append(key)

            # format is delivery-<int> or delivery-*, identify delivery seq
            splits = key.split('-')

            # correct delivery key has just two parts
            if len(splits) == 2:

                # It is the second part we need; when it is a uint
                if splits[1].isdigit():
                    seq = int(splits[1])
                    delivery_configs[str(splits[1])] = leverancier[key]
                    delivery_configs[str(splits[1])]['delivery_naam'] = key
                    delivery_configs[str(splits[1])]['delivery_keus'] = ''

                # or just *
                elif splits[1] == '*':
                    delivery_configs[str(splits[1])] = leverancier[key]
                    delivery_configs[str(splits[1])]['delivery_naam'] = key
                    delivery_configs[str(splits[1])]['delivery_keus'] = ''

                else:
                    logger.error(f'*** Delivery {key} is not a valid delivery')
                    flag = True

                # if
            # if
        # if
    # for

    # all deliveries are stored in delivery_configs
    if len(delivery_configs) > 0:
        # identify which delivery applies
        delivery_index = str(delivery)

        # when deliveries are applied, there must always be a catc-all delivery
        if '*' not in delivery_configs.keys():
            logger.error('*** delivery-* must be specified when specifying deliveries')
            flag = True

        elif delivery_index in delivery_configs.keys():
            delivery_configs[delivery_index]['delivery_keus'] = 'x'
            for key in delivery_configs[delivery_index]:
                leverancier[key] = delivery_configs[delivery_index][key]

        elif '*' in delivery_configs.keys():
            delivery_configs['*']['delivery_keus'] = 'x'
            for key in delivery_configs['*']:
                leverancier[key] = delivery_configs['*'][key]

        # if
    # if

    # set default when key is lacking

    if flag:
        raise DiDoError('Errors in delivery specification.')

    for key in indices:
        leverancier.pop(key)

    return leverancier, delivery_configs

### get_supplier_dict ###


def enhance_cargo_dict(cargo_dict: dict, cargo_name, supplier_name: str):
    """ Returns supplier s info from supplier.

        The relevant delivery info is copied and all info about deliveries
        is deleted.

    Args:
        suppliers (dict): dictionary of suppliers
        supplier (str): name of the supplier to select
        delivery (int ro str): sequence number of delivery to apply

    Returns:
        dict: dictionary of supplier addjusted with correct delivery
    """
    splits = cargo_name.split('_')
    if len(splits) == 2:
        if splits[0] != 'delivery':
            raise_DiDoError(f'*** Delivery should start with "delivery_", error for "{cargo_name}"')

    cargo_dict[ODL_LEVERING_FREK] = splits[1]
    cargo_dict['supplier_id'] = supplier_name

    return cargo_dict

### get_supplier_dict ###


def get_cargo(cargo_config: dict, supplier: str):
    cargo = cargo_config['DELIVERIES'][supplier]

    return cargo

### get_cargo ###


def get_current_delivery_seq(project_name: str, supplier: str, server_config: dict):
    tables = get_table_names(project_name, supplier)

    table_name = tables[TAG_TABLE_DELIVERY]
    try:
        count = st.table_size(table_name, True, server_config)
    except:
        raise DiDoError(f'Table {table_name} does not exist. Are you sure you used create_table with the current config.yaml file?')
    # try..except

    return count

### get_current_delivery_seq ###


def report_psql_use(table: str, servers: dict, tables_exist: bool):
    """ Reports to a user he shoukld use psql. The correct command is displayed

    Args:
        table (str): name of the fiole containing the SQL instructions
        servers (dict): dictionary of server configurations
        tables_exist (bool): True if tablkes already exist, else False
    """
    host = servers['DATA_SERVER_CONFIG']['POSTGRES_HOST']
    user = servers['DATA_SERVER_CONFIG']['POSTGRES_USER']
    db = servers['DATA_SERVER_CONFIG']['POSTGRES_DB']

    report_string = f'$ psql -h {host} -U {user} -f {table}.sql {db}'

    logger.info('')
    logger.info("Don't forget to run psql to create or fill the tables")
    logger.info("This can best be done from your work/sql directory")
    logger.info('Suggested command:')
    logger.info('')
    logger.info(report_string)

    if tables_exist:
        logger.info('')
        logger.error('*** You have been warned that the tables you want to create already exist.')
        logger.error('*** If you really want to recreate these tables, thereby erasing current contents')
        logger.error('*** run dido_kill_supplier.py ***')

    logger.info('')

    return

### report_psql_use ###


def show_database(server_config: dict, table_name: str, pfun = logger.debug):
    if len(table_name) > 0:
        pfun(f'Table:    {table_name}')
        pfun('')

    pfun(f'Host:     {server_config["POSTGRES_HOST"]}')
    pfun(f'Database: {server_config["POSTGRES_DB"]}')
    pfun(f'Port:     {server_config["POSTGRES_PORT"]}')
    pfun(f'Schema:   {server_config["POSTGRES_SCHEMA"]}')

    return

### show_database ###


def delivery_exists(delivery: dict, supplier_id: str, project_name: str, server_configs: dict) -> bool:
    """ Checks whether a delivery exists. Column 'levering_rapportageperiode is used
        to check this.

    Args:
        delivery (dict): dictionary containing the deliverydescription.
            Used to get the key 'levering_rapportageperiode' with thge current delivery
        supplier_id (str): supplier name, used for table name construction
        project_name (str): name of the project, used for table name construction
        server_configs (dict): the existence of deliveries is chcked in the data server database

    Returns:
        bool: True = delivery exists, else not
    """
    # fetch a dataframe with deliveries from the data table using distinct
    # on levering_rapportageperiode

    server_config = server_configs['DATA_SERVER_CONFIG']
    table_name = get_table_name(project_name, supplier_id, TAG_TABLE_DELIVERY, 'data')
    try:
        leveringen = st.sql_select(
            table_name = table_name,
            columns = f'DISTINCT {ODL_LEVERING_FREK}',
            sql_server_config = server_config,
            verbose = False,
        )
    except sqlalchemy.exc.ProgrammingError:
        logging.critical(f'*** Tables do not exist for {supplier_id}_{project_name}_...')
        show_database(server_config, table_name, logger.info)
        raise DiDoError('Error while fetching tables. Did you ever apply create_tables '
                        f'for {supplier_id}_{project_name}?')

    # try..except

    leveringen_lijst = leveringen[ODL_LEVERING_FREK].tolist()
    if len(leveringen_lijst) > 0:
        show_database(server_config, table_name, logger.debug)
        logger.debug(f'Deliveries present in the database: {leveringen_lijst}')

    # if

    current_delivery = delivery[ODL_LEVERING_FREK]
    exists = current_delivery in leveringen_lijst
    if exists:
        logger.debug(f'Delivery {current_delivery} already in the database')

    return exists

### delivery_exists ###
