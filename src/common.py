import os
import re
import time
import pandas as pd

import logging
from logging.config import dictConfig
from os.path import join, splitext, dirname, basename, exists

def create_log(filename: str, level: int = logging.INFO):
    # Code initialisatie: logging
    # create logger
    LOGGING = { 
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
                'stream': 'ext://sys.stdout',  # Default is stderr
            },
            'file': { 
                'level': logging.DEBUG,
                'formatter': 'standard',
                'class': 'logging.FileHandler',
                'filename': filename, 
                'mode': 'w',
            },
        },
        'loggers': {
            '': {
                'level': level,
                'handlers': ['console', 'file']
            },
        },    
    }

    # print(LOGGING)

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logging.config.dictConfig(LOGGING)

    return logger

### create_logger ###


def split_filename(filename: str):
    """ Splits filename into dir, filename and .extension 

    Args:
        filename (str): filename to split

        returns:
            directory, filename without extension, .extension (including period)
    """

    fn, extension = splitext(filename)
    pad = dirname(fn)
    base = basename(fn)

    return pad, base, extension

### split_filename ###


def read_env(filename: str='.env', set_env: bool=True):
    """
    Leest de environment file in en geeft de ingelezen variabelen terug 
    als een dictionary en zet ze in os.environ. De file wordt ingelezen 
    vanuit de config directory.

    Args:
        filename (str, optional): naam van file met omgevingsvariabelen, meestal .env
                                    Defaults to '.env'
        set_env (bool, optional): indien True, zet de variable eveneens in os.environ. 
                                    Defaults to True.

    Returns:
        dict: een dictionary met alle gevonden vartiabelen
    """
    env_vars = {}
    
    try:
        with open(filename) as f:
            for line in f:
                if line.startswith('#') or not line.strip():
                    continue
                
                # Remove leading optional `export `, next split name = value pair
                key, value = line.replace('export ', '', 1).strip().split('=', 1)
    
                # Save to a dict, initialized env_vars = {}
                env_vars[key] = value
                
                if set_env:
                    os.environ[key] = value
                
            # for
            
            f.close()
            
        # with
            
    except FileNotFoundError:
        print(f'Cannot open environment file {filename}')

        raise

    # try..except

    return env_vars

### read_env ###


def change_column_name(col_name: str) -> str:
    """ Changes a string into a valid postgres name.
    
    Starts with letter, only alfanumeric and _ allowed. All non-alfanumerics are removed, spaces converted to
    underscore (_), multiple underscores are converted to one. If the string does not start with a letter
    the empty string is returned. All alfa's converted to lower case.

    Args:
        col_name (str): string to be converted

    Returns:
        str: valid postgres name or empty
    """
    # remove spaces left and right
    col_name = col_name.strip()

    # replace multiple spaces by one
    while '  ' in col_name:
        col_name = col_name.replace('  ', ' ')    

    # what's left of spaces, replace by _
    col_name = col_name.replace(' ', '_')

    # column should start with letter
    i = 0
    while i < len(col_name):
        if col_name[i].isalpha():
            # letter found, create substring and lowercase the stuf
            # does not cater for situations in which no letter is present
            col_name = col_name[i:].lower()

            # only accept alfanums and '_'
            col_name = re.sub(r'\W+', '', col_name)

            # return it
            return col_name

        # if

        # no letter found yet, increase index
        i += 1

    # while

    # nothing found, return empty string
    return ''

### change_column_name ###


def get_headers_and_types(schema: pd.DataFrame):
    headers: list = []
    types: list = {}

    for idx, row in schema.iterrows():
        header: str = row['kolomnaam']
        if header not in ['*', 'type']:
            dtype: str = row['pandastype']

            headers.append(header)
            types[header] = dtype

    # for

    return headers, types

### get_headers_and_types ###


def read_schema_file(filename: str) -> pd.DataFrame:
    # Read mutation schema file
    cpu = time.time()  
    schema = pd.read_csv(filename, 
                         sep = ';', 
                         quotechar='"',
                         keep_default_na = False,
                         encoding = 'UTF-8'
                        )
                        
    cpu = time.time() - cpu

    # in postgres text is surrounded by "'". Just double single quotes to have them accepted
    schema['beschrijving'] = schema['beschrijving'].str.replace("'", "''")

    # two spaces at the end of the line ensure a newline in markdown. Add them to have
    # the beschrijving accepted as markdown in other applications
    schema['beschrijving'] = schema['beschrijving'].str.replace("\n", "  \n")

    return schema

### read_schema_file ###