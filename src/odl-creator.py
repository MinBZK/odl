import os
import sys

from os.path import join, splitext, dirname, exists
from datetime import datetime
from random import randint
import pandas as pd

# import yaml

import dido_common as dc

# print all columns of dataframe
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)


def write_documentation(filename: str, tables: list, root_dir: str, columns_to_write: list):
    """ Generates documentation for each name in schema_names.

    The documentation is written in markdown format with a __TOC__
    header for the Gitlab wiki.importlib-metadata

    Args:
        filename (str): File to write documentation
        schema_names (list): List of schema filenames
    """
    with open(filename, 'w') as outfile:

        # write wiki table of content
        outfile.write('[[_TOC_]]\n\n')

        for table in tables:
            logger.info(f'[Documenting {table}]')

            # get schema file
            schema = tables[table]['schema']
            meta = tables[table]['meta']
            data = tables[table]['data'] if 'data' in tables[table] else None

            # create a name for optional extra markdown information
            markname = join(root_dir, 'docs', table + '.csv.')

            # create documentation
            write_markup_doc(outfile, tables, table, columns_to_write)

        # for

    # with

    logger.info('')
    logger.info(f'=== Documentation written to {filename}')

    return

### write_documentation ###


def write_markup_doc(outfile: object,
                     tables: dict,
                     table_name: str,
                    #  schema: pd.DataFrame,
                    #  meta: pd.DataFrame,
                    #  data: pd.DataFrame,
                     columns_to_write: list,
                    ):

    """ Function to generate markup documentation based on a schema dataframe

    Args:
        outfile (file handle): File to write documentation to
        schema (pd.DataFrame): schema to be used for documentation
        meta_name (str): Name of the associated metafile
        markdown_name (str): name of file with additional markdown info
            to be added at the end; when empty nothing is added
    """
    schema = tables[table_name]['schema']
    meta = tables[table_name]['meta']
    data = tables[table_name]['data'] if 'data' in tables[table_name] else None

    # write name of table or view
    outfile.write(f"# **Tabel: {table_name}**\n\n")

    if 'prefix' in tables[table_name]:
        outfile.write(tables[table_name]['prefix'] + '\n\n')

    table_description: str = meta.loc['Bronbestand beschrijving', 'Waarde'].strip()
    if len(table_description) == 0:
        table_description = f'DOKUMENTATIE ONTBREEKT!'

    # write meta table header
    outfile.write('## Meta-informatie\n\n')
    outfile.write(f"| Meta attribuut | {meta.columns[0]} \n")
    outfile.write("| ---------- | ------ |\n")

    # write descriptor
    for index, row in meta.iterrows():
        outfile.write('| ')
        outfile.write(index + ' | ') # meta.loc[index, 'Meta-attribuut'] + ' | ')
        if index == 'Sysdatum':
            outfile.write(datetime.now().strftime(dc.DATETIME_FORMAT) + ' |\n')

        else:
            outfile.write(meta.loc[index, 'Waarde'] + ' |\n')

        # if

    # for

    # Write the table description
    outfile.write('\n\n## Databeschrijving\n\n')
    outfile.write(table_description)
    outfile.write('\n\n')

    # when columns_to_write is empty this means write all columns
    if len(columns_to_write) == 0:
        columns_to_write = schema.columns

    # write table header
    underscores = ''
    for col in columns_to_write:
        colname = col.replace('_', ' ')
        colname = colname.capitalize()

        outfile.write(f' | {colname} ')
        underscores += ' | ----- '

    # for

    outfile.write(' |\n')
    outfile.write(f'{underscores} |\n')

    # write table info
    for index, row in schema.iterrows():
        outfile.write('| ')

        for col in columns_to_write:
            cell: str = str(schema.loc[index, col]).replace('\n', '<br >')
            cell = cell.replace('\r', '')

            outfile.write(cell)
            outfile.write(' | ')

        # for

        outfile.write('\n')

    # for
    outfile.write('\n')

    if data is not None:
        outfile.write('\n\n')
        outfile.write('## Data\n\n')

        for col in data.columns:
            outfile.write(f' | {col} ')

        outfile.write(' | \n')

        for col in data.columns:
            outfile.write(f' | ------- ')

        outfile.write('| \n')

        for idx, row in data.iterrows():
            for col in data.columns:
                outfile.write(f' | {data.loc[idx, col] }')

            outfile.write(' | \n')

        outfile.write('\n')

    # if
    outfile.write('\n')


    # check if additional markdown exists
    if 'suffix' in tables[table_name]:
        outfile.write(tables[table_name]['suffix'] + '\n\n')

    return

### write_markup_doc ###


def write_sql(sql_filename: str,
              tables: dict,
              template: pd.DataFrame,
              postgres_schema: str,
             ) -> None:

    """ iterate over all elements in table and creates a data description

    When a data DataFrame is passed the table itself will be created and the
    data will be stored into the table.

    Args:
        sql_filename (str): Name of the file to write DDL onto
        tables (dict): dictionary with table names as key and per table name
            points to additional information
        template (pd.DataFrame): DataFrame of bronbestand_attribuut_meta.csv
            containing column information needed for create_table_description
        postgres_schema (str): Schema name of the table
    """

    with open(sql_filename, 'w') as outfile:
        sql_code: str = '\set ON_ERROR_STOP on;\n\nBEGIN;\n'

        for table in tables:
            logger.info(f'[Processing {table}]')

            # get schema file
            schema = tables[table]['schema']
            meta = tables[table]['meta']
            data = tables[table]['data'] if 'data' in tables[table] else None

            # create SQL for the table description
            sql_code += create_table_description(schema, meta, template,
                                                     tables[table]['schema_name'],
                                                     postgres_schema,
                                                     table,
                                                    )

            # Check if there are data present for the current schema
            # if so create a table with the data
            if data is not None:
                logger.info(f'=== Creating data for {table} ===')
                sql_code += create_table(schema, meta, data,
                                             tables[table]['data_name'],
                                             postgres_schema,
                                             table,
                                            )

            # # if

            outfile.write(sql_code)
            outfile.write('\n\n')

        # for
        outfile.write('\nCOMMIT;\n')

    # with

    logger.info('')
    logger.info(f'=== SQL file written to {sql_filename}')

    return

### write_sql ###


def create_table_description(schema: pd.DataFrame,
                             meta: pd.DataFrame,
                             template: pd.DataFrame,
                             filename: str,
                             schema_name: str,
                             table: str,
                            ) -> str:

    """ Creates a description of a table based on schema

    Args:
        schema (pd.DataFrame): DataFrame to create description from
        meta (pd.DataFrame): meta data of the schema
        filename (str): filename to read the data from
        schema_name (str): postgres schema name
        table_(str): Postgres table name

    Returns:
        str: SQL string with DDL
    """

    table_name = table + '_description'
    data_types: str = ''
    line: str = ''
    comments: str = ''
    table_comment: str = ''

    # fetch description from meta data if present nand create table comment
    desc: str = '*** NO DOCUMENTATION PROVIDED ***'
    if len(meta.loc['Bronbestand beschrijving', 'Waarde'].strip()) > 0:
        desc = meta.loc['Bronbestand beschrijving', 'Waarde']
    table_comment = f"COMMENT ON TABLE {schema_name}.{table_name} IS $${desc}$$;\n\n"

    template = template.set_index('kolomnaam')

    # create columns
    for col in schema.columns:
        line = f'   {col} text'

        data_types += line + ',\n'

        beschrijving: str = template.loc[col, 'beschrijving']
        comment: str = f"COMMENT ON COLUMN {schema_name}.{table_name}.{col} " \
                       f"IS $${beschrijving}$$;\n"

        comments += comment

    # for
    template = template.reset_index()

    # remove last comma and newline
    data_types = data_types[:-2]

    # create a table definition and instruction to read starttabel.csv
    tbd: str = f'DROP TABLE IF EXISTS {schema_name}.{table_name} CASCADE;\n\n'
    tbd += f'CREATE TABLE {schema_name}.{table_name}\n(\n'
    tbd += data_types + '\n);\n\n'
    tbd += table_comment + comments + '\n\n'
    tbd += f"\\COPY {schema_name}.{table_name} FROM {filename} DELIMITER ';' CSV HEADER\n\n"

    logger.debug(tbd)

    return tbd

### create_table_description ###


def create_table(schema: pd.DataFrame,
                 meta: pd.DataFrame,
                 data: pd.DataFrame,
                 data_name: str,
                 schema_name: str,
                 table: str,
                ) -> str:

    # create data type for each column
    data_types: str = ''
    line: str = ''
    table_comment: str = ''
    comments: str = ''
    table_name = table + '_data'

    for idx, row in schema.iterrows():
        line = f'   {row["kolomnaam"]} {row["datatype"]}'
        if len(row['constraints']) > 0:
            line += ' ' + row['constraints']

        data_types += line + ',\n'

        comment: str = f"COMMENT ON COLUMN {schema_name}.{table_name}.{row['kolomnaam']} IS "
        description: str = schema.loc[idx, 'beschrijving'].strip()

        if len(description) == 0:
            description = '*** NO DOCUMENTATION PROVIDED ***'

        comment += f"$${description}$$;\n"
        comments += comment

    # for

    # remove last comma and newline
    data_types = data_types[:-2]

    # create a table definition and instruction to read starttabel.csv
    tbd: str = f'DROP TABLE IF EXISTS {schema_name}.{table_name} CASCADE;\n\n'
    tbd += f'CREATE TABLE {schema_name}.{table_name}\n(\n'
    tbd += data_types + '\n);\n\n'
    tbd += table_comment + comments + '\n\n'

    if data is not None:
        tbd += f"\\COPY {schema_name}.{table_name} FROM {data_name} DELIMITER ';' CSV HEADER\n\n"

    logger.debug(tbd)

    return tbd

### create_table ###


def apply_data_odl(template: pd.DataFrame, data: pd.DataFrame, meta: pd.DataFrame, table: str):
    """ Uses data and meta to fill in the ODL template

    Args:
        template (pd.DataFrame): ODL template to be filled out
        data (pd.DataFrame): data file, usually source analysis
        meta (pd.DataFrame): meta file for some important information

    Returns:
        pd.DataFrame: the filled out template
    """
    if 'kolomnaam' not in data.columns:
        raise ValueError(f'* Data "{table}" *moet* "kolomnaam" bevatten. ODL generatie kan daar niet zonder')

    if template is None:
        raise ValueError(f'* Data "{table}": de file "bronbestand_attribuut_meta.csv" file ontbreekt. ODL generatie kan daar niet zonder.')

    if meta is None:
        raise ValueError(f'* Data "{table}": de file "bronbestand_attribuut_meta.meta.csv" file ontbreekt. ODL generatie kan daar niet zonder.')

    # flag non-exiting column error
    errors = False
    for col_name in data.columns:
        if col_name not in template.columns:
            logger.error(f'* Data "{table}": kolomnaam "{col_name}" komt niet in "bronbestand_attribuut_meta.csv" voor')
            errors = True

    # if errors:
    #     raise ValueError('* Onbekende kolomnamen gespecificeerd, verdere controle is niet zinvol')

    today = datetime.now().strftime(dc.DATE_FORMAT)

    new_df = pd.DataFrame(columns = template.columns, index = data.index, dtype = str)

    code_bbs: str = meta.loc['Code bronbestand', 'Waarde']

    # assign defaults
    new_df['avg_classificatie'] = '1'
    new_df['veiligheid_classificatie'] = '1'
    new_df['attribuut_datum_begin'] = meta.loc['Bronbestand datum begin', 'Waarde']
    new_df['attribuut_datum_einde'] = meta.loc['Bronbestand datum einde', 'Waarde']

    # namen die worden voorgedefinieerd en niet worden overgekopieerd
    names_to_skip = ['kolomnaam', 'code_attribuut', 'code_attribuut_sleutel', 'code_bronbestand']

    i = 0
    # generate codes and keys
    for row, _ in new_df.iterrows():
        # ensure that kolomnaam is a postgres accepted column name
        new_name = dc.change_column_name(data.loc[row, 'kolomnaam'])

        # when no name could be created, create a random one
        if len(new_name) == 0:
            new_name = 'kolom_' + str(randint(1000, 9998))

        # assign newly created column name to kolomnaam
        new_df.loc[row, 'kolomnaam'] = new_name

        # create code attribuut
        code_atr: str = ''

        # check if it occurs in data.columns
        if 'code_attribuut' in data.columns:
            code_atr = str(data.loc[row, 'code_attribuut']).strip()

        # not? assign a numeric value
        if len(code_atr) == 0:
            code_atr = f'{i + 1:03d}'

        # assign to new_df
        new_df.loc[row, 'code_attribuut'] = code_atr

        # same for code_attribuut_sleutel
        code_atr_key: str = code_bbs + code_atr
        if 'code_attribuut_sleutel' in data.columns:
            code_atr_key = str(data.loc [row, 'code_attribuut_sleutel']).strip()

        if len(code_atr_key) == 0:
            code_atr_key = code_bbs + code_atr

        new_df.loc[row, 'code_attribuut_sleutel'] = code_atr_key

        if 'positie' not in data.columns or len(data.loc[row, 'positie']) == 0:
            new_df.loc[row, 'positie'] = str(i + 1)

        # assign rest of data values
        for col_name in data.columns:
            if col_name not in names_to_skip:
                value = data.loc[row, col_name].strip()
                if len(value) > 0:
                    new_df.loc[row, col_name] = value

                # if
            # if
        # for

        i += 1

    # for

    # some attributes are overruled by meta data
    new_df['code_bronbestand'] = code_bbs

    # convert all nan values to empty str
    for row in new_df.index:
        for col in new_df.columns:
            try:
                value = new_df.loc[row, col].lower()
                if value == 'nan':
                    new_df.loc[row, col] = ''

            except:
                new_df.loc[row, col] = ''

            # try..except
        # for
    # for

    return new_df

### apply_data_odl ###


def apply_meta_odl(meta: pd.DataFrame, schema: pd.DataFrame):
    n_cols = len(schema.columns)
    n_rows = len(schema)
    meta.loc['Bronbestand aantal attributen', 'Waarde'] = str(n_cols)
    meta.loc['Bronbestand gemiddeld aantal records', 'Waarde'] = str(n_rows)
    meta.loc['Sysdatum', 'Waarde'] = datetime.now().strftime(dc.DATETIME_FORMAT)

    return meta

### apply_meta_odl ###


def preprocess_schemas(tables, server: dict):

    template = None
    for table in tables:
        if tables[table]['template']:
            template = tables[table]['schema']
            break
        # if
    # for

    if template is None:
        raise RuntimeError('*** internal error: no template found')

    meta_data_name = ''

    for table in tables:
        schema = tables[table]['schema']
        meta = tables[table]['meta']
        new_schema = apply_data_odl(template, schema, meta, table)
        tables[table]['schema'] = new_schema

        new_meta = apply_meta_odl(meta, new_schema)
        tables[table]['meta'] = new_meta

        new_schema.to_csv(tables[table]['schema_name'], sep = ';', index = False)
        new_meta.to_csv(tables[table]['meta_name'], sep = ';', index = True)

        # process the data of some schemas
        if 'data' in tables[table]:
            data_table = tables[table]['data']
            # print(tables[table]['data'])

            # substitute special column names by their values
            for col in data_table.columns:
                if col.lower() == 'sysdatum':
                    nu = datetime.now().strftime(dc.DATETIME_FORMAT)
                    data_table.loc[0, col] = nu
                elif col.lower() == 'created_by':
                    value = str(server['POSTGRES_USER'])
                    data_table.loc[0, col] = value

            if 'code_bronbestand' in data_table.columns:
                meta_data_name = tables[table]['data_name']
                # print(data_table)

            data_table.to_csv(tables[table]['data_name'], sep = ';', index = False)

    # for

    return tables, template, meta_data_name

### preprocess_schemas ###


def load_schemas(tables: dict, root: str, work: str, supplier: str) -> dict:
    for table in tables:
        logger.info(f'=== {table} ===')
        schema_root = join(root, 'schemas', supplier)
        schema_work = join(work, 'schemas', supplier)
        doc_root = join(root, 'docs', supplier)
        doc_work = join(work, 'docs', supplier)

        base = tables[table]['from']
        fn, ext = splitext(base)
        filename = join(schema_root, base)
        meta_name = join(schema_root, fn + '.meta' + ext)
        data_name = join(schema_root, fn + '.data' + ext)

        if fn == 'bronbestand_attribuut_meta':
            tables[table]['template'] = True
        else:
            tables[table]['template'] = False

        # read schema
        tables[table]['schema'] = pd.read_csv(
            filename,
            sep = ';',
            dtype = str,
            keep_default_na = False,
            na_values = []
        ).fillna('')

        # remove spaces from column names
        tables[table]['schema'].columns = tables[table]['schema'].columns.str.replace(' ', '')

        # create filename for schema
        tables[table]['schema_name'] = join(work, 'schemas', supplier, base)

        # read the meta information table of the schema
        meta = pd.read_csv(
            meta_name,
            sep = ';',
            dtype = str,
            keep_default_na = False
        ).fillna('')

        # remove spaced from columns
        meta.columns = meta.columns.str.replace(' ', '')

        meta = meta.set_index(meta.columns[0])
        tables[table]['meta'] = meta
        tables[table]['meta_name'] = join(work, 'schemas', supplier, fn + '.meta' + ext)

        if exists(data_name):
            logger.info('[Reading data]')
            tables[table]['data'] = pd.read_csv(
                data_name,
                sep = ';',
                dtype = str,
                keep_default_na = False
            ).fillna('')
            if 'sysdatum' in tables[table]['data'].columns:
                datumtijd = datetime.now().strftime(dc.DATETIME_FORMAT)
                tables[table]['data']['sysdatum'] = datumtijd

            tables[table]['data_name'] = join(work, 'schemas', supplier, fn + '.data' + ext)
        # if

        # load documentation for every table when present
        prefix_name = join(doc_root, fn + '.prefix.md')
        if exists(prefix_name):
            logger.info('[Reading prefix]')
            with open(prefix_name, 'r') as file:
                tables[table]['prefix'] = file.read().strip()

        suffix_name = join(doc_root, fn + '.suffix.md')
        if exists(suffix_name):
            logger.info('[Reading suffix]')
            with open(suffix_name, 'r') as file:
                tables[table]['suffix'] = file.read().strip()

    # for

    return tables

### load_schemas ###


def update_odl_version(config: dict, filename: str):
    """get the meta data from the odl database"""

    logger.info('')

    # fetch the meta data from file
    meta_data = pd.read_csv(
        filename,
        sep = ';',
        dtype = str,
        keep_default_na = False,
        na_values = []
    ).fillna('')

    # get the table name from file name
    _, table_name, _ = dc.split_filename(filename)
    table_name = table_name.replace('.', '_')

    # get the odl server config from config
    server_config = config['SERVER_CONFIGS']['ODL_SERVER_CONFIG']
    meta_table = None

    # fetch the table from the database
    try:
        meta_table = dc.load_odl_table(table_name, server_config)

    # Some error occured, information could not be fetch from the database
    # No updates of the version will take place as this depends on the
    # versions stored in the database.
    except Exception as e:
        logger.warning('!!! Current version could not be fetched from the database.')
        logger.warning('!!! See logfile for details')
        logger.debug(e)

    # try..except

    if meta_table is not None:
        # get the odl versions
        major = int(meta_table.loc[0, dc.ODL_VERSION_MAJOR])
        minor = int(meta_table.loc[0, dc.ODL_VERSION_MINOR])
        patch = int(meta_table.loc[0, dc.ODL_VERSION_PATCH])
        major_date = meta_table.loc[0, dc.ODL_VERSION_MAJOR_DATE]
        minor_date = meta_table.loc[0, dc.ODL_VERSION_MINOR_DATE]
        patch_date = meta_table.loc[0, dc.ODL_VERSION_PATCH_DATE]

        # Update versioning
        major_update = dc.get_par(config, 'UPDATE_MAJOR_VERSION')
        minor_update = dc.get_par(config, 'UPDATE_MINOR_VERSION')
        nu = datetime.now()

        if major_update and minor_update:
            raise dc.DiDoError('Config: Major and Minor version update are set. Only one (or neither) may be set.')

        # if major update, incr major version and set rest to zero
        if major_update:
            major += 1
            minor = 0
            patch = 0
            major_date = nu
            minor_date = nu
            patch_date = nu

            logger.info(f'[Major ODL version will be updated to {major}]')

        # if minor update, incr major version and set patch to zero
        elif minor_update:
            minor += 1
            patch = 0
            minor_date = nu
            patch_date = nu

            logger.info(f'[Minor ODL version will be updated to {minor}]')

        # each run is a patch update
        else:
            patch += 1
            patch_date = nu

            logger.info(f'[Patch ODL version updated to {patch}]')

        # if

        # set new values
        meta_data.loc[0, dc.ODL_VERSION_MAJOR] = str(major)
        meta_data.loc[0, dc.ODL_VERSION_MINOR] = str(minor)
        meta_data.loc[0, dc.ODL_VERSION_PATCH] = str(patch)
        meta_data.loc[0, dc.ODL_VERSION_MAJOR_DATE] = \
            major_date.strftime(dc.DATETIME_FORMAT)
        meta_data.loc[0, dc.ODL_VERSION_MINOR_DATE] = \
            minor_date.strftime(dc.DATETIME_FORMAT)
        meta_data.loc[0, dc.ODL_VERSION_PATCH_DATE] = \
            patch_date.strftime(dc.DATETIME_FORMAT)

        meta_data.to_csv(
            filename,
            sep = ';',
            index = False,
        )

        logger.info(f'[ODL version will be set to {major}.{minor}.{patch}]')
        logger.info('')
    # if

    return

### update_odl_version ###


def create_workdir(workdir: str, sub_dirs: list, subsubdirs: list):
    created = []
    for sub in sub_dirs:
        pad = os.path.join(workdir, sub)
        os.makedirs(pad, exist_ok = True)

        created.append(pad)

    # for

    if subsubdirs is None:
        return

    for map in created:
        create_workdir(map, subsubdirs, None)

    return

### create_workdir ###


if __name__ == '__main__':
    print('')
    print('*********************************************')
    print('*                                           *')
    print('* DiDo - Document Data Definition Generator *')
    print('*                                           *')
    print('*********************************************')
    print('')

    # extract root directory from the call arguments from the script
    print("Argument List:", str(sys.argv))

    # when run stand-alone, then there is just one argument: the path to the script
    if len(sys.argv) == 1:
        src_name = sys.argv[0]
        root_dir = dirname(dirname(src_name))
        postcode_file = None

    # when called from init script there are 3 arguments
    # 1. path to script, 2. current source directory, 3. path to postcodetabel
    elif len(sys.argv) == 3:
        src_name = sys.argv[0]
        root_dir = dirname(sys.argv[1])
        postcode_file = sys.argv[2]

    else:
        raise ValueError('*** wrong number of arguments: either none or two')

    # if

    # credentials = read_env(join(root_dir, 'config', '.env'))
    # with open("config/config.yaml", "r") as infile:
    #     env = yaml.safe_load(infile)

    # read the configuration file
    cwd = os.getcwd()
    config = dc.read_config(cwd)

    # get all configuration
    root_dir: str = config['ROOT_DIR']
    work_dir: str = config['WORK_DIR']
    subdirs: list = config['SUBDIRS'] # subdirectories in root_dir
    work: list = config['SUPPLIERS']  # subdirectories under each subdirectory
    doc: str = config['DOC']          # name of file for all documentaiont
    sql: str = config['SQL']          # name of file for SQL DDL
    server = config['SERVER_CONFIGS']['ODL_SERVER_CONFIG']
    schema_name: str = server['POSTGRES_SCHEMA']
    table_dict: dict = config['TABLES']  # dictionary with all tables to create
    columns_to_write = config['COLUMNS'] # columns to write into documentation

    # read product names
    create_workdir(work_dir, subdirs, work)
    data_model = work[0]

    # ceate logger
    log_file: str = join(work_dir, 'logs', 'odl-creator.log')
    logger = dc.create_log(log_file, level = 'DEBUG')
    logger.info(f'log_file is {log_file}')

    # create the documentation and sql filename
    schema_root: str = join(root_dir, 'schemas', data_model)
    schema_work: str = join(work_dir, 'schemas', data_model)
    doc_name: str = join(work_dir, 'docs', doc)
    sql_name: str = join(work_dir, 'sql', sql)

    schemas = load_schemas(table_dict, root_dir, work_dir, data_model)

    schemas, template, meta_data_filename = preprocess_schemas(schemas, server)
    if len(meta_data_filename) > 0:
        update_odl_version(config, meta_data_filename)

    # give feedback on the filenames
    logger.info('')
    logger.info(f'src_name {src_name}')
    logger.info(f'root_dir {root_dir}')
    logger.info(f'work_dir {work_dir}')
    logger.info(f'doc_name {doc_name}')
    logger.info('')

    # write documentation files
    write_documentation(doc_name, schemas, root_dir, columns_to_write)

    # write sql file
    write_sql(sql_name, schemas, template, schema_name)

    logger.info('[Ready]')