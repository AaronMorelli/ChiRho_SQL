#####
#   Copyright 2016, 2024 Aaron Morelli
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
#	------------------------------------------------------------------------
#
#	PROJECT NAME: ChiRho for SQL Server https://github.com/AaronMorelli/ChiRho_SQL
#
#	PROJECT DESCRIPTION: A T-SQL toolkit for troubleshooting performance and stability problems on SQL Server instances
#
#	FILE NAME: generate_installer_sql.py
#
#	AUTHOR:			Aaron Morelli
#					aaronmorelli@zoho.com
#					@sqlcrossjoin
#					sqlcrossjoin.wordpress.com
#
#	PURPOSE: Uses basic Python functionality to generate the installation .sql files based on config values
# To Execute
# ------------------------
#####
import os
import argparse
import logging

logger = logging.getLogger(__name__)

valid_sql_editions = [
 'enterprise edition'
,'evaluation edition'
,'business intelligence edition'
,'developer edition'
,'express edition'
,'standard edition'
,'web edition'
,'sql azure' # indicates SQL Database or Azure Synapse Analytics
,'azure sql edge developer' # indicates the development only edition for Azure SQL Edge
,'azure sql edge' # indicates the paid edition for Azure SQL Edge
]

create_schema_cmd = """
IF (NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '@@CHIRHO_SCHEMA_OBJECTS@@')) 
BEGIN
    EXEC ('CREATE SCHEMA [@@CHIRHO_SCHEMA_OBJECTS@@] AUTHORIZATION [dbo]')
END"""

def get_script_location():
    # Get the absolute path of the current script
    script_path = os.path.abspath(__file__)
    # Get the directory containing the script
    script_dir = os.path.dirname(script_path)
    return script_dir

def parse_arguments():
    # Create the argument parser
    parser = argparse.ArgumentParser(
        description="A script to install the ChiRho for SQL Server software on a target SQL Server instance."
    )

    parser.add_argument(
        "--debug",
        type=bool,
        default=False,
        help="Print verbose messages to aid in debugging"
    )

    args = parser.parse_args()
    return args

def parse_config_file(file_path):
    config = {}
    with open(file_path, 'r') as file:
        for line in file:
            # Remove comments
            line = line.split('#')[0].strip()
            if not line:  # Skip empty lines
                continue
            
            # Split the key-value pair
            if '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip()
                config[key] = value
    return config


def validate_config(config):
    expected_config_options = ['sql_engine_edition', 'sql_edition', 'product_major_version', 'product_minor_version',
                               'install_in_tempdb', 'chirho_db_name', 'chirho_schema_name', 'install_procs_in_master', 
                               'is_aws_rds', 'include_license', 'sql_time_zone' ]
    exception_string = ""
    extra_options = list(set(config.keys()) - set(expected_config_options))
    if extra_options:
        exception_string = f"The config file contains {len(config)} options, which is more than is expected."
        exception_string = f"The config file has one or more extra, unexpected configuration options. Extra options: {extra_options}."
        raise Exception(exception_string)
    
    missing_options = list(set(expected_config_options) - set(config.keys()))
    if missing_options:
        exception_string = f"The config file is missing one or more expected configuration options. Missing options: {missing_options}."
        exception_string += f" Please re-copy the appropriate config_template file into the file named 'config' to ensure all options are present."
        raise Exception(exception_string)
    
    if config['install_in_tempdb'].upper() not in ['YES', 'NO']:
        exception_string = f"The install_in_tempdb option must be either Yes or No. Value provided: {str(config['install_in_tempdb'])}"
        raise Exception(exception_string)

    if config['install_procs_in_master'].upper() not in ['YES', 'NO']:
        exception_string = f"The install_procs_in_master option must be either Yes or No. Value provided: {str(config['install_procs_in_master'])}"
        raise Exception(exception_string)

    if config['include_license'].upper() not in ['YES', 'NO']:
        exception_string = f"The include_license option must be either Yes or No. Value provided: {str(config['include_license'])}"
        raise Exception(exception_string)
    
    if config['install_in_tempdb'].upper() == 'NO':
        # DB name does not matter if we are installing into tempdb
        if len(config['chirho_db_name']) < 1 or isinstance(config['chirho_db_name'], (int, float)):
            exception_string = f"The chirho_db_name config option must be a valid DB name. Empty strings and numbers are not allowed." 
            exception_string += f"Value provided: {str(config['chirho_db_name'])}"
            raise Exception(exception_string)
        
        if len(config['chirho_schema_name']) < 1 or isinstance(config['chirho_schema_name'], (int, float)):
            exception_string = f"The config option chirho_schema_name must be a valid schema name. Empty strings and numbers are not allowed." 
            exception_string += f"Value provided: {str(config['chirho_schema_name'])}"
            raise Exception(exception_string)
 
    
    if config['sql_engine_edition'] not in ['1', '2', '3', '4', '5', '6', '8', '9', '11']:
        exception_string = f"The sql_engine_edition config option must be set to one of the valid values from SERVERPROPERTY('EngineEdition')."
        exception_string += f" Value provided: {str(config['sql_engine_edition'])}"
        raise Exception(exception_string)
    
    if config['sql_engine_edition'] not in ['2', '3', '5', '8']:
        # ChiRho only supports Azure SQL DB, Azure SQL Managed Instance, and traditional SQL Server instances (which includes AWS RDS)
        exception_string = f"ChiRho for SQL Server only supports Azure SQL DB, Azure SQL Managed Instance, and traditional SQL Server instances (which includes AWS RDS)."
        exception_string = f"ChiRho is not supported for the value {str(config['sql_engine_edition'])} for SERVERPROPERTY('EngineEdition)."
        raise Exception(exception_string)

    sql_edition_found = False
    for cur_ed in valid_sql_editions:
        if cur_ed.lower() in config['sql_edition'].lower():
            sql_edition_found = True
    if not sql_edition_found:
        exception_string = f"The sql_edition config option must be set to one of the valid values from SERVERPROPERTY('Edition')."
        exception_string += f" Value provided: {str(config['sql_edition'])}"
        raise Exception(exception_string)

    if config['product_major_version'] not in ['9', '10', '11', '12', '13', '14', '15', '16']:
        exception_string = f"The product_minor_version config option must be set a valid value from SERVERPROPERTY('ProductMajorVersion')."
        exception_string += f" Value provided: {str(config['product_major_version'])}"
        raise Exception(exception_string)

    if config['product_minor_version'] != '0' and not isinstance(config['product_minor_version'], (int)):
        exception_string = f"The product_minor_version config option must be set to a numeric value, from SERVERPROPERTY('ProductMinorVersion')."
        exception_string += f" Value provided: {str(config['product_minor_version'])}"
        raise Exception(exception_string)

    if config['is_aws_rds'].upper() not in ['YES', 'NO']:
        exception_string = f"The is_aws_rds option must be either Yes or No. Value provided: {str(config['is_aws_rds'])}"
        raise Exception(exception_string)

    logger.debug("Config options have been successfully validated!")
    return True


def normalize_config(config):
    # The config options are specified in a way that is easier for end users to reason about, but for this program we
    # restructure config in a way that makes it easier for this program to reason about.
    normalized_config = dict()

    normalized_config['include_license'] = config['include_license']
    normalized_config['sql_time_zone'] = config['sql_time_zone']
    
    # The type of SQL engine makes a big difference for what we can install, where we can install it, etc.
    # We only support values 2 (Standard), 3 (Enterprise), 5 (Azure SQL DB), and 8 (Managed Instance)
    if config['sql_engine_edition'] in ['2', '3']:
        if config['is_aws_rds'].upper() == 'NO':
            # on-prem, Azure VM or EC2, etc.
            normalized_config['engine_type'] = 'traditional'
        else:
            # TODO: someday implement support for SQL Server on Google Cloud
            normalized_config['engine_type'] = 'aws_rds'
    elif config['sql_engine_edition'] == '5':
        normalized_config['engine_type'] = 'azure_sql_db'
    elif config['sql_engine_edition'] == '8':
        normalized_config['engine_type'] = 'azure_managed_instance'
    else:
        # should not hit this
        normalized_config['engine_type'] = '?'
    
    # What functionality is available? This makes a difference for which parts of ChiRho will work
    # Handle the PaaS cases first, as these are more restrictive.
    if config['sql_engine_edition'] == '5':
        normalized_config['edition_features'] = 'azure_sql_db'
    elif config['sql_engine_edition'] == '8':
        # MI are "near-100% enterprise compatible"
        normalized_config['edition_features'] = 'enterprise'
    elif config['sql_engine_edition'] == '3':
        normalized_config['edition_features'] = 'enterprise'
    elif config['sql_engine_edition'] == '2':
        # TODO: SERVERPROPERTY('EngineEdition') == 2 for not only Standard Edition, but also
        # Business Intelligence Edition and Web Edition. Do we need to handle those cases?
        # It has been a very long time since those were valid SKUs, I think.
        normalized_config['edition_features'] = 'standard'
    else:
        # Should not hit this
        normalized_config['edition_features'] = '?'
    
    if config['product_major_version'] == '9':
        normalized_config['sql_version'] = '2005'
        # do 10
    elif config['product_major_version'] == '10':
        if config['product_minor_version'] == '0':
            normalized_config['sql_version'] = '2008'
        else:
            # minor is .5
            normalized_config['sql_version'] = '2008R2'
    elif config['product_major_version'] == '11':
        normalized_config['sql_version'] = '2012'
    elif config['product_major_version'] == '12':
        normalized_config['sql_version'] = '2014'
    elif config['product_major_version'] == '13':
        normalized_config['sql_version'] = '2016'
    elif config['product_major_version'] == '14':
        normalized_config['sql_version'] = '2017'
    elif config['product_major_version'] == '15':
        normalized_config['sql_version'] = '2019'
    elif config['product_major_version'] == '16':
        normalized_config['sql_version'] = '2022'
    
    # We cannot install into TempDB on Azure SQL DB instances
    if config['install_in_tempdb'].upper() == 'YES' and normalized_config['engine_type'] != 'azure_sql_db':
        normalized_config['db_name_objects'] = 'tempdb'
        # 
    else:
        normalized_config['db_name_objects'] = config['chirho_db_name']
    
    if normalized_config['db_name_objects'] == 'tempdb':
        normalized_config['schema_name_objects'] = '##'
        normalized_config['db_name_end_user'] = 'tempdb'
        normalized_config['schema_name_end_user'] = '##'
    else:
        normalized_config['schema_name_objects'] = config['chirho_schema_name']
        
        if config['install_procs_in_master'].upper() == 'NO' or normalized_config['engine_type'] in ['azure_sql_db', 'aws_rds']:
            # We cannot install procs into master in AWS RDS or Azure SQL DB
            normalized_config['db_name_end_user'] = config['chirho_db_name']
            normalized_config['schema_name_end_user'] = config['chirho_schema_name']
        else:
            normalized_config['db_name_end_user'] = 'master'
            normalized_config['schema_name_end_user'] = 'dbo'
    
    logger.debug(f"Config has been normalized: {normalized_config}")
    # TODO: we need to add another SELECT query to the config_template to detect our permissions (or expand the current SELECT query)
    return normalized_config 


def get_config(script_location):
    config_location = os.path.join(script_location,'Installation','config')
    logger.debug(f"The config file will be looked for at: {config_location}")
    config = parse_config_file(config_location)
    logger.debug(f"The config file has been parsed into a dictionary: {config}")
    if validate_config(config):
        return normalize_config(config)
    else:
        # validate_config() throws an exception if config is bad
        return None

def is_valid_code_line(line, config):
    # Encapsulates logic that evaluates whether a specific line of code should
    # actually be written to the output file.
    if config['include_license'] == 'No' and (
            line.strip().startswith('*****')
            or line.strip().startswith('/*****')):
        # License lines are specially identified by a specific number of asterisks
        return False
    return True

def replace_config_tokens(line, config):
    # These replacements are not dependent on where the ChiRho objects are to be created
    modified_line = line.replace('@@CHIRHO_ENGINE_TYPE@@', config['engine_type']) \
                    .replace('@@CHIRHO_EDITION_FEATURES@@', config['edition_features']) \
                    .replace('@@CHIRHO_SQL_VERSION@@', config['sql_version']) \
                    .replace('@@CHIRHO_SQL_TIMEZONE@@', config['sql_time_zone'])
    
    # This token is not widely used, as we do not qualify the ChiRho objects with the DB name.
    # So for now, it is safe to do this replacement without considering what sort of install is being done
    modified_line = modified_line.replace('@@CHIRHO_DB_OBJECTS@@', config['db_name_objects'])
    
    if config['db_name_objects'] == 'tempdb':
        # For tempdb installs, we want our objects to have "##" prepended, instead of a schema name.
        # So note the extra "." character in the next line:
        modified_line = modified_line.replace('@@CHIRHO_SCHEMA_OBJECTS@@.', '##')
        # But in some places in the repo, the token @@CHIRHO_SCHEMA_OBJECTS@@ appears without a subsequent "."
        # character, so we must search and replace for this also:
        modified_line = modified_line.replace('@@CHIRHO_SCHEMA_OBJECTS@@', config['schema_name_objects'])
    else:
        modified_line = modified_line.replace('@@CHIRHO_SCHEMA_OBJECTS@@', config['schema_name_objects'])
    
    # These tokens are not yet in use in the repo; may need to write logic for them later.
    #                .replace('@@CHIRHO_DB_ENDUSER@@', config['db_name_end_user'])
    #                .replace('@@CHIRHO_SCHEMA_ENDUSER@@', config['schema_name_end_user'])
    return modified_line

def append_regular_ddl_files(outfile, folder, prefix, config):
    for filename in os.listdir(folder):
        # Check if filename starts with "CoreXR" and is a .sql file
        if filename.startswith(prefix) and filename.endswith('.sql'):
            # Construct full path to the source file
            source_path = os.path.join(folder, filename)
            # Open and read the source file
            with open(source_path, 'r') as infile:
                lines = infile.readlines()
                processed_lines = []
                for line in lines:
                    if is_valid_code_line(line, config):
                        #modified_line = line.replace('@@CHIRHO_SCHEMA_OBJECTS@@', config['schema_name_objects'])
                        processed_lines.append(replace_config_tokens(line, config))
                processed_lines.append('')
                outfile.writelines(processed_lines)
                outfile.write('\n')
    return

def generate_tempdb_install(config, script_location):
    return

def generate_database_install(config, script_location):
    target_dir = os.path.join(script_location,'Installation','GeneratedScripts', 'RegularInstall')
    logger.debug(f"Installation scripts will be generated at: {target_dir}")
    corexr_tables = os.path.join(script_location, 'Code', 'Tables')
    corexr_triggers = os.path.join(script_location, 'Code', 'Triggers')
    corexr_views = os.path.join(script_location, 'Code', 'Views')
    corexr_procs = os.path.join(script_location, 'Code', 'Procedures')
    corexr_ddl = os.path.join(target_dir, '01_CoreXR_DDL.sql')
    logger.debug(f"CoreXR DDL created at: {corexr_ddl}")
    # create the file, or if it already exists, truncate it
    with open(corexr_ddl, 'w') as outfile:
        outfile.write(f"USE [{config['db_name_objects']}]")
        outfile.write('\nGO\n')  # yes, this is cross-platform
        if config['schema_name_objects'] != 'dbo':
            outfile.write(replace_config_tokens(create_schema_cmd, config))
            outfile.write('\nGO\n')
        
        # CoreXR objects
        append_regular_ddl_files(outfile, corexr_tables, 'CoreXR', config)
        append_regular_ddl_files(outfile, corexr_triggers, 'CoreXR', config)
        append_regular_ddl_files(outfile, corexr_views, 'CoreXR', config)
        append_regular_ddl_files(outfile, corexr_procs, 'CoreXR', config)
        outfile.write("EXEC @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_InsertConfigData".replace(
                '@@CHIRHO_SCHEMA_OBJECTS@@', config['schema_name_objects']))
        outfile.write('\nGO\n')
    return


def main():
    # Parse command-line arguments
    args = parse_arguments()
    print("Parsed arguments:")
    print(f"debug: {args.debug}")
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO, format="%(message)s")
    
    script_location = get_script_location()
    logger.debug(f"The script is located at: {script_location}")
    config = get_config(script_location)
    # "config" is a dict with these keys (and values that look something like this)
    #  {'include_license' : 'Yes',
    #   'engine_type': 'traditional', 
    #   'edition_features': 'enterprise', 
    #   'sql_version': '2019', 
    #   'db_name_objects': 'XR', 
    #   'schema_name_objects': 'dbo', 
    #   'db_name_end_user': 'master', 
    #   'schema_name_end_user': 'dbo'
    #   'sql_time_zone': '"Pacific Time Zone"}

    if config['db_name_objects'] == 'tempdb':
        generate_tempdb_install(config, script_location)
    else:
        generate_database_install(config, script_location)

if __name__ == "__main__":
    main()