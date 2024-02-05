import os
import socket
import sys
from datetime import datetime, timedelta
from argparse import ArgumentParser, Namespace

import psutil

import utils
from db import DBConfig, DBConnection, DynamicTableGenerator
from loggings import MultipurposeLogger
from messaging import EmailConfig, MultiPurposeEmailSender
from ssh import SSHConfig, SSHTunnelCommandExecutor

logger = MultipurposeLogger(
    name='HDFSRetention'
)
today = datetime.now()
RemoveOnlyBefore = "RemoveOnlyBefore"


def calculate_audits():
    try:
        process = psutil.Process()
        calculated_once = {
            'start_time': today,
            'tbl_dt': int(today.strftime('%Y%m%d')),
            'log_file': logger.get_log_file(),
            'config_file': args.config,
            'feed': None,
            'pid': int(process.parent().pid),
            'puser': process.parent().username(),
            'ppid': int(os.getpid()),
            'ppuser': os.getlogin(),
            'system': f"OS: {sys.platform}, CPU: {int(psutil.cpu_count())}, Memory: {int(psutil.virtual_memory().total)}",
            'node': socket.gethostbyname(socket.gethostname()),
            'end_time': None,
            'removed_partitioned_count': None,
            'list_of_removed_dates_in_this_run': None,
            'all_removed_before': None,
            'alter_status': None,
            'run_status': None,
            'is_test': args.test
        }
        return calculated_once
    except psutil.Error as e:
        logger.warning(f"Failed instantiate audits with error: {e}")
        return


def get_table_structure():  # TODO: should we add this here? or make a config file instead? or just another table?
    from sqlalchemy import BigInteger, Column, String, Integer, DateTime, Boolean, func
    return {
        'id': Column(BigInteger, primary_key=True, autoincrement=True),
        'log_file': Column(String, nullable=False),
        'config_file': Column(String, nullable=False),
        'feed': Column(String, nullable=False),
        'list_of_removed_dates_in_this_run': Column(String),
        'removed_partitioned_count': Column(Integer, default=0),
        'all_removed_before': Column(Integer, nullable=False),
        'tbl_dt': Column(Integer, nullable=False),
        'start_time': Column(DateTime(timezone=True), server_default=func.now(), nullable=False),
        'end_time': Column(DateTime(timezone=True), server_default=func.now(), nullable=False),
        'pid': Column(Integer, nullable=False),
        'puser': Column(String, nullable=False),
        'ppuser': Column(String, nullable=False),
        'ppid': Column(Integer, nullable=False),
        'system': Column(String, nullable=False),
        'node': Column(String, nullable=False),
        'alter_status': Column(String, default='failed'),
        'run_status': Column(String, default='failed'),
        'is_test': Column(Boolean, nullable=False)
    }


def validate_feeds_config(config: dict):
    # Initial validation based on 'args.feed'
    if args.feed == "all":
        logger.info("Validating the provided data config for 'all' the feeds.")
    else:
        logger.info(f"Validating the provided data config for '{args.feed}' feed.")
        config = {args.feed: config[args.feed]}

    # Identify feeds with missing details
    keys_to_remove = []
    for key, value in config.items():
        for field in ["Schema", "TableName", "RetentionPeriodInDays", "PartitionedBy"]:
            if utils.is_dict_field_missing(value, field):
                logger.error(f"{field} is empty or None for the '{key}' feed... dropping the feed from the run.")
                keys_to_remove.append(key)
                break  # No need to check further fields if one is missing
        if utils.is_dict_field_missing(value, "Schema"):
            logger.warning(f"'Schema' is empty or None for the '{key}' "
                           f"feed... This run will pass if the table is fund in the database.")
        if value["RetentionPeriodInDays"] == 0 or not isinstance(value["RetentionPeriodInDays"], int):
            logger.error(
                f"'RetentionPeriodInDays' equals to '0' or is not integer for the feed '{key}'... "
                f"dropping this path from the list.\n"
                f"Allowed Minimum value for this field is '1'."
            )
            keys_to_remove.append(key)
            continue
        config[key][RemoveOnlyBefore] = int(
            (today - timedelta(days=value["RetentionPeriodInDays"] - 1)).strftime('%Y%m%d')
        )

    # Remove feeds with missing details
    for key in keys_to_remove:
        config.pop(key)

    if not config:
        return False

    logger.info(f"Config after validation: {config}.")
    return config.copy()


def validate_configs(config: dict):
    if args.feed.lower() != 'all' and args.feed not in config['data']:
        logger.error(f"Provided feed '{args.feed}' not found in the CONFIG.")
        raise ValueError(f"Provided feed '{args.feed}' not found in the CONFIG.")

    config['data'] = validate_feeds_config(config['data'].copy())
    return config.copy()


def main():
    config = utils.load_json_file(args.config)
    logger.info(f"Loaded config: {config}")
    config = validate_configs(config)
    if not config:
        logger.warning(f"Ending the file execution as the config is empty after validation.")
        return

    logger.info(f"Initiating SSHConfig.")
    sshconfig = SSHConfig(  # ES connection
        host=config['ssh']['host'],
        port=config['ssh']['port'],
        username=config['ssh']['username'],
        password=config['ssh']['password'],
        auth_key=config['ssh']['auth_key'],
    )

    logger.info(f"Initiating Data DBConfig.")
    dbconfig = DBConfig(
        delicate=config['database']['delicate'],
        host=config['database']['host'],
        port=config['database']['port'],
        database=config['database']['database'],
        username=config['database']['username'],
        password=config['database']['password'],
        auth_file=config['database']['auth_file'],
        query=config['database']['query'],
        stream=config['database']['stream'],
        echo=config['database']['echo'],
    )

    logger.info(f"Initiating Audit DBConfig.")
    audit_dbconfig = DBConfig(
        delicate=config['audit']['delicate'],
        host=config['audit']['host'],
        port=config['audit']['port'],
        database=config['audit']['database'],
        username=config['audit']['username'],
        password=config['audit']['password'],
        auth_file=config['audit']['auth_file'],
        query=config['audit']['query'],
        stream=config['audit']['stream'],
        echo=config['audit']['echo'],
    )

    logger.info(f"Config before start removal: {config}")

    executor = SSHTunnelCommandExecutor(
        sshconfig, logger=logger
    )
    executor.connect_client()

    dbconnection = DBConnection(
        dbconfig, logger=logger
    )
    audit_dbconnection = DBConnection(
        audit_dbconfig, logger=logger
    )

    generator = DynamicTableGenerator(
        audit_dbconnection, logger=logger
    )
    # STEP HEER IS VERY IMPORTANT, THE VARIABLE HERE IS A CLASS NOT OBJECT
    AuditTableClass = generator.create_table_class(
        name=config['audit']['table'],
        columns=get_table_structure(),
        schema=config['audit']['schema']
    )
    generator.create_tables()

    for feed, values in config['data'].items():
        audits = AuditTableClass(**calculate_audits())
        audits.all_removed_before = values[RemoveOnlyBefore]
        audits.feed = feed
        try:
            generator.session.add(audits)
            generator.session.commit()
        except Exception as e:
            generator.session.rollback()
            logger.error(f'Failed to insert audits {audits} into the database for {feed}: {e}')
            raise e

        # TODO: below is a hive statement, does it actually drop?, as we are executing on presto
        # TODO: we can convert it into a terminal command aligned with hive
        sql = f""" 
            ALTER TABLE {values['Schema']}.{values['TableName']}
            DROP IF EXISTS PARTITION(
                {values['PartitionedBy']} < {values[RemoveOnlyBefore]}
            )
        """
        try:
            df = dbconnection.select(f"""
                SELECT 
                    distinct {values['PartitionedBy']}
                from 
                    {values['Schema']}.{values['TableName']}
                where 
                    {values['PartitionedBy']} < {values[RemoveOnlyBefore]}
            """)
            df.info()
            audits.removed_partitioned_count = df.shape[0]
            logger.info(f"total number of partitions to remove: {audits.removed_partitioned_count}")
            audits.list_of_removed_dates_in_this_run = df[values['PartitionedBy']].sort_values().astype(
                str).values.tolist().__str__()
            logger.info(f"list of partitions to remove: {audits.list_of_removed_dates_in_this_run}")

            if args.test:
                logger.info(f"Test run will execute: {sql}")
                res = True
            else:
                res = dbconnection.execute(sql)

            if res:
                logger.info(
                    f'Dropping the requested partitions before {values[RemoveOnlyBefore]} '
                    f'was done successfully for {feed}.'
                )
                audits.alter_status = 'success'
            else:
                logger.error(f'Error: Failed dropping the requested partitions.')
                audits.alter_status = 'failed'
                continue
            audits.run_status = 'success'
        except Exception as e:
            logger.error(f"Failed for unknown error {e}")
            audits.run_status = 'failed'
            raise e
        finally:
            audits.end_time = datetime.now()
            try:
                generator.session.commit()
            except Exception as e:
                generator.session.rollback()
                logger.error(f'Failed to insert audits {audits} into the database for {feed}: {e}')
                raise e

    dbconnection.close()
    audit_dbconnection.close()
    executor.close()

    logger.info(f"Reading the email template message.")
    try:
        with open(config['email']['template'], 'tr') as file:
            data = file.read().format(
                feed=args.feed, date=today.strftime('%Y%m%d'), status=audits.run_status
            )
        logger.info(f"Reading the email template message.")
    except FileNotFoundError as e:
        logger.error(f"File {config['email']['template']} not found: {e}")
        data = f"Email of Automated retention {today.strftime('%Y%m%d')} with status {audits.run_status}"

    try:
        email_config = EmailConfig(
            username=config['email']['username'],
            password=config['email']['password'],
            server=config['email']['server'],
            port=config['email']['port'],
        )
        sender = MultiPurposeEmailSender(
            config=email_config,
            logger=logger
        )
        attachments = config['email']['attachments'].split(',') if config['email']['attachments'] else config['email'][
            'attachments']
        sender.send_email(
            subject=config['email']['subject'].format(
                feed=args.feed, date=today.strftime('%Y%m%d'), status=audits.run_status
            ),
            body=data,
            receivers=config['email']['recipients'],
            attachments=attachments
        )
    except Exception as e:
        logger.error(f"Could not send email due known reason: {e}")


def cli() -> Namespace:
    """Configure argument parser and parse cli arguments."""

    parser = ArgumentParser(description="HDFS Retention Policy Implementer.")
    parser.add_argument(
        "--config",
        required=True,
        type=str,
        help="The path to the config .json file.",
    )
    parser.add_argument(
        "--log_dir",
        type=str,
        default='logs',
        help="The path to the generated logs directory.",
    )

    parser.add_argument(
        "--feed",
        # type=str.lower,
        type=str,
        required=True,
        default='all',
        help="The name of the feed as appeared in the CONFIG file. Note: Accepts upper case only.",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="The column identifying the food item.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    # Take all the input parameters
    args = cli()

    if not os.path.exists(args.config) or not os.path.isfile(args.config) or not args.config.endswith('.json'):
        logger.error("Error: Provided Config path 1- Not exists or,\n2- Not a file or,\n3- Not JSON file.")
        raise ValueError("Provided Config path 1- Not exists or,\n2- Not a file or,\n3- Not JSON file.")

    if args.test:
        logger.info("============== Test Execution, Nothing will be Removed ==============")

    main()

# run example
# python .\main.py --config .\config.json  --feed all --test --log_dir logs
