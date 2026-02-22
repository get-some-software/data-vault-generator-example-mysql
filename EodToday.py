# This is a sample Python script.
import os

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import requests
from xml.etree import ElementTree
import mysql.connector

import Eod2Oracle

class EodToday(object):

    # Press the green button in the gutter to run the script.
    def __init__(self, eoduserName, eodpassWord, eodExchange, debug):
        self.eoduserName = eoduserName
        self.eodpassWord = eodpassWord
        self.eodExchange = eodExchange
        self.debug = debug;

        #try:
            # establish a new connection
        with mysql.connector.connect(
                user="mtm",
                password=os.environ.get("MYSQL_PASSWORD"),
                host=os.environ.get("MYSQL_HOST")) as connection:

            with connection.cursor() as cursor:

                sql = """
                INSERT INTO src_file_log (
                    seq_no,
                    load_date,
                    record_source,
                    system_id,
                    data_package_id,
                    file_id,
                    file_name,
                    file_path,
                    file_size,
                    file_hk,
                    file_hk_prev,
                    file_modified_flag,
                    file_creation_date,
                    file_modification_date,
                    file_extraction_id,
                    file_extraction_date,
                    file_copied_flag,
                    file_copy_date,
                    file_read_flag,
                    file_read_date,
                    file_processed_flag,
                    file_processed_date,
                    bv_processed_flag,
                    bv_refresh_start_date,
                    bv_refresh_end_date
                )
                SELECT
                    NULL AS seq_no,
                    NOW() AS load_date,
                    'EodToday.py' AS record_source,
                    'EOD' AS system_id,
                    'DP001' AS data_package_id,
                    NULL AS file_id,
                    NULL AS file_name,
                    NULL AS file_path,
                    NULL AS file_size,
                    NULL AS file_hk,
                    NULL AS file_hk_prev,
                    'N' AS file_modified_flag,
                    CURDATE() AS file_creation_date,
                    CURDATE() AS file_modification_date,
                    CONCAT(DATE_FORMAT(CURDATE(), '%Y%m%d-%H:%i:%s'), %s) AS file_extraction_id,
                    CURDATE() AS file_extraction_date,
                    NULL AS file_copied_flag,
                    NULL AS file_copy_date,
                    NULL AS file_read_flag,
                    NOW() AS file_read_date,
                    'N' AS file_processed_flag,
                    NULL AS file_processed_date,
                    NULL AS bv_processed_flag,
                    NULL AS bv_refresh_start_date,
                    NULL AS bv_refresh_end_date
                WHERE DAYOFWEEK(CURDATE()) NOT IN (1, 7)
                    AND CURDATE() NOT IN (
                        SELECT DATE(se_bank_holiday)
                        FROM exn1.se_bank_holiday
                    )
                ON DUPLICATE KEY UPDATE
                    load_date = NOW(),
                    file_read_date = NOW(),
                    file_modification_date = NOW(),
                    file_modified_flag = 'Y',
                    file_processed_flag = 'N'
                """
                cursor.execute(sql, (self.eodExchange,))
                connection.commit()
            with connection.cursor() as cursor:
                sql = """SELECT
                            MIN(DATE_FORMAT(sfl.file_extraction_date, '%Y%m%d')) AS file_extraction_date,
                            MIN(sfl.file_extraction_id) AS file_extraction_id
                        FROM src_file_log sfl
                        WHERE sfl.data_package_id = 'DP001'
                            AND sfl.file_processed_flag = 'N'"""
                cursor.execute(sql)
                for (file_extraction_date, file_extraction_id) in cursor:
                    if file_extraction_date is not None:
                        if self.debug:
                            print("file_extraction_date:", file_extraction_date)
                            print("file_extraction_id:", file_extraction_id)
                        Eod2Oracle.eod2oracle(self.eoduserName, self.eodpassWord, self.eodExchange, file_extraction_date, self.debug)


                        with mysql.connector.connect(
                                user="oditmp",
                                password=os.environ.get("MYSQL_PASSWORD"),
                                host=os.environ.get("MYSQL_HOST")) as connectionODITMP:

                                with connectionODITMP.cursor() as cursorODITMP:
                                    sql = "SET ROLE dwh_public_role;CALL ODITMP.RDV_EOD_DP001_CTL()"
                                    cursorODITMP.execute(sql)
                                    connectionODITMP.commit()

                                with connection.cursor() as cursor:
                                    sql = """UPDATE src_file_log
                                                SET
                                                    file_processed_flag = 'Y',
                                                    file_processed_date = NOW()
                                                WHERE
                                                    data_package_id = 'DP001'
                                                    AND file_extraction_id = %s
                                                    AND file_processed_flag = 'N'"""
                                    cursor.execute(sql, (file_extraction_id,))
                                    connection.commit()

                                    with connection.cursor() as cursor:
                                        sql = """INSERT INTO src_db_log (
                                                    seq_no,
                                                    load_date,
                                                    record_source,
                                                    system_id,
                                                    version_id,
                                                    data_package_id,
                                                    db_extraction_id,
                                                    db_extraction_date,
                                                    db_read_flag,
                                                    db_read_date,
                                                    db_processed_flag,
                                                    db_processed_date )
                                                VALUES
                                                    ( NULL,
                                                    NOW(),
                                                    'EodToday.py',
                                                    'STOCKALERT',
                                                    '100',
                                                    'DP003',
                                                    CONCAT(%s, 'STOCKALERT'),
                                                    NOW(),
                                                    'Y',
                                                    NOW(),
                                                    'N',
                                                    NULL )
                                                ON DUPLICATE KEY UPDATE
                                                    db_processed_flag = 'N',
                                                    db_read_flag = 'Y',
                                                    db_processed_date = NULL"""
                                        cursor.execute(sql, (file_extraction_date,))
                                        connection.commit()

                                    with connectionODITMP.cursor() as cursorODITMP:
                                        sql = "CALL ODITMP.RDV_STOCKALERT_DP003_CTL()"
                                        cursorODITMP.execute(sql)
                                        connectionODITMP.commit()


                                    with mysql.connector.connect(
                                        user="mtm",
                                        password=os.environ.get("MYSQL_PASSWORD"),
                                        host=os.environ.get("MYSQL_HOST")) as connection2:

                                        with connection2.cursor() as cursor:
                                            sql = """UPDATE src_db_log
                                                    SET
                                                        db_processed_flag = 'Y',
                                                        db_processed_date = NOW()
                                                    WHERE
                                                        db_extraction_id = CONCAT(%s, 'STOCKALERT')"""
                                            cursor.execute(sql, (file_extraction_date,))
                                            connection2.commit()

        # except oracledb.Error as error:
        #     print('Error occurred in EodToday:')
        #     print(error)

