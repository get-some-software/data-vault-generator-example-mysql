# This is a sample Python script.
import os

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import requests
from xml.etree import ElementTree
import mysql.connector
import Eod2Oracle

class EodHistory(object):

    # Press the green button in the gutter to run the script.
    def __init__(self, eoduserName, eodpassWord, eodExchange, daysBack, debug):
        self.eoduserName = eoduserName
        self.eodpassWord = eodpassWord
        self.eodExchange = eodExchange
        self.daysBack  = daysBack
        self.debug = debug

        # try:
            # establish a new connection
        with mysql.connector.connect(
            user="mtm",
            password=os.environ.get("MYSQL_PASSWORD"),
            host=os.environ.get("MYSQL_HOST")) as connection:

            with connection.cursor() as cursor:
                sql = """
                WITH RECURSIVE days AS (
                    SELECT 1 AS lvl, CURDATE() - INTERVAL 1 DAY AS extraction_date
                    UNION ALL
                    SELECT lvl + 1, CURDATE() - INTERVAL (lvl + 1) DAY
                    FROM days
                    WHERE lvl < %s
                )
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
                    'EodHistory.py' AS record_source,
                    'EOD' AS system_id,
                    'DP001' AS data_package_id,
                    NULL AS file_id,
                    NULL AS file_name,
                    NULL AS file_path,
                    NULL AS file_size,
                    NULL AS file_hk,
                    NULL AS file_hk_prev,
                    'N' AS file_modified_flag,
                    d.extraction_date AS file_creation_date,
                    d.extraction_date AS file_modification_date,
                    CONCAT(DATE_FORMAT(d.extraction_date, '%Y%m%d-%H:%i:%s'), %s) AS file_extraction_id,
                    d.extraction_date AS file_extraction_date,
                    NULL AS file_copied_flag,
                    NULL AS file_copy_date,
                    NULL AS file_read_flag,
                    NOW() AS file_read_date,
                    'N' AS file_processed_flag,
                    NULL AS file_processed_date,
                    NULL AS bv_processed_flag,
                    NULL AS bv_refresh_start_date,
                    NULL AS bv_refresh_end_date
                FROM days d
                LEFT JOIN src_file_log sfl
                    ON sfl.file_extraction_date = d.extraction_date
                    AND sfl.file_extraction_id = CONCAT(DATE_FORMAT(d.extraction_date, '%Y%m%d-%H:%i:%s'), %s)
                    AND sfl.data_package_id = 'DP001'
                WHERE sfl.file_extraction_id IS NULL
                    AND d.extraction_date NOT IN (
                        SELECT file_extraction_date
                        FROM src_file_log
                        WHERE data_package_id = 'DP001'
                            AND file_processed_flag = 'Y'
                            AND file_extraction_id LIKE CONCAT('%', %s, '%')
                    )
                    AND DAYOFWEEK(d.extraction_date) NOT IN (1, 7)
                    AND DATE(d.extraction_date) NOT IN (
                        SELECT DATE(se_bank_holiday)
                        FROM exn1.se_bank_holiday
                    )
                ORDER BY d.extraction_date ASC
                """
                cursor.execute(sql, (int(self.daysBack), self.eodExchange, self.eodExchange, self.eodExchange))
            connection.commit()
            if self.debug:
                print("Merged workload")

            with mysql.connector.connect(
                user="mtm",
                password=os.environ.get("MYSQL_PASSWORD"),
                host=os.environ.get("MYSQL_HOST"))  as connection3:
                with connection3.cursor() as cursor:
                    sql = """SELECT
    DATE_FORMAT(sfl.file_extraction_date, '%Y%m%d') AS file_extraction_date,
    sfl.file_extraction_id
    FROM src_file_log sfl
    WHERE sfl.data_package_id = 'DP001'
    AND sfl.file_processed_flag = 'N'
    AND sfl.file_extraction_id LIKE CONCAT('%', %s, '%')
    ORDER BY sfl.file_extraction_id
    """
                    cursor.execute(sql, (self.eodExchange,))
                    if self.debug:
                        print("Selecting a day to process")
                    for (file_extraction_date, file_extraction_id) in cursor:
                        if self.debug:
                            print("file_extraction_date:", file_extraction_date)
                            print("file_extraction_id:", file_extraction_id)
                        Eod2Oracle.eod2oracle(self.eoduserName, self.eodpassWord, self.eodExchange, file_extraction_date, debug)

                        with mysql.connector.connect(
                            user="oditmp",
                            password=os.environ.get("MYSQL_PASSWORD"),
                            host=os.environ.get("MYSQL_HOST"))  as connectionODITMP:
                            if self.debug:
                                print("Processing EOD  {} {}".format(self.eodExchange, file_extraction_date))
                            if self.debug:
                                print("Trying to start ODITMP DB Processing EOD  {} {} status connection {}".format(self.eodExchange, file_extraction_date, connectionODITMP.is_healthy))
                            with connectionODITMP.cursor() as cursorODITMP:
                                sql = "SET ROLE dwh_public_role; CALL ODITMP.RDV_EOD_DP001_CTL()"
                                cursorODITMP.execute(sql)
                                connectionODITMP.commit()
                                if self.debug:
                                    print("ODITMP DB process started. Processing EOD  {} {}".format(self.eodExchange, file_extraction_date))

                                with mysql.connector.connect(
                                    user="mtm",
                                    password=os.environ.get("MYSQL_PASSWORD"),
                                    host=os.environ.get("MYSQL_HOST"))  as connection2:
                                    with connection2.cursor() as cursor:
                                        sql = """UPDATE src_file_log
                                        SET
                                            file_processed_flag = 'Y',
                                            file_processed_date = NOW()
                                        WHERE
                                            data_package_id = 'DP001'
                                            AND file_extraction_id = %s
                                            AND file_processed_flag = 'N'"""
                                        cursor.execute(sql, (file_extraction_id,))
                                        connection2.commit()

    #except oracledb.Error as error:
    #    print('Error occurred in EodHistory:')
    #    print(error)
