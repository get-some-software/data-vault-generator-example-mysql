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

                sql=("""MERGE INTO src_file_log sfl
                            USING (
                            WITH days AS (
                                SELECT
                                    trunc(sysdate)         AS extraction_date
                                FROM
                                    dual
                            ), sfl_m AS (
                            SELECT
                                - 1                 AS seq_no,
                                sysdate             AS load_date,
                                'EodToday.py'     AS record_source,
                                'EOD'               AS system_id,
                                'DP001'             AS data_package_id,
                                NULL                AS file_id,
                                NULL                AS file_name,
                                NULL                AS file_path,
                                NULL                AS file_size,
                                NULL                AS file_hk,
                                NULL                AS file_hk_prev,
                                'N'                AS file_modified_flag,
                                extraction_date     AS file_creation_date,
                                extraction_date     AS file_modification_date,
                                to_char(
                                    extraction_date, 'YYYYMMDD-HH24:MI:SS'
                                ) || '"""+ eodExchange+"""' AS file_extraction_id,
                                extraction_date     AS file_extraction_date,
                                NULL                AS file_copied_flag,
                                NULL                AS file_copy_date,
                                NULL                AS file_read_flag,
                                sysdate             AS file_read_date,
                                'N'                 AS file_processed_flag,
                                NULL                AS file_processed_date,
                                NULL                AS bv_processed_flag,
                                NULL                AS bv_refresh_start_date,
                                NULL                AS bv_refresh_end_date
                            FROM
                                days
                            WHERE
                                    to_char(
                                    extraction_date, 'd'
                                ) NOT IN ( '1',
                                '7')
                                AND trunc(extraction_date) NOT IN (
                                    SELECT
                                        trunc(se_bank_holiday)
                                    FROM
                                        exn1.se_bank_holiday
                                )
                            ORDER BY
                                extraction_date ASC
                        )
                        SELECT
                            *
                        FROM
                            sfl_m
                        WHERE 1=1
                        ) sfl_n ON ( sfl.file_extraction_date = sfl_n.file_extraction_date
                                    AND sfl.file_extraction_id = sfl_n.file_extraction_id
                                    AND sfl.data_package_id = sfl_n.data_package_id )
                        WHEN MATCHED THEN UPDATE SET          
                                    sfl.load_date = sysdate,
                                    sfl.file_read_date = sysdate,
                                    sfl.file_modification_date = sysdate,
                                    sfl.file_modified_flag = 'Y',
                                    sfl.file_processed_flag = 'N' 
                        WHEN NOT MATCHED THEN
                        INSERT (
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
                        bv_refresh_end_date )
                        VALUES
                        ( src_file_log_seq.NEXTVAL,
                        sfl_n.load_date,
                        sfl_n.record_source,
                        sfl_n.system_id,
                        sfl_n.data_package_id,
                        sfl_n.file_id,
                        sfl_n.file_name,
                        sfl_n.file_path,
                        sfl_n.file_size,
                        sfl_n.file_hk,
                        sfl_n.file_hk_prev,
                        sfl_n.file_modified_flag,
                        sfl_n.file_creation_date,
                        sfl_n.file_modification_date,
                        sfl_n.file_extraction_id,
                        sfl_n.file_extraction_date,
                        sfl_n.file_copied_flag,
                        sfl_n.file_copy_date,
                        sfl_n.file_read_flag,
                        sfl_n.file_read_date,
                        sfl_n.file_processed_flag,
                        sfl_n.file_processed_date,
                        sfl_n.bv_processed_flag,
                        sfl_n.bv_refresh_start_date,
                        sfl_n.bv_refresh_end_date )""")
                cursor.execute(sql)
                connection.commit()
            with connection.cursor() as cursor:
                sql = ("""SELECT
                            MIN(to_char(
                                sfl.file_extraction_date, 'YYYYMMDD'
                            )) file_extraction_date,
                            MIN(
                                sfl.file_extraction_id
                            ) file_extraction_id
                        FROM
                            src_file_log sfl
                        WHERE
                            sfl.data_package_id = 'DP001'
                            AND sfl.file_processed_flag = 'N'""")
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
                                    sql = ("""BEGIN 
                                    ODITMP.RDV_EOD_DP001_CTL.MAIN();
                                    END;""")
                                    cursorODITMP.execute(sql)

                                with connection.cursor() as cursor:
                                    sql = ("""UPDATE src_file_log
                                                SET
                                                    file_processed_flag = 'Y'
                                                 , file_processed_date = sysdate 
                                                WHERE
                                                    data_package_id = 'DP001'
                                                    AND file_extraction_id = '"""+file_extraction_id+ """'
                                                    AND file_processed_flag = 'N'""")
                                    cursor.execute(sql)
                                    connection.commit()

                                    with connection.cursor() as cursor:
                                        sql = """MERGE INTO src_db_log sdl
                                                USING (
                                                    SELECT
                                                        '{}' || 'STOCKALERT'  as db_extraction_id
                                                    FROM
                                                        dual
                                                ) n ON ( sdl.db_extraction_id = n.db_extraction_id )
                                                WHEN MATCHED THEN UPDATE
                                                SET sdl.db_processed_flag = 'N',
                                                    sdl.db_read_flag = 'Y',
                                                    sdl.db_processed_date = NULL
                                                WHEN NOT MATCHED THEN
                                                INSERT (
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
                                                    ( src_db_log_seq.NEXTVAL,
                                                    sysdate,
                                                    'EodToday.py',
                                                    'STOCKALERT',
                                                    '100',
                                                    'DP003',
                                                    '{}' || 'STOCKALERT',
                                                    sysdate,
                                                    'Y',
                                                    sysdate,
                                                    'N',
                                                    NULL )""".format(file_extraction_date, file_extraction_date)
                                        cursor.execute(sql)
                                        connection.commit()

                                    with connectionODITMP.cursor() as cursorODITMP:
                                        sql = ("""BEGIN 
                                        ODITMP.RDV_STOCKALERT_DP003_CTL.MAIN();
                                        ODITMP.STOCKALERT_SENDMAIL.MAIN();
                                        --DBMS_SNAPSHOT.REFRESH( '"STOCK_EDW"."EOD_BB"','C');
                                        END;""")
                                        cursorODITMP.execute(sql)
                                        connectionODITMP.commit()


                                    with mysql.connector.connect(
                                        user="mtm",
                                        password=os.environ.get("MYSQL_PASSWORD"),
                                        host=os.environ.get("MYSQL_HOST")) as connection2:

                                        with connection2.cursor() as cursor:
                                            sql = ("""UPDATE src_db_log
                                                    SET
                                                        db_processed_flag = 'Y'
                                                        ,db_processed_date = sysdate
                                                    WHERE
                                                        db_extraction_id = '{}'||'STOCKALERT'""".format(file_extraction_date))
                                            cursor.execute(sql)
                                            connection2.commit()

        # except oracledb.Error as error:
        #     print('Error occurred in EodToday:')
        #     print(error)

