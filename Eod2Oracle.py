# This is a sample Python script.
import os
import datetime

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import requests
from xml.etree import ElementTree
import mysql.connector

class eod2oracle(object):

    # Press the green button in the gutter to run the script.
    def __init__(self, eoduserName, eodpassWord, eodExchange, eodQuoteDate, debug):
        self.eoduserName = eoduserName
        self.eodpassWord = eodpassWord
        self.eodExchange = eodExchange
        self.eodQuoteDate = eodQuoteDate
        self.debug = debug
        self.eodurllogin  = 'http://ws.eoddata.com/data.asmx/Login'
        self.eodloginPARAMS = {'Username': self.eoduserName, 'Password':self.eodpassWord}
        self.eodurlExchange = 'http://ws.eoddata.com/data.asmx/QuoteListByDate'
        r = requests.post(url=self.eodurllogin, data=self.eodloginPARAMS)
        xmlTree =  ElementTree.fromstring(r.content)
        now = datetime.datetime.now()
        print("{} processing EOD  {} {}".format(now, self.eodExchange, eodQuoteDate))
        if self.debug:
            print("Processing EOD username " + self.eoduserName)
            print(r.text)
            print(xmlTree.get('Token'))
        self.eodToken = xmlTree.get('Token')
        self.eodExchangePARAMS = {'Token':self.eodToken,'Exchange':self.eodExchange,'QuoteDate':self.eodQuoteDate}

        exchange = requests.post(url=self.eodurlExchange, data=self.eodExchangePARAMS)
        # if self.debug:
        #     print(exchange.content)
        xmlTreeExchange = ElementTree.fromstring(exchange.content)
        #print(xmlTreeExchange.().__getitem__(0).get('QUOTE'))
        root = xmlTreeExchange

        # try:
            # establish a new connection
        with mysql.connector.connect(
                user=os.environ.get("MYSQL_DATABASE"),
                password=os.environ.get("MYSQL_PASSWORD"),
                host=os.environ.get("MYSQL_HOST")) as connection:

            with connection.cursor() as cursor:
                sql = ('TRUNCATE TABLE eod')
                cursor.execute(sql)
            with connection.cursor() as cursor:
                for child in root:
                    for grandchild in child:
                        l_se = self.eodExchange
                        l_symbol = grandchild.get('Symbol')
                        # MySQL uses different date format
                        l_eod_date = self.eodQuoteDate
                        l_open = grandchild.get('Open')
                        l_high = grandchild.get('High')
                        l_low = grandchild.get('Low')
                        l_close = grandchild.get('Close')
                        l_volume = grandchild.get('Volume')
                        # Using parameterized query for better security
                        sql = "INSERT INTO eod (se, symbol, eod_date, open, high, low, close, volume) VALUES (%s, %s, STR_TO_DATE(%s, '%Y%m%d'), %s, %s, %s, %s, %s)"
                        cursor.execute(sql, (l_se, l_symbol, l_eod_date, l_open, l_high, l_low, l_close, l_volume))
            if self.debug:
                print("Committing day " + self.eodQuoteDate + " for Stock Exchange " + self.eodExchange)
                connection.commit()

        # except oracledb.Error as error:
        #     print('Error occurred:'+self.eodExchange + ":"+self.eodQuoteDate)
        #     print(error)
