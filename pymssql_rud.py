"""
THIS CLASS LET YOU TO CONNECT PYTHON WITH PYODBC TO MICROSOFT SQL SERVER (TESTED WITH MSSQL 2017 EXPRESS INSTALLED ON UBUNTU SERVER 16.04.3 LTS; QUERYS ARE LANCHED FROM WINDOWS 10 PRO CLIENT)
"""


import pyodbc

class pymssql_rud(object):

    # SERVER PARAMETERS
    server = ''
    database = ''
    username = ''
    password = ''
    driver = '{ODBC Driver 13 for SQL Server}'
    DSN_String = ''
    trusted_Conn = False

    # DEBUG FLAG
    debug = False

    # CONNECTION STRING AND CURSORS
    conn_string = ''
    conn = None

    # QUERY
    query_text = ''

    # QUERY COLUMN
    query_column_name = None
    query_column_type = None
    
    # QUERY TYPE
    # 0 = SELECT
    # 1 = INSERT
    # 2 = UPDATE
    # 3 = EXECUTE
    # 4 = MERGE
    query_type = 0

    """
    EXAMPLE OF DRIVER's:
    {SQL Server} - released with SQL Server 2000
    {SQL Native Client} - released with SQL Server 2005 (also known as version 9.0)
    {SQL Server Native Client 10.0} - released with SQL Server 2008
    {SQL Server Native Client 11.0} - released with SQL Server 2012
    {ODBC Driver 11 for SQL Server} - supports SQL Server 2005 through 2014
    {ODBC Driver 13 for SQL Server} - supports SQL Server 2005 through 2016
    """

    ### SET & GET METHODS for CLASS VARIABLES ###

    ### DEBUG ###
    # SET Debug Status
    def setDebug(self, status = False):
        self.debug = status

    # GET Debug Status
    def getDebug(self):
        return self.debug

    ### SERVER ADDRESS ###
    # SET Server Address
    def setServerAddress(self, srv = '127.0.0.1'):
        if self.debug == True:
            self.server = srv
            return self.server
        else:
            self.server = srv
    
    # GET Server Address
    def getServerAddress(self):
        return self.server

    ### DATABASE NAME ###
    # SET Database Name
    def setDatabaseName(self, db = 'SQLEXPRESS'):
        if self.debug == True:
            self.database = db
        else:
            self.database = db

    # GET Database Name
    def getDatabaseName(self):
        return self.database

    ### USERNAME ###
    # SET Username
    def setUserName(self, user = ''):
        if self.debug == True:
            self.username = user
            return self.username
        else:
            self.username = user

    # GET Username
    def getUserName(self):
        return self.username

    ### PASSWORD ###
    # SET Password
    def setPassword(self, pwd = ''):
        if self.debug == True:
            self.password = pwd
            return self.password
        else:
            self.password = pwd
    
    # GET Password
    def getPassword(self):
        return self.password

    ### ODBC DRIVER ###
    # SET Driver DB (without {})
    def setDriverDB(self, drv = ''):
        if self.debug == True:
            self.driver = '{' + drv + '}'
            return self.driver
        else:
            self.driver = '{' + drv + '}'

    # GET Driver DB
    def getDriverDB(self):
        return self.driver

    ### DSN STRING ###
    # SET DSN STRING
    def setDSN_String(self, dsn=''):
        if self.debug == True:
            self.DSN_String = dsn
            return self.password
        else:
            self.password = pwd
    
    # GET DSN STRING
    def getDSN_String(self):
        return self.DSN_String

    ### TRUSTED CONNECTION ###
    # SET Trusted Connection
    def setTrustedConnection(self, trst=False):
        if self.debug == True:
            self.trusted_Conn = trst
            return self.trusted_Conn
        else:
            self.trusted_Conn = trst

    # GET Trusted Connection
    def getTrustedConnection(self):
        return self.trusted_Conn

    ### CONNECTION STRING ###
    # SET Connection String
    def setConnectionString(self, dsn=None, drv=None, srv=None, db=None, usr=None, pwd=None, pwd_none=False, trsc=None):
        #if self.debug == True:
        if dsn !=None:
            self.DSN_String = dsn
            if self.debug == True:
                print("Changing :" + self.DSN_String + " --> " + str(dsn))
        if drv !=None:
            self.driver = drv
            if self.debug == True:
                print("Changing :" + self.driver + " --> " + str(drv))
        if srv !=None:
            self.server = srv
            if self.debug == True:
                print("Changing :" + self.server + " --> " + str(srv))
        if db !=None:
            self.database = db
            if self.debug == True:
                print("Changing :" + self.database + " --> " + str(db))
        if usr !=None:
            self.username = usr
            if self.debug == True:
                print("Changing :" + self.username + " --> " + str(usr))
        if pwd !=None:
            self.password = pwd
            if self.debug == True:
                print("Changing :" + self.password + " --> " + str(pwd))
        if pwd_none == True:
            self.password = ''
            if self.debug == True:
                print("Changing :" + self.password + " --> Password NONE = TRUE")
        if trsc == False:
            self.trusted_Conn = False
            if self.debug == True:
                print("Changing :" + self.trusted_Conn + " --> Trusted Connection = No")
        if trsc == True:
            self.trusted_Conn = True
            if self.debug == True:
                print("Changing :" + self.trusted_Conn + " --> Trusted Connection = Yes")

            
        if self.DSN_String == '' and self.trusted_Conn == False:
            self.conn_string = 'DRIVER=' + self.driver + ';SERVER=' + self.server + ';DATABASE=' + self.database + ';UID=' + self.username + ';PWD='+ self.password + ';'
            if self.debug == True:
                print("Done! --> DSN = None and Trusted Conn = No")
                print(self.getConnectionString())
                return 0
            return 0
        elif self.DSN_String == '' and self.trusted_Conn == True:
            self.conn_string = 'DRIVER=' + self.driver + ';SERVER=' + self.server + ';DATABASE=' + self.database + ';Trusted_Connection=yes;'
            if self.debug == True:
                print("Done! --> DSN = None and Trusted Conn = Yes")
                print(self.getConnectionString())
                return 0
            return 0
        elif self.DSN_String != '':
            self.conn_string = 'DSN=' + self.DSN_String + ';UID=' + self.username + ';PWD='+ self.password + ';'
            if self.debug == True:
                print("Done! --> DSN != None")
                print(self.getConnectionString())
                return 0
            return 0
        else:
            self.conn_string = '';
            if self.debug == True:
                print("Something is wrong!" + self.getConnectionString())
                return -1
            return -1

    
    # GET Connection String
    def getConnectionString(self):
        return self.conn_string

    
    
    ### END - SET & GET METHODS for CLASS VARIABLES ###


    ### OPEN CLOSE SECTION ###
    # Create a Connection
    def openConnection(self):
        try:
                self.conn = pyodbc.connect(self.conn_string)
                if self.debug == True:
                    print("Connection Created with this string: " + self.conn_string)
                    return 0
                return 0
        except:
            if self.debug == True:
                print("Opening connection Failed! Please check the connection string parameters!")
                return -1
            return -1

    # Close the Connection
    def closeConnection(self):
        try:
            self.conn.close()
            if self.debug == True:
                print("Connection Closed!")
                return 0
            return 0
        except:
            if self.debug == True:
                print("Closing Connection Failed! Maybe it could be already closed!")
                return -1
            return -1
    ### END - OPEN CLOSE SECTION ###

    """
    ### CURSORS SECTION ###
    # Create a Cursor
    def createCursor(self):
        try:
            self.cursor = self.conn.cursor()
            return "Cursor Created!"
        except:
            return "Error! Can't create new cursor"


    # Delete a Cursor
    def deleteCursor(self):
        try:
            self.cursor.close()
            return "Cursor Closed!"
        except:
            return "Error! Can't close cursor! Maybe it could be already closed!"

    ### END - CURSORS SECTION ###
    """


    ### QUERY LAUNCHER ###
    # SET QUERY #
    def setQuery(self, qtext, qtype=0):
        try:
            # DEDURRE DI QUALE QUERY SI TRATTA, SELECT, INSERT, UPDATE, EXECUTE #
            self.query_type = qtype
            self.query_text = qtext

            if self.debug == True:
                print('Query Type: ' + str(self.query_type) + ' Query Text: ' + self.query_text)
                return 0
        except:
            if self.debug == True:
                print("Something is wrong!")
                return -1
            return -1

    def getQueryText(self):
        return self.query_text

    def getQueryType(self):
        return self.query_type

    # GET COLUMNS NAME
    # ATTENTION!!!: IT REQUIRES DATABASE's PERMISSION TO LANCH sys.dm_exec_describe_first_result
    def getQueryColumnsName(self):
        try:
            # CREATING NEW CURSOR
            cursor = self.conn.cursor()
            if self.debug == True: 
                print("New Cursor Created!")

            # EXTRACT QUERY
            query_c = "SELECT name FROM sys.dm_exec_describe_first_result_set ('" + self.query_text + "', NULL, 0)"

            if self.debug == True:
                print(query_c)
            
            # CREATING LIST VARIABLE
            query_column_name = []
            
            if self.debug == True: 
                print("New List Created!")

            # DATA TYPE
            for row in cursor.execute(query_c):
                query_column_name.append(str(row.name))

            if self.debug == True: 
                print("Saving Data Type into List")
                
            return query_column_name
        except:
            if self.debug == True:
                print("Error while tryng to retrieve columns name! Maybe you have not granted to do this action on database.")
            return [].append("NO_DATA")
        finally:
            # DELETE CURSOR
            cursor.close()
            if self.debug == True: 
                print("Cursor Closed!")

    # GET COLUMNS DATA TYPE
    # ATTENTION!!!: IT REQUIRES DATABASE's PERMISSION TO LANCH sys.dm_exec_describe_first_result
    def getQueryColumnsDataType(self):
        try:
            # CREATING NEW CURSOR
            cursor = self.conn.cursor()
            if self.debug == True: 
                print("New Cursor Created!")

            # EXTRACT QUERY
            query_c = "SELECT system_type_name FROM sys.dm_exec_describe_first_result_set ('" + self.query_text + "', NULL, 0)"
            if self.debug == True:
                print(query_c)
            
            # CREATING LIST VARIABLE
            query_column_type = []
            
            if self.debug == True: 
                print("New List Created!")

            # DATA TYPE
            for row in cursor.execute(query_c):
                query_column_type.append(str(row.system_type_name))

            if self.debug == True: 
                print("Saving Data Type into List")
            
            return query_column_type
        except:
            if self.debug == True:
                print("Error while tryng to retrieve columns data type")
            return [].append("NO_DATA")
        finally:
            # DELETE CURSOR
            cursor.close()
            if self.debug == True: 
                print("Cursor Closed!")

    # GET TABLE NAME & TYPE
    # ATTENTION!!!: IT REQUIRES DATABASE's PERMISSION TO LANCH sys.dm_exec_describe_first_result
    def getQueryColumnsDetails(self):
        columnDet = []
        columnDet.append(self.getQueryColumnsName()) 
        columnDet.append(self.getQueryColumnsDataType())

        if self.debug == True:
            print(columnDet)
        return columnDet


    # GET QUERY SELECT DATASET
    # HOWTO:
    # - Pass the Number of records you want to extract; default = -1, it means that all records will be extracted
    # ATTENTION: The performance decrease where tables have tons of data
    # RETURN:
    #  0 --> OK TRANSCATION SUCCEDED
    # -1 --> ERROR! ROLLBACK TRANSACTION
    # -2 --> OK TRANSACTION SUCCEDED, BUT IT COULDN'T CLOSE CURSOR
    # -3 --> ERROR! ROLLBACK TRANSACTION, BUT IT COULDN'T CLOSE CURSOR
    # -4 --> ANYTHING TRANSACTION WAS COMMITTED
    #
    # return transaction_result, dataset[]
    def getQuerySelectDataset(self, records=-1):
        # VARIABLE TO RETURN
        transaction_result = 0
        dataset = []
        # CHECK IF QUERY is a SELECT
        sql_methods_allowed = ['SELECT']
        if any(word in self.query_text.upper()[:6] for word in sql_methods_allowed):
            if self.debug == True:
                print(self.query_text.upper().find("SELECT"))
            try:
                # CREATING NEW CURSOR
                cursor = self.conn.cursor()
                if self.debug == True: 
                    print("New Cursor Created!")

                # EXTRACT QUERY
                cursor.execute(self.query_text)
                if self.debug == True:
                    print(self.query_text)

                # FETCH RECORDS
                if records >= 1:
                    i = 1
                    while True and i <= records:
                        row = cursor.fetchone()
                        if row == None:
                            break
                        if self.debug == True:
                            print(str(row))
                        dataset.append(str(row))
                        i = i + 1
                else:
                    while True:
                        row = cursor.fetchone()
                        if row == None:
                            break
                        if self.debug == True:
                            print(str(row))
                        dataset.append(str(row))

                    if self.debug == True:
                        for row in cursor.fetchall():
                            print(str(row))
            
                transaction_result = 0
            except:
                if self.debug == True: 
                    print("Error! Return NO_DATA")
                transaction_result = -1
                dataset.append("NO_DATA")
            finally:
                try:
                    # DELETE CURSOR
                    cursor.close()
                    if self.debug == True: 
                        print("Cursor Closed!")
                    return transaction_result, dataset
                except:
                    # CURSOR WAS NOT OPENED
                    if self.debug == True: 
                        print("Cursor was already closed!")
                    if transaction_result == 0:
                        transaction_result = -2
                        return transaction_result, dataset
                    else:
                        transaction_result = -3
                        return transaction_result, dataset
        else:
            if self.debug == True: 
                print(self.query_text.find("SELECT"))
                print("SELECT query not valid! Check if your query is a SELECT query!")
            transaction_result = -4
            dataset.append("NO_DATA")
            return transaction_result, dataset
            

    # INSERT/UPDATE/DELETE/TRUNCATE QUERY
    # RETURN:
    #  0 --> OK TRANSCATION SUCCEDED
    # -1 --> ERROR! ROLLBACK TRANSACTION
    # -2 --> OK TRANSACTION SUCCEDED, BUT IT COULDN'T CLOSE CURSOR
    # -3 --> ERROR! ROLLBACK TRANSACTION, BUT IT COULDN'T CLOSE CURSOR
    # -4 --> ANYTHING TRANSACTION WAS COMMITTED
    def insUpdDelQueryData(self):
        # TRANSACTION RESULT
        transaction_result = 0
        # CHECK IF QUERY is <> SELECT
        sql_methods_allowed = ['INSERT', 'DELETE', 'UPDATE', 'TRUNCATE', 'MERGE']
        if any(word in self.query_text.upper()[:8] for word in sql_methods_allowed):
            if self.debug == True:
                print("Ok, query accepted!")
            try:
                # CREATING NEW CURSOR
                cursor = self.conn.cursor()
                if self.debug == True: 
                    print("New Cursor Created!")

                # EXECUTE QUERY
                cursor.execute(self.query_text)
                if self.debug == True:
                    print(self.query_text)

                # SAVE RESULT TO DB
                cursor.commit()
                if self.debug == True:
                    print("Data successfully saved!")

                transaction_result = 0
            except:
                # CANCEL PREVIOUS CHANGES
                cursor.rollback()
                if self.debug == True: 
                    print("Error! No Data inserted, abort process! Rollback")
                transaction_result = -1
            finally:
                try:
                    # DELETE CURSOR
                    cursor.close()
                    if self.debug == True: 
                        print("Cursor Closed!")
                    return transaction_result
                except:
                    # CURSOR WAS NOT OPENED
                    if self.debug == True: 
                        print("Cursor was already closed!")
                    if transaction_result == 0:
                        transaction_result = -2
                        return transaction_result
                    else:
                        transaction_result = -3
                        return transaction_result
        else:
            if self.debug == True: 
                print(self.query_text.find("Query doesn't match with allowed methods!"))
                print("Error! Query isn't INSERT or UPDATE or DELETE or TRUNCATE!")
            transaction_result = -4
            return transaction_result