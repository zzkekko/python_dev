# Python_Dev

This is a simple way to launch, through python code, to Microsoft SQL Server, some queries like SELECT, INSERT, UPDATE, DELETE, TRUNCATE, MERGE.

To try this class you have to remind:
- IP or DSN address of your MS SQL Server
- Username
- Password
- Database Name
- to have pyodbc package installed

i.e
- "192.168.0.100" --> ServerAddress
- "DB_TEST" --> DatabaseName
- "user_test" --> Username
- "pwd_test" --> Password

a driver to connect:
EXAMPLE OF DRIVER's:
- {SQL Server}
- {SQL Native Client}
- {SQL Server Native Client 10.0}
- {SQL Server Native Client 11.0}
- {ODBC Driver 11 for SQL Server}
- {ODBC Driver 13 for SQL Server} --> Default


## TRY THIS ##
- from pymssql_rud import *;
- c = pymssql_rud();
## (optional: if you want to enable debug, default = False) ##
- c.setDebug(True);
- c.setServerAddress("192.168.0.100");
- c.setDatabaseName("DB_TEST");
- c.setUserName("user_test");
- c.setPassword("pwd_test");
- c.setConnectionString();
- c.openConnection();
- c.check_conn();
- c.closeConnection();

Generally, when 0 is returned, it means that all processes are good!


