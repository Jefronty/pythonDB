# pythonDB
package to connect and interact with MySQL/MariaDB, MS SQL Server, and PostgreSQL databases

prerequisites*:
- MySQL: MySQLdb module for MySQL or MariaDB database
- MSSQL: pymssql module for MS SQL Server database
- PostgreSQL: psycopg2 module for PostgreSQL database

optional:
- xlsxwriter: used in save_result method if you choose to output to XLSX file

DB class contains multiple methods that can be used by each type of database.
MySQL, MSSQL, PSQL subclasses set type and utilize the methods of the parent class

\* now can be used if only a single database package is available for import
