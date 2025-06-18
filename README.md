# pythonDB
package to connect and interact with MS SQL Server and MySQL/MariaDB databases

prerequisites:
- MySQL: MySQLdb module for MySQL or MariaDB database
- MSSQL: pymssql module for MS SQL Server database

optional:
- xlsxwriter: used in save_result method if you choose to output to XLSX file

DB class contains multiple methods that can be used by either type of database.
MySQL and MSSQL subclasses set type and utilize the methods of the parent class
