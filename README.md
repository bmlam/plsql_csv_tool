# plsql_csv_tool
A tool written in PLSQL for converting data in csv file into database tables. The database table can be created on the fly
because the first line/record must contain the column names. The csv data is passed as varchar2 parameter, hence the 32k size limit applies

To install and use, you need an Oracle database account with CREATE TABLE and PROCEDURE privileges and tablespace quota for your data. The installed code comprises of a single PLSQL package. It has been tested with SQLPLUS. To install type in the following line after login:

@pkg_utl_csv-def.sql
@pkg_utl_csv-impl.sql

To use, look at one of the two test scripts:

test-pkg_utl_csv-insert2table-create_tab.sql
test-pkg_utl_csv-insert2table.sql

The former instructs to create the target table before loading data. In this case all columns are defined as varchar2 but you can specify a uniform length of all columns. The latter assume the table with the proper column names and data types already exists.
