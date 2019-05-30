CREATE OR REPLACE PACKAGE dev_Pkg_utl_csv 
AUTHID CURRENT_USER 
as
/* **************************************************************************
* $HeadUrl: $
* $Id: pkg_utl_csv-def.sql 65 2015-05-28 11:57:09Z Lam.Bon-Minh $ 
* **************************************************************************/
PROCEDURE insert2table (
   p_csv_string                   VARCHAR2
 , p_target_object                VARCHAR2
 , p_target_schema                VARCHAR2 DEFAULT user
 , p_delete_before_insert2table   BOOLEAN DEFAULT TRUE
 , p_col_sep varchar2 default ';' 
 , p_decimal_point_char varchar2 default '.'
 , p_date_format varchar2 default 'dd.mm.yyyy'
 , p_create_table boolean default false
 , p_create_column_length integer default 100
 , p_standalone_head_line varchar2 default null
 )
/* **************************************************************************
* this procedure insert2tables data into a specified target table or view.in a given schema.  
* The source data is the content of a CSV-file passed as input parameter (up to 32 KB).
* Following assumptions are made:  
* -- The target column names is found in the first line
* -- The column values for each row is given as text characters completely in one text line
* -- Both column names and column values are separated by the input parameter P_COL_SEP which
*    is never part of any column name or value
* -- if more column values are found in one line than there column names, the trailing columns
*    are silently ignored
* -- if fewer column values are found in one line than there are column names, the missing columns
*    are treated as nulls
* 
* Argument description
*  p_csv_string                 
*  p_target_object              
*  p_target_schema              
*  p_delete_before_insert2table 
*  p_col_sep varchar2 	  
*  p_decimal_point_char   in some countries, a comma is used as decimal point, in such case the dafault must be overriden
*  p_date_format 		  the date mask to convert an string into date type in case the target column requires so 
*  p_create_table         signals if the table needs to be created based on the extracted or specified column names
*  p_create_column_length if p_create_table is set, the uniform varchar2 length for all new columns
*  p_standalone_head_line in p_csv_string may, the head line containing the column names may be omitted, then it needs to be supplied here
* 
* This approach works well for target columns of type integer and character. For other data types
* such as date and real number, a more elaborate solution would be required.      
*
* Usage: 
  begin
   pkg_utl_csv.insert2table( p_target_object => '?'
     ,p_target_schema => user -- optional
     ,p_csv_string => 'put your data here'
     );
   end;
***************************************************************/ 
;
FUNCTION gen_update_for_unquoting( 
   p_target_object       VARCHAR2
 , p_target_column       VARCHAR2
 , p_target_schema       VARCHAR2 DEFAULT user
) RETURN VARCHAR2 
;


FUNCTION get_ext_table_ddl (
   p_header_line varchar2 
 , p_target_object     VARCHAR2 DEFAULT NULL
 , p_file_name         VARCHAR2 DEFAULT NULL
 , p_ora_directory     VARCHAR2 DEFAULT NULL
 , p_col_sep varchar2 default ';' 
 , p_rec_sep varchar2 default CHR(10) 
 , p_create_column_length integer default 1000
)
RETURN VARCHAR2
/* **************************************************************************
* this function generates a DDL statemet for creating an external table.
* It is useful when your CSV data is larger than 32K so that it cannot be passed as a literal
* parameter. You can upload the data as CSV file if you can talk the DBA into cooperating. 
* When the DBA grant your schema read write access to an Oracle directory and you have uploaded
* run the CREATE TABLE statement. After that you just need to select the external table to view
* or use the data.
* 
* Argument description
*  p_target_object              The name of the external table
*  p_ora_directory              
*  p_file_name 
*  p_col_sep varchar2 	  
*  p_create_column_length if p_create_table is set, the uniform varchar2 length for all new columns
*  p_header_line  the head line containing the column names of the CSV file
* 
/**************************************************************/
;

PROCEDURE insert2table_from_file (
   p_file                              VARCHAR2
 , p_directory                         VARCHAR2
 , p_target_object                     VARCHAR2
 , p_target_schema                     VARCHAR2 DEFAULT user
 , p_delete_before_insert2table              BOOLEAN DEFAULT TRUE
 , p_col_sep varchar2 default ';' 
 , p_decimal_point_char varchar2 default '.'
 , p_date_format varchar2 default 'dd.mm.yyyy'
 , p_create_table boolean default false
 , p_create_column_length integer default 100
 , p_standalone_head_line varchar2 default null
 , p_max_records_expected NUMBER default 10000
)
;

END;
/

SHOW errors