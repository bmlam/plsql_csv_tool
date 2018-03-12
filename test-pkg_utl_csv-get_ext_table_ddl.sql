set linesize 400 pages 100

SELECT pkg_utl_csv.get_ext_table_ddl(
                 P_header_line =>   upper('c1;c2;abc')
                ,P_TARGET_OBJECT =>   upper('test_ext_table')
                ,P_ora_directory =>   'test_dir'
                ,P_file_name =>   'test_file'
                ,P_col_sep =>   ';'
                )
from dual;