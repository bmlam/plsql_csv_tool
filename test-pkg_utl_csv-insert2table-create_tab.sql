col ch1 format a20
col ch2 format a20
col DT format a20

set pagesize 100 lines 120

alter session set nls_date_format = 'yyyy-mon-dd hh24:mi:';

begin 	
        PKG_UTL_CSV.INSERT2TABLE(
                P_CSV_STRING => 
q'{CH1;CH2;DT
aaaa;bbbb;2010.12.31
xxx;;
kkkk;lll;
;beta;
}'				
                ,P_TARGET_OBJECT =>   upper('test_csv_insert_dyn_create')
            --    ,P_TARGET_SCHEMA =>     ?P_TARGET_SCHEMA
                ,P_DELETE_BEFORE_INSERT2TABLE => true
                ,P_COL_SEP =>   ';'
           --     ,P_DECIMAL_POINT_CHAR =>        ?P_DECIMAL_POINT_CHAR
           --     ,P_DATE_FORMAT =>     'yyyy.mm.dd'
                ,P_CREATE_TABLE =>   true
                ,P_CREATE_column_length => 50
				);
end;
/				


select * from test_csv_insert_dyn_create;