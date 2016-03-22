--drop table test_csv_insert2table ;

create table test_csv_insert2table (
	ch1 varchar2(10)
	,ch2 varchar2(10)
	,dt date --default sysdate: does not work since the insert procedure does bind a value
);

begin 	
        PKG_UTL_CSV.INSERT2TABLE(
                P_CSV_STRING => 
q'{CH1;CH2;DT
aaaa;bbbb;2014.02.13
xxx;;
kiku;lll;
;agfa;
}'				
                ,P_TARGET_OBJECT =>   upper('test_csv_insert2table')
            --    ,P_TARGET_SCHEMA =>     ?P_TARGET_SCHEMA
                ,P_DELETE_BEFORE_INSERT2TABLE => true
                ,P_COL_SEP =>   ';'
           --     ,P_DECIMAL_POINT_CHAR =>        ?P_DECIMAL_POINT_CHAR
                ,P_DATE_FORMAT =>     'yyyy.mm.dd'
                ,P_CREATE_TABLE =>   false);
end;
/				

alter session set nls_date_format =   'yyyy.mm.dd hh24:mi:ss';

select *
from test_csv_insert2table;