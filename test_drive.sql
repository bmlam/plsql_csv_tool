sta /Users/bmlam/Dropbox/github-bmlam/csv_tool/plsql_csv_tool/pkg_utl_csv-def.sql

sta /Users/bmlam/Dropbox/github-bmlam/csv_tool/plsql_csv_tool/pkg_utl_csv-impl.sql

desc log_table

select * from all_directories
;
alter session set nls_date_format = 'rr.mm.dd hh24:mi:ss';

set serveroutput on
exec pck_std_log.switch_debug( true );
begin
dev_pkg_utl_csv.insert2table_from_file( '655467696_Umsatzliste-4.csv', 'CSV_UTIL_LOAD_DIR',  'test_hvb'
, p_col_sep=>';'
, P_DELETE_BEFORE_INSERT2TABLE=>true
, p_create_table=>true
, p_create_column_length=>500
);
end;
/

select * from log_table
where timestamp > trunc( sysdate )
order by id desc
;
select * from dict where table_name liKE '%DATABASE%'
;
select * from NLS_DATABASE_PARAMETERS where 1=1
;
drop table test_hvb
;
select * from test_hvb;
with dat_ as ( 
select  '
create table test_hvb (
k o n t o  varchar2(100)
, b u c h s d a t u m varchar2(100)
)
' as foo from dual
) 
select foo
,length(foo) len_org
, length( replace( foo, chr(0) ) ) len_replaced
, dump(foo)
from dat_
;
