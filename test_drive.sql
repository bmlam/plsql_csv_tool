
desc log_table

select * from all_directories
;
alter session set nls_date_format = 'rr.mm.dd hh24:mi:ss';

exec dbms_session.reset_package;
exec pck_std_log.switch_debug( true );
begin
 pkg_utl_csv.insert2table_from_file( p_file=> 'short.csv', p_directory=> 'LAM_EXT_TABLES'
			,P_TARGET_OBJECT =>    'IMP_BANK_XACT'
			,P_TARGET_SCHEMA =>    user
		  ,P_DELETE_BEFORE_INSERT2TABLE =>      TRUE
			,P_COL_SEP =>  ';'
			,P_DECIMAL_POINT_CHAR =>        ','
	--        ,P_DATE_FORMAT =>       ?P_DATE_FORMAT
			--,P_CREATE_TABLE =>     true
			--,P_CREATE_COLUMN_LENGTH =>    60
			,P_STANDALONE_HEAD_LINE =>  -- Note that column names are embraced automatically with double quotes!
	q'{Kontonummer;Buchungsdatum;Valuta;Empfaenger 1;Empfaenger 2;Verwendungszweck;Betrag;Waehrung}'		
--, p_create_table=>true
--, p_create_column_length=>500
);
end;
/

select id, info_level il, err_code err
, log_ts
, replace ( caller_position, 'package body ' ) call_pos
, text
from log_table
where log_ts > trunc( sysdate-0 ) --
and id >= 326761
order by id asc
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
