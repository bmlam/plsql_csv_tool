CREATE OR REPLACE PACKAGE BODY dev_Pkg_utl_csv 
as
/* **************************************************************************
* $HeadUrl: $
* $Id: pkg_utl_csv-impl.sql 65 2015-05-28 11:57:09Z Lam.Bon-Minh $ 
***************************************************************/ 
   
   gc_pkg_name constant varchar2(30) := $$plsql_unit;
   gc_nl        CONSTANT VARCHAR2 (10)      := CHR (10); 
   gc_dos_eol        CONSTANT VARCHAR2 (10)      := CHR (13) || CHR (10);   -- DOS style End Of Line
   gc_unix_eol       CONSTANT VARCHAR2 (10)      := CHR (10);   -- Unix style End Of Line
   gc_dos_eol_len    CONSTANT PLS_INTEGER        := length/*c*/ (gc_dos_eol);
   gc_unix_eol_len   CONSTANT PLS_INTEGER        := length/*c*/ (gc_unix_eol);
   g_col_sep             varchar2(10) ;

  type t_column_dtype_map is table of all_tab_columns.data_type%type index by varchar2(30);
  type t_column_dtype_tab is table of all_tab_columns.data_type%type ;
   
 PROCEDURE gp_set_num_chars_with_backup -- forward declaration
  (  p_new_decimal_point VARCHAR2
   , po_old_value OUT VARCHAR2 ) 
  ;
 PROCEDURE gp_create_target_table -- forward declaration
( p_target_schema VARCHAR2
, p_table_name  VARCHAR2
, ptab_col_name dbms_sql.varchar2a
, p_create_column_length VARCHAR2
)
  ; 
PROCEDURE gp_compose_insert_stmt -- forward declaration
( p_target_schema VARCHAR2
, p_table_name  VARCHAR2
, ptab_col_name dbms_sql.varchar2a
, po_sql_text OUT VARCHAR2
);
PROCEDURE gp_set_date_format_with_backup-- forward declaration
( p_new_value VARCHAR2
  ,po_old_value OUT VARCHAR2
);

   $IF $$logging_tool_available = 0 $THEN 
	   procedure loginfo ( p1  varchar2, p2 varchar2) as
	   begin null;
	   end loginfo;
	   
	   procedure logerror ( p1  varchar2, p_err_code integer, p3 varchar2) as
	   begin null;
	   end logerror;
	   
	   procedure debug ( p1  varchar2, p2 varchar2) as
	   begin dbms_output.put_line(p2);
	   end debug;
   $END 
      /**************************************************************/
   FUNCTION get_column_dtype_map (
      ptab_column                              dbms_sql.varchar2a
      ,p_table varchar2
      ,p_schema varchar2
      )
      RETURN t_column_dtype_map
   /**************************************************************/
   ; -- forward declaration
/**************************************************************/
   function quote_str( 
	p_inp varchar2
	,p_quote_char varchar2 default '"'
   ) 
   return varchar2
   /**************************************************************/
   as
   begin return p_quote_char||p_inp||p_quote_char;
   end quote_str;

   /**************************************************************/
   FUNCTION get_all_columns (
      p_line                              VARCHAR2
	  )
      RETURN DBMS_SQL.varchar2a 
   /**************************************************************/
      AS
	  lc_cntxt constant varchar2(61) := gc_pkg_name||'.get_all_columns';
      l_sep_pos     INTEGER;
      l_line_len    INTEGER            := LENGTH (p_line);
      l_scan_pos    INTEGER            := 1;
      l_column      LONG;
      ltab_column   DBMS_SQL.varchar2a;
      lc_col_sep_len CONSTANT INTEGER := LENGTH( g_col_sep );
   BEGIN
      debug(lc_cntxt,'l_line_len: '||l_line_len||' First 10 chars: '||substr(p_line, 1,10) );
      WHILE l_scan_pos < l_line_len LOOP
         l_sep_pos := INSTR (p_line, g_col_sep, l_scan_pos);
         debug(lc_cntxt, 'line:'||$$plsql_line||' l_scan_pos: '||to_char(l_scan_pos) ||' l_sep_pos: '||to_char(l_sep_pos) );
         l_column :=
            CASE
               WHEN l_sep_pos > 0 THEN substr/*c*/ (p_line, l_scan_pos, l_sep_pos - l_scan_pos)
               ELSE substr/*c*/ (p_line, l_scan_pos)
            END;
         ltab_column (ltab_column.COUNT + 1) := l_column;
         debug(lc_cntxt, 'line:'||$$plsql_line||' l_column=' || substr(l_column, 1, 30)||case when length(l_column) > 30 then '..' end  );
         l_scan_pos := l_scan_pos + 
            case when l_column is not null then length/*c*/ (l_column) else 0 end
            + lc_col_sep_len;
         debug(lc_cntxt, 'line:'||$$plsql_line||' l_scan_pos: '||l_scan_pos);
      END LOOP;
		debug(lc_cntxt, 'returning ltab_column.count: '||ltab_column.count);

      RETURN ltab_column;
EXCEPTION
   WHEN OTHERS THEN
	logerror(lc_cntxt, sqlcode, dbms_utility.format_error_backtrace);
      raise;
   END get_all_columns;

   /**************************************************************/
   FUNCTION get_column_dtype_map (
      ptab_column                              dbms_sql.varchar2a
      ,p_table varchar2
      ,p_schema varchar2
      )
      RETURN t_column_dtype_map
   /**************************************************************/
      AS
	  lc_cntxt constant varchar2(61) := gc_pkg_name||'.get_column_dtype_map';
      ltab_return t_column_dtype_map;
      l_column_dbx VARCHAR2(100);
   begin
    for i in 1 .. ptab_column.count loop
       l_column_dbx := p_table||'.'||ptab_column(i);
       select data_type into ltab_return( ptab_column (i) )
       from all_tab_columns
       where table_name = p_table
         and owner = p_schema
         and column_name = ptab_column(i)
         ;
    end loop; -- over ptab_column
    return ltab_return;
EXCEPTION
   WHEN NO_DATA_FOUND THEN
      RAISE_APPLICATION_ERROR( -20000, 'Could not determine data type for '||l_column_dbx );
   WHEN OTHERS THEN
	logerror(lc_cntxt, sqlcode, dbms_utility.format_error_backtrace);
      raise;
   end get_column_dtype_map;
   
/**************************************************************/
PROCEDURE insert2table (
   p_csv_string                        VARCHAR2
 , p_target_object                     VARCHAR2
 , p_target_schema                     VARCHAR2 DEFAULT user
 , p_delete_before_insert2table              BOOLEAN DEFAULT TRUE
 , p_col_sep varchar2 default ';' 
 , p_decimal_point_char varchar2 default '.'
 , p_date_format varchar2 default 'dd.mm.yyyy'
 , p_create_table boolean default false
 , p_create_column_length integer default 100
 , p_standalone_head_line varchar2 default null
)
--
AS
   /**************************************************************/
	  lc_procname constant varchar2(61) :='insert2table';
	  lc_cntxt constant varchar2(61) := gc_pkg_name||'.'||lc_procname;
  lmap_column_dtype t_column_dtype_map;
   l_scan_pos                 PLS_INTEGER        := 1;
   l_line_no                  INTEGER            := 0;
   l_line                     LONG;
   l_tot_len                  INTEGER;
   l_ins_cnt                  INTEGER := 0;
   ltab_col_nam               DBMS_SQL.varchar2a;
   ltab_col_val               DBMS_SQL.varchar2a;
   l_insert2table_stmt              LONG;
   l_cur                      INTEGER            := DBMS_SQL.open_cursor;
   l_stat                     INTEGER;
   l_nls_sess_num_chars varchar2(100);
   l_nls_sess_date_format varchar2(100);
   l_num_bind number;
   l_sql long;

   /**************************************************************/
   procedure i$reset_line_cur
   /**************************************************************/
      AS
	begin
		l_scan_pos    := 1;
		l_line_no     := 0;
   end i$reset_line_cur;
   
   /**************************************************************/
   FUNCTION i$get_next_line
      RETURN VARCHAR2 
   /**************************************************************/
      AS
      l_eol_pos   PLS_INTEGER;
      l_skip      PLS_INTEGER := 0;
      l_line      LONG;
   BEGIN
      l_eol_pos := instrc (p_csv_string, gc_dos_eol, l_scan_pos);
	  
      IF l_eol_pos > 0 THEN
         l_skip := gc_dos_eol_len;
      ELSE
         l_eol_pos := instrc (p_csv_string, gc_unix_eol, l_scan_pos);

         IF l_eol_pos > 0 THEN
            l_skip := gc_unix_eol_len;
         END IF;   -- found Unix style
      END IF;   -- found DOS style

      l_line :=
         CASE
            WHEN l_eol_pos > 0 THEN substr/*c*/ (p_csv_string, l_scan_pos, l_eol_pos - l_scan_pos)
            ELSE substr/*c*/ (p_csv_string, l_scan_pos)
         END;
      l_scan_pos := l_scan_pos + length/*c*/ (l_line) + l_skip;
      debug (lc_cntxt, 'eol_pos=' || l_eol_pos || ' skip=' || l_skip || ' scan_pos=' || l_scan_pos);
      RETURN l_line;
   END i$get_next_line;

   
BEGIN
   -- termination criteria for loop over lines in CSV string
   l_tot_len := length/*c*/ (p_csv_string);

   debug (lc_cntxt, 'l_tot_len ='||l_tot_len
		||' p_standalone_head_line ='||p_standalone_head_line
   );
   --
   g_col_sep                         := p_col_sep; 
   i$reset_line_cur;
   -- get the column names from the first line
	if p_standalone_head_line is null then
		l_line := i$get_next_line;
		if length(l_line) = 1 or l_line is null  then
			raise_application_error(-20000, 'the first line of the CSV text appears to be empty!');
		end if; -- header line empty
		ltab_col_nam := get_all_columns (l_line);
	else
		ltab_col_nam := get_all_columns (p_standalone_head_line);
	end if; -- check p_standalone_head_line
   debug (lc_cntxt, 'Col count: ' || ltab_col_nam.COUNT);
   
   /* Create target table if applicable
   */
  if p_create_table then
    gp_create_target_table( p_target_schema=> p_target_schema, p_table_name => p_target_object
      , ptab_col_name => ltab_col_nam
      , p_create_column_length=> p_create_column_length
    );
   end if; -- p_create_table
   
   -- set up dynamic insert2table statement
   gp_compose_insert_stmt( p_target_schema => p_target_schema
    , p_table_name  => p_target_object
    , ptab_col_name => ltab_col_nam
    , po_sql_text => l_insert2table_stmt
    );
    BEGIN
      DBMS_SQL.parse (l_cur, l_insert2table_stmt, DBMS_SQL.native);
   EXCEPTION
      WHEN OTHERS THEN
         loginfo (lc_cntxt, SUBSTR ('Error on parse: ' || l_insert2table_stmt, 1, 500));
         RAISE;
   END parse_sql;
   lmap_column_dtype := get_column_dtype_map(p_schema=> p_target_schema,
    p_table => p_target_object , ptab_column => ltab_col_nam
    );

  gp_set_num_chars_with_backup( p_new_decimal_point=> p_decimal_point_char, po_old_value => l_nls_sess_num_chars );
  gp_set_date_format_with_backup( p_new_value => p_date_format, po_old_value=> l_nls_sess_date_format );

  
   IF p_delete_before_insert2table THEN
		l_sql := 'delete ' || CASE WHEN p_target_schema IS NOT NULL THEN p_target_schema || '.'
                        END || p_target_object;
		loginfo(lc_cntxt, l_sql);
      EXECUTE IMMEDIATE l_sql;
   END IF;   -- check delete flag


   -- Process lines containing the column values
   WHILE l_scan_pos < l_tot_len
                                   --
                               --    AND l_line_no < 10   -- test only
   LOOP
      l_line := i$get_next_line;
      l_line_no := l_line_no + 1;

      debug (lc_cntxt, 'line ' || l_line_no || ' starts with: ' || SUBSTR (l_line, 1, 30));
      IF l_line IS NOT NULL THEN
         ltab_col_val := get_all_columns (l_line);

         -- bind column values that are specified in the CSV line (it can be null!)
         FOR i IN 1 .. ltab_col_val.COUNT LOOP
            EXIT WHEN i > ltab_col_nam.COUNT;

            BEGIN
               DBMS_OUTPUT.put_line (SUBSTR ('co1 value: ' || ltab_col_val (i), 1, 255) );
               DBMS_SQL.bind_variable (c =>          l_cur, NAME => ':B' || TO_CHAR (i), VALUE => 
                -- for numeric columns, we need to eliminate numeric group separators
                case when lmap_column_dtype( ltab_col_nam(i) ) = 'NUMBER' 
                THEN replace( ltab_col_val (i)
                  , case when p_decimal_point_char=',' then '.' 
                  else ',' end -- specifiy group separator that might occur based on decimal point 
                  ) -- end replace 
                else
                  ltab_col_val (i)
                end -- check data type 
               );
            EXCEPTION
               WHEN OTHERS THEN
                  raise_application_error(-20000, 'Error on bind: Line=' || l_line_no || ' column index ' || TO_CHAR (i)
                                             );
            END bind_column_value;
         END LOOP;   -- over declaration of bind variables

         -- bind column values that are not specified in the CSV line because it contains less items
         -- than the column count
         FOR i IN ltab_col_val.COUNT + 1 .. ltab_col_nam.COUNT LOOP
            BEGIN
               DBMS_SQL.bind_variable (c =>          l_cur, NAME => ':B' || TO_CHAR (i), VALUE => TO_char (NULL));
            EXCEPTION
               WHEN OTHERS THEN
                  raise_application_error(-200000, 'Error on bind: Line=' || l_line_no || ' column index ' || TO_CHAR (i)  );
            END bind_null_value;
         END LOOP;   -- over declaration of bind variables
		begin
			l_stat := DBMS_SQL.EXECUTE (l_cur);
			l_ins_cnt := l_ins_cnt + 1;
        EXCEPTION
               WHEN OTHERS THEN
                  raise_application_error(-20000, 'Line=' || l_line_no ||': '||sqlerrm);
                  RAISE;
        END exec_insert2table_stmt;
      END IF;   -- line not empty

   END LOOP;   -- over CSV string
   loginfo(lc_cntxt, 'Value rows parsed: ' || l_line_no ||' insert2tableed: '||l_ins_cnt);

   COMMIT;
   /* restore nls setting 
   */
   execute immediate 'alter session set NLS_NUMERIC_CHARACTERS = '''
	||l_nls_sess_num_chars
	||''''
	;
   execute immediate 'alter session set NLS_DATE_FORMAT = '''
	||l_nls_sess_date_format
	||''''
	;
EXCEPTION
   WHEN OTHERS THEN
	logerror(lc_cntxt, sqlcode, dbms_utility.format_error_backtrace );
      ROLLBACK;
      raise;
END insert2table;

FUNCTION gen_update_for_unquoting( 
   p_target_object       VARCHAR2
 , p_target_column       VARCHAR2
 , p_target_schema       VARCHAR2 DEFAULT user
) RETURN VARCHAR2 
AS
   l_prolog VARCHAR2(1000);
   l_stmt VARCHAR2(1000);
BEGIN
   l_prolog :=
    'UPDATE '||p_target_schema||'.'||p_target_object||CHR(10)
   ||' SET '||p_target_column||' = '
   ;
   l_stmt := 
   l_prolog
   || q'[ REPLACE ( <column>, '""', '"' ) ]' ||CHR(10)
   || q'[ WHERE <column> LIKE '%""%' ; ]'||CHR(10)
   ||l_prolog
   || q'[ SUBSTR(<column>, 2 ) ]'||CHR(10)
   || q'[ WHERE <column> LIKE '"%'; ]'||CHR(10)
   ||l_prolog
   || q'[ SUBSTR(<column>, 1, LENGTH( <column> ) - 1 ) ]'||CHR(10)
   || q'[ WHERE SUBSTR( <column>, -1 ) =  '"'; ]'||CHR(10)
   ;
   RETURN REPLACE(l_stmt, '<column>', p_target_column );
END gen_update_for_unquoting;

/**************************************************************/

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
--
AS
  lc_procname constant varchar2(61) :='get_ext_table_ddl';
  lc_cntxt constant varchar2(61) := gc_pkg_name||'.'||lc_procname;
   ltab_col_nam               DBMS_SQL.varchar2a;
   l_column_list_source VARCHAR2(4000 CHAR);
   l_column_list_target VARCHAR2(4000 CHAR);
   l_rec_sep_code INTEGER;
   l_ddl_template CONSTANT VARCHAR2(32000 CHAR) := 
q'{CREATE TABLE <table_name> ( 
   <column_list_target>
)
ORGANIZATION EXTERNAL (
  TYPE ORACLE_LOADER
  DEFAULT DIRECTORY <directory>
  ACCESS PARAMETERS (
    RECORDS DELIMITED BY <rec_sep>
    FIELDS TERMINATED BY <col_sep> OPTIONALLY ENCLOSED BY '"'
      ( <column_list_source>
      )
    )
    LOCATION (
      <file_name>
    )
)
}';
   l_ddl VARCHAR2(32000 CHAR);
BEGIN 
   g_col_sep  := p_col_sep;   
	ltab_col_nam := get_all_columns (p_header_line);
	for i in 1 .. ltab_col_nam.count loop
      dbms_output.put_line( 'line '||$$plsql_line||' i:'||i );
      IF i > 1 THEN
         l_column_list_target := l_column_list_target ||', ';
         l_column_list_source := l_column_list_source ||', ';
         dbms_output.put_line( 'line '||$$plsql_line||' i:'||i ||' l_column_list_target:'||l_column_list_target );
      END IF;
		l_column_list_source:= l_column_list_source||quote_str( ltab_col_nam(i) )||' CHAR('||p_create_column_length||')';
		l_column_list_target:= l_column_list_target||quote_str( ltab_col_nam(i) )||' VARCHAR2('||p_create_column_length||')';
	end loop; -- over column names
   l_ddl := REPLACE( l_ddl_template, '<table_name>', p_target_object );
   --
   l_ddl := REPLACE( l_ddl, '<col_sep>', quote_str( p_col_sep, '''' ) );
   l_ddl := REPLACE( l_ddl, '<column_list_source>', l_column_list_source );
   l_ddl := REPLACE( l_ddl, '<column_list_target>', l_column_list_target );
   --
   IF LENGTH( p_rec_sep ) > 1 OR LENGTH( p_rec_sep ) IS NULL THEN
      RAISE_APPLICATION_ERROR( -20001, 'Currently we only support record separator with length of 1 character!' );
   END IF;
   l_rec_sep_code := ASCII( SUBSTR(p_rec_sep, 1, 1))
   ;
   l_ddl := REPLACE( l_ddl, '<rec_sep>', '0x'||quote_str( l_rec_sep_code, '''' ) );
   IF p_ora_directory IS NOT NULL THEN
      l_ddl := REPLACE( l_ddl, '<file_name>', quote_str( p_file_name, '''' ) );
   END IF;
   IF p_file_name IS NOT NULL THEN
      l_ddl := REPLACE( l_ddl, '<directory>', p_ora_directory );
   END IF;
   RETURN l_ddl;
END get_ext_table_ddl;


  /**************************************************************/
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
--
AS
   /**************************************************************/
-- Setup to  be done by DBA: 
--create directory csv_util_load_dir as '/oradata/csv_util_load';
--grant read on directory csv_util_load_dir to service;
-- test: exec dev_pkg_utl_csv.insert2table_from_file( 'test.txt', 'CSV_UTIL_LOAD_DIR',  'no such table' );

  c_cntxt CONSTANT VARCHAR2(100 CHAR) := gc_pkg_name||'.insert2table_from_file';
  c_32k_minus_1 CONSTANT INTEGER := 32767;
  v_fh UTL_FILE.FILE_TYPE;
  v_buf  VARCHAR2(32767 CHAR);
  v_ln_cnt NUMBER := 0;
  v_countdown NUMBER := COALESCE( p_max_records_expected, 10000 );

  vtab_col_nam               DBMS_SQL.varchar2a;
  vtab_col_val               DBMS_SQL.varchar2a;
  v_insert2table_stmt              LONG;
  v_cur                      INTEGER            := DBMS_SQL.open_cursor;
  v_stat                     INTEGER;
  v_nls_sess_num_chars varchar2(100);
  v_nls_sess_date_format varchar2(100);
  v_num_bind number;
  v_sql long;
 
BEGIN
	loginfo( gc_pkg_name||'.'||$$plsql_line, 'file:'||p_file||' p_directory:'||p_directory );
  v_fh:= UTL_FILE.FOPEN( location=>p_directory, filename=> p_file, open_mode=> 'R', max_linesize => c_32k_minus_1
    );
  
  WHILE v_countdown >= 0 
  LOOP 
    BEGIN 
      UTL_FILE.GET_LINE(v_fh, v_buf);
      v_ln_cnt := v_ln_cnt + 1;
      loginfo( c_cntxt, 'input line '||v_ln_cnt||' len:'|| lengthc( v_buf)||' start with: '||substrc( v_buf, 1, 20) );
    EXCEPTION
      WHEN no_data_found THEN 
        loginfo( c_cntxt, 'No more lines found after '||v_ln_cnt||' records');
        v_countdown := 0; 
    END;

    IF vtab_col_nam.COUNT = 0 THEN 
      if p_standalone_head_line is null then
        if length(v_buf) = 1 or v_buf is null  then
          raise_application_error(-20000, 'the first line of the CSV text appears to be empty!');
        end if; -- header line empty
        vtab_col_nam := get_all_columns (v_buf);
      else
        vtab_col_nam := get_all_columns (p_standalone_head_line);
      end if; -- check p_standalone_head_line
      loginfo (c_cntxt, 'Col count: ' || vtab_col_nam.COUNT);

       /* Create target table if applicable
       */
      if p_create_table then
        gp_create_target_table( p_target_schema=> p_target_schema, p_table_name => p_target_object
          , ptab_col_name => vtab_col_nam
          , p_create_column_length=> p_create_column_length
        );
      end if; -- p_create_table
      
      gp_compose_insert_stmt( p_target_schema => p_target_schema
      , p_table_name  => p_target_object
      , ptab_col_name => vtab_col_nam
      , po_sql_text => v_insert2table_stmt
      );

      gp_set_num_chars_with_backup( p_new_decimal_point=> p_decimal_point_char, po_old_value => v_nls_sess_num_chars );
      gp_set_date_format_with_backup( p_new_value=> p_date_format, po_old_value=> v_nls_sess_date_format );
      
    END IF; -- check column names are known
    v_countdown := v_countdown - 1;
    --dbms_output.put_line(v_buf);
  END LOOP; -- over lines 
  UTL_FILE.FCLOSE( v_fh );
  loginfo( c_cntxt, 'Lines found '||v_ln_cnt );
EXCEPTION  
  WHEN OTHERS THEN 
    logerror(c_cntxt, sqlcode, dbms_utility.format_error_backtrace);
    IF utl_file.is_open( v_fh ) THEN 
      utl_file.fclose( v_fh );
    END IF;
    raise;
    
END insert2table_from_file;
  
PROCEDURE gp_set_num_chars_with_backup
  (  p_new_decimal_point VARCHAR2
   , po_old_value OUT VARCHAR2 )
AS
BEGIN
     /* save current numeric_chars setting before changing them
      */
    select value into po_old_value
    from nls_session_parameters
    where parameter = 'NLS_NUMERIC_CHARACTERS'
    ;
     execute immediate 'alter session set NLS_NUMERIC_CHARACTERS = '''
    ||case when p_new_decimal_point = ',' then ',.' else '.,' end
    ||''''
    ;
END gp_set_num_chars_with_backup;

PROCEDURE gp_create_target_table
( p_target_schema VARCHAR2
, p_table_name  VARCHAR2
, ptab_col_name dbms_sql.varchar2a
, p_create_column_length VARCHAR2
) AS
   l_create_tab_stmt long;
BEGIN
		for i in 1 .. ptab_col_name.count loop
			l_create_tab_stmt := 
			case
				when i = 1 then 'create table '||p_target_schema||'.'||p_table_name||'('
				else l_create_tab_stmt||','
			end ||gc_nl
			||quote_str( ptab_col_name(i) )||' varchar2('||p_create_column_length||')'
			;
		end loop; -- over column names
		-- finalize column list
		l_create_tab_stmt := l_create_tab_stmt||')';
		begin
			pck_std_log.info_long(gc_pkg_name, $$plsql_line, 'creating target table with stmt: '||l_create_tab_stmt );
			execute immediate l_create_tab_stmt;
		exception
			when others then
				loginfo($$plsql_unit||':'||$$plq_line, 'DDL stmt was: '|| l_create_tab_stmt);
				raise;
		end create_table;
END gp_create_target_table; 

PROCEDURE gp_compose_insert_stmt
( p_target_schema VARCHAR2
, p_table_name  VARCHAR2
, ptab_col_name dbms_sql.varchar2a
, po_sql_text OUT VARCHAR2
) AS
BEGIN
   po_sql_text :=
          'insert into ' || CASE
             WHEN p_target_schema IS NOT NULL THEN p_target_schema || '.'
          END || p_table_name|| '(';

   FOR i IN 1 .. ptab_col_name.COUNT LOOP
      po_sql_text := po_sql_text || CASE
                          WHEN i > 1 THEN ','
                       END || quote_str(ptab_col_name (i));
   END LOOP;   -- over column names

   po_sql_text := po_sql_text || ') values (';

   FOR i IN 1 .. ptab_col_name.COUNT LOOP
      po_sql_text := po_sql_text || CASE
                          WHEN i > 1 THEN ', '
                       END || ':B' || TO_CHAR (i);
   END LOOP;   -- over declaration of bind variables

   po_sql_text := po_sql_text || ')';
END gp_compose_insert_stmt;


PROCEDURE gp_set_date_format_with_backup
( p_new_value VARCHAR2
  ,po_old_value OUT VARCHAR2
) AS
BEGIN
	select value into po_old_value
	from nls_session_parameters
	where parameter = 'NLS_DATE_FORMAT'
	;
   execute immediate 'alter session set NLS_DATE_FORMAT = '''
	||p_new_value
	||''''
	;
END gp_set_date_format_with_backup;

end; -- package 
/

SHOW errors
