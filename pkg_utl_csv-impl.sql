CREATE OR REPLACE PACKAGE BODY Pkg_utl_csv 
as
/* **************************************************************************
* $HeadUrl: $
* $Id: pkg_utl_csv-impl.sql 65 2015-05-28 11:57:09Z Lam.Bon-Minh $ 
***************************************************************/ 
   gc_pkg_name constant varchar2(30) := 'Pkg_utl_csv';
   gc_nl        CONSTANT VARCHAR2 (10)      := CHR (10); 
   gc_dos_eol        CONSTANT VARCHAR2 (10)      := CHR (13) || CHR (10);   -- DOS style End Of Line
   gc_unix_eol       CONSTANT VARCHAR2 (10)      := CHR (10);   -- Unix style End Of Line
   gc_dos_eol_len    CONSTANT PLS_INTEGER        := length/*c*/ (gc_dos_eol);
   gc_unix_eol_len   CONSTANT PLS_INTEGER        := length/*c*/ (gc_unix_eol);
   g_col_sep             varchar2(10) ;
   g_col_sep_len             INTEGER ;

  type t_column_dtype_map is table of all_tab_columns.data_type%type index by varchar2(30);
  type t_column_dtype_tab is table of all_tab_columns.data_type%type ;
   
   procedure loginfo ( p1  varchar2, p2 varchar2) as
   begin null;
   end loginfo;
   
   procedure logerror ( p1  varchar2, p_err_code integer, p3 varchar2) as
   begin null;
   end logerror;
   
   procedure debug ( p1  varchar2, p2 varchar2) as
   begin null;
   end debug;
   
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
   BEGIN
      debug(lc_cntxt,'l_line_len: '||l_line_len||' First 10 chars: '||substr(p_line, 1,10) );
      WHILE l_scan_pos < l_line_len LOOP
         l_sep_pos := INSTR (p_line, g_col_sep, l_scan_pos);
		debug(lc_cntxt, 'l_scan_pos: '||to_char(l_scan_pos) ||' l_sep_pos: '||to_char(l_sep_pos) );
         l_column :=
            CASE
               WHEN l_sep_pos > 0 THEN substr/*c*/ (p_line, l_scan_pos, l_sep_pos - l_scan_pos)
               ELSE substr/*c*/ (p_line, l_scan_pos)
            END;
         ltab_column (ltab_column.COUNT + 1) := l_column;
         debug(lc_cntxt, 'l_column=' || substr(l_column, 1, 30)||case when length(l_column) > 30 then '..' end  );
         l_scan_pos := l_scan_pos + 
		case when l_column is not null then length/*c*/ (l_column) else 0 end
		+ g_col_sep_len;
		debug(lc_cntxt, 'l_scan_pos: '||l_scan_pos);
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
   l_create_tab_stmt long;
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
   g_col_sep_len                     := length/*c*/ (p_col_sep);
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
		for i in 1 .. ltab_col_nam.count loop
			l_create_tab_stmt := 
			case
				when i = 1 then 'create table '||p_target_schema||'.'||p_target_object||'('
				else l_create_tab_stmt||','
			end ||gc_nl
			||quote_str( ltab_col_nam(i) )||' varchar2('||p_create_column_length||')'
			;
		end loop; -- over column names
		-- finalize column list
		l_create_tab_stmt := l_create_tab_stmt||')';
		begin
			--pck_std_log.info_long(gc_pkg_name, lc_procname, 'creating target table with stmt: '||l_create_tab_stmt );
			execute immediate l_create_tab_stmt;
--		exception
--			when others then
--				loginfo(lc_cntxt, 'DDL stmt was: '|| l_create_tab_stmt);
--				raise;
		end create_table;
   end if; -- p_create_table
   -- set up dynamic insert2table statement
   l_insert2table_stmt :=
          'insert into ' || CASE
             WHEN p_target_schema IS NOT NULL THEN p_target_schema || '.'
          END || p_target_object || '(';

   FOR i IN 1 .. ltab_col_nam.COUNT LOOP
      l_insert2table_stmt := l_insert2table_stmt || CASE
                          WHEN i > 1 THEN ','
                       END || quote_str(ltab_col_nam (i));
   END LOOP;   -- over column names

   l_insert2table_stmt := l_insert2table_stmt || ') values (';

   FOR i IN 1 .. ltab_col_nam.COUNT LOOP
      l_insert2table_stmt := l_insert2table_stmt || CASE
                          WHEN i > 1 THEN ', '
                       END || ':B' || TO_CHAR (i);
   END LOOP;   -- over declaration of bind variables

   l_insert2table_stmt := l_insert2table_stmt || ')';

   BEGIN
      DBMS_SQL.parse (l_cur, l_insert2table_stmt, DBMS_SQL.native);
   EXCEPTION
      WHEN OTHERS THEN
         loginfo (lc_cntxt, SUBSTR ('Error on parse: ' || l_insert2table_stmt, 1, 255));
         RAISE;
   END parse_sql;
   lmap_column_dtype := get_column_dtype_map(p_schema=> p_target_schema,
    p_table => p_target_object , ptab_column => ltab_col_nam
    );

         /* save current numeric_chars setting before changing them
    */
	select value into l_nls_sess_num_chars
	from nls_session_parameters
	where parameter = 'NLS_NUMERIC_CHARACTERS'
	;
   execute immediate 'alter session set NLS_NUMERIC_CHARACTERS = '''
	||case when p_decimal_point_char = ',' then ',.' else '.,' end
	||''''
	;

	select value into l_nls_sess_date_format
	from nls_session_parameters
	where parameter = 'NLS_DATE_FORMAT'
	;
   execute immediate 'alter session set NLS_DATE_FORMAT = '''
	||p_date_format
	||''''
	;

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

END insert2table;

end; -- package 
/

SHOW errors