use testdb1;
GO

IF OBJECT_ID ( 'pkg_utl_csv__insert2table', 'P' ) IS NOT NULL
    DROP PROCEDURE pkg_utl_csv__insert2table;
GO


-- insert the CSV literal into the given target table
-- it is strongly recommended to use staging/import table
-- hence an option to delete the table content beforehand is provided
-- if @a_header_line is not provided, it is assumed to be the first line in the csv literal 








CREATE PROCEDURE pkg_utl_csv__insert2table
 @p_target_object    VARCHAR(100)
 ,@p_csv_string      NVARCHAR(4000)
 ,@p_col_sep     NVARCHAR(10) = N';'
 ,@p_standalone_head_line VARCHAR(1000) = NULL 
 ,@p_delete_before_insert2table VARCHAR(1) = 'N'

AS
BEGIN
DECLARE 
   @records  TABLE ( columnValue NVARCHAR(4000) )
DECLARE 
   @tgtColumns  TABLE ( columnName NVARCHAR(4000) )
DECLARE @msg VARCHAR(1000)
   ,@insertHandle Int
   ,@loopIx Int
   ,@insertColumnClause NVARCHAR(1000)
   ,@insertValueClause NVARCHAR(1000)
   ,@bindVarsSpecs   NVARCHAR(1000)
   ,@DOS_LINE_BREAK NVARCHAR(2)
   ,@UNIX_LINE_BREAK NVARCHAR(2)
   ,@lineBreakStyleIsDOS BIT
   ,@recordCount Int
   ,@columnHeadLine NVARCHAR(1000)
   ,@tgtColName    NVARCHAR(100)

   -- 
   SET @msg = 'tgt table ' + @p_target_object + ' csv size: ' + CAST ( LEN(@p_csv_string) as varchar(5))
   EXEC pkg_std_log__dbx @msg 

   -- the following does not seem to work!
   -- exec pkg_std_log__set_quota 'session_dbx_quota', 50

   -- Determine line break style before splitting into lines

   SET @DOS_LINE_BREAK = CAST( CHAR(13) + CHAR(10) AS NVARCHAR(2))
   SET @UNIX_LINE_BREAK = CAST( CHAR(10) AS NVARCHAR(2))

   IF CHARINDEX( @DOS_LINE_BREAK, @p_csv_string) > 0 SET @lineBreakStyleIsDOS = 1
   ELSE  SET @lineBreakStyleIsDOS = 0

   IF @lineBreakStyleIsDOS = 1
   BEGIN
      INSERT @records ( columnValue )
      SELECT columnValue FROM tools__split2StringElements( @p_csv_string, @DOS_LINE_BREAK )
   END 
   ELSE
   BEGIN
      INSERT @records ( columnValue )
      SELECT columnValue FROM tools__split2StringElements( @p_csv_string, @UNIX_LINE_BREAK )
   END
   SELECT @recordCount = COUNT(1) FROM @records
   
   SET @msg = 'recordCount: ' + CAST( @recordCount AS NVARCHAR(10) ) 
   EXEC pkg_std_log__dbx @msg

   -- Check which column header we should use 
   -- either it is the first line of CSV string
   -- or provided explicitly as input parameter
   IF @p_standalone_head_line IS NULL 
   BEGIN 
      SELECT TOP 1 @columnHeadLine = columnValue FROM @records
   END
   ELSE SET @columnHeadLine = @p_standalone_head_line

   INSERT @tgtColumns ( columnName )
   SELECT columnValue FROM tools__split2StringElements( @columnHeadLine, @p_col_sep )

   -- construct INSERT statement 

   DECLARE tgtColCursor CURSOR FOR 
      SELECT columnName FROM @tgtColumns

   OPEN tgtColCursor
   FETCH NEXT FROM tgtColCursor INTO @tgtColName 

   SET @loopIx = 0 
   WHILE @@FETCH_STATUS = 0
   BEGIN
      SET @loopIx += 1 
      SET @insertColumnClause = 
         CASE WHEN @insertColumnClause IS NULL 
         THEN 
            N'INSERT ' + @p_target_object + N'('
         ELSE 
            @insertColumnClause + N', '
         END
         + @tgtColName

       SET @insertValueClause = 
         CASE WHEN @insertValueClause IS NULL 
         THEN 
            N' VALUES ( '
         ELSE 
            @insertValueClause + N', '
         END
         + N'@b' + CAST( @loopIx AS NVARCHAR(3))

       SET @bindVarsSpecs = 
         CASE WHEN @bindVarsSpecs IS NOT NULL 
         THEN 
            @bindVarsSpecs + N', '
         ELSE N''
         END
         + N'@b' + CAST( @loopIx AS NVARCHAR(3)) + N' NVARCHAR(1000)'

      FETCH NEXT FROM tgtColCursor INTO @tgtColName 
   END
   SET @insertColumnClause += N')'
   SET @insertValueClause  += N')'

   CLOSE tgtColCursor

   SET @msg = N'insert column clause: ' + @insertColumnClause 
   EXEC pkg_std_log__dbx @msg 
   SET @msg = N'insert values clause: ' + @insertValueClause 
   EXEC pkg_std_log__dbx @msg 
   SET @msg = N'bind var specs: ' + @bindVarsSpecs 
   EXEC pkg_std_log__dbx @msg 

END;
GO

