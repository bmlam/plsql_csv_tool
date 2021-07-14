use testdb1;
GO





--IF OBJECT_ID ( 'pkg_utl_csv__insert2table', 'P' ) IS NULL 
   CREATE PROCEDURE pkg_utl_csv__insert2table AS BEGIN DECLARE @dummy INT; END;
--GO
ALTER PROCEDURE pkg_utl_csv__insert2table
 @p_target_object    VARCHAR(100)
 ,@p_csv_string      NVARCHAR(4000)
 ,@p_col_sep     NVARCHAR(10) = N';'
 ,@p_standalone_head_line VARCHAR(1000) = NULL 
 ,@p_delete_before_insert2table VARCHAR(1) = 'N'
 ,@p_create_table VARCHAR(1) = 'N'
 ,@p_new_column_size Int = 100
AS
-- insert the CSV literal into the given target table
-- it is strongly recommended to use staging/import table
-- hence an option to delete the table content beforehand is provided
-- if @a_header_line is not provided, it is assumed to be the first line in the csv literal 
BEGIN
DECLARE 
   @records  TABLE ( id_ Int, columnValue NVARCHAR(4000) )
DECLARE 
   @tgtColumns  TABLE ( id_ Int, columnName NVARCHAR(4000) )
DECLARE 
   @columnValues  TABLE ( id_ Int, columnValue NVARCHAR(4000) )
DECLARE @msg VARCHAR(1000)
   --,@insertHandle Int
   ,@loopIx Int
   ,@colIx Int
   ,@recordIx Int
   ,@createTableStatement NVARCHAR(4000)
   ,@recordCsv  NVARCHAR(4000)
   ,@columnLiteral  NVARCHAR(4000)
   ,@insertColumnClause NVARCHAR(1000)
   ,@insertValueClause NVARCHAR(1000)
   ,@insertStatement NVARCHAR(4000)
   --,@bindVarsSpecs   NVARCHAR(1000)
   ,@DOS_LINE_BREAK NVARCHAR(2)
   ,@UNIX_LINE_BREAK NVARCHAR(2)
   ,@lineBreakStyleIsDOS BIT
   ,@recordCount Int
   ,@tgtColCount Int
   ,@deletedCnt  Int
   ,@columnHeadLine NVARCHAR(1000)
   ,@tgtColName    NVARCHAR(100)
   ,@shortDML NVARCHAR(1000)

   -- 
   SET @msg = 'tgt table ' + @p_target_object + ' csv size: ' + CAST ( LEN(@p_csv_string) as varchar(5))
   EXEC pkg_std_log__dbx @msg 

   -- the following does not seem to work!
   -- exec pkg_std_log__set_quota 'session_dbx_quota', 50
   --
   -- Determine line break style before splitting into lines
   --
   SET @DOS_LINE_BREAK = CAST( CHAR(13) + CHAR(10) AS NVARCHAR(2))
   SET @UNIX_LINE_BREAK = CAST( CHAR(10) AS NVARCHAR(2))

   IF CHARINDEX( @DOS_LINE_BREAK, @p_csv_string) > 0 SET @lineBreakStyleIsDOS = 1
   ELSE  SET @lineBreakStyleIsDOS = 0

   IF @lineBreakStyleIsDOS = 1
   BEGIN
      INSERT @records ( id_, columnValue )
      SELECT id_, columnValue FROM tools__split2StringElements( @p_csv_string, @DOS_LINE_BREAK )
      ORDER BY id_ 
   END 
   ELSE
   BEGIN
      INSERT @records ( id_, columnValue )
      SELECT id_, columnValue FROM tools__split2StringElements( @p_csv_string, @UNIX_LINE_BREAK )
      ORDER BY id_ 
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
      DELETE TOP (1) FROM @records 
      SET @msg = 'recordCount: ' + CAST( @recordCount AS NVARCHAR(10) ) 
      EXEC pkg_std_log__dbx @msg
   END
   ELSE SET @columnHeadLine = @p_standalone_head_line

   INSERT @tgtColumns ( id_, columnName )
   SELECT id_, columnValue FROM tools__split2StringElements( @columnHeadLine, @p_col_sep )
   ORDER BY id_ 

   SELECT @tgtColCount = COUNT(1) FROM @tgtColumns

   --
   -- construct fragments of INSERT statement , but also of CREATE TABLE 
   --
   DECLARE tgtColCursor CURSOR FOR 
      SELECT columnName FROM @tgtColumns ORDER BY id_

   OPEN tgtColCursor
   FETCH NEXT FROM tgtColCursor INTO @tgtColName 

   SET @loopIx = 0 
   WHILE @@FETCH_STATUS = 0
   BEGIN
      SET @loopIx += 1 

      IF @p_create_table = 'Y'
         SET @createTableStatement = 
            CASE WHEN @createTableStatement IS NULL 
            THEN 
               N'CREATE TABLE ' + @p_target_object + N'('
            ELSE 
               @createTableStatement + N', '
            END
            + @tgtColName + ' NVARCHAR(' + CAST( @p_new_column_size AS NVARCHAR(4)) + ')'
            + CASE WHEN @loopIx = @tgtColCount THEN N')' ELSE N'' END

      SET @insertColumnClause = 
         CASE WHEN @insertColumnClause IS NULL 
         THEN 
            N'INSERT ' + @p_target_object + N'('
         ELSE 
            @insertColumnClause + N', '
         END
         + @tgtColName

       --SET @insertValueClause = 
       --  CASE WHEN @insertValueClause IS NULL 
       --  THEN 
       --     N' VALUES ( '
       --  ELSE 
       --     @insertValueClause + N', '
       --  END
       --  + N'@b' + CAST( @loopIx AS NVARCHAR(3))
--
       --SET @bindVarsSpecs = 
       --  CASE WHEN @bindVarsSpecs IS NOT NULL 
       --  THEN 
       --     @bindVarsSpecs + N', '
       --  ELSE N''
       --  END
       --  + N'@b' + CAST( @loopIx AS NVARCHAR(3)) + N' NVARCHAR(1000)'

      FETCH NEXT FROM tgtColCursor INTO @tgtColName 
   END
   SET @insertColumnClause += N')'
   SET @insertValueClause  += N')'

   CLOSE tgtColCursor

   SET @msg = N'column clause: ' + @insertColumnClause 
   EXEC pkg_std_log__dbx @msg 
   --SET @msg = N'values clause: ' + @insertValueClause 
   --EXEC pkg_std_log__dbx @msg 
   --SET @msg = N'bind var specs: ' + @bindVarsSpecs 
   --EXEC pkg_std_log__dbx @msg 

   --
   -- Create table if appropiate 
   --
   IF @p_create_table = 'Y'
   BEGIN 
      SET @msg = N'DDL: ' + @createTableStatement 
      EXEC pkg_std_log__dbx @msg 

      BEGIN TRY 
         EXEC sp_executeSql @createTableStatement
          
      END TRY
      BEGIN CATCH 
         SET @msg = N'On sp_executeSql From procedure ' + ISNULL( CAST( ERROR_PROCEDURE() AS NVARCHAR(100)), N'?' )
            + N' line ' + ISNULL( CAST( ERROR_LINE() AS NVARCHAR(100)), N'?' )
            + N' message: ' + ISNULL( CAST( ERROR_MESSAGE() AS NVARCHAR(1000)), N'?' )
            + N' sev ' + ISNULL( CAST( ERROR_SEVERITY() AS NVARCHAR(100)), N'?' )
            + N' state ' + ISNULL( CAST( ERROR_STATE() AS NVARCHAR(100)), N'?' )
            + N' errno ' + ISNULL( CAST( ERROR_NUMBER() AS NVARCHAR(100)), N'?' )
         EXEC pkg_std_log__err @msg 

         RAISERROR( @msg ,16 ,1 )
      END CATCH
   END  

   --
   -- empty table if needed
   --
   IF @p_delete_before_insert2table = 'Y'
   BEGIN
      SET @shortDML =  N'DELETE FROM ' + @p_target_object 
       --  + CHAR(10) + N'COMMIT' 
       --  + CHAR(10) + N'END'
      EXEC pkg_std_log__dbx @shortDML 

      BEGIN TRY 
         BEGIN transaction 
         EXEC sp_executeSql @shortDML
         SET @deletedCnt = @@ROWCOUNT
         COMMIT transaction
      END TRY
      BEGIN CATCH 
         SELECT @msg = dbo.tools__formatErrMsg( N'On delete target')
         EXEC pkg_std_log__err @msg 

      END CATCH

      SET @msg = N'Rows deleted: ' + ISNULL( CONVERT( NVARCHAR, @deletedCnt), N'?' ) 
      EXEC pkg_std_log__info @msg 

   END  

   --
   -- would be better to use prepare INSERT statement 
   -- UNFORTUNATELY there is no way to pass the bind variables as one single input table 
   --
   --SET @insertStatement = @insertColumnClause + ' ' + @insertValueClause
   --EXEC sp_prepare @insertHandle OUTPUT
   --   ,@bindVarsSpecs
   --   ,@insertStatement

   --
   -- Insert the CSV records 
   --
   DECLARE cursorRecords CURSOR FOR 
      SELECT id_, columnValue FROM @records ORDER BY id_ 

   
   OPEN cursorRecords
   FETCH NEXT FROM cursorRecords INTO @recordIx, @recordCsv  

   WHILE @@FETCH_STATUS = 0
   BEGIN
      INSERT @columnValues (id_, columnValue ) SELECT id_, columnValue 
      FROM tools__split2StringElements( @recordCsv, N';' )
      --
      -- construct column literals of INSERT. Unfortunately it is not possible to use prepared statement
      --
      DECLARE cursorBindVars CURSOR FOR 
         SELECT id_, columnValue FROM @columnValues ORDER BY id_ 
      OPEN cursorBindVars
      FETCH NEXT FROM cursorBindVars INTO @colIx, @columnLiteral
      WHILE @@FETCH_STATUS = 0
      BEGIN
 
         SET @insertValueClause = 
           CASE WHEN @colIx = 1
           THEN 
              N' VALUES ( '
           ELSE 
              @insertValueClause + N', ' 
           END
           + N'''' + @columnLiteral + N''''
         
         SET @insertStatement = @insertColumnClause + N' ' + @insertColumnClause
         EXEC sp_executeSql 

         FETCH NEXT FROM cursorBindVars INTO @colIx, @columnLiteral 
      END
      CLOSE cursorBindVars

      FETCH NEXT FROM cursorRecords INTO @recordIx, @recordCsv  
   END
   CLOSE cursorRecords

   SELECT @msg = N'Records processed: ' + CONVERT( NVARCHAR, COUNT(1)) FROM @records
   EXEC pkg_std_log__dbx @msg 

   --EXEC sp_unprepare @insertHandle
END;
GO

