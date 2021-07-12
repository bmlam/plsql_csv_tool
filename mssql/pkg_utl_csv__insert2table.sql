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
   @records  TABLE ( colVar NVARCHAR(4000) )
DECLARE @v_msg VARCHAR(1000)
   ,@preparedStmt Int
   ,@DOS_LINE_BREAK NVARCHAR(2)
   ,@UNIX_LINE_BREAK NVARCHAR(2)
   ,@lineBreakStyleIsDOS BIT
   ,@recordCount Int

   SET @v_msg = 'tgt table ' + @p_target_object + ' csv size: ' + CAST ( LEN(@p_csv_string) as varchar(5))
   EXEC pkg_std_log__info @v_msg 

   SET @DOS_LINE_BREAK = CAST( CHAR(13) + CHAR(10) AS NVARCHAR(2))
   SET @UNIX_LINE_BREAK = CAST( CHAR(10) AS NVARCHAR(2))

   IF CHARINDEX( @DOS_LINE_BREAK, @p_csv_string) > 0 SET @lineBreakStyleIsDOS = 1
   ELSE  SET @lineBreakStyleIsDOS = 0

   IF @lineBreakStyleIsDOS = 1
   BEGIN
      INSERT @records ( colVar )
      SELECT colVar FROM tools__split2StringElements( @p_csv_string, @DOS_LINE_BREAK )
   END 
   ELSE
   BEGIN
      INSERT @records ( colVar )
      SELECT colVar FROM tools__split2StringElements( @p_csv_string, @UNIX_LINE_BREAK )
   END
   SELECT @recordCount = COUNT(1) FROM @records
   
   SET @v_msg = 'recordCount: ' + CAST( @recordCount AS NVARCHAR(10) ) 
   EXEC pkg_std_log__info @v_msg
END;
GO

