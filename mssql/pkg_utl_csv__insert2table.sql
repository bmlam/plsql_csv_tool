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
 @p_target_object    varchar(100)
 ,@p_csv_string      nvarchar(4000)
 ,@p_col_sep     nvarchar(10) = N';'
 ,@p_standalone_head_line varchar(1000) = NULL 
 ,@p_delete_before_insert2table varchar(1) = 'N'

AS
DECLARE 
   @v_msg varchar(1000)
   ;
BEGIN
   SET @v_msg = 'tgt table ' + @p_target_object + ' csv size: ' + CAST ( LEN(@p_csv_string) as varchar(5))
   EXEC pkg_std_log__info @v_msg 
   
END;
GO

