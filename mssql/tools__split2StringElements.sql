use testdb1;
GO






-- convert a delimiter separated string into a collection variable 

CREATE OR ALTER FUNCTION tools__split2StringElements (
@p_src_string NVARCHAR(4000)
,@p_sep NVARCHAR(10) = N';' 
)
RETURNS @retVal  TABLE ( id_ Int, columnValue NVARCHAR(4000) )
AS
BEGIN
DECLARE @buf NVARCHAR(4000), @found Int , @elem NVARCHAR(4000), @sepLen Int , @MAX_ITERATIONS Int, @loopIx Int 
    SET @sepLen = LEN( @p_sep )
    SET @buf = @p_src_string
    SET @found = CHARINDEX( @p_sep, @buf)
    SET @MAX_ITERATIONS = 1000
    SET @loopIx = 1
    WHILE ( @found > 0 AND @loopIx < @MAX_ITERATIONS )
    BEGIN 
        -- TSQL automatically truncates the string to fit! 
        SET @elem = SUBSTRING( @buf, 1, (@found - 1) )
        INSERT INTO @retVal  VALUES ( @loopIx, @elem )
        SET @buf = SUBSTRING( @buf, @found + @sepLen, 9999 )
        SET @found = CHARINDEX( @p_sep, @buf)
        SET @loopIx += 1
    END 
    IF len( @buf ) > 0 
    BEGIN
        INSERT INTO @retVal VALUES ( @loopIx, @buf )
    END
    RETURN 
END
GO