use testdb1;
GO






-- convert a delimiter separated string into a collection variable 

CREATE OR ALTER FUNCTION tools__split2StringElements (
@p_src_string NVARCHAR(4000)
,@p_sep NVARCHAR(10) = N';' 
)
RETURNS @retVal  TABLE ( colVar NVARCHAR(100) )
AS
BEGIN
DECLARE @buf NVARCHAR(1000), @found Int , @elem NVARCHAR(100), @sepLen Int , @countDown Int 
    SET @sepLen = LEN( @p_sep )
    SET @buf = @p_src_string
    SET @found = CHARINDEX( @p_sep, @buf)
    SET @countDown = 1000
    WHILE ( @found > 0 AND @countDown > 0)
    BEGIN 
        -- TSQL automatically truncates the string to fit! 
        SET @elem = SUBSTRING( @buf, 1, (@found - 1) )
        INSERT INTO @retVal VALUES ( @elem )
        SET @buf = SUBSTRING( @buf, @found + @sepLen, 9999 )
        SET @found = CHARINDEX( @p_sep, @buf)
        SET @countDown -= 1
    END 
    IF len( @buf ) > 0 
    BEGIN
        INSERT INTO @retVal VALUES ( @buf )
    END
    RETURN 
END
GO