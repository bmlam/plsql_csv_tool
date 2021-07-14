use testdb1;
GO






-- 

CREATE OR ALTER FUNCTION dbo.tools__formatErrMsg (
        @p_prolog NVARCHAR(200)
)
RETURNS NVARCHAR(2000) 
AS
BEGIN
DECLARE @retVal NVARCHAR(2000)

    SET @retVal = @p_prolog
            + ISNULL( CAST( ERROR_PROCEDURE() AS NVARCHAR(100)), N'?' )
            + N' line ' + ISNULL( CAST( ERROR_LINE() AS NVARCHAR(100)), N'?' )
            + N' message: ' + ISNULL( CAST( ERROR_MESSAGE() AS NVARCHAR(1000)), N'?' )
            + N' sev ' + ISNULL( CAST( ERROR_SEVERITY() AS NVARCHAR(100)), N'?' )
            + N' state ' + ISNULL( CAST( ERROR_STATE() AS NVARCHAR(100)), N'?' )
            + N' errno ' + ISNULL( CAST( ERROR_NUMBER() AS NVARCHAR(100)), N'?' )
    RETURN @retVal
END
GO