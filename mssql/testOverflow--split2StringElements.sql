use testdb1
GO

select * from tools__split2StringElements( N'apple;bananaloooonnnngbananaloooonnnngbananaloooonnnngbananaloooonnnngbananaloooonnnngbananaloooonnnngbananaloooonnnngbananaloooonnnng;orange' , default )

-- MSSQL truncates the long element to fit the declared size