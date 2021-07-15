use testdb1
GO

 select * from tools__split2StringElements( N'apple;banana;orange' , default )
 go
 select * from tools__split2StringElements( N'apple;;banana;;orange' , ';;' )
 go
 select * from tools__split2StringElements( N';;banana;;orange' , ';;' )
 go
 select * from tools__split2StringElements( N'banana;;orange;;' , ';;' )
 go
 select * from tools__split2StringElements( N';;banana;;orange;;' , ';;' )
 go
 select * from tools__split2StringElements( N'' , ';;' )
 go
 