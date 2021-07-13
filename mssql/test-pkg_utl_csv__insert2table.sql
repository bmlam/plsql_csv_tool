use testdb1
GO 

EXEC pkg_utl_csv__insert2table 'dummy_table', 'product;price;description
apple;1.5;tasty fruit
banana;;another tasty fruit
chip;;no price
;;;
;3;no such product
' 
,  default, default, default 