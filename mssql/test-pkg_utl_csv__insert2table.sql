use testdb1
GO 

EXEC pkg_utl_csv__insert2table @p_target_object= 'dummy_table', @p_csv_string = N'product;price;description
apple;1.5;tasty fruit
banana;;another tasty fruit
chip;;no price
;;;
;3;no such product
' , @p_create_table = 'Y'
