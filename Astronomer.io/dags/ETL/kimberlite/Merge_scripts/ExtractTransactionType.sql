--Populate ExtractTransactionType Lookup Table

--SET IDENTITY_INSERT [StatsRpt].[ExtractTransactionType] ON;
    
CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.ExtractTransactionType`
(
	ID  INT64, 
	ExtractSortOrder INT64, 
	ExtractTransactionType STRING
);	
MERGE  `{project}.{dest_dataset}.ExtractTransactionType`  AS t
USING ( 
SELECT 1 as ID, 1 as ExtractSortOrder, 'Premium' as ExtractTransactionType UNION ALL
SELECT 2, 5 ,'LossPaid' UNION ALL
SELECT 3, 8 ,'LossResv'
) as s
--([ID], [ExtractSortOrder], [ExtractTransactionType])
ON ( t.ID = s.ID )
WHEN MATCHED THEN UPDATE SET
    ExtractSortOrder = s.ExtractSortOrder,
    ExtractTransactionType = s.ExtractTransactionType
 WHEN NOT MATCHED BY TARGET THEN
    INSERT(ID, ExtractSortOrder, ExtractTransactionType)
    VALUES(s.ID, s.ExtractSortOrder, s.ExtractTransactionType)
WHEN NOT MATCHED BY SOURCE THEN DELETE; 
 
--PRINT 'ExtractTransactionType: ' + CAST(@@ROWCOUNT AS VARCHAR(100));
 
--SET IDENTITY_INSERT [StatsRpt].[ExtractTransactionType]  OFF;