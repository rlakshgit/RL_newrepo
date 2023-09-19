
--DUPES In Extarct
SELECT 'DUPES BY COVERAGE LEVEL' as UnitTest, FinancialTransactionKey, CoverageLevel, TransactionPublicID, count(*) as NumRecords, DATE('{date}') AS bq_load_date  
FROM `{project}.{dest_dataset}.FinancialTransactionIMDirect` WHERE bq_load_date = DATE({partition_date})
--where Jobnumber=ISNULL(@jobnumber,JobNumber)
--AND PolicyNumber = ISNULL(@policynumber, PolicyNumber)
group by FinancialTransactionKey, TransactionPublicID, CoverageLevel
having count(*)>1 --dupe check