/*** FinancialTransactionIMDirect_DupesOverall.sql ***

	*****  Change History  *****

	06/14/2021	SLJ			INIT
---------------------------------------------------------------------------------------------------
*/
--DUPES In Extarct
SELECT 'DUPES OVERALL' as UnitTest,FinancialTransactionKey, TransactionPublicID, count(*)  as NumRecords, DATE('{date}') AS bq_load_date
FROM `{project}.{dest_dataset}.FinancialTransactionIMDirect` WHERE bq_load_date = DATE({partition_date})
GROUP BY FinancialTransactionKey, TransactionPublicID
HAVING	COUNT(*)>1 --dupe check