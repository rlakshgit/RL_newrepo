/*** FinancialTransactionUMBDirect_MissingRisks.sql ***

	*****  Change History  *****

	06/29/2021	DROBAK		Updated fields for consistent output
---------------------------------------------------------------------------------------------------
*/

	--MISSING Risks In Extract
		SELECT 'MISSING RISKS' as UnitTest, CoverageLevel, TransactionPublicID, CoveragePublicID, RiskLocationKey, DATE('{date}') AS bq_load_date 
		from (SELECT * FROM `{project}.{dest_dataset}.FinancialTransactionUMBDirect` WHERE bq_load_date = DATE({partition_date}))
		where (RiskLocationKey is null ) 