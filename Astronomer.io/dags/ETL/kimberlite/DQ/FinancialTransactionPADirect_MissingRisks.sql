/*** FinancialTransactionPADirect_MissingRisks.sql ***

	*****  Change History  *****

	06/25/2021	DROBAK		Updated select output
-----------------------------------------------------
*/
	--MISSING Risks In Extract
		SELECT 'MISSING RISKS' as UnitTest, CoverageLevel, TransactionPublicID, RiskPAJewelryKey, DATE('{date}') AS bq_load_date 
		from (SELECT * FROM `{project}.{dest_dataset}.FinancialTransactionPADirect` WHERE bq_load_date = DATE({partition_date}))
		where CoverageLevel = 'ScheduledCoverage'
		and RiskPAJewelryKey is null
