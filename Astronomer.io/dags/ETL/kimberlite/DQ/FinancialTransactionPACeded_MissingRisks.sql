/*** FinancialTransactionPACeded_MissingRisks.sql ***

	*****  Change History  *****

	06/29/2021	DROBAK		add CoveragePublicID, RiskPAJewelryKey
---------------------------------------------------------------------------------------------------
*/
--MISSING Risks In Extract
		SELECT 'MISSING RISKS' as UnitTest, CoverageLevel, TransactionPublicID, CoveragePublicID, RiskPAJewelryKey, DATE('{date}') AS bq_load_date 
		from (SELECT * FROM `{project}.{dest_dataset}.FinancialTransactionPACeded` WHERE bq_load_date = DATE({partition_date}))
		where CoverageLevel = 'ScheduledCoverage'
		and RiskPAJewelryKey is null
