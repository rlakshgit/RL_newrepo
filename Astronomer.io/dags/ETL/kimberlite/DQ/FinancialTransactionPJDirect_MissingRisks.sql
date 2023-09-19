-- tag: FinancialTransactionPJDirect_MissingRisks - tag ends/
/*** FinancialTransactionPJDirect_MissingRisks.sql ***

	*****  Change History  *****

	06/28/2021	DROBAK		Replaced PA named field with RiskJewelryItemKey; add CoveragePublicID
	09/28/2022	DROBAK		Fixed CoverageLevel value from 'ScheduledCoverage' to 'ScheduledCov'

---------------------------------------------------------------------------------------------------
*/
--MISSING Risks In Extract
	SELECT 'MISSING RISKS' as UnitTest, CoverageLevel, CoveragePublicID, TransactionPublicID, RiskJewelryItemKey, DATE('{date}') AS bq_load_date 
	from (SELECT * FROM `{project}.{dest_dataset}.FinancialTransactionPJDirect` WHERE bq_load_date = DATE({partition_date}))
	where CoverageLevel = 'ScheduledCov'
	and RiskJewelryItemKey is null