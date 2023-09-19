-- tag: FinancialTransactionPJCeded_MissingRisks - tag ends/
/*** FinancialTransactionPJCeded_MissingRisks.sql ***

	*****  Change History  *****

	06/29/2021	DROBAK		add CoveragePublicID, RiskJewelryItemKey
	09/28/2022	DROBAK		Correction: CoverageLevel = 'ScheduledCov' from 'ScheduledCoverage' 

---------------------------------------------------------------------------------------------------
*/
	--MISSING Risks In Extract
		SELECT	'MISSING RISKS' AS UnitTest
				, CoverageLevel
				, TransactionPublicID
				, CoveragePublicID
				, RiskJewelryItemKey
				, DATE('{date}') AS bq_load_date 
		FROM (SELECT * FROM `{project}.{dest_dataset}.FinancialTransactionPJCeded` WHERE bq_load_date = DATE({partition_date}))
		WHERE CoverageLevel = 'ScheduledCov'
		AND RiskJewelryItemKey IS NULL

