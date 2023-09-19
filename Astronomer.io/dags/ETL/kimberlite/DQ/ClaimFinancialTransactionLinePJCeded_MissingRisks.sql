-- tag: ClaimFinancialTransactionLinePJCeded_MissingRisks - tag ends/
/*** ClaimFinancialTransactionLinePJCeded_MissingRisks.sql ***

	*****  Change History  *****

	09/28/2022	DROBAK		Init
	11/07/2022	DROBAK		Kimberlite Table Name Change

---------------------------------------------------------------------------------------------------
*/
	--MISSING Risks In Extract
		SELECT	'MISSING RISKS' AS UnitTest
				, CoverageLevel
				, TransactionPublicID
				, CoveragePublicID
				, RiskJewelryItemKey
				, DATE('{date}') AS bq_load_date 
		FROM (SELECT * FROM `{project}.{dest_dataset}.ClaimFinancialTransactionLinePJCeded` WHERE bq_load_date = DATE({partition_date}))
		WHERE CoverageLevel = 'ScheduledCov'
		AND RiskJewelryItemKey IS NULL
