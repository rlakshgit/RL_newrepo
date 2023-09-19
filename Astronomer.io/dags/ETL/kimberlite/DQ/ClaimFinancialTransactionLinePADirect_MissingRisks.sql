-- tag: ClaimFinancialTransactionLinePADirect_MissingRisks - tag ends/
/*** ClaimFinancialTransactionLinePADirect_MissingRisks.sql ***

	*****  Change History  *****

	10/21/2022	DROBAK		Init
	11/07/2022	DROBAK		Kimberlite Table Name Changed
---------------------------------------------------------------------------------------------------
*/
--MISSING Risks In Extract
	SELECT	'MISSING RISKS' AS UnitTest
			, CoverageLevel
			, CoveragePublicID
			, TransactionPublicID
			, PolicyNumber
			, RiskPAJewelryKey
			, DATE('{date}') AS bq_load_date 
	FROM (SELECT * FROM `{project}.{dest_dataset}.ClaimFinancialTransactionLinePADirect` WHERE bq_load_date = DATE({partition_date}))
	WHERE CoverageLevel = 'ScheduledCov'
	AND RiskPAJewelryKey IS NULL