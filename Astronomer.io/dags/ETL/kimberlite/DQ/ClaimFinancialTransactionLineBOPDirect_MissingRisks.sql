-- tag: ClaimFinancialTransactionLineBOPDirect_MissingRisks - tag ends/
/*** ClaimFinancialTransactionLineBOPDirect_MissingRisks.sql ***

	*****  Change History  *****

	01/13/2023	DROBAK		Init
---------------------------------------------------------------------------------------------------
*/
--MISSING Location Risks In Extract
	SELECT	'MISSING RISKS' AS UnitTest
			, CoverageLevel
			, CoveragePublicID
			, TransactionPublicID
			, PolicyNumber
			, RiskLocationKey
			, RiskBuildingKey
			, DATE('{date}') AS bq_load_date 
	FROM (SELECT * FROM `{project}.{dest_dataset}.ClaimFinancialTransactionLineBOPDirect` WHERE bq_load_date = DATE({partition_date}))
	WHERE CoverageLevel = 'Location'
	AND RiskLocationKey IS NULL
	AND IsTransactionSliceEffective != 0

UNION ALL

--MISSING Building Risks In Extract
	SELECT	'MISSING RISKS' AS UnitTest
			, CoverageLevel
			, CoveragePublicID
			, TransactionPublicID
			, PolicyNumber
			, RiskLocationKey
			, RiskBuildingKey
			, DATE('{date}') AS bq_load_date 
	FROM (SELECT * FROM `{project}.{dest_dataset}.ClaimFinancialTransactionLineBOPDirect` WHERE bq_load_date = DATE({partition_date}))
	WHERE CoverageLevel = 'Building'
	AND RiskBuildingKey IS NULL
	AND IsTransactionSliceEffective != 0