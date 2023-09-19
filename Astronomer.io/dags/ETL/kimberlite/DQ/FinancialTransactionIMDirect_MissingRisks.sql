/*** FinancialTransactionIMDirect_MissingRisks.sql ***

	*****  Change History  *****

	06/28/2021	SLJ			INIT
	02/04/2022	DROBAK		Separated Keys, added IsTransactionSliceEffective, PolicyNumber
---------------------------------------------------------------------------------------------------
*/
--MISSING Risks In Extract
--all IM coverage should be tied to a risk (either location or stock)
SELECT
	'MISSING RISKS'					AS UnitTest
	,PolicyNumber
	,JobNumber
	,CoverageLevel
	,TransactionPublicID
	,RiskLocationKey
	,RiskStockKey
	,IsTransactionSliceEffective
	,DATE('{date}')					AS bq_load_date
FROM (SELECT * FROM `{project}.{dest_dataset}.FinancialTransactionIMDirect` WHERE bq_load_date = DATE({partition_date}))
WHERE	RiskLocationKey IS NULL
		AND IsTransactionSliceEffective != 0

UNION ALL

SELECT
	'MISSING RISKS'					AS UnitTest
	,PolicyNumber
	,JobNumber
	,CoverageLevel
	,TransactionPublicID
	,RiskLocationKey
	,RiskStockKey
	,IsTransactionSliceEffective
	,DATE('{date}')					AS bq_load_date
FROM (SELECT * FROM `{project}.{dest_dataset}.FinancialTransactionIMDirect` WHERE bq_load_date = DATE({partition_date}))
WHERE	RiskStockKey IS NULL
		AND CoverageLevel like '%Stock%'
		AND IsTransactionSliceEffective != 0


/* ORIG
SELECT	'MISSING RISKS'			AS UnitTest
		, CoverageLevel
		, TransactionPublicID
		, RiskLocationKey
		, RiskStockKey
		, DATE('{date}')		AS bq_load_date
from `{project}.{dest_dataset}.FinancialTransactionIMDirect`
WHERE bq_load_date = DATE({partition_date})
and (RiskLocationKey is null AND RiskStockKey is null)
*/
