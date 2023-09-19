-- tag: ClaimFinancialTransactionLinePJDirect_MissingRisks - tag ends/
/*** ClaimFinancialTransactionLinePJDirect_MissingRisks.sql ***

	*****  Change History  *****

	09/28/2022	DROBAK		Init
	11/07/2022	DROBAK		Kimberlite Table Name Change
	03/21/2023	DROBAK		Add WHERE clauses for PolicyNumber and source_record_exceptions

---------------------------------------------------------------------------------------------------
*/
--MISSING Risks In Extract
SELECT	'MISSING RISKS' AS UnitTest
		, CoverageLevel
		, CoveragePublicID
		, TransactionPublicID
		, PolicyNumber
		, RiskJewelryItemKey
		, DATE('{date}') AS bq_load_date 
FROM (SELECT * FROM `{project}.{dest_dataset}.ClaimFinancialTransactionLinePJDirect` WHERE bq_load_date = DATE({partition_date}))
WHERE CoverageLevel = 'ScheduledCov'
AND RiskJewelryItemKey IS NULL
AND PolicyNumber NOT LIKE 'UP-%'	--Exclude legacy policy system records created only for processing claim payments
AND CoveragePublicID NOT IN (SELECT KeyCol1 FROM `{project}.{dest_dataset_core_DQ}.source_record_exceptions` WHERE TableName='ClaimFinancialTransactionLinePJDirect' AND DQTest='MISSING RISKS' AND KeyCol1Name='CoveragePublicID')