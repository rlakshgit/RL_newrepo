/**** CoverageBOP_MissingRisks.sql ********

 *****  Change History  *****

	06/28/2021	DROBAK		Add IsTransactionSliceEffective
	01/19/2022	DROBAK		Add Jobnumber
	02/09/2022	DROBAK		And Where clause for new DQ source_record_exceptions table
-------------------------------------------------------------------------------------------------------------
*/
/* ORIG
			--MISSING Risks In Extract
			SELECT 'MISSING RISKS' as UnitTest, CoverageLevel, CoveragePublicID, RiskLocationKey, RiskBuildingKey, Jobnumber
					,IsTransactionSliceEffective, DATE('{date}') AS bq_load_date
			from (SELECT * FROM `{project}.{dest_dataset}.CoverageBOP` WHERE bq_load_date = DATE({partition_date}))
			where (RiskLocationKey is null AND RiskBuildingKey is null) --all bop coverage should be tied to a risk (either location or building)
			and IsTransactionSliceEffective != 0
*/
	--MISSING Risks In Extract
	SELECT	'MISSING RISKS'		AS UnitTest
			,PolicyNumber
			,JobNumber
			,CoverageLevel
			,CoveragePublicID
			,RiskLocationKey
			,RiskBuildingKey
			,DATE('{date}')		AS bq_load_date
	FROM (SELECT * FROM `{project}.{dest_dataset}.CoverageBOP` WHERE bq_load_date = DATE({partition_date}))
	WHERE RiskLocationKey IS NULL	--all bop coverage should be tied to a risk (either location or building)
	AND IsTransactionSliceEffective != 0
	AND	JobNumber NOT IN (	SELECT KeyCol1 FROM `{project}.{dest_dataset_DQ}.source_record_exceptions`
							WHERE TableName='CoverageBOP' AND DQTest='MISSING RISKS' AND KeyCol1Name='JobNumber')

	UNION ALL

	SELECT	'MISSING RISKS'		AS UnitTest
			,PolicyNumber
			,JobNumber
			,CoverageLevel
			,CoveragePublicID
			,RiskLocationKey
			,RiskBuildingKey
			,DATE('{date}')		AS bq_load_date
	FROM (SELECT * FROM `{project}.{dest_dataset}.CoverageBOP` WHERE bq_load_date = DATE({partition_date}))
	WHERE RiskBuildingKey IS NULL	--all bop coverage should be tied to a risk (either location or building)
	AND CoverageLevel like '%Building%'
	AND IsTransactionSliceEffective != 0
	AND	JobNumber NOT IN (	SELECT KeyCol1 FROM `{project}.{dest_dataset_DQ}.source_record_exceptions`
							WHERE TableName='CoverageBOP' AND DQTest='MISSING RISKS' AND KeyCol1Name='JobNumber')
