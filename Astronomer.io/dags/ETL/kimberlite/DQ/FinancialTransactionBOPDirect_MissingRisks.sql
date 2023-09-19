/**** FinancialTransactionBOPDirect ********
 
 *****  Change History  *****

	06/25/2021	DROBAK		Add IsTransactionSliceEffective != 0
*/
	--MISSING Risks In Extract
		SELECT 'MISSING RISKS' as UnitTest, CoverageLevel, TransactionPublicID, RiskLocationKey, RiskBuildingKey, DATE('{date}') AS bq_load_date 
		from (SELECT * FROM `{project}.{dest_dataset}.FinancialTransactionBOPDirect` WHERE bq_load_date = DATE({partition_date}))
		where (RiskLocationKey is null AND RiskBuildingKey is null) 
		and IsTransactionSliceEffective != 0
