
	--MISSING Risks In Extract
		SELECT 'MISSING RISKS' as UnitTest, CoverageLevel, TransactionPublicID, RiskLocationKey, RiskStockKey, DATE('{date}') AS bq_load_date 
		from (SELECT * FROM `{project}.{dest_dataset}.FinancialTransactionIMCeded` WHERE bq_load_date = DATE({partition_date}))
		WHERE (RiskLocationKey is null AND RiskStockKey is null) 
