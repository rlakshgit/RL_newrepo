
SELECT 
  'RiskStock to RiskLocation' AS UnitTest,
  RiskStockIM.JobNumber,
  RiskStockIM.PolicyPeriodPublicID,
  RiskStockIM.StockFixedID,
  RiskStockIM.LocationPublicID,
  RiskLocationIM.LocationNumber
FROM (SELECT * FROM `{project}.{dest_dataset}.RiskStockIM` 
        WHERE bq_load_date = DATE({partition_date}) AND FixedStockRank = 1 AND IsTransactionSliceEffective = 1) AS RiskStockIM	
  INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.PolicyTransaction` WHERE bq_load_date = DATE({partition_date})) AS PolicyTransaction 
  ON PolicyTransaction.PolicyTransactionKey = RiskStockIM.PolicyTransactionKey
  LEFT JOIN (SELECT * FROM `{project}.{dest_dataset}.RiskLocationIM`
              WHERE bq_load_date = DATE({partition_date}) AND FixedLocationRank = 1 AND IsTransactionSliceEffective = 1 ) AS RiskLocationIM 
  ON RiskLocationIM.PolicyTransactionKey = RiskStockIM.PolicyTransactionKey
  AND RiskLocationIM.RiskLocationKey = RiskStockIM.RiskLocationKey
WHERE 1 = 1
      AND PolicyTransaction.TransactionStatus = 'Bound'
      AND RiskLocationIM.LocationNumber IS NULL

UNION ALL

SELECT 
  'RiskLocation to RiskStock' AS UnitTest,
  PolicyTransaction.JobNumber,
  RiskLocationIM.PolicyPeriodPublicID,
  RiskStockIM.StockFixedID,
  RiskStockIM.LocationPublicID,
  RiskLocationIM.LocationNumber

FROM `{project}.{dest_dataset}.PolicyTransaction` AS PolicyTransaction 

  INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.PolicyTransactionProduct` WHERE bq_load_date = DATE({partition_date})) AS PolicyTransactionProduct 
  ON PolicyTransactionProduct.PolicyTransactionKey = PolicyTransaction.PolicyTransactionKey
  AND PolicyTransactionProduct.PolicyLineCode IN ('ILMLine_JMIC')

  INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.RiskLocationIM`
              WHERE bq_load_date = DATE({partition_date}) AND FixedLocationRank = 1 AND IsTransactionSliceEffective = 1) AS RiskLocationIM 
  ON RiskLocationIM.PolicyTransactionKey = PolicyTransaction.PolicyTransactionKey

  LEFT JOIN (SELECT * FROM `{project}.{dest_dataset}.RiskStockIM`
              WHERE bq_load_date = DATE({partition_date}) AND FixedStockRank = 1 AND IsTransactionSliceEffective = 1) AS RiskStockIM 
  ON RiskStockIM.PolicyTransactionKey = RiskLocationIM.PolicyTransactionKey
  AND RiskStockIM.RiskLocationKey = RiskLocationIM.RiskLocationKey
  
WHERE 1 = 1
      AND PolicyTransaction.bq_load_date = DATE({partition_date})
	    AND PolicyTransaction.TransactionStatus = 'Bound'
      AND RiskStockIM.StockFixedID IS NULL
ORDER BY JobNumber DESC
;