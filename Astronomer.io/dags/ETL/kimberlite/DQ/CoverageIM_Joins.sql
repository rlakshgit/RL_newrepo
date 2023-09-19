SELECT 
  'Coverage to RiskLocation' AS UnitTest,
  CoverageIM.JobNumber,
  CoverageIM.PolicyPeriodPublicID,
  CoverageIM.IMLocationPublicID AS LocationPublicID,
  CoverageIM.IMStockPublicID AS StockPublicID,
  RiskLocationIM.LocationNumber,
  CoverageIM.CoverageLevel,
  CoverageIM.CoverageFixedID

FROM (SELECT * FROM `{project}.{dest_dataset}.CoverageIM` 
        WHERE bq_load_date = DATE({partition_date}) AND FixedCoverageInBranchRank = 1 AND IsTransactionSliceEffective = 1 AND CoverageLevel NOT IN ('Stock','SubStock')) AS CoverageIM	

  INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.PolicyTransaction` WHERE bq_load_date = DATE({partition_date})) AS PolicyTransaction 
  ON PolicyTransaction.PolicyTransactionKey = CoverageIM.PolicyTransactionKey

  LEFT JOIN (SELECT * FROM `{project}.{dest_dataset}.RiskLocationIM`
              WHERE bq_load_date = DATE({partition_date}) AND FixedLocationRank = 1 AND IsTransactionSliceEffective = 1 ) AS RiskLocationIM 
  ON CoverageIM.PolicyTransactionKey = RiskLocationIM.PolicyTransactionKey
  AND CoverageIM.RiskLocationKey = RiskLocationIM.RiskLocationKey

WHERE 1 = 1
      AND PolicyTransaction.TransactionStatus = 'Bound'
      AND RiskLocationIM.LocationNumber IS NULL
      -- AND CoverageIM.IMLocationPublicID IS NOT NULL

UNION ALL 

SELECT 
  'RiskLocation to Coverage' AS UnitTest,
  PolicyTransaction.JobNumber,
  RiskLocationIM.PolicyPeriodPublicID,
  RiskLocationIM.LocationPublicID,
  NULL AS StockPublicID,
  RiskLocationIM.LocationNumber,
  CoverageIM.CoverageLevel,
  CoverageIM.CoverageFixedID

FROM `{project}.{dest_dataset}.PolicyTransaction` AS PolicyTransaction

  INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.PolicyTransactionProduct` WHERE bq_load_date = DATE({partition_date})) AS PolicyTransactionProduct 
  ON PolicyTransactionProduct.PolicyTransactionKey = PolicyTransaction.PolicyTransactionKey
  AND PolicyTransactionProduct.PolicyLineCode IN ('ILMLine_JMIC')

  INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.RiskLocationIM`
              WHERE bq_load_date = DATE({partition_date}) AND FixedLocationRank = 1 AND IsTransactionSliceEffective = 1 ) AS RiskLocationIM 
  ON RiskLocationIM.PolicyTransactionKey = PolicyTransaction.PolicyTransactionKey

  LEFT JOIN (SELECT * FROM `{project}.{dest_dataset}.CoverageIM` WHERE bq_load_date = DATE({partition_date}) AND FixedCoverageInBranchRank = 1 AND IsTransactionSliceEffective = 1 AND CoverageLevel NOT IN ('Stock','SubStock')) AS CoverageIM 
  ON CoverageIM.PolicyTransactionKey = RiskLocationIM.PolicyTransactionKey
  AND CoverageIM.RiskLocationKey = RiskLocationIM.RiskLocationKey
  
WHERE 1 = 1
      AND PolicyTransaction.bq_load_date = DATE({partition_date})
	    AND PolicyTransaction.TransactionStatus = 'Bound'
      AND CoverageIM.CoverageFixedID IS NULL

UNION ALL

SELECT 
  'Coverage to RiskStockIM' AS UnitTest,
  CoverageIM.JobNumber,
  CoverageIM.PolicyPeriodPublicID,
  CoverageIM.IMLocationPublicID AS LocationPublicID,
  CoverageIM.IMStockPublicID AS StockPublicID,
  NULL AS LocationNumber,
  CoverageIM.CoverageLevel,
  CoverageIM.CoverageFixedID

FROM (SELECT * FROM `{project}.{dest_dataset}.CoverageIM` 
        WHERE bq_load_date = DATE({partition_date}) AND FixedCoverageInBranchRank = 1 AND IsTransactionSliceEffective = 1 AND CoverageLevel IN ('Stock','SubStock')) AS CoverageIM	

  INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.PolicyTransaction` WHERE bq_load_date = DATE({partition_date})) AS PolicyTransaction 
  ON PolicyTransaction.PolicyTransactionKey = CoverageIM.PolicyTransactionKey

  LEFT JOIN (SELECT * FROM `{project}.{dest_dataset}.RiskStockIM`
              WHERE bq_load_date = DATE({partition_date}) AND FixedStockRank = 1 AND IsTransactionSliceEffective = 1) AS RiskStockIM 
  ON CoverageIM.PolicyTransactionKey = RiskStockIM.PolicyTransactionKey
  AND CoverageIM.RiskLocationKey = RiskStockIM.RiskLocationKey
  AND CoverageIM.RiskStockKey = RiskStockIM.RiskStockKey

WHERE 1 = 1
      AND PolicyTransaction.TransactionStatus = 'Bound'
      AND RiskStockIM.LocationNumber IS NULL

UNION ALL 

SELECT 
  'RiskStockIM to Coverage' AS UnitTest,
  PolicyTransaction.JobNumber,
  RiskLocationIM.PolicyPeriodPublicID,
  RiskLocationIM.LocationPublicID,
  CoverageIM.IMStockPublicID AS StockPublicID,
  RiskLocationIM.LocationNumber,
  CoverageIM.CoverageLevel,
  CoverageIM.CoverageFixedID

FROM `{project}.{dest_dataset}.PolicyTransaction` AS PolicyTransaction

  INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.PolicyTransactionProduct` WHERE bq_load_date = DATE({partition_date})) AS PolicyTransactionProduct 
  ON PolicyTransactionProduct.PolicyTransactionKey = PolicyTransaction.PolicyTransactionKey
  AND PolicyTransactionProduct.PolicyLineCode IN ('ILMLine_JMIC')

  INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.RiskLocationIM`
              WHERE bq_load_date = DATE({partition_date}) AND FixedLocationRank = 1 AND IsTransactionSliceEffective = 1) AS RiskLocationIM 
  ON RiskLocationIM.PolicyTransactionKey = PolicyTransaction.PolicyTransactionKey

  INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.RiskStockIM` 
              WHERE bq_load_date = DATE({partition_date}) AND FixedStockRank = 1 AND IsTransactionSliceEffective = 1) AS RiskStockIM
  ON RiskStockIM.PolicyTransactionKey = RiskLocationIM.PolicyTransactionKey
  AND RiskStockIM.RiskLocationKey = RiskLocationIM.RiskLocationKey

  LEFT JOIN (SELECT * FROM `{project}.{dest_dataset}.CoverageIM` WHERE bq_load_date = DATE({partition_date}) AND FixedCoverageInBranchRank = 1 AND IsTransactionSliceEffective = 1 AND CoverageLevel IN ('Stock','SubStock')) AS CoverageIM 
  ON CoverageIM.PolicyTransactionKey = RiskStockIM.PolicyTransactionKey
  AND CoverageIM.RiskLocationKey = RiskStockIM.RiskLocationKey
  AND CoverageIM.RiskStockKey = RiskStockIM.RiskStockKey
  
WHERE 1 = 1
      AND PolicyTransaction.bq_load_date = DATE({partition_date})
	    AND PolicyTransaction.TransactionStatus = 'Bound'
      AND CoverageIM.CoverageFixedID IS NULL
;