SELECT 
  RiskBuilding.JobNumber,
  RiskBuilding.PolicyPeriodPublicID,
  RiskBuilding.BuildingFixedID,
  RiskBuilding.LocationPublicID,
  RiskLocationBusinessOwners.LocationNumber
FROM (SELECT * FROM `{project}.{dest_dataset}.RiskBuilding` 
        WHERE bq_load_date = DATE({partition_date}) AND FixedBuildingRank = 1 AND IsTransactionSliceEffective = 1) AS RiskBuilding	
  INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.PolicyTransaction` WHERE bq_load_date = DATE({partition_date})) AS PolicyTransaction 
  ON PolicyTransaction.PolicyTransactionKey = RiskBuilding.PolicyTransactionKey
  LEFT JOIN (SELECT * FROM `{project}.{dest_dataset}.RiskLocationBusinessOwners`
              WHERE bq_load_date = DATE({partition_date}) AND FixedLocationRank = 1 AND IsTransactionSliceEffective = 1 ) AS RiskLocationBusinessOwners 
  ON RiskLocationBusinessOwners.PolicyTransactionKey = RiskBuilding.PolicyTransactionKey
  AND RiskLocationBusinessOwners.RiskLocationKey = RiskBuilding.RiskLocationKey
WHERE 1 = 1
      AND PolicyTransaction.TransactionStatus = 'Bound'
      AND RiskLocationBusinessOwners.LocationNumber IS NULL

UNION ALL

SELECT 
  PolicyTransaction.JobNumber,
  RiskLocationBusinessOwners.PolicyPeriodPublicID,
  RiskBuilding.BuildingFixedID,
  RiskBuilding.LocationPublicID,
  RiskLocationBusinessOwners.LocationNumber

FROM `{project}.{dest_dataset}.PolicyTransaction` AS PolicyTransaction 

  INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.PolicyTransactionProduct` WHERE bq_load_date = DATE({partition_date})) AS PolicyTransactionProduct 
  ON PolicyTransactionProduct.PolicyTransactionKey = PolicyTransaction.PolicyTransactionKey
  AND PolicyTransactionProduct.PolicyLineCode IN ('BusinessOwnersLine')

  INNER JOIN (SELECT * FROM `{project}.{dest_dataset}.RiskLocationBusinessOwners`
              WHERE bq_load_date = DATE({partition_date}) AND FixedLocationRank = 1 AND IsTransactionSliceEffective = 1) AS RiskLocationBusinessOwners 
  ON RiskLocationBusinessOwners.PolicyTransactionKey = PolicyTransaction.PolicyTransactionKey

  LEFT JOIN (SELECT * FROM `{project}.{dest_dataset}.RiskBuilding`
              WHERE bq_load_date = DATE({partition_date}) AND FixedBuildingRank = 1 AND IsTransactionSliceEffective = 1) AS RiskBuilding 
  ON RiskBuilding.PolicyTransactionKey = RiskLocationBusinessOwners.PolicyTransactionKey
  AND RiskBuilding.RiskLocationKey = RiskLocationBusinessOwners.RiskLocationKey
  
WHERE 1 = 1
      AND PolicyTransaction.bq_load_date = DATE({partition_date})
	    AND PolicyTransaction.TransactionStatus = 'Bound'
      AND RiskBuilding.BuildingFixedID IS NULL
;
