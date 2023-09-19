
CREATE OR REPLACE TABLE `{project}.{dataset}.pl_recast_all` AS

WITH a_recast AS 
(
  SELECT * FROM `{project}.{dataset}.pl_recast_after_2014`
  WHERE PlanYear = CAST(LEFT(CAST({ddate} AS STRING),4) AS INT) 
)
,b_recast AS 
(
  SELECT * FROM `{project}.{dataset}.pl_recast_before_2014`
  WHERE PlanYear = CAST(LEFT(CAST({ddate} AS STRING),4) AS INT)
    
)

SELECT 
  recast.PlanYear,
  recast.AccountNumber,
  recast.PolicyNumber,
  recast.Recast,
  recast.SubRecast,
  partitionMap.PartitionCode,
  partitionMap.PartitionName,
  partitionMap.DistributionSource,
  partitionMap.DistributionChannel
FROM
(
  SELECT * FROM a_recast
  UNION ALL 
  SELECT * FROM b_recast WHERE PolicyNumber NOT IN (SELECT PolicyNumber FROM a_recast)
) recast

  LEFT JOIN `{project}.{dataset}.pl_partition_mapping` AS partitionMap
  ON partitionMap.Recast = recast.Recast
  AND partitionMap.SubRecast = recast.SubRecast
  AND partitionMap.PlanYear = recast.PlanYear
