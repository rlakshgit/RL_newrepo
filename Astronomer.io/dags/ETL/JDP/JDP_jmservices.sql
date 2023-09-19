with CTE_address as
(
  SELECT * FROM `{project}.{dataset}.jm_Address_tb` WHERE DATE(_PARTITIONTIME) = DATE("{{{{ ds }}}}")
),
CTE_Jeweler as
(
  SELECT JewelerId,JewelerCode,CompanyName,AddressID FROM `{project}.{dataset}.jm_Jeweler_tb` WHERE DATE(_PARTITIONTIME) = DATE("{{{{ ds }}}}")
)
select distinct a.* except(AddressId),b.* except(AddressId,bq_load_date),DATE("{{{{ ds }}}}") as bq_load_date
from Cte_jeweler a join cte_address b on a.AddressId =b.AddressId 
