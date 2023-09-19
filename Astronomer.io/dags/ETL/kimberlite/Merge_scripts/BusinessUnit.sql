-- tag: BusinessUnit - tag ends/
/*******************************************************
	Kimberlite - Core
		Reference and Lookup Tables
			BusinessUnit.sql
--------------------------------------------------------------------------------------------
	*****  Change History  *****

	05/01/2023	DROBAK		Init create (table and script)

--------------------------------------------------------------------------------------------
--Ref: List via Finance Dept.
*********************************************************/

CREATE OR REPLACE TABLE `{project}.{core_dataset}.BusinessUnit`
(
  BusinessUnitID INT64,
  BusinessUnitName STRING,
  BusinessUnitShortName STRING,
  BusinessUnitCode STRING,
  Active INT64,
  bq_load_date DATE
)
  PARTITION BY
	bq_load_date
  OPTIONS(
      description="Contains all JM Company business unit names."
);

INSERT INTO `{project}.{core_dataset}.BusinessUnit`
VALUES	(1, 'Personal Lines Insurance', 'Personal', 'PL', 1, CURRENT_DATE())
		,(2, 'Commercial Lines Insurance', 'Commercial', 'CL', 1, CURRENT_DATE())
		,(3, 'Shipping', 'Shipping', 'SH', 1, CURRENT_DATE())
		,(4, 'General Agency', 'General Agency', 'GA', 1, CURRENT_DATE())
		,(5, 'Care Plan', 'Care Plan', 'CP', 1, CURRENT_DATE())
		,(6, 'PL Cross Sell', 'PL Cross Sell', 'PLCS', 1, CURRENT_DATE())
		,(7, 'Zing MarketPlace', 'Zing MarketPlace', 'ZM', 1, CURRENT_DATE())
