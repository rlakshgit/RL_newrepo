-- tag: Account_MissingByX - tag ends/
/**********************************************************************************************
	Kimberlite - DQ Check
		Account_MissingByX.sql

	*****  Change History  *****

	03/23/2023	DROBAK		Initial Create
	
------------------------------------------------------------------------------------------------
*/	
--Missing By X
	SELECT	'MISSING KEY'		AS UnitTest
			,AccountKey
			,AccountPublicID
			,DATE('{date}')		AS bq_load_date

	FROM (SELECT * FROM `{project}.{core_dataset}.Account` WHERE bq_load_date = DATE({partition_date}))
	WHERE AccountKey IS NULL
  
UNION ALL

--Missing By X
	SELECT	'MISSING ACCOUNTNUMBER'				AS UnitTest
			,AccountKey
			,AccountPublicID
			,DATE('{date}')		AS bq_load_date

	FROM (SELECT * FROM `{project}.{core_dataset}.Account` WHERE bq_load_date = DATE({partition_date}))
	WHERE AccountNumber IS NULL

UNION ALL

--Missing by X
	SELECT	'MISSING ACCOUNTS'									AS UnitTest
			,COALESCE(pc_account.PublicID, bc_account.PublicID) AS SourceAccountPublicID
			,DATE('{date}')										AS bq_load_date

	FROM (SELECT * FROM `{project}.{pc_dataset}.pc_account` WHERE _PARTITIONTIME = {partition_date}) AS pc_account
			FULL OUTER JOIN (SELECT * FROM `{project}.{bc_dataset}.bc_account` WHERE _PARTITIONTIME = {partition_date}) AS bc_account
				ON bc_account.AccountNumber = pc_account.AccountNumber
	WHERE COALESCE(pc_account.PublicID, bc_account.PublicID) NOT IN (SELECT AccountPublicID FROM (SELECT * FROM `{project}.{core_dataset}.Account` WHERE bq_load_date = DATE({partition_date})))