-- tag: t_lkup_LegalEntity - tag ends/
/*******************************************************
	Kimberlite - Building Block
		Reference and Lookup Tables
			t_lkup_LegalEntity.sql
--------------------------------------------------------------------------------------------
	*****  Change History  *****

	04/30/2023	DROBAK		Init create (table and merge script)
	05/23/2023	DROBAK		Add WHERE clause to LegalEntity for partitions
	06/08/2023	DROBAK		COrrected path for LegalEntity, Add UWCompanyCode to Merge update fields

--------------------------------------------------------------------------------------------
--Ref: select * from PolicyCenter.dbo.pc_uwcompany
*********************************************************/
CREATE OR REPLACE TABLE `{project}.{dest_dataset}.t_lkup_LegalEntity`
(
  SourceSystem STRING,
  SourceSystemID STRING,
  LegalEntityID INT64,
  UWCompanyCode STRING,
  Retired INT64,
  bq_load_date DATE
)
  OPTIONS(
      description="Contains all JM Company (legal entity) names."
);

MERGE INTO `{project}.{dest_dataset}.t_lkup_LegalEntity` AS Target 
USING (
        WITH LegalEntityList AS
        (		SELECT 
					'GW'							AS SourceSystem
                    ,PublicID						AS SourceSystemID
			        ,LegalEntityID					AS LegalEntityID
                    ,pctl_uwcompanycode.TYPECODE	AS UWCompanyCode --same code in pctl_uwcompanycode, bctl_uwcompany, and cctl_underwritingcompanytype
                    ,pc_uwcompany.Retired			AS Retired

                FROM (SELECT * FROM `{project}.{pc_dataset}.pc_uwcompany` WHERE _PARTITIONTIME = {partition_date}) AS pc_uwcompany
                INNER JOIN `{project}.{pc_dataset}.pctl_uwcompanycode` AS pctl_uwcompanycode 
                  ON pctl_uwcompanycode.ID = pc_uwcompany.Code
                  AND pc_uwcompany.Retired = 0
                  AND pctl_uwcompanycode.Retired = false
			    LEFT JOIN (SELECT * FROM `{project}.{core_dataset}.LegalEntity` WHERE TIMESTAMP(bq_load_date) = {partition_date}) AS LegalEntity
                  ON LegalEntity.LegalEntityCode = CASE WHEN pc_uwcompany.ParentName = 'Jewelers Mutual Insurance Company' THEN 'JMIC'
                                                        WHEN pc_uwcompany.ParentName = 'JM Specialty Insurance Company' THEN 'JMSI'
                                                        ELSE CAST(NULL AS STRING) END
                WHERE (pc_uwcompany.ParentName LIKE 'JM%' OR pc_uwcompany.ParentName LIKE 'Jewelers Mutual%')
				--WHERE _PARTITIONTIME = (SELECT MAX(_PARTITIONTIME) FROM `{project}.{pc_dataset}.pc_uwcompany`)	--for testing
                /*
                UNION ALL

                SELECT "Platform" AS SourceSystem
                        ,NULL AS SourceSystemID
                        ,NULL  AS LegalEntityID
                        -- ,NULL AS SourceSystemTable
                        -- ,NULL     AS SourceSystemField
                        ,0        AS Retired
                */
		)
        SELECT 
                SourceSystem
                ,SourceSystemID
                ,LegalEntityID
				,UWCompanyCode
                ,Retired
                ,DATE('{date}') AS bq_load_date
                --,CURRENT_DATE() AS bq_load_date
        FROM 
                LegalEntityList
) AS Source
ON	Target.SourceSystem = Source.SourceSystem
AND Target.SourceSystemID = Source.SourceSystemID

WHEN MATCHED THEN UPDATE
SET      SourceSystem			= Source.SourceSystem
        ,SourceSystemID			= Source.SourceSystemID
        ,LegalEntityID			= Source.LegalEntityID
		,UWCompanyCode			= Source.UWCompanyCode
        ,Retired				= Source.Retired
        ,bq_load_date			= Source.bq_load_date

WHEN NOT MATCHED BY TARGET THEN
	INSERT (SourceSystem,SourceSystemID,LegalEntityID,UWCompanyCode,Retired,bq_load_date)
	VALUES (SourceSystem,SourceSystemID,LegalEntityID,UWCompanyCode,Retired,bq_load_date)

WHEN NOT MATCHED BY SOURCE THEN 
	DELETE;