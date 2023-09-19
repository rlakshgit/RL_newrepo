-- tag: LegalEntity - tag ends/
/*******************************************************
	Kimberlite - Building Blocks
		Reference and Lookup Tables
			LegalEntity.sql
--------------------------------------------------------------------------------------------
	*****  Change History  *****

	03/30/2023	DROBAK		Init create (table and script)
	05/23/2023	DROBAK		Adjusted date to be today minus 1 day to be consistent with the rest of the tables

--------------------------------------------------------------------------------------------
--Ref: select * from PolicyCenter.dbo.pc_uwcompany
*********************************************************/

--DROP TABLE `qa-edl.B_QA_ref_kimberlite_core.LegalEntity`;
CREATE TABLE IF NOT EXISTS `{project}.{dest_dataset}.LegalEntity`
(
  LegalEntityID INTEGER,
  LegalEntityName STRING,
  LegalEntityCode STRING,
  NAICCode STRING,
  Active INT64,
  bq_load_date DATE
)
  PARTITION BY
	bq_load_date
  OPTIONS(
      description="Contains all JM Company (legal entity) names."
);

INSERT INTO `{project}.{dest_dataset}.LegalEntity`
VALUES	(1, 'Jewelers Mutual Insurance Company, SI', 'JMIC', '1010-00001', 1, CURRENT_DATE()-1)
		,(2, 'JM Specialty Insurance Company', 'JMSI', '1010-16116', 1, CURRENT_DATE()-1)
		,(3, 'JM Facets Group', 'JM Facets', CAST(NULL AS STRING), 1, CURRENT_DATE()-1)
	--	,(4, 'Jewelers Mutual Insurance Agency', 'JMIA', CAST(NULL AS STRING), 1, CURRENT_DATE()-1)
