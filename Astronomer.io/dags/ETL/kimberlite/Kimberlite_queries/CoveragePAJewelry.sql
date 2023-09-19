/*TEST*/
--DECLARE vjobNumber STRING; DECLARE vpolicynumber STRING; DECLARE {partition_date} Timestamp;
--SET jobNumber = '716605';	--'2748860';  '2484655'
--SET vpolicynumber = 'P000001729';  --	'55-011405' , 'P000000016', 'P000000181' , 'P000000169', 'P000000029'
/* Set universal latest partitiontime -	In future will come from Airflow DAG (Today - 1)
*/
--SET {partition_date} = (SELECT MAX(_PARTITIONTIME) FROM `{project}.{pc_dataset}.pc_policyperiod`);



/**********************************************************
************* CoveragePAJewelry **************************/
/**********************************************************
			--BigQuery Version--

	COVERAGE ATTRIBUTE INFO:
		--2 Sections UNION(ed)
			--Personal Article (SCHeduled)
			--Personal Article Line (UNScheduled)

***********************************************************/
/*
--------------------------------------------------------------------------------------------
 *****  Change History  *****

	02/26/2021	DROBAK		Cost Join fix applied (now a left join)
	03/12/2021	DROBAK		From INNER to LEFT JOIN -- pctl_jpacost_jm and pctl_chargepattern
	03/12/2021	DROBAK		Corrected pcx_personalarticle_jm join line to: AND pcx_personalarticle_jm.FixedID = pcx_personalarticlecov_jm.PersonalArticle
	03/15/2021	DROBAK		Added CoverageCode & CostPublicID; Renamed fields to ItemLimit and ItemDeductible
	04/05/2021	DROBAK		Changed JPACoverageKey to PAJewelryCoverageKey and ,'NONE' to ,'None'
	04/05/2021	DROBAK		Changed ArticlePublicID to PAJewelryPublicID
	07/07/2021	DROBAK		FixedCoverageInBranchRank fix applied (added IsTransactionSliceEffective)
	07/07/2021	DROBAK		Added CASE Logic to Keys
	08/27/2021	DROBAK		Change ArticleLevelCoverage Value to match with RiakPAJewelry sql for PAJewelryCoverageKey
	11/08/2021	DROBAK		Make CoverageLevel Values = Financial PA Direct; align Key for Risk Key Values = RiskPAJewelry SQL
	11/09/2021	DROBAK		(BQ Only) Replace 'None' with CAST(NULL AS STRING) AS PAJewelryPublicID
	12/15/2021	SLJ			Joins to pc_policy and pc_policyterm removed to match with the process for other queries
	12/15/2021	SLJ			CostRank modified to include expiration date logic
	12/15/2021	SLJ			FixedCoverageInBranchRank modified in the final select to include coverage level in the partition by
	12/15/2021	SLJ			UnitTest modified 

--------------------------------------------------------------------------------------------
*/
WITH FPACoverageConfig AS (
  SELECT 'SourceSystem' as Key,'GW' as Value UNION ALL
  SELECT 'HashKeySeparator','_' UNION ALL
  SELECT 'HashAlgorithm','SHA2_256' UNION ALL
  SELECT 'LineCode','PersonalArtclLine_JM' UNION ALL
  SELECT 'LineLevelCoverage','Line' UNION ALL
  SELECT 'ScheduledCoverage','ScheduledCov' UNION ALL
  SELECT 'UnScheduledCoverage','UnScheduledCov'UNION ALL
  SELECT 'CostCoverage','PolicyLineCost' UNION ALL
  SELECT 'ArticleRisk', 'PersonalArticleJewelry'	--This value matches RiskPAJewelry
)

SELECT 
	sourceConfig.Value as SourceSystem
		--SK For PK <Source>_<CoveragePublicID>_<Level>
	,CASE WHEN CoveragePublicID IS NOT NULL 
		THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,CoveragePublicID,hashKeySeparator.Value,CoverageLevel)) 
	 END AS PAJewelryCoverageKey
		--SK For FK <Source>_<PolicyPeriodPublicID>
	,CASE WHEN PolicyPeriodPublicID IS NOT NULL 
		THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,PolicyPeriodPublicID))
	 END AS PolicyTransactionKey
		--SK For FK <Source>_<JPALocationPublicID>_<Level>
	,CASE WHEN PAJewelryPublicID IS NOT NULL 
		THEN SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value,PAJewelryPublicID,hashKeySeparator.Value,jpaRisk.Value)) 
	 END AS RiskPAJewelryKey
	,jpaCoverages.CoveragePublicID
	,jpaCoverages.CoverageLevel
	,jpaCoverages.JobNumber
	,jpaCoverages.PAJewelryPublicID
	,jpaCoverages.PAJewelryCoverageFixedID
	,jpaCoverages.PolicyPeriodPublicID
	,jpaCoverages.CoverageTypeCode
	,jpaCoverages.PolicyNumber
	,jpaCoverages.CoverageNumber
	,jpaCoverages.EffectiveDate
	,jpaCoverages.ExpirationDate
	,jpaCoverages.IsTempCoverage
	,jpaCoverages.ItemLimit
	,jpaCoverages.ItemDeductible
	,jpaCoverages.ItemValue
	,jpaCoverages.ItemAnnualPremium
	,jpaCoverages.CoverageCode
	,jpaCoverages.CostPublicID
	,DENSE_RANK() OVER(PARTITION BY jpaCoverages.PAJewelryCoverageFixedID, jpaCoverages.PolicyPeriodPublicID, jpaCoverages.CoverageLevel
						ORDER BY	jpaCoverages.IsTransactionSliceEffective DESC, jpaCoverages.FixedCoverageInBranchRank
					) AS FixedCoverageInBranchRank
	,jpaCoverages.IsTransactionSliceEffective
    ,DATE('{date}') as bq_load_date		

FROM (
	/***********************************************************
				Personal Article Line (UNScheduled)
	************************************************************/

	SELECT 
		pcx_personalartcllinecov_jm.PublicID									AS CoveragePublicID
		,CAST(NULL AS STRING)													AS PAJewelryPublicID
		,pc_policyperiod.PublicID												AS PolicyPeriodPublicID
		,pcx_jpacost_jm.PublicID												AS CostPublicID
		,pcx_personalartcllinecov_jm.FixedID									AS PAJewelryCoverageFixedID
		,pc_policyperiod.PolicyNumber											AS PolicyNumber
		,pc_job.JobNumber														AS JobNumber
		,COALESCE(pc_policyline.PersonalJewelryAutoNumberSeq,0)					AS CoverageNumber
		,coverageLineLevelConfig.Value										AS CoverageLevel
		,pcx_personalartcllinecov_jm.PatternCode								AS CoverageCode
		,'UNS'																	AS CoverageTypeCode
		,CASE	WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_personalartcllinecov_jm.EffectiveDate,pc_policyperiod.PeriodStart)
					AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_personalartcllinecov_jm.ExpirationDate,pc_policyperiod.PeriodEnd) 
				THEN 1 ELSE 0 
		 END																	AS IsTransactionSliceEffective
		,CASE	WHEN pcx_personalartcllinecov_jm.FinalPersistedTempFromDt_JMIC IS NOT NULL 
				THEN FinalPersistedTempFromDt_JMIC 
				ELSE COALESCE(pcx_personalartcllinecov_jm.EffectiveDate,pc_policyperiod.PeriodStart) 
		 END																	AS EffectiveDate
		,CASE	WHEN pcx_personalartcllinecov_jm.FinalPersistedTempToDt_JMIC IS NOT NULL 
				THEN FinalPersistedTempToDt_JMIC 
				ELSE COALESCE(pcx_personalartcllinecov_jm.ExpirationDate,pc_policyperiod.PeriodEnd) 
		 END																	AS ExpirationDate
		,CASE	WHEN pcx_personalartcllinecov_jm.FinalPersistedTempFromDt_JMIC IS NOT NULL 
				THEN 1 
				ELSE 0 
		 END																	AS IsTempCoverage
		,pcx_personalartcllinecov_jm.FinalPersistedLimit_JMIC					AS ItemLimit
		,pcx_personalartcllinecov_jm.FinalPersistedDeductible_JMIC				AS ItemDeductible
		,NULL																	AS ItemValue
		,CASE	WHEN pcx_jpacost_jm.ChargeGroup = 'UnscheduledPremium' 
				THEN pcx_jpacost_jm.ActualTermAmount 
				ELSE 0
				END																AS ItemAnnualPremium	/* or ActualTermAmount or BaseRateActualTermAmount */
		
		,DENSE_RANK() OVER(	PARTITION BY	pcx_personalartcllinecov_jm.ID
							ORDER BY		IFNULL(pcx_personalartcllinecov_jm.ExpirationDate,pc_policyperiod.PeriodEnd) DESC)	AS CoverageRank
		,DENSE_RANK() OVER(	PARTITION BY	pcx_personalartcllinecov_jm.FixedID, pc_policyperiod.ID
							ORDER BY		IFNULL(pcx_personalartcllinecov_jm.ExpirationDate,pc_policyperiod.PeriodEnd) DESC)	AS FixedCoverageInBranchRank
		,DENSE_RANK() OVER(	PARTITION BY	pcx_personalartcllinecov_jm.ID
							ORDER BY		IFNULL(pcx_jpacost_jm.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,pcx_jpacost_jm.ID DESC)															AS CostRank		

	FROM `{project}.{pc_dataset}.pc_policyperiod` AS pc_policyperiod

		-- Personal Line Coverage (unscheduled)
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_personalartcllinecov_jm` WHERE _PARTITIONTIME = {partition_date}) AS pcx_personalartcllinecov_jm
		ON pcx_personalartcllinecov_jm.BranchID = pc_policyperiod.ID

		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
		ON pc_job.ID = pc_policyperiod.JobID

		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
		ON pc_policyperiod.ID = pc_policyline.BranchID
		AND COALESCE(pcx_personalartcllinecov_jm.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_personalartcllinecov_jm.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)

		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
		ON pc_policyline.SubType = pctl_policyline.ID
		--AND pctl_policyline.TYPECODE = 'PersonalArtclLine_JM'

		INNER JOIN FPACoverageConfig lineConfig 
		ON lineConfig.Key = 'LineCode' 
		AND lineConfig.Value = pctl_policyline.TYPECODE	
						
		INNER JOIN FPACoverageConfig coverageLineLevelConfig
		ON coverageLineLevelConfig.Key = 'UnScheduledCoverage' 

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jpacost_jm` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jpacost_jm
		ON pcx_jpacost_jm.BranchID = pc_policyperiod.ID
		AND pcx_jpacost_jm.PersonalArtclLineCov = pcx_personalartcllinecov_jm.FixedID
		AND COALESCE(pcx_personalartcllinecov_jm.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_jpacost_jm.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_personalartcllinecov_jm.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_jpacost_jm.ExpirationDate,pc_policyperiod.PeriodEnd)

		LEFT JOIN  `{project}.{pc_dataset}.pctl_jpacost_jm` AS pctl_jpacost_jm
		ON pcx_jpacost_jm.Subtype = pctl_jpacost_jm.ID

		LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` AS pctl_chargepattern
		ON pcx_jpacost_jm.ChargePattern = pctl_chargepattern.ID

	WHERE	1 = 1
			AND pc_policyperiod._PARTITIONTIME = {partition_date}
			--AND pc_policyperiod.PolicyNumber = ISNULL(@policynumber, PolicyNumber)

	UNION ALL

	/***********************************************************
					Personal Article (SCHeduled) -- check select again
	************************************************************/
	SELECT
		pcx_personalarticlecov_jm.PublicID										AS CoveragePublicID
		,pcx_personalarticle_jm.PublicID										AS PAJewelryPublicID
		,pc_policyperiod.PublicID												AS PolicyPeriodPublicID
		,pcx_jpacost_jm.PublicID												AS CostPublicID
		,pcx_personalarticlecov_jm.FixedID										AS PAJewelryCoverageFixedID
		,pc_policyperiod.PolicyNumber											AS PolicyNumber
		,pc_job.JobNumber														AS JobNumber
		,pcx_personalarticle_jm.ItemNumber										AS CoverageNumber
		,coverageLevelConfig.Value											AS CoverageLevel
		,pcx_personalarticlecov_jm.PatternCode									AS CoverageCode
		,'SCH'																	AS CoverageTypeCode
		,CASE	WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_personalarticlecov_jm.EffectiveDate,pc_policyperiod.PeriodStart)
					AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_personalarticlecov_jm.ExpirationDate,pc_policyperiod.PeriodEnd) 
				THEN 1 ELSE 0 
		 END																	AS IsTransactionSliceEffective
		,CASE	WHEN pcx_personalarticlecov_jm.FinalPersistedTempFromDt_JMIC IS NOT NULL 
				THEN pcx_personalarticlecov_jm.FinalPersistedTempFromDt_JMIC 
				ELSE COALESCE(pcx_personalarticlecov_jm.EffectiveDate,pc_policyperiod.PeriodStart) 
		 END																	AS EffectiveDate
		,CASE	WHEN pcx_personalarticlecov_jm.FinalPersistedTempToDt_JMIC IS NOT NULL 
				THEN pcx_personalarticlecov_jm.FinalPersistedTempToDt_JMIC 
				ELSE COALESCE(pcx_personalarticlecov_jm.ExpirationDate,pc_policyperiod.PeriodEnd) 
		 END																	AS ExpirationDate
		,CASE	WHEN pcx_personalarticlecov_jm.FinalPersistedTempFromDt_JMIC IS NOT NULL 
				THEN 1 
				ELSE 0 
		 END																	AS IsTempCoverage
		,pcx_personalarticlecov_jm.FinalPersistedLimit_JMIC						AS ItemLimit
		,pcx_personalarticlecov_jm.FinalPersistedDeductible_JMIC				AS ItemDeductible
		,pcx_personalarticlecov_jm.DirectTerm1									AS ItemValue
		,CASE	WHEN pcx_jpacost_jm.ChargeGroup = 'PREMIUM' 
				THEN pcx_jpacost_jm.ActualTermAmount 
				ELSE 0
				END																AS ItemAnnualPremium	/* or ActualTermAmount or BaseRateActualTermAmount */
		
		,DENSE_RANK() OVER(	PARTITION BY	pcx_personalarticlecov_jm.ID
							ORDER BY		IFNULL(pcx_personalarticlecov_jm.ExpirationDate,pc_policyperiod.PeriodEnd) DESC)	AS CoverageRank
		,DENSE_RANK() OVER(	PARTITION BY	pcx_personalarticlecov_jm.FixedID, pc_policyperiod.ID
							ORDER BY		IFNULL(pcx_personalarticlecov_jm.ExpirationDate,pc_policyperiod.PeriodEnd) DESC)	AS FixedCoverageInBranchRank
		,DENSE_RANK() OVER(	PARTITION BY	pcx_personalarticlecov_jm.ID
							ORDER BY		IFNULL(pcx_jpacost_jm.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,pcx_jpacost_jm.ID DESC)															AS CostRank	

	FROM `{project}.{pc_dataset}.pc_policyperiod` AS pc_policyperiod

		-- Jewelry Article Coverage
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_personalarticlecov_jm` WHERE _PARTITIONTIME = {partition_date}) AS pcx_personalarticlecov_jm
		ON pcx_personalarticlecov_jm.BranchID = pc_policyperiod.ID

		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
		ON pc_job.ID = pc_policyperiod.JobID

		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
		ON pc_policyperiod.ID = pc_policyline.BranchID
		AND COALESCE(pcx_personalarticlecov_jm.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_personalarticlecov_jm.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)

		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
		ON pc_policyline.SubType = pctl_policyline.ID
		--AND pctl_policyline.TYPECODE = 'PersonalArtclLine_JM'

		INNER JOIN FPACoverageConfig lineConfig 
		ON lineConfig.Key = 'LineCode' AND lineConfig.Value = pctl_policyline.TYPECODE
							
		INNER JOIN FPACoverageConfig coverageLevelConfig
		ON coverageLevelConfig.Key = 'ScheduledCoverage' 

		--  Article
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_personalarticle_jm` WHERE _PARTITIONTIME = {partition_date}) AS pcx_personalarticle_jm
		ON pcx_personalarticle_jm.BranchID = pc_policyperiod.ID
		AND pcx_personalarticle_jm.FixedID = pcx_personalarticlecov_jm.PersonalArticle 
		AND COALESCE(pcx_personalarticlecov_jm.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_personalarticle_jm.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_personalarticlecov_jm.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_personalarticle_jm.ExpirationDate,pc_policyperiod.PeriodEnd)

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jpacost_jm` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jpacost_jm
		ON pcx_jpacost_jm.BranchID = pc_policyperiod.ID
		AND pcx_jpacost_jm.PersonalArticleCov = pcx_personalarticlecov_jm.FixedID
		AND COALESCE(pcx_personalarticlecov_jm.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_jpacost_jm.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_personalarticlecov_jm.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_jpacost_jm.ExpirationDate,pc_policyperiod.PeriodEnd)

		LEFT JOIN `{project}.{pc_dataset}.pctl_jpacost_jm` AS pctl_jpacost_jm
		ON pcx_jpacost_jm.Subtype = pctl_jpacost_jm.ID

		LEFT JOIN `{project}.{pc_dataset}.pctl_chargepattern` AS pctl_chargepattern
		ON pcx_jpacost_jm.ChargePattern = pctl_chargepattern.ID
	
	WHERE	1 = 1
			AND pc_policyperiod._PARTITIONTIME = {partition_date}
			--AND pc_policyperiod.PolicyNumber = ISNULL(@policynumber, PolicyNumber)

) jpaCoverages

	INNER JOIN FPACoverageConfig sourceConfig
	ON sourceConfig.Key='SourceSystem'

	INNER JOIN FPACoverageConfig hashKeySeparator
	ON hashKeySeparator.Key='HashKeySeparator'

	INNER JOIN FPACoverageConfig jpaRisk
	ON jpaRisk.Key='ArticleRisk'

	INNER JOIN FPACoverageConfig hashAlgorithm
	ON hashAlgorithm.Key = 'HashAlgorithm'

WHERE 1=1
	AND CoveragePublicID IS NOT NULL
	AND CoverageRank = 1
	AND CostRank = 1	--this removes some dupes; comment this line to see them
	--AND JobNumber = ISNULL(@jobnumber, JobNumber)
	--AND PolicyNumber = ISNULL(@policynumber, PolicyNumber)
