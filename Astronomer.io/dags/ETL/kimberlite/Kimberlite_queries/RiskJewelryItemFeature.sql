/****************************************
  KIMBERLITE EXTRACT
    RiskJewelryItemFeature
		BQ CONVERTED
--------------------------------------------------------------------------------------------------------------------------
	--FeatureTypes
		--Center Stone
		--Grams
		--Length check J_5835670 9
		--Milli Meter
		--Model Number
		--Mounting
		--Other
		--Pearls
		--Pre Owned
		--Serial Number
		--Side Stones
		--Watch Mounting
--------------------------------------------------------------------------------------------------------------------------

 *****  Change History  *****

	02/26/2021	DROBAK		Cost Join fix applied (now a left join)
	01/14/2022	DROBAK		Use pcx_JwlryFeature_JMIC_PL as base table and in date logic (just like PA)
	01/14/2022	DROBAK		Added IsTransactionSliceEffective; Changed Rank to use Feature not Feature Detail table
	01/17/2022	DROBAK		Change to Left Joins: pcx_jewelryitem_jmic_pl, pctl_featuretype_jmic, pcx_JwlryFeatDetail_JMIC_PL, pctl_jwlryfeatdetail_jmic_pl
							Added CASE Stmnt for RiskJewelryItemKey
	02/01/2022	SLJ/DROBAK	Adjust RiskItemFeatureKey to use ItemFeaturePublicID in place of ItemFeatureDetailPublicID
							Join to feature detail dates logic modified
							JobNumber added (used in DQ checks too)

--------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE `{project}.B_QA_ref_kimberlite_config.RiskItemFeaturesConfig`
	(
		Key STRING,
		Value STRING
	);
	INSERT INTO `{project}.B_QA_ref_kimberlite_config.RiskItemFeaturesConfig`
		VALUES
			('SourceSystem','GW')
			,('HashKeySeparator','_')
			,('HashingAlgorithm','SHA2_256')
			,('PJILineCode','PersonalJewelryLine_JMIC_PL')
			,('PJALineCode','PersonalArtclLine_JM')
			,('ILMLineCode','ILMLine_JMIC')
			,('BOPLineCode','BusinessOwnersLine')
			,('PJILevelRisk','PersonalJewelryItem')
			,('ILMLocationLevelRisk','ILMLocation')
			,('BOPLocationLevelRisk','BusinessOwnersLocation');

*/
--DELETE `qa-edl.B_QA_ref_kimberlite.dar_RiskJewelryItemFeature` WHERE bq_load_date = "2022-02-02";
--INSERT INTO `qa-edl.B_QA_ref_kimberlite.dar_RiskJewelryItemFeature`


WITH RiskItemFeaturesConfig AS (
  SELECT 'SourceSystem' as Key,'GW' as Value UNION ALL
  SELECT 'HashKeySeparator','_' UNION ALL
  SELECT 'HashingAlgorithm','SHA2_256' UNION ALL
  SELECT 'PJILineCode','PersonalJewelryLine_JMIC_PL' UNION ALL
  SELECT 'PJALineCode','PersonalArtclLine_JM' UNION ALL
  SELECT 'ILMLineCode','ILMLine_JMIC' UNION ALL
  SELECT 'BOPLineCode','BusinessOwnersLine' UNION ALL
  SELECT 'PJILevelRisk','PersonalJewelryItem' UNION ALL
  SELECT 'ILMLocationLevelRisk','ILMLocation' UNION ALL
  SELECT 'BOPLocationLevelRisk','BusinessOwnersLocation'
)

SELECT
	ConfigSource.Value AS SourceSystem
	--SK For PK [<Source>_<FeatureDetailPublicID>]
	,SHA256(CONCAT(ConfigSource.Value,ConfigHashSep.Value, cRiskJIFeatures.ItemFeaturePublicID)) AS RiskItemFeatureKey
	--SK For FK [<Source>_<ItemPublicID>_<Level>]
	,CASE WHEN cRiskJIFeatures.ItemPublicID IS NOT NULL
		THEN SHA256(CONCAT(ConfigSource.Value,ConfigHashSep.Value, cRiskJIFeatures.ItemPublicID,ConfigHashSep.Value, cRiskJIFeatures.RiskLevel))
		END AS RiskJewelryItemKey
	--SK For FK [<Source>_<PolicyPeriodPublicID>]
	,SHA256(CONCAT(ConfigSource.Value,ConfigHashSep.Value, cRiskJIFeatures.PolicyPeriodPublicID)) AS PolicyTransactionKey
	,DENSE_RANK() OVER(PARTITION BY	cRiskJIFeatures.ItemFeaturePublicID, cRiskJIFeatures.PolicyPeriodPublicID
					   ORDER BY		cRiskJIFeatures.IsTransactionSliceEffective DESC, cRiskJIFeatures.FixedFeatureRank ASC
					   ) AS FixedFeatureRank

	,cRiskJIFeatures.ItemFeaturePublicID
	,cRiskJIFeatures.ItemFeatureDetailPublicID
	,cRiskJIFeatures.ItemFeatureDetailFixedID
	,cRiskJIFeatures.ItemPublicID
	,cRiskJIFeatures.ItemFixedID
	,cRiskJIFeatures.PolicyPeriodPublicID
	,cRiskJIFeatures.JobNumber
	,cRiskJIFeatures.JewelryItemNumber
	,cRiskJIFeatures.RiskLevel
	,cRiskJIFeatures.FeatureType
	,cRiskJIFeatures.FeatureDetailType
	,cRiskJIFeatures.IsTransactionSliceEffective

	--Center Stone
	,cRiskJIFeatures.NumberOfCntrStones					-- (635638185)
	,cRiskJIFeatures.CntrStoneWeight					-- (635638185)
	,cRiskJIFeatures.CntrStoneMilliMeter				-- (57010976)
	,cRiskJIFeatures.CntrStoneCut						-- (635638185)
	,cRiskJIFeatures.CntrStoneCutOtherDesc				-- (1429621703)
	,cRiskJIFeatures.CntrStoneType						-- (635638185)
	,cRiskJIFeatures.CntrStoneOtherDesc					-- (Populated when stone type is pearl except for few records; 143844956)
	,cRiskJIFeatures.CntrStonePearlType					-- (Populated when stone type is pearl except for few records; 244288419)
	,cRiskJIFeatures.ColorofCntrStone					-- (635638185)
	,cRiskJIFeatures.CntrStoneClarity					-- (635638185)
	,cRiskJIFeatures.CntrStoneClarityEnhancedType		-- (90704412)
	,cRiskJIFeatures.CntrGemCert						-- (1431101311)

	--Side Stones
	,cRiskJIFeatures.NumberOfSideStones					-- (256264640)
	,cRiskJIFeatures.SideStoneWeight					-- (256264640)
	,cRiskJIFeatures.SideStoneMilliMeter				-- (256264640)
	,cRiskJIFeatures.SideStoneCut						-- (256264640)
	,cRiskJIFeatures.SideStoneCutOtherDesc				-- (1309420282)
	,cRiskJIFeatures.SideStoneType						-- (256264640)
	,cRiskJIFeatures.SideStoneOtherDesc					-- (721766993)
	,cRiskJIFeatures.SideStonePearlType					-- (345716)
	,cRiskJIFeatures.ColorofSideStone					-- (256264640)
	,cRiskJIFeatures.SideStoneClarity					-- (1309420282)
	,cRiskJIFeatures.SideStoneClarityEnhancedType		-- (952662274)
	,cRiskJIFeatures.SideGemCert						-- (155719785)

	--Center Stone
	--Side Stones
	,cRiskJIFeatures.CertNo								-- (Center Stone 1431101311;Side Stones 155719785)

	--Grams
	,cRiskJIFeatures.GramsOrDWT							-- (6529326,2194030)

	--Length
	,cRiskJIFeatures.Length								-- (1532725539,43041)

	--MilliMeter
	,cRiskJIFeatures.MilliMeter							-- (1472231,6514469)

	--Model Number
	,cRiskJIFeatures.ModelNo							-- (31056072,1330225530)

	--Mounting
	,cRiskJIFeatures.MountingType						-- (1071190259,784524140)
	,cRiskJIFeatures.MountingOtherDesc					-- (3661940)

	--Other
	,cRiskJIFeatures.DescOfOther						-- (6007211,6007324)

	--Pearls
	,cRiskJIFeatures.NumberOfPearls						-- (7162331)
	,cRiskJIFeatures.PearlType							-- (5433773)

	--Center Stone
	--Side Stones
	--Pearls
	,cRiskJIFeatures.PearlTypeOtherDesc					-- (Center Stone 244288419;Side Stones 1499981;Pearls 3325301)

	--Pre Owned
	,CAST(cRiskJIFeatures.PreOwned AS INT64) as PreOwned	-- (2568957,3819718)

	--Serial Number
	,cRiskJIFeatures.SerialNo							-- (6391499,1167409)

	--Watch Mounting
	,cRiskJIFeatures.WatchMountingType
	,cRiskJIFeatures.WatchMountingOtherDesc

	--all
	,cRiskJIFeatures.FeatureNotes
    ,DATE('{date}') as bq_load_date

FROM (
	SELECT
		DENSE_RANK() OVER(PARTITION BY	pcx_JwlryFeature_JMIC_PL.ID
							ORDER BY	IFNULL(pcx_jewelryitem_jmic_pl.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
										,IFNULL(pcx_JwlryFeature_JMIC_PL.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
							) AS FeatureRank
		,DENSE_RANK() OVER(PARTITION BY pcx_JwlryFeature_JMIC_PL.FixedID, pc_policyperiod.ID
							ORDER BY	IFNULL(pcx_jewelryitem_jmic_pl.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
										,IFNULL(pcx_JwlryFeature_JMIC_PL.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
							) AS FixedFeatureRank
		,DENSE_RANK() OVER(PARTITION BY pcx_JwlryFeature_JMIC_PL.ID
							ORDER BY	IFNULL(pcx_cost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
										,pcx_cost_jmic.ID DESC
							) AS CostRank
		,pcx_JwlryFeature_JMIC_PL.PublicID			AS ItemFeaturePublicID
		,pcx_JwlryFeatDetail_JMIC_PL.PublicID		AS ItemFeatureDetailPublicID
		,pcx_JwlryFeatDetail_JMIC_PL.FixedID		AS ItemFeatureDetailFixedID
		,pcx_jewelryitem_jmic_pl.PublicID			AS ItemPublicID
		,pcx_jewelryitem_jmic_pl.FixedID			AS ItemFixedID
		,pc_job.JobNumber							AS JobNumber
		,pcx_jewelryitem_jmic_pl.ItemNumber			AS JewelryItemNumber
		,pc_policyperiod.PublicID					AS PolicyPeriodPublicID
		,ConfigRisk.Value							AS RiskLevel
		,pctl_featuretype_jmic.NAME					AS FeatureType
		,pctl_jwlryfeatdetail_jmic_pl.NAME			AS FeatureDetailType
		,CASE WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_JwlryFeature_JMIC_PL.EffectiveDate,pc_policyperiod.PeriodStart)
			AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_JwlryFeature_JMIC_PL.ExpirationDate,pc_policyperiod.PeriodEnd) THEN 1 ELSE 0 END AS IsTransactionSliceEffective

		--Center Stone
		,pcx_JwlryFeatDetail_JMIC_PL.NumberOfCntrStones									-- (635638185)
		,pcx_JwlryFeatDetail_JMIC_PL.CntrStoneWeight									-- (635638185)
		,pcx_JwlryFeatDetail_JMIC_PL.CntrStoneMilliMeter								-- (57010976)
		,pctlCntrStoneCut.NAME						AS CntrStoneCut						-- (635638185)
		,pcx_JwlryFeatDetail_JMIC_PL.CntrStoneCutOtherDesc								-- (1429621703)
		,pctlCntrStoneType.NAME						AS CntrStoneType					-- (635638185)
		,pcx_JwlryFeatDetail_JMIC_PL.CntrStoneOtherDesc									-- (Populated when stone type is pearl except for few records; 143844956)
		,pctlCntrStonePearlType.NAME				AS CntrStonePearlType				-- (Populated when stone type is pearl except for few records; 244288419)
		,pcx_JwlryFeatDetail_JMIC_PL.ColorofCntrStone									-- (635638185)
		,pcx_JwlryFeatDetail_JMIC_PL.CntrStoneClarity									-- (635638185)
		,pctlCntrStoneClarityEnhancedType.NAME		AS CntrStoneClarityEnhancedType		-- (90704412)
		,pctlCntrStoneGemCert.NAME					AS CntrGemCert						-- (1431101311)

		--Side Stones
		,pcx_JwlryFeatDetail_JMIC_PL.NumberOfSideStones									-- (256264640)
		,pcx_JwlryFeatDetail_JMIC_PL.SideStoneWeight									-- (256264640)
		,pcx_JwlryFeatDetail_JMIC_PL.SideStoneMilliMeter								-- (256264640)
		,pctlSideStoneCut.NAME						AS SideStoneCut						-- (256264640)
		,pcx_JwlryFeatDetail_JMIC_PL.SideStoneCutOtherDesc								-- (1309420282)
		,pctlSideStoneType.NAME						AS SideStoneType					-- (256264640)
		,pcx_JwlryFeatDetail_JMIC_PL.SideStoneOtherDesc									-- (721766993)
		,pctlSideStonePearlType.NAME				AS SideStonePearlType				-- (345716)
		,pcx_JwlryFeatDetail_JMIC_PL.ColorofSideStone									-- (256264640)
		,pcx_JwlryFeatDetail_JMIC_PL.SideStoneClarity									-- (1309420282)
		,pctlSideStoneClarityEnhancedType.NAME		AS SideStoneClarityEnhancedType		-- (952662274)
		,pctlSideStoneGemCert.NAME					AS SideGemCert						-- (155719785)

		--Center Stone
		--Side Stones
		,pcx_JwlryFeatDetail_JMIC_PL.CertNo												-- (Center Stone 1431101311;Side Stones 155719785)

		--Grams
		,pcx_JwlryFeatDetail_JMIC_PL.Grams			AS GramsOrDWT						-- (6529326,2194030)

		--Length
		,pcx_JwlryFeatDetail_JMIC_PL.Length												-- (1532725539,43041)

		--Milli Meter
		,pcx_JwlryFeatDetail_JMIC_PL.MilliMeter											-- (1472231,6514469)

		--Model Number
		,pcx_JwlryFeatDetail_JMIC_PL.ModelNo											-- (31056072,1330225530)

		--Mounting
		,pctl_mounting_jmic_pl.NAME							AS MountingType				-- (1071190259,784524140)
		,pcx_JwlryFeatDetail_JMIC_PL.MountingOtherDesc									-- (3661940)

		--Other
		,pcx_JwlryFeatDetail_JMIC_PL.Other			AS DescOfOther						-- (6007211,6007324)

		--Pearls
		,pcx_JwlryFeatDetail_JMIC_PL.NumberOfPearls										-- (7162331)
		,pctlPearlType.NAME							AS PearlType						-- (5433773)

		--Center Stone
		--Side Stones
		--Pearls
		,pcx_JwlryFeatDetail_JMIC_PL.PearlTypeOtherDesc									-- (Center Stone 244288419;Side Stones 1499981;Pearls 3325301)

		--Pre Owned
		,pcx_JwlryFeatDetail_JMIC_PL.PreOwned											-- (2568957,3819718)

		--Serial Number
		,pcx_JwlryFeatDetail_JMIC_PL.SerialNo											-- (6391499,1167409)

		--Watch Mounting
		,pcx_JwlryFeatDetail_JMIC_PL.WatchMountingType
		,pcx_JwlryFeatDetail_JMIC_PL.WatchMountingOtherDesc

		--all
		,pcx_JwlryFeatDetail_JMIC_PL.FeatureNotes

	FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) pc_policyperiod

		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_JwlryFeature_JMIC_PL` WHERE _PARTITIONTIME = {partition_date}) pcx_JwlryFeature_JMIC_PL
			ON pcx_JwlryFeature_JMIC_PL.BranchID = pc_policyperiod.ID

		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) pc_policyline
			ON pc_policyline.BranchID = pc_policyperiod.ID
			AND COALESCE(pcx_JwlryFeature_JMIC_PL.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_JwlryFeature_JMIC_PL.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)

		INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline
			ON pctl_policyline.ID = pc_policyline.SubType
		INNER JOIN RiskItemFeaturesConfig AS ConfigPJILine
			ON ConfigPJILine.Value = pctl_policyline.TYPECODE
			AND ConfigPJILine.Key = 'PJILineCode'
		INNER JOIN RiskItemFeaturesConfig AS ConfigRisk
			ON ConfigRisk.Key='PJILevelRisk'
		INNER JOIN `{project}.{pc_dataset}.pctl_policyperiodstatus` pctl_policyperiodstatus
			ON pctl_policyperiodstatus.ID = pc_policyperiod.Status
		INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) pc_job
			ON pc_job.ID = pc_policyperiod.JobID

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jewelryitem_jmic_pl` WHERE _PARTITIONTIME = {partition_date}) pcx_jewelryitem_jmic_pl
			ON pcx_JwlryFeature_JMIC_PL.PersonalItem = pcx_jewelryitem_jmic_pl.FixedID
			AND pcx_JwlryFeature_JMIC_PL.BranchID = pcx_jewelryitem_jmic_pl.BranchID
			AND COALESCE(pcx_JwlryFeature_JMIC_PL.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_jewelryitem_jmic_pl.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_JwlryFeature_JMIC_PL.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pcx_jewelryitem_jmic_pl.ExpirationDate,pc_policyperiod.PeriodEnd)

		LEFT JOIN `{project}.{pc_dataset}.pctl_featuretype_jmic` AS pctl_featuretype_jmic
			ON pctl_featuretype_jmic.ID = pcx_JwlryFeature_JMIC_PL.FeatureType
		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_JwlryFeatDetail_JMIC_PL` WHERE _PARTITIONTIME = {partition_date}) AS pcx_JwlryFeatDetail_JMIC_PL
			ON pcx_JwlryFeatDetail_JMIC_PL.Feature = pcx_JwlryFeature_JMIC_PL.FixedID
			AND pcx_JwlryFeatDetail_JMIC_PL.BranchID = pcx_JwlryFeature_JMIC_PL.BranchID
			AND pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_JwlryFeatDetail_JMIC_PL.EffectiveDate,pc_policyperiod.PeriodStart)
			AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_JwlryFeatDetail_JMIC_PL.ExpirationDate,pc_policyperiod.PeriodEnd)
		LEFT JOIN `{project}.{pc_dataset}.pctl_jwlryfeatdetail_jmic_pl` AS pctl_jwlryfeatdetail_jmic_pl
			ON pctl_jwlryfeatdetail_jmic_pl.ID = pcx_JwlryFeatDetail_JMIC_PL.Subtype

		LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_cost_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_cost_jmic
			ON pcx_cost_jmic.BranchID = pc_policyperiod.ID
			AND pcx_cost_jmic.JewelryItem_JMIC_PL = pcx_jewelryitem_jmic_pl.FixedID
			AND COALESCE(pcx_JwlryFeature_JMIC_PL.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_cost_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_JwlryFeature_JMIC_PL.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pcx_cost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)

		--Center Stone
		LEFT JOIN `{project}.{pc_dataset}.pctl_cut_jmic_pl` AS pctlCntrStoneCut
			ON pctlCntrStoneCut.ID = pcx_JwlryFeatDetail_JMIC_PL.CntrStoneCut
		LEFT JOIN `{project}.{pc_dataset}.pctl_stones_jmic_pl` AS pctlCntrStoneType
			ON pctlCntrStoneType.ID = pcx_JwlryFeatDetail_JMIC_PL.CntrStoneType
		LEFT JOIN `{project}.{pc_dataset}.pctl_pearltype_jmic` AS pctlCntrStonePearlType
			ON pctlCntrStonePearlType.ID = pcx_JwlryFeatDetail_JMIC_PL.CntrStonePearlType
		LEFT JOIN `{project}.{pc_dataset}.pctl_clarityenhanced_jmic_pl` AS pctlCntrStoneClarityEnhancedType
			ON pctlCntrStoneClarityEnhancedType.ID = pcx_JwlryFeatDetail_JMIC_PL.CntrStoneClarityEnhancedType
		LEFT JOIN `{project}.{pc_dataset}.pctl_gemcert_jmic_pl` AS pctlCntrStoneGemCert
			ON pctlCntrStoneGemCert.ID = pcx_JwlryFeatDetail_JMIC_PL.CntrGemCert
		--Side Stones
		LEFT JOIN `{project}.{pc_dataset}.pctl_cut_jmic_pl` AS pctlSideStoneCut
			ON pctlSideStoneCut.ID = pcx_JwlryFeatDetail_JMIC_PL.SideStoneCut
		LEFT JOIN `{project}.{pc_dataset}.pctl_stones_jmic_pl` AS pctlSideStoneType
			ON pctlSideStoneType.ID = pcx_JwlryFeatDetail_JMIC_PL.SideStoneType
		LEFT JOIN `{project}.{pc_dataset}.pctl_pearltype_jmic` AS pctlSideStonePearlType
			ON pctlSideStonePearlType.ID = pcx_JwlryFeatDetail_JMIC_PL.SideStonePearlType
		LEFT JOIN `{project}.{pc_dataset}.pctl_clarityenhanced_jmic_pl` AS pctlSideStoneClarityEnhancedType
			ON pctlSideStoneClarityEnhancedType.ID = pcx_JwlryFeatDetail_JMIC_PL.SideStoneClarityEnhancedType
		LEFT JOIN `{project}.{pc_dataset}.pctl_gemcert_jmic_pl` AS pctlSideStoneGemCert
			ON pctlSideStoneGemCert.ID = pcx_JwlryFeatDetail_JMIC_PL.SideGemCert
		--Mounting
		LEFT JOIN `{project}.{pc_dataset}.pctl_mounting_jmic_pl` AS pctl_mounting_jmic_pl
			ON pctl_mounting_jmic_pl.ID = pcx_JwlryFeatDetail_JMIC_PL.MountingType
		--Pearls
		LEFT JOIN `{project}.{pc_dataset}.pctl_pearltype_jmic` AS pctlPearlType
			ON pctlPearlType.ID = pcx_JwlryFeatDetail_JMIC_PL.PearlType

	WHERE 1 = 1
	--AND pctlPolPerSta.NAME = 'Bound'
	--AND JobNumber = '5835670' and ItemNumber = 9

) cRiskJIFeatures

	INNER JOIN RiskItemFeaturesConfig AS ConfigSource
		ON ConfigSource.Key='SourceSystem'
	INNER JOIN RiskItemFeaturesConfig AS ConfigHashSep
		ON ConfigHashSep.Key='HashKeySeparator'
	INNER JOIN RiskItemFeaturesConfig AS ConfigHashAlgo
		ON ConfigHashAlgo.Key='HashingAlgorithm'

WHERE 1 = 1
AND cRiskJIFeatures.FeatureRank = 1
AND cRiskJIFeatures.CostRank = 1;
