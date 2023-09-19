/*******************************************************************************************
	Kimberlite Target Table
		RiskPAJewelryFeature
---------------------------------------------------------------------------------------------------------------
	--FeatureType
		--Center Stone
		--Grams
		--Length
		--Milli Meter
		--Model Number
		--Mounting
		--Pearls
		--Pre Owned
		--Serial Number
		--Side Stones
		--Watch Mounting
		--Other
---------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

	2/26/2021	DROBAK		Cost Join fix applied (now a left join)
	5/26/2021   SLJ			pcx_personalarticle_jm: join modified
	5/26/2021   SLJ			cntrStonesPearlType: join modified to pctl table
	5/26/2021   SLJ			sideStonesPearlType: join modified to pctl table
	5/26/2021   SLJ			pcx_JwlryFeatDetail_JMIC_PL: join modified to left join ex: pc:43523469
	5/26/2021   SLJ			columns added/removed/renamed

	,FeatureReportNumber						-- (renamed) GradingReportNumber
	,FeatureSerialNumber						-- (renamed) SerialNumber
	,FeatureGradingReport						-- (renamed) CenterStoneGradingReport & SideStoneGradingReport
	,FeatureMaterialType						-- (renamed) MaterialType
	,NumberOfCntrStones							-- (renamed) NumberOfCenterStones
	,CenterStoneSizeMM							-- (renamed) CenterStoneMilliMeter
	,CntrStoneOtherDesc							-- (renamed) CenterStoneOtherDesc
	,CntrStoneCutOtherDesc						-- (renamed) CenterStoneCutOtherDesc
	,StoneClarity								-- (renamed) CenterStoneClarity & SideStoneClarity
	,Other										-- (renamed) DescOfOther
	,StoneOtherDesc								-- (removed) All Null Values
	,CutOtherDesc								-- (removed) All Null Values
	,CrystalOtherDesc							-- (removed) All Null Values
	,MarkersOtherDesc							-- (removed) All Null Values
	,BraceletStrapDesc							-- (removed) All Null Values
	,DialOtherDesc								-- (removed) All Null Values
	,CaseSizeOtherDesc							-- (removed) All Null Values
	,BezelOtherDesc								-- (removed) All Null Values
	,PAJewelryFeatureDetailPublicID				-- (removed)
	,PAJewelryFeatureDetailFixedID				-- (removed)
	
	05/28/2021  SLJ			FixedRank fix applied
	01/17/2022	DROBAK		Change to Left Join: pcx_personalarticle_jm, pctl_personalarticle_jm, ConfigType
							Add Case Stmt for RiskPAJewelryKey
---------------------------------------------------------------------------------------------------------------
*/
--DELETE `qa-edl.B_QA_ref_kimberlite.dar_RiskPAJewelryFeature` WHERE bq_load_date = "2022-01-17";
--INSERT INTO `qa-edl.B_QA_ref_kimberlite.dar_RiskPAJewelryFeature`

WITH RiskJPAFeaturesConfig AS (
  SELECT 'SourceSystem' as Key,'GW' as Value UNION ALL
  SELECT 'HashKeySeparator','_' UNION ALL
  SELECT 'HashingAlgorithm','SHA2_256' UNION ALL
  SELECT 'PJALineCode','PersonalArtclLine_JM' UNION ALL
  SELECT 'PJALevelRisk','PersonalArticleJewelry' UNION ALL
  SELECT 'PAType', 'JewelryItem_JM'
)
	SELECT 
		sourceConfig.Value AS SourceSystem
		--SK For PK [<Source>_<FeatureDetailPublicID>]
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value, JPARiskFeature.PAJewelryFeaturePublicID)) AS RiskPAJewelryFeatureKey
		--SK For FK [<Source>_<ItemPublicID>_<Level>]
		,CASE WHEN JPARiskFeature.PAJewelryPublicID IS NOT NULL THEN
				SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value, JPARiskFeature.PAJewelryPublicID,hashKeySeparator.Value, JPARiskFeature.RiskLevel)) 
			END AS RiskPAJewelryKey
		--SK For FK [<Source>_<PolicyPeriodPublicID>]
		,SHA256(CONCAT(sourceConfig.Value,hashKeySeparator.Value, JPARiskFeature.PolicyPeriodPublicID)) AS PolicyTransactionKey
		,DENSE_RANK() OVER(PARTITION BY JPARiskFeature.PAJewelryFeatureFixedID, JPARiskFeature.PolicyPeriodPublicID
							ORDER BY	JPARiskFeature.IsTransactionSliceEffective DESC, JPARiskFeature.FixedFeatureRank ASC
						   ) AS FixedFeatureRank
	
		,JPARiskFeature.PAJewelryFeaturePublicID
		,JPARiskFeature.PAJewelryFeatureFixedID
		,JPARiskFeature.RiskLevel
		,JPARiskFeature.PAJewelryPublicID
		,JPARiskFeature.PAJewelryFixedID		
		,JPARiskFeature.PolicyPeriodPublicID
		,JPARiskFeature.JewelryArticleNumber
		,JPARiskFeature.PolicyNumber
		,JPARiskFeature.JobNumber
		,JPARiskFeature.EffectiveDate
		,JPARiskFeature.ExpirationDate
		,JPARiskFeature.IsTransactionSliceEffective 
	
		,JPARiskFeature.FeatureCode
		,JPARiskFeature.FeatureType
		,JPARiskFeature.FeatureDetailType

		--Center Stone
		,JPARiskFeature.NumberOfCenterStones							
		,JPARiskFeature.CenterStoneWeight					-- (added)			
		,JPARiskFeature.CenterStoneMilliMeter				-- (prior column name: CenterStoneSizeMM)				
		,JPARiskFeature.CenterStoneCut						-- (added)
		,JPARiskFeature.CenterStoneCutOtherDesc				-- (prior column name: CntrStoneCutOtherDesc)						
		,JPARiskFeature.CenterStoneType					
		,JPARiskFeature.CenterStoneOtherDesc				-- (prior column name: CntrStoneOtherDesc)				
		,JPARiskFeature.CenterStonePearlType				 
		,JPARiskFeature.ColorofCenterStone					-- (added)				
		,JPARiskFeature.CenterStoneClarity					-- (prior column name: StoneClarity coalesce column)			
		,JPARiskFeature.CenterStoneClarityEnhancedType		-- (added)
		,JPARiskFeature.CenterStoneGradingReport			-- (prior column name: FeatureGradingReport coalesce column)

		--Side Stones
		,JPARiskFeature.NumberOfSideStones					-- (added)			
		,JPARiskFeature.SideStoneWeight						-- (added)		
		,JPARiskFeature.SideStoneMilliMeter					-- (added)				
		,JPARiskFeature.SideStoneCut						-- (added)
		,JPARiskFeature.SideStoneCutOtherDesc								
		,JPARiskFeature.SideStoneType					
		,JPARiskFeature.SideStoneOtherDesc								
		,JPARiskFeature.SideStonePearlType				 
		,JPARiskFeature.ColorofSideStone					-- (added)						
		,JPARiskFeature.SideStoneClarity					-- (prior column name: StoneClarity coalesce column)					
		,JPARiskFeature.SideStoneClarityEnhancedType		-- (added)
		,JPARiskFeature.SideStoneGradingReport				-- (prior column name: FeatureGradingReport coalesce column)

		--Center Stone
		--Side Stones
		,JPARiskFeature.GradingReportNumber					-- (prior column name: FeatureReportNumber)
		,JPARiskFeature.MaterialType						-- (prior column name: FeatureMaterialType)				

		--Grams
		,JPARiskFeature.GramsOrDWT							-- (added)		

		--Length
		,JPARiskFeature.Length								

		--Milli Meter
		,JPARiskFeature.MilliMeter							-- (added)	

		--Model Number
		,JPARiskFeature.ModelNumber							-- (added)			

		--Mounting
		,JPARiskFeature.MountingType									
		,JPARiskFeature.MountingOtherDesc													

		--Pearls
		,JPARiskFeature.NumberOfPearls						-- (added)						
		,JPARiskFeature.PearlType							-- (added)		

		--Center Stone
		--Side Stones
		--Pearls
		,JPARiskFeature.PearlTypeOtherDesc									

		--Pre Owned
		,JPARiskFeature.PreOwned							-- (added)					

		--Serial Number
		,JPARiskFeature.SerialNumber						-- (prior column name: FeatureSerialNumber)									

		--Watch Mounting
		,JPARiskFeature.WatchMountingType					-- (added)
		,JPARiskFeature.WatchMountingOtherDesc

		--Other
		,JPARiskFeature.DescOfOther							-- (prior column name: Other)

		--Center Stone
		--Side Stones
		--Grams
		--Length
		--Milli Meter
		--Model Number
		--Mounting
		--Other
		--Pearls
		--Pre Owned
		--Serial Number
		--Watch Mounting
		,JPARiskFeature.FeatureNotes
		, DATE('{date}') as bq_load_date

	--INTO #PAJwlryFeatureData
	FROM (
		SELECT 
			DENSE_RANK() OVER(PARTITION BY	pcx_JwlryFeature_JMIC_PL.ID
								ORDER BY	IFNULL(pcx_personalarticle_jm.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,IFNULL(pcx_JwlryFeature_JMIC_PL.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
							  ) AS FeatureRank
			,DENSE_RANK() OVER(PARTITION BY pcx_JwlryFeature_JMIC_PL.FixedID, pc_policyperiod.ID
								ORDER BY	IFNULL(pcx_personalarticle_jm.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,IFNULL(pcx_JwlryFeature_JMIC_PL.ExpirationDate,pc_policyperiod.PeriodEnd) DESC	
							   ) AS FixedFeatureRank
			,DENSE_RANK() OVER(PARTITION BY pcx_JwlryFeature_JMIC_PL.ID
								ORDER BY	IFNULL(pcx_jpacost_jm.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,pcx_jpacost_jm.ID DESC
							   ) AS CostRank
		
			,pcx_JwlryFeature_JMIC_PL.PublicID							AS PAJewelryFeaturePublicID
			,pcx_JwlryFeature_JMIC_PL.FixedID							AS PAJewelryFeatureFixedID
			,ConfigRisk.Value											AS RiskLevel
			,pcx_personalarticle_jm.PublicID							AS PAJewelryPublicID
			,pcx_personalarticle_jm.FixedID								AS PAJewelryFixedID		
			,pc_policyperiod.PublicID									AS PolicyPeriodPublicID
			,pcx_personalarticle_jm.ItemNumber							AS JewelryArticleNumber		   
			,pc_policyperiod.PolicyNumber								AS PolicyNumber
			,pc_job.JobNumber											AS JobNumber
			,pcx_JwlryFeature_JMIC_PL.EffectiveDate						AS EffectiveDate
			,pcx_JwlryFeature_JMIC_PL.ExpirationDate					AS ExpirationDate
			,CASE WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_JwlryFeature_JMIC_PL.EffectiveDate,pc_policyperiod.PeriodStart)
						AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_JwlryFeature_JMIC_PL.ExpirationDate,pc_policyperiod.PeriodEnd) THEN 1 ELSE 0 END AS IsTransactionSliceEffective
	
			,pcx_JwlryFeature_JMIC_PL.FeatureType						AS FeatureCode
			,pctl_featuretype_jmic.NAME									AS FeatureType
			,pctl_jwlryfeatdetail_jmic_pl.NAME							AS FeatureDetailType

			--Center Stone
			,pcx_JwlryFeatDetail_JMIC_PL.NumberOfCntrStones				AS NumberOfCenterStones							
			,pcx_JwlryFeatDetail_JMIC_PL.CntrStoneWeight				AS CenterStoneWeight				
			,pcx_JwlryFeatDetail_JMIC_PL.CntrStoneMilliMeter			AS CenterStoneMilliMeter			
			,pctlCntrStoneCut.NAME										AS CenterStoneCut					
			,pcx_JwlryFeatDetail_JMIC_PL.CntrStoneCutOtherDesc			AS CenterStoneCutOtherDesc				
			,pctlCntrStoneType.NAME										AS CenterStoneType					
			,pcx_JwlryFeatDetail_JMIC_PL.CntrStoneOtherDesc				AS CenterStoneOtherDesc				
			,pctlCntrStonePearlType.NAME								AS CenterStonePearlType				
			,pcx_JwlryFeatDetail_JMIC_PL.ColorofCntrStone				AS ColorofCenterStone				
			,pcx_JwlryFeatDetail_JMIC_PL.CntrStoneClarity				AS CenterStoneClarity				
			,pctlCntrStoneClarityEnhancedType.NAME						AS CenterStoneClarityEnhancedType	
			,pctlCntrStoneGemCert.NAME									AS CenterStoneGradingReport			

			--Side Stones
			,pcx_JwlryFeatDetail_JMIC_PL.NumberOfSideStones				AS NumberOfSideStones				
			,pcx_JwlryFeatDetail_JMIC_PL.SideStoneWeight				AS SideStoneWeight					
			,pcx_JwlryFeatDetail_JMIC_PL.SideStoneMilliMeter			AS SideStoneMilliMeter				
			,pctlSideStoneCut.NAME										AS SideStoneCut						
			,pcx_JwlryFeatDetail_JMIC_PL.SideStoneCutOtherDesc			AS SideStoneCutOtherDesc			
			,pctlSideStoneType.NAME										AS SideStoneType					
			,pcx_JwlryFeatDetail_JMIC_PL.SideStoneOtherDesc				AS SideStoneOtherDesc				
			,pctlSideStonePearlType.NAME								AS SideStonePearlType				
			,pcx_JwlryFeatDetail_JMIC_PL.ColorofSideStone				AS ColorofSideStone					
			,pcx_JwlryFeatDetail_JMIC_PL.SideStoneClarity				AS SideStoneClarity							
			,pctlSideStoneClarityEnhancedType.NAME						AS SideStoneClarityEnhancedType		
			,pctlSideStoneGemCert.NAME									AS SideStoneGradingReport			

			--Center Stone
			--Side Stones
			,pcx_JwlryFeatDetail_JMIC_PL.CertNo							AS GradingReportNumber			
			,pctlMaterialType.NAME										AS MaterialType								

			--Grams
			,pcx_JwlryFeatDetail_JMIC_PL.Grams							AS GramsOrDWT					

			--Length
			,pcx_JwlryFeatDetail_JMIC_PL.Length							AS Length						

			--Milli Meter
			,pcx_JwlryFeatDetail_JMIC_PL.MilliMeter						AS MilliMeter					

			--Model Number
			,pcx_JwlryFeatDetail_JMIC_PL.ModelNo						AS ModelNumber					

			--Mounting
			,pctlMounting.NAME											AS MountingType					
			,pcx_JwlryFeatDetail_JMIC_PL.MountingOtherDesc				AS MountingOtherDesc			

			--Pearls
			,pcx_JwlryFeatDetail_JMIC_PL.NumberOfPearls					AS NumberOfPearls				
			,pctlPearlType.NAME											AS PearlType					

			--Center Stone
			--Side Stones
			--Pearls
			,pcx_JwlryFeatDetail_JMIC_PL.PearlTypeOtherDesc				AS PearlTypeOtherDesc			

			--Pre Owned
			,CAST(pcx_JwlryFeatDetail_JMIC_PL.PreOwned as INT64)						AS PreOwned						

			--Serial Number
			,pcx_JwlryFeatDetail_JMIC_PL.SerialNo						AS SerialNumber												

			--Watch Mounting
			,pcx_JwlryFeatDetail_JMIC_PL.WatchMountingType				AS WatchMountingType			
			,pcx_JwlryFeatDetail_JMIC_PL.WatchMountingOtherDesc			AS WatchMountingOtherDesc

			--Other
			,pcx_JwlryFeatDetail_JMIC_PL.Other							AS DescOfOther					

			--Center Stone
			--Side Stones
			--Grams
			--Length
			--Milli Meter
			--Model Number
			--Mounting
			--Other
			--Pearls
			--Pre Owned
			--Serial Number
			--Watch Mounting
			,pcx_JwlryFeatDetail_JMIC_PL.FeatureNotes					AS FeatureNotes

		FROM (SELECT * FROM `{project}.{pc_dataset}.pc_policyperiod` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyperiod

			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_job` WHERE _PARTITIONTIME = {partition_date}) AS pc_job
			ON pc_job.ID = pc_policyperiod.JobID

			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_JwlryFeature_JMIC_PL` WHERE _PARTITIONTIME = {partition_date}) AS pcx_JwlryFeature_JMIC_PL
			ON pc_policyperiod.ID = pcx_JwlryFeature_JMIC_PL.BranchID

			INNER JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_policyline` WHERE _PARTITIONTIME = {partition_date}) AS pc_policyline
			ON pc_policyline.BranchID = pc_policyperiod.ID
			AND COALESCE(pcx_JwlryFeature_JMIC_PL.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_JwlryFeature_JMIC_PL.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)
	
			INNER JOIN `{project}.{pc_dataset}.pctl_policyline` AS pctl_policyline
			ON pctl_policyline.ID = pc_policyline.SubType
			--AND pctl_policyline.TYPECODE = 'PersonalArtclLine_JM'

			INNER JOIN RiskJPAFeaturesConfig AS ConfigPJALine
			ON ConfigPJALine.Value = pctl_policyline.TYPECODE
			AND ConfigPJALine.Key = 'PJALineCode' 
	
			INNER JOIN RiskJPAFeaturesConfig AS ConfigRisk
			ON ConfigRisk.Key='PJALevelRisk'

			LEFT JOIN (SELECT * FROM  `{project}.{pc_dataset}.pcx_personalarticle_jm` WHERE _PARTITIONTIME = {partition_date}) AS pcx_personalarticle_jm
			ON pcx_personalarticle_jm.BranchID = pcx_JwlryFeature_JMIC_PL.BranchID
			AND pcx_personalarticle_jm.FixedID = pcx_JwlryFeature_JMIC_PL.PAItem
			AND COALESCE(pcx_JwlryFeature_JMIC_PL.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_personalarticle_jm.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_JwlryFeature_JMIC_PL.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pcx_personalarticle_jm.ExpirationDate,pc_policyperiod.PeriodEnd)

			LEFT JOIN `{project}.{pc_dataset}.pctl_personalarticle_jm` AS pctl_personalarticle_jm
			ON pcx_personalarticle_jm.Subtype = pctl_personalarticle_jm.ID	
			--AND pctl_Personalarticle_jm.TYPECODE = 'JewelryItem_JM'

			LEFT JOIN RiskJPAFeaturesConfig AS ConfigType
			ON ConfigType.Value = pctl_Personalarticle_jm.TYPECODE
			AND ConfigType.Key='PAType'

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_jpacost_jm` WHERE _PARTITIONTIME = {partition_date}) AS pcx_jpacost_jm
			ON pcx_jpacost_jm.BranchID = pc_policyperiod.id
			AND pcx_personalarticle_jm.FixedID = pcx_jpacost_jm.PersonalArticle  --pcx_JwlryFeature_JMIC_PL.PAItem
			AND COALESCE(pcx_personalarticle_jm.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_jpacost_jm.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_personalarticle_jm.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pcx_jpacost_jm.ExpirationDate,pc_policyperiod.PeriodEnd)

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_JwlryFeatDetail_JMIC_PL` WHERE _PARTITIONTIME = {partition_date}) AS pcx_JwlryFeatDetail_JMIC_PL
			ON pcx_JwlryFeatDetail_JMIC_PL.BranchID = pcx_JwlryFeature_JMIC_PL.BranchID
			AND pcx_JwlryFeatDetail_JMIC_PL.Feature = pcx_JwlryFeature_JMIC_PL.FixedID
			AND COALESCE(pcx_JwlryFeature_JMIC_PL.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_JwlryFeatDetail_JMIC_PL.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_JwlryFeature_JMIC_PL.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pcx_JwlryFeatDetail_JMIC_PL.ExpirationDate,pc_policyperiod.PeriodEnd)

			--Type
			LEFT JOIN `{project}.{pc_dataset}.pctl_featuretype_jmic` AS pctl_featuretype_jmic
			ON pctl_featuretype_jmic.ID = pcx_JwlryFeature_JMIC_PL.FeatureType

			LEFT JOIN `{project}.{pc_dataset}.pctl_jwlryfeatdetail_jmic_pl` AS pctl_jwlryfeatdetail_jmic_pl
			ON pctl_jwlryfeatdetail_jmic_pl.ID = pcx_JwlryFeatDetail_JMIC_PL.Subtype

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

			LEFT OUTER JOIN `{project}.{pc_dataset}.pctl_gemcert_jmic_pl` AS pctlSideStoneGemCert
			ON pctlSideStoneGemCert.ID = pcx_JwlryFeatDetail_JMIC_PL.SideGemCert

			--Mounting
			LEFT JOIN `{project}.{pc_dataset}.pctl_mounting_jmic_pl` AS pctlMounting
			ON pctlMounting.ID = pcx_JwlryFeatDetail_JMIC_PL.MountingType

			--Pearls
			LEFT JOIN `{project}.{pc_dataset}.pctl_pearltype_jmic` AS pctlPearlType
			ON pctlPearlType.ID = pcx_JwlryFeatDetail_JMIC_PL.PearlType

			--Center Stone
			--Side Stones
			LEFT JOIN `{project}.{pc_dataset}.pctl_jpamaterialtype_jm` AS pctlMaterialType
			ON pctlMaterialType.ID = pcx_JwlryFeatDetail_JMIC_PL.MaterialType
	
		WHERE	1 = 1
	
	)JPARiskFeature

		INNER JOIN RiskJPAFeaturesConfig AS sourceConfig
		ON sourceConfig.Key='SourceSystem'

		INNER JOIN RiskJPAFeaturesConfig AS hashKeySeparator
		ON hashKeySeparator.Key='HashKeySeparator'

		INNER JOIN RiskJPAFeaturesConfig AS hashAlgorithm
		ON hashAlgorithm.Key='HashingAlgorithm'

	WHERE	1 = 1
		AND JPARiskFeature.FeatureRank = 1
		AND JPARiskFeature.CostRank = 1
