/******************************************************
  KIMBERLITE EXTRACT - RiskJewelryItem - BQ CONVERTED
*******************************************************
-----------------------------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

	02/26/2021	HALI		Cost Join fix applied (now a left join)
	06/07/2021	DROBAK		Replaced CostID with CostPublicID
	06/09/2021	DROBAK		Added PolicyNumber, IsTransactionSliceEffective, Fix to FixedItemRank, Unit Tests updated

-----------------------------------------------------------------------------------------------------------------------------------
 *****	Foreign Keys Origin	*****
-----------------------------------------------------------------------------------------------------------------------------------
	pcx_jewelryitem_jmic_pl.PublicID		AS	RiskJewelryItemKey
	pc_policyperiod.PublicID				AS	PolicyTransactionKey
-----------------------------------------------------------------------------------------------------------------------------------
*/
/*	Set universal latest partitiontime; In future will come from Airflow DAG (Today - 1)
	DECLARE {partition_date} Timestamp;
	SET {partition_date} = '2020-11-30'; -- (SELECT MAX(_PARTITIONTIME) FROM `{project}.{pc_dataset}.pc_policy`);

	CREATE OR REPLACE TABLE `{project}.B_QA_ref_kimberlite_config.RiskItemConfig`
	(
	  key STRING,
	  value STRING
	);

	  INSERT INTO 
				`{project}.B_QA_ref_kimberlite_config.RiskItemConfig`(Key,Value)
			VALUES	
				('SourceSystem','GW')
				,('ConfigHashSep','_')
				,('HashingAlgorithm','SHA2_256')
				,('PJILineCode','PersonalJewelryLine_JMIC_PL')
				,('PJILevelRisk','PersonalJewelryItem')
	;
*/

	WITH RiskItemConfig AS (
	  SELECT 'SourceSystem' as Key, 'GW' as Value UNION ALL
	  SELECT 'ConfigHashSep','_' UNION ALL
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
		--SK For PK [<Source>_<ItemPublicID>_<Level>]
		,SHA256(CONCAT(ConfigSource.Value,ConfigHashSep.Value, cRiskJI.ItemPublicID,ConfigHashSep.Value, cRiskJI.RiskLevel)) AS RiskJewelryItemKey
		--SK For FK [<Source>_<PolicyPeriodPublicID>]
		,SHA256(CONCAT(ConfigSource.Value,ConfigHashSep.Value, cRiskJI.PolicyPeriodPublicID)) AS PolicyTransactionKey 
		,DENSE_RANK() OVER(PARTITION BY	cRiskJI.ItemFixedID
										,cRiskJI.PolicyPeriodPublicID
										,cRiskJI.RiskLevel
							ORDER BY	cRiskJI.IsTransactionSliceEffective DESC
										,cRiskJI.FixedItemRank) AS FixedItemRank	
		
		,cRiskJI.IsTransactionSliceEffective
		,cRiskJI.PolicyNumber
		,cRiskJI.JobNumber
		,cRiskJI.ItemPublicID
		,cRiskJI.ItemFixedID
		,cRiskJI.PolicyPeriodPublicID
		,cRiskJI.RiskLevel
		,cRiskJI.ItemNumber
		,cRiskJI.ItemEffectiveDate
		,cRiskJI.ItemExpirationDate
		,cRiskJI.ItemDescription
		,cRiskJI.ItemClassOtherDescText
		,cRiskJI.ItemBrand
		,cRiskJI.ItemStyle
		,cRiskJI.ItemStyleOtherDescText
		,cRiskJI.ItemInitialValue
		,cRiskJI.ItemDescriptionDate
		,cRiskJI.ItemAppraisalReceived
		,cRiskJI.ItemAppraisalDocType
		,CAST(cRiskJI.ItemAppraisalViewEntireDoc AS INT64) as ItemAppraisalViewEntireDoc
		,cRiskJI.ItemIVADate
		,cRiskJI.ItemIVAPercentage
		,CAST(cRiskJI.ItemHasIVAApplied AS INT64) as ItemHasIVAApplied
		,CAST(cRiskJI.ItemUseInitialLimit AS INT64) as ItemUseInitialLimit
		,cRiskJI.ItemPremiumDiffForIVA
		,cRiskJI.ItemJewelerAppraiser
		,cRiskJI.ItemValuationType
		,cRiskJI.ItemBankVault
		,CAST(cRiskJI.ItemDamage AS INT64) as ItemDamage
		,cRiskJI.ItemDamagaeDescText
		,cRiskJI.ItemStored
		,cRiskJI.ItemPLSafe
		,CAST(cRiskJI.ItemSafe AS INT64) as ItemSafe
		,cRiskJI.ItemExpressDescText
		,CAST(cRiskJI.ItemExpressDescIsAppraisal AS INT64) as ItemExpressDescIsAppraisal
		,CAST(cRiskJI.IsItemInactive AS INT64) as IsItemInactive	
		,cRiskJI.InactiveReason
		,cRiskJI.CostPublicID
		,DATE('{date}') as bq_load_date

	FROM (
		SELECT 
			DENSE_RANK() OVER(PARTITION BY	pcx_jewelryitem_jmic_pl.PublicID
								ORDER BY	IFNULL(pcx_jewelryitem_jmic_pl.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
							) AS ItemRank
			,DENSE_RANK() OVER(PARTITION BY pcx_jewelryitem_jmic_pl.FixedID, pc_policyperiod.PublicID
								ORDER BY	IFNULL(pcx_jewelryitem_jmic_pl.ExpirationDate,pc_policyperiod.PeriodEnd) DESC	
							) AS FixedItemRank
			,DENSE_RANK() OVER(PARTITION BY pcx_jewelryitem_jmic_pl.PublicID
								ORDER BY	IFNULL(pcx_cost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd) DESC
											,pcx_cost_jmic.ID DESC
							) AS CostRank
			,pcx_jewelryitem_jmic_pl.PublicID														AS ItemPublicID
			,pcx_jewelryitem_jmic_pl.FixedID														AS ItemFixedID
			,pc_policyperiod.PublicID																AS PolicyPeriodPublicID
			,ConfigRisk.Value																		AS RiskLevel
			,pcx_jewelryitem_jmic_pl.ItemNumber														AS ItemNumber
			,pcx_jewelryitem_jmic_pl.EffectiveDate													AS ItemEffectiveDate
			,pcx_jewelryitem_jmic_pl.ExpirationDate													AS ItemExpirationDate
			,pctl_classcodetype_jmic_pl.L_en_US														AS ItemDescription
			,pcx_jewelryitem_jmic_pl.ClassOtherDesc													AS ItemClassOtherDescText
			,pctl_brandtype_jmic_pl.NAME															AS ItemBrand
			,pctl_jewelryitemstyle_jmic_pl.NAME														AS ItemStyle
			,pcx_jewelryitem_jmic_pl.StyleOtherDesc													AS ItemStyleOtherDescText
			,pcx_jewelryitem_jmic_pl.InitialLimit_JMIC												AS ItemInitialValue
			,pcx_jewelryitem_jmic_pl.InspectionDate													AS ItemDescriptionDate
			,CASE	WHEN pcx_jewelryitem_jmic_pl.AppraisalReceived_JMIC = true
							OR pcx_jewelryitem_jmic_pl.InspectionDate IS NOT NULL THEN 'Yes'
					ELSE 'No'
			 END																					AS ItemAppraisalReceived
			,pcx_jewelryitem_jmic_pl.AppraisalDocMimeType_JMIC										AS ItemAppraisalDocType
			,pcx_jewelryitem_jmic_pl.AppraisalViewEntireDoc_JMIC									AS ItemAppraisalViewEntireDoc
			,pcx_jewelryitem_jmic_pl.IVADate_JMIC													AS ItemIVADate
			,pcx_jewelryitem_jmic_pl.IVAPercentage_JMIC												AS ItemIVAPercentage
			,pcx_jewelryitem_jmic_pl.HasIVAApplied_JMIC												AS ItemHasIVAApplied
			,pcx_jewelryitem_jmic_pl.UseInitialLimit_JMIC											AS ItemUseInitialLimit
			,pcx_jewelryitem_jmic_pl.PremiumDiffForIVA_JMIC											AS ItemPremiumDiffForIVA
			,pcJewApprContact.Name																	AS ItemJewelerAppraiser
			,pctl_valuationtype_jmic_pl.NAME														AS ItemValuationType
			,pcVaultContact.Name																	AS ItemBankVault
			,pcx_jewelryitem_jmic_pl.Damage															AS ItemDamage
			,pcx_jewelryitem_jmic_pl.DamageDescription												AS ItemDamagaeDescText
			,pctl_wherestored_jmic.NAME																AS ItemStored
			,pcx_jewelryitem_jmic_pl.PLSafe															AS ItemPLSafe
			,pcx_jewelryitem_jmic_pl.Safe															AS ItemSafe
			,pcx_jewelryitem_jmic_pl.PartnerDescription_JMIC										AS ItemExpressDescText
			,pcx_jewelryitem_jmic_pl.PartnerDescIsAppraisal_JMIC									AS ItemExpressDescIsAppraisal
			,pcx_jewelryitem_jmic_pl.IsItemInactive													AS IsItemInactive	
			,pcx_jewelryitem_jmic_pl.InactiveReason													AS InactiveReason
			,pc_job.JobNumber
			,pc_policyperiod.PolicyNumber
			,pcx_cost_jmic.ID																		AS CostPublicID
			,CASE WHEN	pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_jewelryitem_jmic_pl.EffectiveDate,pc_policyperiod.PeriodStart)
					AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_jewelryitem_jmic_pl.ExpirationDate,pc_policyperiod.PeriodEnd) 
				THEN 1 ELSE 0
				END AS IsTransactionSliceEffective
		
		FROM `{project}.{pc_dataset}.pc_policyperiod` pc_policyperiod
		
			INNER JOIN `{project}.{pc_dataset}.pcx_jewelryitem_jmic_pl` pcx_jewelryitem_jmic_pl
			ON pcx_jewelryitem_jmic_pl.BranchID = pc_policyperiod.ID

			INNER JOIN `{project}.{pc_dataset}.pc_job` pc_job
			ON pc_job.ID = pc_policyperiod.JobID

			INNER JOIN `{project}.{pc_dataset}.pc_policyline` pc_policyline
			ON pc_policyline.BranchID = pc_policyperiod.ID
			AND COALESCE(pcx_jewelryitem_jmic_pl.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_jewelryitem_jmic_pl.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)

			INNER JOIN `{project}.{pc_dataset}.pctl_policyline` pctl_policyline
			ON pctl_policyline.ID = pc_policyline.SubType

			INNER JOIN RiskItemConfig AS ConfigBOPLine
			ON ConfigBOPLine.Value = pctl_policyline.TYPECODE
			AND ConfigBOPLine.Key = 'PJILineCode' 

			INNER JOIN RiskItemConfig AS ConfigRisk 
			ON ConfigRisk.Key='PJILevelRisk'

			INNER JOIN `{project}.{pc_dataset}.pctl_policyperiodstatus` pctl_policyperiodstatus
			ON pctl_policyperiodstatus.ID = pc_policyperiod.Status

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pcx_cost_jmic` WHERE _PARTITIONTIME = {partition_date}) pcx_cost_jmic
			ON pcx_cost_jmic.BranchID = pc_policyperiod.ID
			AND pcx_cost_jmic.JewelryItem_JMIC_PL = pcx_jewelryitem_jmic_pl.FixedID
			AND COALESCE(pcx_jewelryitem_jmic_pl.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_cost_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
			AND COALESCE(pcx_jewelryitem_jmic_pl.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pcx_cost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)

			LEFT JOIN `{project}.{pc_dataset}.pctl_classcodetype_jmic_pl` pctl_classcodetype_jmic_pl
			ON pctl_classcodetype_jmic_pl.ID = pcx_jewelryitem_jmic_pl.ClassCodeType

			LEFT JOIN `{project}.{pc_dataset}.pctl_brandtype_jmic_pl` pctl_brandtype_jmic_pl
			ON pctl_brandtype_jmic_pl.ID = pcx_jewelryitem_jmic_pl.PersonalItemBrand_JM

			LEFT JOIN `{project}.{pc_dataset}.pctl_jewelryitemstyle_jmic_pl` pctl_jewelryitemstyle_jmic_pl
			ON pctl_jewelryitemstyle_jmic_pl.ID = pcx_jewelryitem_jmic_pl.PersonalItemStyle_JM

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_contact` WHERE _PARTITIONTIME = {partition_date}) AS pcJewApprContact
			ON pcJewApprContact.ID = pcx_jewelryitem_jmic_pl.JewelerAppraiser

			LEFT JOIN `{project}.{pc_dataset}.pctl_valuationtype_jmic_pl` pctl_valuationtype_jmic_pl
			ON pctl_valuationtype_jmic_pl.ID = pcx_jewelryitem_jmic_pl.ValuationType

			LEFT JOIN (SELECT * FROM `{project}.{pc_dataset}.pc_contact` WHERE _PARTITIONTIME = {partition_date}) AS pcVaultContact
			ON pcVaultContact.ID = pcx_jewelryitem_jmic_pl.VaultInfo_JMIC

			LEFT JOIN `{project}.{pc_dataset}.pctl_wherestored_jmic` pctl_wherestored_jmic
			ON pctl_wherestored_jmic.ID = pcx_jewelryitem_jmic_pl.ItemWhereStored_JMIC

		WHERE 1 = 1
		AND pc_policyperiod._PARTITIONTIME = {partition_date}
		AND pcx_jewelryitem_jmic_pl._PARTITIONTIME = {partition_date}
		AND pc_job._PARTITIONTIME = {partition_date}
		AND pc_policyline._PARTITIONTIME = {partition_date}
		/* testing */
		--AND pctl_policyperiodstatus.NAME = 'Bound'
		--AND pc_job.JobNumber = '6601037'
		--AND pc_policyperiod.policynumber = '24-035290'

	) cRiskJI

		INNER JOIN RiskItemConfig AS ConfigSource
		ON ConfigSource.Key='SourceSystem'

		INNER JOIN RiskItemConfig AS ConfigHashSep
		ON ConfigHashSep.Key='ConfigHashSep'

	WHERE	1 = 1
		AND cRiskJI.ItemRank = 1
		AND cRiskJI.CostRank = 1

