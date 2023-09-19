
;WITH ModifierDiscount AS 
(
	SELECT
		pcx_jwlrymodjmicpl.JewelryItem_JMIC_PL
		,pcx_jwlrymodjmicpl.BranchID
		,SUM(CASE WHEN pcx_jwlrymodjmicpl.PatternCode like 'GemID%'		
						AND pcx_jwlrymodjmicpl.BooleanModifier = 1 THEN 1 ELSE 0 END)							AS GemID
		,SUM(CASE WHEN pcx_jwlrymodjmicpl.PatternCode like 'Safe%' 
						AND pcx_jwlrymodjmicpl.BooleanModifier = 1 THEN 1 ELSE 0 END)							AS Safe
		,SUM(CASE WHEN pcx_jwlrymodjmicpl.PatternCode like 'Valuation%' 
						AND pcx_jwlrymodjmicpl.BooleanModifier = 1 THEN 1 ELSE 0 END)							AS Valuation
		,SUM(CASE WHEN pcx_jwlrymodjmicpl.PatternCode like 'Vault%' 
						AND pcx_jwlrymodjmicpl.BooleanModifier = 1 THEN 1 ELSE 0 END)							AS Vault
		,SUM(CASE WHEN pcx_jwlrymodjmicpl.PatternCode like 'IRPM%' THEN pcx_jwlrymodjmicpl.RateModifier END)	AS IRPM
	FROM PolicyCenter.dbo.pc_policyperiod AS pc_policyperiod
       
		INNER JOIN PolicyCenter.dbo.pcx_jewelryitem_jmic_pl AS pcx_jewelryitem_jmic_pl
		on pcx_jewelryitem_jmic_pl.BranchID = pc_policyperiod.ID

		INNER JOIN PolicyCenter.dbo.pc_policyline AS pc_policyline
		ON pc_policyline.BranchID = pc_policyperiod.ID
		AND COALESCE(pcx_jewelryitem_jmic_pl.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_jewelryitem_jmic_pl.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)

		INNER JOIN PolicyCenter.dbo.pctl_policyline AS pctl_policyline
		ON pctl_policyline.ID = pc_policyline.SubType
		AND pctl_policyline.TYPECODE = 'PersonalJewelryLine_JMIC_PL'

		INNER JOIN PolicyCenter.dbo.pcx_jwlrymodjmicpl AS pcx_jwlrymodjmicpl
		ON pcx_jwlrymodjmicpl.JewelryItem_JMIC_PL = pcx_jewelryitem_jmic_pl.FixedID
		AND pcx_jwlrymodjmicpl.BranchID = pcx_jewelryitem_jmic_pl.BranchID
		AND COALESCE(pcx_jewelryitem_jmic_pl.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_jwlrymodjmicpl.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_jewelryitem_jmic_pl.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_jwlrymodjmicpl.ExpirationDate,pc_policyperiod.PeriodEnd)

	WHERE YEAR(pc_policyperiod.PeriodStart) >= 2017-- AND MONTH(pc_policyperiod.PeriodStart) = 1 

	GROUP BY
		pcx_jwlrymodjmicpl.JewelryItem_JMIC_PL
		,pcx_jwlrymodjmicpl.BranchID
)
,Alarm AS
(
	SELECT DISTINCT
		pc_job.JobNumber																			AS JobNumber
		,ISNULL(pctl_alarm_jmic_pl.NAME,'N/A')														AS PolicyAlarm
		,DENSE_RANK() OVER(PARTITION BY pc_job.JobNumber
							ORDER BY	pc_policyline.Alarm ASC)									AS AlarmSelect

	FROM PolicyCenter.dbo.pc_policyperiod AS pc_policyperiod

		INNER JOIN PolicyCenter.dbo.pc_job AS pc_job
		ON pc_job.ID = pc_policyperiod.JobID 

		INNER JOIN PolicyCenter.dbo.pc_policyline AS pc_policyline
		ON pc_policyline.BranchID = pc_policyperiod.ID
		AND pc_policyperiod.EditEffectiveDate >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
		AND pc_policyperiod.EditEffectiveDate < COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)

		INNER JOIN PolicyCenter.dbo.pctl_policyline AS pctl_policyline
		ON pctl_policyline.ID = pc_policyline.SubType
		AND pctl_policyline.TYPECODE = 'PersonalJewelryLine_JMIC_PL'

		LEFT JOIN PolicyCenter.dbo.pctl_alarm_jmic_pl AS pctl_alarm_jmic_pl
		ON pctl_alarm_jmic_pl.ID = pc_policyline.Alarm

	WHERE YEAR(pc_policyperiod.PeriodStart) >= 2017-- AND MONTH(pc_policyperiod.PeriodStart) = 1
)
,Scheduled AS (
	SELECT DISTINCT
		'J_'+CAST(pc_job.JobNumber AS varchar(250))												AS JobNumber
		,ISNULL(cAlarm.PolicyAlarm,'N/A')														AS PolicyAlarm
		,CASE WHEN pc_policyperiod.TotalMinimumPremiumRPT_JMIC > 0 
						AND pc_policyperiod.TotalSchedPremiumRPT_JMIC = 0 THEN 'Yes' ELSE 'No' END AS PolicyTransactionSubSchZero
		,CASE WHEN pc_policyperiod.TotalMinimumPremiumRPT_JMIC > 0 THEN 'Yes' ELSE 'No' END		AS PolicyTransactionMinPremFlag

		,pcx_jewelryitem_jmic_pl.ItemNumber														AS ItemNumber
		,CASE	WHEN pcx_jewelryitem_jmic_pl.AppraisalReceived_JMIC = 1 
						OR pcx_jewelryitem_jmic_pl.InspectionDate IS NOT NULL THEN 1
				ELSE 0
		 END																					AS ItemAppraisalReceived
		,CASE	WHEN pcx_jewelryitem_jmic_pl.Damage = 1 THEN 1 ELSE 0 END						AS ItemDamage
		,CASE	WHEN pcx_jewelryitem_jmic_pl.Vault = 1 OR cModDisc.Vault >= 1 THEN 1 
				ELSE 0 
		 END																					AS ItemModifierVault
		,CASE	WHEN cModDisc.GemID >= 1 THEN 1 ELSE 0 END										AS ItemModifierGemID
		,CASE	WHEN pcx_jewelryitem_jmic_pl.Safe = 1 THEN 1
				WHEN pctl_wherestored_jmic.NAME = 'Safe' THEN 1
				WHEN pcx_jewelryitem_jmic_pl.PLSafe IS NOT NULL THEN 1
				ELSE 0
		 END																					AS ItemSafeMultiCombi
		,CASE	WHEN cModDisc.Safe >= 1 THEN 1 
				ELSE 0 
		 END																					AS ItemModifierSafe
		,CASE	WHEN cModDisc.Valuation >= 1 THEN 1 
				ELSE 0 
		 END																					AS ItemModifierValuation
		,CASE	WHEN cModDisc.IRPM <> 0 THEN 1 
				ELSE 0 
		 END																					AS ItemModifierIRPM
		,DENSE_RANK() OVER(PARTITION BY	pcx_jewelryitem_jmic_pl.PublicID
							ORDER BY	ISNULL(pcx_jewelryitem_jmic_pl.ExpirationDate
												,pc_policyperiod.PeriodEnd) DESC)				AS ItemRank
		,DENSE_RANK() OVER(PARTITION BY pcx_jewelryitem_jmic_pl.FixedID
										,pc_policyperiod.PublicID
							ORDER BY	ISNULL(pcx_jewelryitem_jmic_pl.ExpirationDate
												,pc_policyperiod.PeriodEnd) DESC)				AS FixedItemRank
		,DENSE_RANK() OVER(PARTITION BY pcx_jewelryitem_jmic_pl.PublicID
							ORDER BY	ISNULL(pcx_cost_jmic.ExpirationDate
												,pc_policyperiod.PeriodEnd) DESC
										,pcx_cost_jmic.ID DESC)									AS CostRank
		,CASE WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_jewelryitem_jmic_pl.EffectiveDate,pc_policyperiod.PeriodStart)
					AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_jewelryitem_jmic_pl.ExpirationDate,pc_policyperiod.PeriodEnd) 
			  THEN 1 ELSE 0
			  END																				AS IsTransactionSliceEffective

	FROM PolicyCenter.dbo.pc_policyperiod AS pc_policyperiod
       
		INNER JOIN PolicyCenter.dbo.pcx_jewelryitem_jmic_pl AS pcx_jewelryitem_jmic_pl
		on pcx_jewelryitem_jmic_pl.BranchID = pc_policyperiod.ID

		INNER JOIN PolicyCenter.dbo.pc_job AS pc_job
		ON pc_job.ID = pc_policyperiod.JobID 

		INNER JOIN PolicyCenter.dbo.pc_policyline AS pc_policyline
		ON pc_policyline.BranchID = pc_policyperiod.ID
		AND COALESCE(pcx_jewelryitem_jmic_pl.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_jewelryitem_jmic_pl.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)

		INNER JOIN PolicyCenter.dbo.pctl_policyline AS pctl_policyline
		ON pctl_policyline.ID = pc_policyline.SubType
		AND pctl_policyline.TYPECODE = 'PersonalJewelryLine_JMIC_PL'

		INNER JOIN PolicyCenter.dbo.pctl_policyperiodstatus AS pctl_policyperiodstatus
		ON pctl_policyperiodstatus.ID = pc_policyperiod.Status 

		LEFT JOIN PolicyCenter.dbo.pcx_cost_jmic AS pcx_cost_jmic
		ON pcx_cost_jmic.BranchID = pc_policyperiod.ID
		AND pcx_cost_jmic.JewelryItem_JMIC_PL = pcx_jewelryitem_jmic_pl.FixedID
		AND COALESCE(pcx_jewelryitem_jmic_pl.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_cost_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_jewelryitem_jmic_pl.EffectiveDate,pc_policyperiod.EditEffectiveDate) <  COALESCE(pcx_cost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)

		LEFT JOIN PolicyCenter.dbo.pctl_wherestored_jmic AS pctl_wherestored_jmic
		ON pctl_wherestored_jmic.ID = pcx_jewelryitem_jmic_pl.ItemWhereStored_JMIC

		LEFT JOIN ModifierDiscount AS cModDisc
		ON cModDisc.JewelryItem_JMIC_PL = pcx_jewelryitem_jmic_pl.FixedID
		AND cModDisc.BranchID = pcx_jewelryitem_jmic_pl.BranchID

		LEFT JOIN Alarm AS cAlarm
		ON cAlarm.JobNumber = pc_job.JobNumber
		AND cAlarm.AlarmSelect = 1

	WHERE	1 = 1
			AND pctl_policyperiodstatus.NAME = 'Bound' 
			AND pc_policyperiod.ModelNumber IS NOT NULL
			AND YEAR(pc_policyperiod.PeriodStart) >= 2017-- AND MONTH(pc_policyperiod.PeriodStart) = 1
)
,Unscheduled AS 
(
	SELECT DISTINCT
		'J_'+CAST(pc_job.JobNumber AS varchar(250))												AS JobNumber
		,ISNULL(cAlarm.PolicyAlarm,'N/A')														AS PolicyAlarm
		,CASE WHEN pc_policyperiod.TotalMinimumPremiumRPT_JMIC > 0 
						AND pc_policyperiod.TotalSchedPremiumRPT_JMIC = 0 THEN 'Yes' ELSE 'No' END AS PolicyTransactionSubSchZero
		,CASE WHEN pc_policyperiod.TotalMinimumPremiumRPT_JMIC > 0 THEN 'Yes' ELSE 'No' END		AS PolicyTransactionMinPremFlag

		,0																						AS ItemNumber
		,0																						AS ItemAppraisalReceived
		,0																						AS ItemDamage
		,0																						AS ItemModifierVault
		,0																						AS ItemModifierGemID
		,0																						AS ItemSafeMultiCombi
		,0																						AS ItemModifierSafe
		,0																						AS ItemModifierValuation
		,0																						AS ItemModifierIRPM
		,DENSE_RANK() OVER(PARTITION BY pcx_jmpersonallinecov.ID
							ORDER BY	ISNULL(pcx_jmpersonallinecov.ExpirationDate
												,pc_policyperiod.PeriodEnd) DESC)				AS ItemRank
		,DENSE_RANK() OVER(PARTITION BY pcx_jmpersonallinecov.FixedID
										,pc_policyperiod.ID
							ORDER BY	ISNULL(pcx_jmpersonallinecov.ExpirationDate
												,pc_policyperiod.PeriodEnd) DESC)				AS FixedItemRank
		,DENSE_RANK() OVER(PARTITION BY pcx_jmpersonallinecov.ID
							ORDER BY	ISNULL(pcx_cost_jmic.ExpirationDate
												,pc_policyperiod.PeriodEnd) DESC
										,pcx_cost_jmic.ID DESC)									AS CostRank
		,CASE	WHEN pc_policyperiod.EditEffectiveDate >= COALESCE(pcx_jmpersonallinecov.EffectiveDate,pc_policyperiod.PeriodStart)
					AND pc_policyperiod.EditEffectiveDate <  COALESCE(pcx_jmpersonallinecov.ExpirationDate,pc_policyperiod.PeriodEnd) 
				THEN 1 ELSE 0 
		 END																					AS IsTransactionSliceEffective

	FROM PolicyCenter.dbo.pc_policyperiod AS pc_policyperiod
       
		INNER JOIN PolicyCenter.dbo.pcx_jmpersonallinecov AS pcx_jmpersonallinecov
		on pcx_jmpersonallinecov.BranchID = pc_policyperiod.ID

		INNER JOIN PolicyCenter.dbo.pc_job AS pc_job
		ON pc_job.ID = pc_policyperiod.JobID 

		INNER JOIN PolicyCenter.dbo.pc_policyline AS pc_policyline
		ON pc_policyline.BranchID = pc_policyperiod.ID
		AND COALESCE(pcx_jmpersonallinecov.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pc_policyline.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_jmpersonallinecov.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pc_policyline.ExpirationDate,pc_policyperiod.PeriodEnd)

		INNER JOIN PolicyCenter.dbo.pctl_policyline AS pctl_policyline
		ON pctl_policyline.ID = pc_policyline.SubType
		AND pctl_policyline.TYPECODE = 'PersonalJewelryLine_JMIC_PL'

		INNER JOIN PolicyCenter.dbo.pctl_policyperiodstatus AS pctl_policyperiodstatus
		ON pctl_policyperiodstatus.ID = pc_policyperiod.Status 
			
		LEFT JOIN PolicyCenter.dbo.pcx_cost_jmic AS pcx_cost_jmic
		ON pcx_cost_jmic.BranchID = pc_policyperiod.ID
		AND pcx_cost_jmic.JewelryLineCov_JMIC_PL = pcx_jmpersonallinecov.FixedID
		AND COALESCE(pcx_jmpersonallinecov.EffectiveDate,pc_policyperiod.EditEffectiveDate) >= COALESCE(pcx_cost_jmic.EffectiveDate,pc_policyperiod.PeriodStart)
		AND COALESCE(pcx_jmpersonallinecov.EffectiveDate,pc_policyperiod.EditEffectiveDate) < COALESCE(pcx_cost_jmic.ExpirationDate,pc_policyperiod.PeriodEnd)

		LEFT JOIN Alarm AS cAlarm
		ON cAlarm.JobNumber = pc_job.JobNumber
		AND cAlarm.AlarmSelect = 1

	WHERE	1 = 1
				AND pctl_policyperiodstatus.NAME = 'Bound' 
				AND pc_policyperiod.ModelNumber IS NOT NULL
				AND COALESCE(pcx_jmpersonallinecov.FinalPersistedLimit_JMIC,pcx_jmpersonallinecov.DirectTerm1,0) > 0
			AND YEAR(pc_policyperiod.PeriodStart) >= 2017-- AND MONTH(pc_policyperiod.PeriodStart) = 1
)
SELECT 
	JobNumber
	,PolicyAlarm
	,PolicyTransactionSubSchZero
	,PolicyTransactionMinPremFlag
	,ItemNumber
	,ItemAppraisalReceived
	,ItemDamage
	,ItemModifierVault
	,ItemModifierGemID
	,ItemSafeMultiCombi
	,ItemModifierSafe
	,ItemModifierValuation
	,ItemModifierIRPM
	,IsTransactionSliceEffective

FROM (
	SELECT * FROM Scheduled 
	UNION ALL 
	SELECT * FROM Unscheduled
)t
WHERE ItemRank = 1 AND CostRank = 1 AND FixedItemRank = 1

