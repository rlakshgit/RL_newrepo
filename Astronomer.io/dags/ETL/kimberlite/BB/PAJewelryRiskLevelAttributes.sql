/*
	Kimberlite - Building Blocks
	PAJewelryRiskLevelAttributes - Personal Article Jewelry Attributes and Feature Details
	---------------------------------------------------------------------------------------
	--Change Log--
	---------------------------------------------------------------------------------------
	05/05/2021	DROBAK		Init
	06/20/2021	SLJ			Updated fields output
	07/15/2021	DROBAK		Added SourceSystem; IsTransactionSliceEffective
	07/20/2021	DROBAK		Table name is: PAJewelryRiskLevelAttributes (old name: RiskPAJewelryAttributes)
	07/21/2021	DROBAK		Updated fields due to changes in Kimberlite source table; turned off where clause IsTransactionSliceEffective
	08/27/2021	DROBAK		Removed INNER JOIN to PolicyTransaction table (deemed unnecessary); corrected Group By syntax error

	---------------------------------------------------------------------------------------
*/
DELETE `{project}.{dest_dataset}.PAJewelryRiskLevelAttributes` WHERE bq_load_date = DATE({partition_date});
INSERT INTO `{project}.{dest_dataset}.PAJewelryRiskLevelAttributes`
(
        SourceSystem		,
		RiskPAJewelryKey		,
		PolicyTransactionKey		,
		JobNumber		,
		RiskLevel		,
		JewelryArticleNumber		,
		EffectiveDate		,
		ExpirationDate		,
		IsTransactionSliceEffective		,
		ArticleType		,
		ArticleSubType		,
		ArticleGender		,
		IsWearableTech		,
		ArticleBrand		,
		ArticleStyle		,
		InitialValue		,
		IsFullDescOverridden		,
		FullDescription			,
		IsAppraisalRequested		,
		IsAppraisalReceived		,
		AppraisalDate		,
		InspectionDate		,
		IVAPercentage		,
		IsIVADeclined		,
		IVADate		,
		IsIVAApplied		,
		ValuationType		,
		IsDamaged		,
		DamageType		,
		IsArticleInactive	,
		InactiveReason		,
		ArticleStored		,
		SafeDetails		,
		TimeOutOfVault		,
		HasCarePlan		,
		CarePlanID		,
		CarePlanExpirationDate	,
		DurationWithOtherInsurer	,
		ArticleHowAcquired		,
		ArticleYearAcquired		,
		CenterStoneFeatures		,
		SideStoneFeatures		,
		GramsFeatures		,
		LengthFeatures		,
		MilliMeterFeatures		,
		ModelNoFeatures			,
		MountingFeatures		,
		PearlFeatures			,
		PreOwnedFeatures		,
		SerialNoFeatures		,
		WatchMountingFeatures		,
		OtherFeatures		,
		bq_load_date
)
WITH ArticleFeatures AS (
SELECT
	SourceSystem
	,RiskPAJewelryKey
	,bq_load_date
	,MIN(CenterStoneFeatures)   AS CenterStoneFeatures
	,MIN(SideStoneFeatures)     AS SideStoneFeatures
	,MIN(GramsFeatures)         AS GramsFeatures
	,MIN(LengthFeatures)        AS LengthFeatures
	,MIN(MilliMeterFeatures)    AS MilliMeterFeatures
	,MIN(ModelNoFeatures)       AS ModelNoFeatures
	,MIN(MountingFeatures)      AS MountingFeatures
	,MIN(PearlFeatures)         AS PearlFeatures
	,MIN(PreOwnedFeatures)      AS PreOwnedFeatures
	,MIN(SerialNoFeatures)      AS SerialNoFeatures
	,MIN(WatchMountingFeatures) AS WatchMountingFeatures
	,MIN(OtherFeatures)         AS OtherFeatures
FROM
(
	SELECT
		RiskPAJewelry.SourceSystem
		,RiskPAJewelry.RiskPAJewelryKey
		,RiskPAJewelry.bq_load_date
		,RiskPAJewelryFeature.FeatureDetailType
		,CASE WHEN RiskPAJewelryFeature.FeatureDetailType = 'Center Stone' THEN
			CONCAT(CONCAT('{lbracket}"NumberOfCenterStones"',':[',STRING_AGG(TO_JSON_STRING(NumberOfCenterStones)), '],')
					,CONCAT('"CenterStoneWeight"',':[',STRING_AGG(TO_JSON_STRING(CenterStoneWeight)), '],')
					,CONCAT('"CenterStoneMilliMeter"',':[',STRING_AGG(TO_JSON_STRING(CenterStoneMilliMeter)), '],')
					,CONCAT('"CenterStoneCut"',':[', STRING_AGG(TO_JSON_STRING(CenterStoneCut)), '],' )
					,CONCAT('"CenterStoneCutOtherDesc"',':[', STRING_AGG(TO_JSON_STRING(CenterStoneCutOtherDesc)), '],' )
					,CONCAT('"CenterStoneType"',':[',STRING_AGG(TO_JSON_STRING(CenterStoneType)), '],')
					,CONCAT('"CenterStoneOtherDesc"',':[',STRING_AGG(TO_JSON_STRING(CenterStoneOtherDesc)), '],')
					,CONCAT('"CenterStonePearlType"',':[',STRING_AGG(TO_JSON_STRING(CenterStonePearlType)), '],')
					,CONCAT('"PearlTypeOtherDesc"',':[',STRING_AGG(TO_JSON_STRING(PearlTypeOtherDesc)), '],')
					,CONCAT('"ColorofCenterStone"',':[',STRING_AGG(TO_JSON_STRING(ColorofCenterStone)), '],')
					,CONCAT('"CenterStoneClarity"',':[',STRING_AGG(TO_JSON_STRING(CenterStoneClarity)), '],')
					,CONCAT('"CenterStoneClarityEnhancedType"',':[',STRING_AGG(TO_JSON_STRING(CenterStoneClarityEnhancedType)), '],')
					,CONCAT('"CenterStoneGradingReport"',':[',STRING_AGG(TO_JSON_STRING(CenterStoneGradingReport)), '],')
					,CONCAT('"GradingReportNumber"',':[',STRING_AGG(TO_JSON_STRING(GradingReportNumber)), '],')
					,CONCAT('"MaterialType"',':[',STRING_AGG(TO_JSON_STRING(MaterialType)), '],')
					,CONCAT('"FeatureNotes"',':[',STRING_AGG(TO_JSON_STRING(FeatureNotes)), ']{rbracket}')
				) END AS CenterStoneFeatures
		,CASE WHEN RiskPAJewelryFeature.FeatureDetailType = 'SideStone' THEN
			CONCAT(CONCAT('{lbracket}"NumberOfSideStones"',':[',STRING_AGG(TO_JSON_STRING(NumberOfSideStones)), '],')
					,CONCAT('"SideStoneWeight"',':[',STRING_AGG(TO_JSON_STRING(SideStoneWeight)), '],')
					,CONCAT('"SideStoneMilliMeter"',':[',STRING_AGG(TO_JSON_STRING(SideStoneMilliMeter)), '],')
					,CONCAT('"SideStoneCut"',':[', STRING_AGG(TO_JSON_STRING(SideStoneCut)), '],' )
					,CONCAT('"SideStoneCutOtherDesc"',':[', STRING_AGG(TO_JSON_STRING(SideStoneCutOtherDesc)), '],' )
					,CONCAT('"SideStoneType"',':[',STRING_AGG(TO_JSON_STRING(SideStoneType)), '],')
					,CONCAT('"SideStoneOtherDesc"',':[',STRING_AGG(TO_JSON_STRING(SideStoneOtherDesc)), '],')
					,CONCAT('"SideStonePearlType"',':[',STRING_AGG(TO_JSON_STRING(SideStonePearlType)), '],')
					,CONCAT('"PearlTypeOtherDesc"',':[',STRING_AGG(TO_JSON_STRING(PearlTypeOtherDesc)), '],')
					,CONCAT('"ColorofSideStone"',':[',STRING_AGG(TO_JSON_STRING(ColorofSideStone)), '],')
					,CONCAT('"SideStoneClarity"',':[',STRING_AGG(TO_JSON_STRING(SideStoneClarity)), '],')
					,CONCAT('"SideStoneClarityEnhancedType"',':[',STRING_AGG(TO_JSON_STRING(SideStoneClarityEnhancedType)), '],')
					,CONCAT('"SideStoneGradingReport"',':[',STRING_AGG(TO_JSON_STRING(SideStoneGradingReport)), '],')
					,CONCAT('"GradingReportNumber"',':[',STRING_AGG(TO_JSON_STRING(GradingReportNumber)), '],')
					,CONCAT('"MaterialType"',':[',STRING_AGG(TO_JSON_STRING(MaterialType)), '],')
					,CONCAT('"FeatureNotes"',':[',STRING_AGG(TO_JSON_STRING(FeatureNotes)), ']{rbracket}')
				) END AS SideStoneFeatures
		,CASE WHEN RiskPAJewelryFeature.FeatureDetailType = 'Grams' THEN
			CONCAT(CONCAT('{lbracket}"GramsOrDWT"',':[',STRING_AGG(TO_JSON_STRING(GramsOrDWT)), '],')
					,CONCAT('"FeatureNotes"',':[',STRING_AGG(TO_JSON_STRING(FeatureNotes)), ']{rbracket}')
				) END AS GramsFeatures
		,CASE WHEN RiskPAJewelryFeature.FeatureDetailType = 'Length' THEN
			CONCAT(CONCAT('{lbracket}"Length"',':[',STRING_AGG(TO_JSON_STRING(RiskPAJewelryFeature.Length)), '],')
					,CONCAT('"FeatureNotes"',':[',STRING_AGG(TO_JSON_STRING(FeatureNotes)), ']{rbracket}')
				) END AS LengthFeatures
		,CASE WHEN RiskPAJewelryFeature.FeatureDetailType = 'MilliMeter' THEN
			CONCAT(CONCAT('{lbracket}"MilliMeter"',':[',STRING_AGG(TO_JSON_STRING(MilliMeter)), '],')
					,CONCAT('"FeatureNotes"',':[',STRING_AGG(TO_JSON_STRING(FeatureNotes)), ']{rbracket}')
				) END AS MilliMeterFeatures
		,CASE WHEN RiskPAJewelryFeature.FeatureDetailType = 'Model No' THEN
			CONCAT(CONCAT('{lbracket}"ModelNumber"',':[',STRING_AGG(TO_JSON_STRING(ModelNumber)), '],')
					,CONCAT('"FeatureNotes"',':[',STRING_AGG(TO_JSON_STRING(FeatureNotes)), ']{rbracket}')
				) END AS ModelNoFeatures
		,CASE WHEN RiskPAJewelryFeature.FeatureDetailType = 'Mounting' THEN
			CONCAT(CONCAT('{lbracket}"MountingType"',':[',STRING_AGG(TO_JSON_STRING(MountingType)), '],')
					,CONCAT('"MountingOtherDesc"',':[',STRING_AGG(TO_JSON_STRING(MountingOtherDesc)), ']')
					,CONCAT('"FeatureNotes"',':[',STRING_AGG(TO_JSON_STRING(FeatureNotes)), ']{rbracket}')
				) END AS MountingFeatures
		,CASE WHEN RiskPAJewelryFeature.FeatureDetailType = 'Pearl' THEN
			CONCAT(CONCAT('{lbracket}"NumberOfPearls"',':[',STRING_AGG(TO_JSON_STRING(NumberOfPearls)), '],')
					,CONCAT('"PearlType"',':[',STRING_AGG(TO_JSON_STRING(PearlType)), '],')
					,CONCAT('"PearlTypeOtherDesc"',':[',STRING_AGG(TO_JSON_STRING(PearlTypeOtherDesc)), '],')
					,CONCAT('"FeatureNotes"',':[',STRING_AGG(TO_JSON_STRING(FeatureNotes)), ']{rbracket}')
				) END AS PearlFeatures
		,CASE WHEN RiskPAJewelryFeature.FeatureDetailType = 'PreOwned' THEN
			CONCAT(CONCAT('{lbracket}"PreOwned"',':[',STRING_AGG(TO_JSON_STRING(PreOwned)), '],')
					,CONCAT('"FeatureNotes"',':[',STRING_AGG(TO_JSON_STRING(FeatureNotes)), ']{rbracket}')
				) END AS PreOwnedFeatures
		,CASE WHEN RiskPAJewelryFeature.FeatureDetailType = 'SerialNo' THEN
			CONCAT(CONCAT('{lbracket}"SerialNumber"',':[',STRING_AGG(TO_JSON_STRING(SerialNumber)), '],')
					,CONCAT('"FeatureNotes"',':[',STRING_AGG(TO_JSON_STRING(FeatureNotes)), ']{rbracket}')
				) END AS SerialNoFeatures
		,CASE WHEN RiskPAJewelryFeature.FeatureDetailType = 'Watch Mounting' THEN
			CONCAT(CONCAT('{lbracket}"WatchMountingType"',':[',STRING_AGG(TO_JSON_STRING(WatchMountingType)), '],')
					,CONCAT('"WatchMountingOtherDesc"',':[',STRING_AGG(TO_JSON_STRING(WatchMountingOtherDesc)), '],')
					,CONCAT('"FeatureNotes"',':[',STRING_AGG(TO_JSON_STRING(FeatureNotes)), ']{rbracket}')
				) END AS WatchMountingFeatures
		,CASE WHEN RiskPAJewelryFeature.FeatureDetailType = 'Other' THEN
			CONCAT(CONCAT('{lbracket}"DescOfOther"',':[',STRING_AGG(TO_JSON_STRING(DescOfOther)), '],')
					,CONCAT('"FeatureNotes"',':[',STRING_AGG(TO_JSON_STRING(FeatureNotes)), ']{rbracket}')
				) END AS OtherFeatures

	FROM `{project}.{core_dataset}.RiskPAJewelry` RiskPAJewelry

		INNER JOIN `{project}.{core_dataset}.RiskPAJewelryFeature` RiskPAJewelryFeature
			ON RiskPAJewelryFeature.RiskPAJewelryKey = RiskPAJewelry.RiskPAJewelryKey

	WHERE 	1 = 1
			AND RiskPAJewelryFeature.FixedFeatureRank = 1
			AND RiskPAJewelryFeature.IsTransactionSliceEffective = 1
            AND RiskPAJewelryFeature.bq_load_date =  DATE({partition_date})
            AND RiskPAJewelry.bq_load_date  =  DATE({partition_date})
			# AND RiskPAJewelry.PAJewelryPublicID = 'pc:4155'
	GROUP BY
        RiskPAJewelry.SourceSystem
		,RiskPAJewelry.RiskPAJewelryKey
		,RiskPAJewelry.bq_load_date
		,RiskPAJewelryFeature.FeatureDetailType
)t
GROUP BY
	SourceSystem
	,RiskPAJewelryKey
	,bq_load_date
)
SELECT
	RiskPAJewelry.SourceSystem
    ,RiskPAJewelry.RiskPAJewelryKey
    ,RiskPAJewelry.PolicyTransactionKey
    ,RiskPAJewelry.JobNumber
    ,RiskPAJewelry.RiskLevel
    ,RiskPAJewelry.JewelryArticleNumber
    ,RiskPAJewelry.EffectiveDate
    ,RiskPAJewelry.ExpirationDate
	,RiskPAJewelry.IsTransactionSliceEffective
	,RiskPAJewelry.ArticleType
	,RiskPAJewelry.ArticleSubType
	,RiskPAJewelry.ArticleGender
	,RiskPAJewelry.IsWearableTech
	,RiskPAJewelry.ArticleBrand
	,RiskPAJewelry.ArticleStyle
	,RiskPAJewelry.InitialValue
	,RiskPAJewelry.IsFullDescOverridden
    ,RiskPAJewelry.FullDescription
	,RiskPAJewelry.IsAppraisalRequested
	,RiskPAJewelry.IsAppraisalReceived
	,RiskPAJewelry.AppraisalDate
	,RiskPAJewelry.InspectionDate
	,RiskPAJewelry.IVAPercentage
	,RiskPAJewelry.IsIVADeclined
	,RiskPAJewelry.IVADate
	,RiskPAJewelry.IsIVAApplied
	,RiskPAJewelry.ValuationType
	,RiskPAJewelry.IsDamaged
	,RiskPAJewelry.DamageType
    ,RiskPAJewelry.IsInactive AS IsArticleInactive
	,RiskPAJewelry.InactiveReason
	,RiskPAJewelry.ArticleStored
	,RiskPAJewelry.SafeDetails
    ,RiskPAJewelry.TimeOutOfVault
	,RiskPAJewelry.HasCarePlan
	,RiskPAJewelry.CarePlanID
	,RiskPAJewelry.CarePlanExpirationDate
	,RiskPAJewelry.DurationWithOtherInsurer
	,RiskPAJewelry.ArticleHowAcquired
	,RiskPAJewelry.ArticleYearAcquired
	,ArticleFeatures.CenterStoneFeatures
	,ArticleFeatures.SideStoneFeatures
	,ArticleFeatures.GramsFeatures
	,ArticleFeatures.LengthFeatures
	,ArticleFeatures.MilliMeterFeatures
	,ArticleFeatures.ModelNoFeatures
	,ArticleFeatures.MountingFeatures
	,ArticleFeatures.PearlFeatures
	,ArticleFeatures.PreOwnedFeatures
	,ArticleFeatures.SerialNoFeatures
	,ArticleFeatures.WatchMountingFeatures
	,ArticleFeatures.OtherFeatures
	,DATE('{date}') as bq_load_date

FROM `{project}.{core_dataset}.RiskPAJewelry` AS RiskPAJewelry
	LEFT JOIN (select * from ArticleFeatures Where bq_load_date =  DATE({partition_date})) ArticleFeatures
		ON ArticleFeatures.RiskPAJewelryKey = RiskPAJewelry.RiskPAJewelryKey

WHERE 	1 = 1
	AND RiskPAJewelry.FixedArticleRank = 1
	--AND RiskPAJewelry.IsTransactionSliceEffective = 1
	AND RiskPAJewelry.bq_load_date =  DATE({partition_date})
