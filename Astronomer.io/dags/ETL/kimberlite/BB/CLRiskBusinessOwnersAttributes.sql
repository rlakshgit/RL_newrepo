-- tag: CLRiskBusinessOwnersAttributes - tag ends/
/*
	Kimberlite - Building Blocks
		CLRiskBusinessOwnersAttributes.sql
			CL Business Owners Building Attributes and Location Details

	------------------------------------------------------------------------------
	--Change Log--
	------------------------------------------------------------------------------
	05/10/2021	DROBAK		Init
	05/24/2021	DROBAK		Added 2 fields from RiskBuilding (BldgEffDate, BldgExpDate)
	07/12/2021	SLJ			Added IsTransactionSliceEffective conditions
	07/12/2021	SLJ			Added TransactionStatus field
	07/12/2021	SLJ			Removed BuildingPublicID field
	07/21/2021	DROBAK		Turned off IsTransactionSliceEffective where clauses
	07/27/2021	DROBAK		Add fields: IsTransactionSliceEffLocn & IsTransactionSliceEffBldg
	09/16/2022	DROBAK		Add fields: IsPrimaryBuildingLocation, IsPrimaryLocation

	------------------------------------------------------------------------------
*/
-----------------------------------------------------------------------------------------------

DELETE `{project}.{dest_dataset}.CLRiskBusinessOwnersAttributes` WHERE bq_load_date = DATE({partition_date});
INSERT INTO `{project}.{dest_dataset}.CLRiskBusinessOwnersAttributes` 
	( 
		SourceSystem	,
		RiskLocationKey	,
		PolicyTransactionKey	,
		RiskBuildingKey	,
		JobNumber	,
		TransactionStatus,
		IsTransactionSliceEffLocn ,
		IsTransactionSliceEffBldg ,
		RiskLevel	,
		EffectiveDate	,
		ExpirationDate	,
		IsPrimaryLocation ,
		LocationNumber	,
		LocationAddress1	,
		LocationAddress2	,
		LocationCity	,
		LocationState	,
		LocationStateCode	,
		LocationCountry	,
		LocationPostalCode	,
		LocationAddressStatus	,
		LocationCounty	,
		LocationCountyFIPS	,
		LocationCountyPopulation	,
		TerritoryCode	,
		Coastal	,
		CoastalZone	,
		SegmentationCode	,
		RetailSale	,
		RepairSale	,
		AppraisalSale	,
		WholesaleSale	,
		ManufacturingSale	,
		RefiningSale	,
		GoldBuyingSale	,
		PawnSale	,
		Casting	,
		Plating	,
		AllOtherMfg	,
		FullTimeEmployees	,
		PartTimeEmployees	,
		Owners	,
		PublicProtection	,
		LocationType	,
		LocationTypeName	,
		LocationTypeOtherDescription	,
		AnnualSales	,
		AnnualSalesAttributableToMfg	,
		WindOrHailDeductiblePercent	,
		IsBusIncomeAndExtraExpDollarLimit	,
		NuclBioChemRadExcl	,
		JwlryExclBusIncomeExtraExpTheftExcl	,
		RemoveInsToValueProvision	,
		OutdoorTheftExcl	,
		PunitiveDamagesCertTerrorismExcl	,
		AmdCancProvisionsCovChg	,
		AmdCancProvisionsCovChgDaysAdvNotice	,
		ForkliftExtCond	,
		ForkliftExtCondBlanketLimit	,
		ForkliftExtCondDeduct	,
		PattrnFrmsCond	,
		PattrnFrmsCondLimit	,
		BusinessPremise	,
		InviteJewelryClients	,
		SignageAtHome	,
		AnimalsInPremise	,
		WhatSpecies	,
		SwimmingPool	,
		Trampoline	,
		MerchandiseAccessChildren	,
		DescribeHow	,
		HasBeenInBusiness	,
		PreviousLocationAddress	,
		PastLosses	,
		ExplainLoss	,
		ExposureToFlammables	,
		ExplainExposure	,
		ContainLead	,
		ExplainContainLead	,
		--BuildingPublicID	,
		LocationLevelRisk	,
		BuildingLevelRisk	,
		BldgEffDate	,
		BldgExpDate	,
		IsPrimaryBuildingLocation ,
		BuildingNumber	,
		BuildingDescription	,
		LocationClassPredominantBldgOccupancyCode	,
		LocationClassPredominantBldgOccupancyClass	,
		LocationClassInsuredBusinessCode	,
		LocationClassInsuredBusinessClass	,
		BldgCodeEffectivenessGrade	,
		BldgLimitToValuePercent	,
		BPPLimitToValuePercent	,
		IsTheftExclusion	,
		IsBrandsAndLabels	,
		IsWindOrHail	,
		PublicUtilities	,
		BuildingClass	,
		EQTerritory	,
		PremiumBasisAmount	,
		ConstructionCode	,
		ConstructionType	,
		ConstructionYearBuilt	,
		NumberOfFloors	,
		TotalBuildngAreaSQFT	,
		AreaOccupiedSQFT	,
		Basement	,
		IsBasementFinished	,
		RoofMaterial	,
		SmokeDetectors	,
		PercentSprinklered	,
		HeatingYear	,
		PlumbingYear	,
		RoofingYear	,
		WiringYear	,
		LastBldgInspectionDate	,
		LastBldgValidationDate	,
		PercentOccupied	,
		AdjacentOccupancies	,
		SharedPremises	,
		PremisesSharedWith	,
		BldgHandlePawnPropOtherJwlry	,
		BldgNatlPawnAssociation	,
		BldgMemberOtherPawnAssociations	,
		BldgHavePawnLocalLicense	,
		BldgHavePawnStateLicense	,
		BldgHavePawnFederalLicense	,
		BldgPawnLicenseAreYouBounded	,
		BldgTotalAnnualSales	,
		BldgSalesPawnPercent	,
		BldgSalesRetailPercent	,
		BldgSalesCheckCashingPercent	,
		BldgSalesGunsAndAmmunitionPercent	,
		BldgSalesAutoPawnPercent	,
		BldgSalesTitlePawnPercent	,
		BldgSalesOtherPercent	,
		BldgSalesOtherPercentDescription	,
		BldgAvgDailyAmtOfNonJewelryPawnInventory	,
		BldgReportTransactionsRealTime	,
		BldgReportTransactionsDaily	,
		BldgReportTransactionsWeekly	,
		BldgReportTransactionsOther	,
		BldgReportTransactionsOtherDesc	,
		BldgLoanPossessOrSellFireArms	,
		BldgFedFirearmsDealerLicense	,
		BldgTypesFedFirearmsLicenses	,
		BldgFirearmsDisplayLockAndKey	,
		BldgHowSecureLongGuns	,
		BldgHowSecureOtherFirearms	,
		BldgShootingRangeOnPrem	,
		BldgHowSafeguardAmmoGunPowder	,
		BuildingAddlCommentsAboutBusiness	,
		bq_load_date
	)

--CREATE OR REPLACE table `qa-edl.B_QA_ref_kimberlite.CLRiskBusinessOwnersAttributes`
--AS

SELECT
    RiskLocationBusinessOwners.SourceSystem
    ,RiskLocationBusinessOwners.RiskLocationKey
    ,RiskLocationBusinessOwners.PolicyTransactionKey
    ,RiskBuilding.RiskBuildingKey
	,PolicyTransaction.JobNumber
    ,PolicyTransaction.TransactionStatus
	,RiskLocationBusinessOwners.IsTransactionSliceEffective AS IsTransactionSliceEffLocn
	,RiskBuilding.IsTransactionSliceEffective AS IsTransactionSliceEffBldg
	---BusinessOwners Location Attributes
    ,RiskLocationBusinessOwners.RiskLevel
    ,RiskLocationBusinessOwners.EffectiveDate
    ,RiskLocationBusinessOwners.ExpirationDate
	,RiskLocationBusinessOwners.IsPrimaryLocation
    ,RiskLocationBusinessOwners.LocationNumber
    ,RiskLocationBusinessOwners.LocationAddress1
	,RiskLocationBusinessOwners.LocationAddress2
	,RiskLocationBusinessOwners.LocationCity
	,RiskLocationBusinessOwners.LocationState
	,RiskLocationBusinessOwners.LocationStateCode
	,RiskLocationBusinessOwners.LocationCountry
	,RiskLocationBusinessOwners.LocationPostalCode
	,RiskLocationBusinessOwners.LocationAddressStatus
	,RiskLocationBusinessOwners.LocationCounty
	,RiskLocationBusinessOwners.LocationCountyFIPS
	,RiskLocationBusinessOwners.LocationCountyPopulation
	,RiskLocationBusinessOwners.TerritoryCode
	,RiskLocationBusinessOwners.Coastal
	,RiskLocationBusinessOwners.CoastalZone
	,RiskLocationBusinessOwners.SegmentationCode
	,RiskLocationBusinessOwners.RetailSale
	,RiskLocationBusinessOwners.RepairSale
	,RiskLocationBusinessOwners.AppraisalSale
	,RiskLocationBusinessOwners.WholesaleSale
	,RiskLocationBusinessOwners.ManufacturingSale
	,RiskLocationBusinessOwners.RefiningSale
	,RiskLocationBusinessOwners.GoldBuyingSale
	,RiskLocationBusinessOwners.PawnSale
	,RiskLocationBusinessOwners.Casting
	,RiskLocationBusinessOwners.Plating
	,RiskLocationBusinessOwners.AllOtherMfg
	,RiskLocationBusinessOwners.FullTimeEmployees
	,RiskLocationBusinessOwners.PartTimeEmployees
	,RiskLocationBusinessOwners.Owners
	,RiskLocationBusinessOwners.PublicProtection
	,RiskLocationBusinessOwners.LocationType
	,RiskLocationBusinessOwners.LocationTypeName
	,RiskLocationBusinessOwners.LocationTypeOtherDescription
	,RiskLocationBusinessOwners.AnnualSales
	,RiskLocationBusinessOwners.AnnualSalesAttributableToMfg
	,RiskLocationBusinessOwners.WindOrHailDeductiblePercent
	,RiskLocationBusinessOwners.IsBusIncomeAndExtraExpDollarLimit
	,RiskLocationBusinessOwners.NuclBioChemRadExcl
	,RiskLocationBusinessOwners.JwlryExclBusIncomeExtraExpTheftExcl
	,RiskLocationBusinessOwners.RemoveInsToValueProvision
	,RiskLocationBusinessOwners.OutdoorTheftExcl
	,RiskLocationBusinessOwners.PunitiveDamagesCertTerrorismExcl
	,RiskLocationBusinessOwners.AmdCancProvisionsCovChg
	,RiskLocationBusinessOwners.AmdCancProvisionsCovChgDaysAdvNotice
	,RiskLocationBusinessOwners.ForkliftExtCond
	,RiskLocationBusinessOwners.ForkliftExtCondBlanketLimit
	,RiskLocationBusinessOwners.ForkliftExtCondDeduct
	,RiskLocationBusinessOwners.PattrnFrmsCond
	,RiskLocationBusinessOwners.PattrnFrmsCondLimit
	,RiskLocationBusinessOwners.BusinessPremise
	,RiskLocationBusinessOwners.InviteJewelryClients
	,RiskLocationBusinessOwners.SignageAtHome
	,RiskLocationBusinessOwners.AnimalsInPremise
	,RiskLocationBusinessOwners.WhatSpecies
	,RiskLocationBusinessOwners.SwimmingPool
	,RiskLocationBusinessOwners.Trampoline
	,RiskLocationBusinessOwners.MerchandiseAccessChildren
	,RiskLocationBusinessOwners.DescribeHow
	,RiskLocationBusinessOwners.HasBeenInBusiness
	,RiskLocationBusinessOwners.PreviousLocationAddress
	,RiskLocationBusinessOwners.PastLosses
	,RiskLocationBusinessOwners.ExplainLoss
	,RiskLocationBusinessOwners.ExposureToFlammables
	,RiskLocationBusinessOwners.ExplainExposure
	,RiskLocationBusinessOwners.ContainLead
	,RiskLocationBusinessOwners.ExplainContainLead
	---Building Attributes
    --,RiskBuilding.BuildingPublicID
    ,RiskBuilding.LocationLevelRisk
    ,RiskBuilding.BuildingLevelRisk
	,RiskBuilding.EffectiveDate	AS BldgEffDate
	,RiskBuilding.ExpirationDate As BldgExpDate
	,RiskBuilding.IsPrimaryBuildingLocation
	,RiskBuilding.BuildingNumber
	,RiskBuilding.BuildingDescription
	,RiskBuilding.LocationClassPredominantBldgOccupancyCode
	,RiskBuilding.LocationClassPredominantBldgOccupancyClass
	,RiskBuilding.LocationClassInsuredBusinessCode
	,RiskBuilding.LocationClassInsuredBusinessClass
	,RiskBuilding.BldgCodeEffectivenessGrade
	,RiskBuilding.BldgLimitToValuePercent
	,RiskBuilding.BPPLimitToValuePercent
	,RiskBuilding.IsTheftExclusion
	,RiskBuilding.IsBrandsAndLabels
	,RiskBuilding.IsWindOrHail
	,RiskBuilding.PublicUtilities
	,RiskBuilding.BuildingClass
	,RiskBuilding.EQTerritory
	,RiskBuilding.PremiumBasisAmount
	,RiskBuilding.ConstructionCode
	,RiskBuilding.ConstructionType
	,RiskBuilding.ConstructionYearBuilt
	,RiskBuilding.NumberOfFloors
	,RiskBuilding.TotalBuildngAreaSQFT
	,RiskBuilding.AreaOccupiedSQFT
	,RiskBuilding.Basement
	,RiskBuilding.IsBasementFinished
	,RiskBuilding.RoofMaterial
	,RiskBuilding.SmokeDetectors
	,RiskBuilding.PercentSprinklered
	,RiskBuilding.HeatingYear
	,RiskBuilding.PlumbingYear
	,RiskBuilding.RoofingYear
	,RiskBuilding.WiringYear
	,RiskBuilding.LastBldgInspectionDate
	,RiskBuilding.LastBldgValidationDate
	,RiskBuilding.PercentOccupied
	,RiskBuilding.AdjacentOccupancies
	,RiskBuilding.SharedPremises
	,RiskBuilding.PremisesSharedWith
	,RiskBuilding.BldgHandlePawnPropOtherJwlry
	,RiskBuilding.BldgNatlPawnAssociation
	,RiskBuilding.BldgMemberOtherPawnAssociations
	,RiskBuilding.BldgHavePawnLocalLicense
	,RiskBuilding.BldgHavePawnStateLicense
	,RiskBuilding.BldgHavePawnFederalLicense
	,RiskBuilding.BldgPawnLicenseAreYouBounded
	,RiskBuilding.BldgTotalAnnualSales
	,RiskBuilding.BldgSalesPawnPercent
	,RiskBuilding.BldgSalesRetailPercent
	,RiskBuilding.BldgSalesCheckCashingPercent
	,RiskBuilding.BldgSalesGunsAndAmmunitionPercent
	,RiskBuilding.BldgSalesAutoPawnPercent
	,RiskBuilding.BldgSalesTitlePawnPercent
	,RiskBuilding.BldgSalesOtherPercent
	,RiskBuilding.BldgSalesOtherPercentDescription
	,RiskBuilding.BldgAvgDailyAmtOfNonJewelryPawnInventory
	,RiskBuilding.BldgReportTransactionsRealTime
	,RiskBuilding.BldgReportTransactionsDaily
	,RiskBuilding.BldgReportTransactionsWeekly
	,RiskBuilding.BldgReportTransactionsOther
	,RiskBuilding.BldgReportTransactionsOtherDesc
	,RiskBuilding.BldgLoanPossessOrSellFireArms
	,RiskBuilding.BldgFedFirearmsDealerLicense
	,RiskBuilding.BldgTypesFedFirearmsLicenses
	,RiskBuilding.BldgFirearmsDisplayLockAndKey
	,RiskBuilding.BldgHowSecureLongGuns
	,RiskBuilding.BldgHowSecureOtherFirearms
	,RiskBuilding.BldgShootingRangeOnPrem
	,RiskBuilding.BldgHowSafeguardAmmoGunPowder
	,RiskBuilding.BuildingAddlCommentsAboutBusiness
	,DATE('{date}') as bq_load_date	

FROM `{project}.{core_dataset}.RiskLocationBusinessOwners` RiskLocationBusinessOwners

	INNER JOIN `{project}.{core_dataset}.RiskBuilding` RiskBuilding
		ON RiskLocationBusinessOwners.RiskLocationKey = RiskBuilding.RiskLocationKey
		AND RiskLocationBusinessOwners.PolicyTransactionKey = RiskBuilding.PolicyTransactionKey

	LEFT JOIN (SELECT * FROM `{project}.{core_dataset}.PolicyTransaction` WHERE bq_load_date = DATE({partition_date})) PolicyTransaction
		ON PolicyTransaction.PolicyTransactionKey = RiskLocationBusinessOwners.PolicyTransactionKey

WHERE RiskLocationBusinessOwners.bq_load_date = DATE({partition_date}) 
	AND RiskBuilding.bq_load_date = DATE({partition_date})
	AND RiskBuilding.BuildingNumber IS NOT NULL
	AND RiskLocationBusinessOwners.FixedLocationRank = 1
	AND	RiskBuilding.FixedBuildingRank = 1
    --AND RiskLocationBusinessOwners.IsTransactionSliceEffective = 1
    --AND RiskBuilding.IsTransactionSliceEffective = 1



/******************* UNIT TESTS ****************************************************
---dupe example
select *
FROM `{project}.{dest_dataset}.dave_CLRiskBusinessOwnersAttributes` 
where to_base64(RiskLocationKey) IN ('dqGeChqTpLdYHG7QtaCBXVipzNx5JZxMhgOaG41fi5E=','8GPjo4m456rthZR82TxbsW7Cm3aNXjGGdaCLAtCaIXU=')

---need long-term?  Aids in finding specific dupe records from above
select * from `{project}.{dest_dataset}.RiskBuilding`
where to_base64(RiskBuildingKey) = 'O6dHLsvjdEDzKViypKtbnUpEJrdsqMmxtHpc4WIBnRo='
		select * from `{project}.{dest_dataset}.RiskLocationBusinessOwners` 
		where to_base64(RiskLocationKey) = 'Jwj9mlfAFEmowrd9B/BtqAy83zTW/qgzy3NvejsSY4E='

************************************************************************************/