-- tag: CLRiskInLandMarineAttributes - tag ends/
/*
	Kimberlite - Building Blocks
		CLRiskInLandMarineAttributes - CL Inland Marine Stock Attributes and Location Details
	
	---------------------------------------------------------------------------------------------------------------
	--Change Log--
	---------------------------------------------------------------------------------------------------------------
	05/10/2021	SLJ			Init
	07/01/2021	DROBAK		Added: AND IsTransactionSliceEffective = 1
	07/08/2021	DROBAK		Added: PolicyTransaction.TransactionStatus
	07/21/2021	DROBAK		Made PolicyTransaction a LEFT JOIN; Turned off IsTransactionSliceEffective where clauses
	07/27/2021	DROBAK		Make visible IsTransactionSliceEffStock, IsTransactionSliceEffLocn
	08/18/2022  DROBAK		Add RiskLocationIM.IsPrimaryLocation
	----------------------------------------------------------------------------------------------------------------
*/
/*
DELETE `{project}.{dest_dataset}.CLRiskInLandMarineAttributes` WHERE bq_load_date = DATE({partition_date});
INSERT INTO `{project}.{dest_dataset}.CLRiskInLandMarineAttributes`
	(

  SourceSystem ,
  JobNumber ,
  PolicyTransactionKey ,
  RiskLocationKey ,
  RiskStockKey ,
  TransactionStatus ,
  IsTransactionSliceEffStock ,
  IsTransactionSliceEffLocn ,
  LocationEffectiveDate ,
  LocationExpirationDate ,
  StockEffectiveDate ,
  StockExpirationDate ,
  LocationNumber ,
  IsPrimaryLocation ,
  LocationFixedID ,
  LocationAddress1 ,
  LocationAddress2 ,
  LocationCity ,
  LocationState ,
  LocationStateCode ,
  LocationCountry ,
  LocationPostalCode ,
  LocationAddressStatus ,
  LocationCounty ,
  LocationCountyFIPS ,
  LocationCountyPopulation ,
  TerritoryCode ,
  Coastal ,
  CoastalZone ,
  SegmentationCode ,
  RetailSale ,
  RepairSale ,
  AppraisalSale ,
  WholesaleSale ,
  ManufacturingSale ,
  RefiningSale ,
  GoldBuyingSale ,
  PawnSale ,
  RatedAS ,
  FullTimeEmployees ,
  PartTimeEmployees ,
  Owners ,
  PublicProtection ,
  LocationTypeCode ,
  LocationTypeName ,
  LocationTypeOtherDescription ,
  AnnualSales ,
  AreSalesPerformedViaInternet ,
  InternetSalesAmount ,
  NormalBusinessHours ,
  TotalValueShippedInLast12Months ,
  ConstructionCode ,
  ConstructionType ,
  ConstructionYearBuilt ,
  NumberOfFloors ,
  FloorNumbersOccupied ,
  TotalBuildngAreaSQFT ,
  AreaOccupiedSQFT ,
  Basement ,
  SmokeDetectors ,
  PercentSprinklered ,
  BuildingClass ,
  LocationClassInsuredBusinessCode ,
  LocationClassInsuredBusinessClassification ,
  HeatingYear ,
  PlumbingYear ,
  RoofingYear ,
  WiringYear ,
  AdjacentOccupancies ,
  SharedPremises ,
  PremisesSharedWith ,
  HasShowCaseWindows ,
  NumberOfShowWindows ,
  WindowsEquippedWithLocks ,
  WindowsKeptLocked ,
  WindowsKeptUnlockedReason ,
  WindowsMaxValue ,
  LockedDoorBuzzer ,
  BarsonWindows ,
  SteelCurtainsGates ,
  MonitoredFireAlarm ,
  DoubleCylDeadBoltLocks ,
  SafetyBollardCrashProt ,
  GlassProtection ,
  ManTrapEntry ,
  RecordingCameras ,
  DaytimeArmedUniformGuard ,
  ElecTrackingRFTags ,
  MultipleAlarmSystems ,
  DualMonitoring ,
  SecuredBuilding ,
  AddlProtNotMentioned ,
  OperatingCameraSystem ,
  CameraCovPremExclRestrooms ,
  CameraOperRecording ,
  CameraBackUpRecOffSite ,
  CameraAccessRemotely ,
  IsCamerasOnExterior ,
  PremiseBurglarAlarmDetMotion ,
  CoverAreaMerchLeftOutSafeVault ,
  CoverAlarmControlPanel ,
  CoverSafeVaultArea ,
  HoldupAlarmSystem ,
  MobileDevicesUsed ,
  BurglarAlarmSysMonitored ,
  BurglarAlarmWhenClosed ,
  BurglarAlarmWhenClosedReason ,
  OpeningClosingMonitored ,
  OpeningClosingSupervised ,
  NbrOnCallInAlarmConditions ,
  RespondToAlarmConditions ,
  RespondToAlarmConditionsNoReason ,
  OtherEmployAbleDeactivateAlarm ,
  SafeScatteredOnPremiseOrInOneArea ,
  SafeVaultStkroomUsedByOther ,
  SafeVaultStkroomUsedByOtherReason ,
  AnySafeVaultStkroomsOnExtWall ,
  IsSafeOnExterior ,
  LeastNbrEmployOnPremiseBusHrs ,
  YearsInBusiness ,
  JBTFinancial ,
  Inventory ,
  ClaimsFree ,
  PhysicalProtection ,
  ArmedUniformGuard ,
  MetalDetManTrap ,
  ElecTrackDevices ,
  HoldUpAlarm ,
  ElectronicProt ,
  PhysProtectedDoorWindow ,
  NoExtGrndFloorExposure ,
  LockedDoorBuzzSys ,
  SecuredBldg ,
  MonitoredFireAlarmSys ,
  ClosedBusStoragePractices ,
  CameraSystem ,
  DblCylDeadBoltLocks ,
  MltplAlarmsMonitoring ,
  TotalInStoragePercent ,
  BankVaultPercent ,
  OutOfStorageAmount ,
  BurglarylInclSpecProp ,
  BurglaryInclSpecPropValue ,
  ExclBurglary ,
  ExclBurglaryClosed ,
  ExclBurglaryClosedHoursOpen ,
  ExclBurglaryClosedTimeZone ,
  ExclBurglaryClosedTravelLimit ,
  ExclBurglaryJwlWtchSpecified ,
  ExclBurglaryJwlWtchSpecifiedValue ,
  ExclBurglaryJwlySpecifiedAmt ,
  ExclBurglaryJwlySpecifiedAmtValue ,
  ExclOutOfSafe ,
  ExclFireLightningSmoke ,
  HasSpecifiedBurglaryLimit ,
  SpecifiedBurglaryLimitValue ,
  AlarmSignalResponseReq ,
  BankVaultReq ,
  BankVaultReqOutOfSafeVaultPercent ,
  BankVaultReqInSafeVaultPercent ,
  BankVaultReqBankVaultPercent ,
  BurglaryDeductible ,
  BurglaryDeductibleValue ,
  EstimatedInventory ,
  IndivSafeMaxLimit ,
  IndivSafeMaxLimitInSafeVaultStkPercent ,
  IndivSafeMaxLimitMfg ,
  IndivSafeVaultMaxCap ,
  InnerSafeChest ,
  InnerSafeChestInSafePercent ,
  InSafePercent ,
  InSafePercentIndivSafeVaultMaxCapacity ,
  KeyedInvestigatorResponse ,
  KeyedInvestigatorResponseReq ,
  LockedCabinets ,
  LockedCabinetsPercentKept ,
  IsMaxDollarLimit ,
  MaxLimitOutOfSafeAmt ,
  MaxLimitBurglary ,
  MaxLimitBurglaryInSafeVaultStkPct ,
  MaxLimitBurglaryBurgLimit ,
  MaxLimitBurglaryAOPLimit ,
  MaxLimitFinishedMerch ,
  MaxLimitFinishedMerchOutOfSafeVaultAmt ,
  MaxLimitWarranty ,
  MaxLimitWarrantyOutOfSafeVaultAmt ,
  MaxStockValueOutWhenClosed ,
  MaxJwlryValueOutWhenClosed ,
  MaxNonJwlyValueOutWhenClosed ,
  MaxOutofSafeWhenClosed ,
  MaxOutWhenClosedMaxOutOfSafeVault ,
  MaxOutWhenClosedWithWarranty ,
  MaxOutWhenClosedWithWarrantyMaxOutOfSafeVault ,
  MaxOutOfLockedSafeVaultLimitSched ,
  MaxPerItemSafeVault ,
  MaxPerItemSafeVaultCostPerItem ,
  MaxPerItemSafeVaultStkroom ,
  MaxPerItemSafeVaultStkroomCostPerItem ,
  MaxValueInVault ,
  MaxValueInVaultInSafePercent ,
  MinMaxProportionInSafe ,
  MinMaxProportionInSafePercent ,
  MinNbrEmployeeCond ,
  MinNbrEmployeeCondNumber ,
  MinProportionValueSafeVault ,
  MinProportionValueSafeVaultInnerSafe ,
  MinProportionValueStkroom ,
  RobberyDeductible ,
  RobberyDeductibleValue ,
  SafeBurglaryDeductible ,
  SafeBurglaryDeductibleValue ,
  SafeMinLimit ,
  SafeVaultHurrWarningReq ,
  SaefVaultHurrWarningReqTX ,
  SafeVaultHurrWarningReqDeductible ,
  SafeVaultHurrWarningReqDeductibleValue ,
  ShowJewelryConditions ,
  SharedPremiseSecurity ,
  StkroomMaxDollarLimit ,
  StkroomMaxLimitOutOfSafeVaultAmt ,
  StkroomMaxLimit ,
  StkroomMaxLimitInSafePercent ,
  TheftProtection ,
  TheftProtectionDesc ,
  TheftProtectionGuardWarranty ,
  TheftProtectionSecurityDevice ,
  TheftProtectionSecurityDeviceDesc ,
  TotalPercentInSafe ,
  TotalPercentInSafeInSafeVaultStkrmPct ,
  TotalPercentInSafeNotToExceedAmt ,
  ShowcaseOrWindowCondition ,
  BusinessPremise ,
  InviteJewelryClients ,
  SignageAtHome ,
  AnimalsOnPremise ,
  AnimalSpecies ,
  SwimmingPool ,
  Trampoline ,
  MerchandiseAccessibleToChildren ,
  DescribeHowAccessRestricted ,
  IsCompleteInvAnnualOrMore ,
  LastInventoryTotal ,
  LastInventoryDate ,
  InventoryType ,
  HasDetailedInvRecords ,
  HasPurchaseInvoices ,
  HasAwayListings ,
  PriorInventoryTotal ,
  PriorInventoryDate ,
  MaxStockValue ,
  MaxDailyScrapValue ,
  IsCustPropertyRecorded ,
  CustomerProperyAverage ,
  ConsignmentPropertyAverage ,
  InventoryPremiumBase ,
  OutofSafeVltStkrmExposure ,
  PawnPropertyHandled ,
  PawnCoverageIncluded ,
  PawnLastInventoryTotal ,
  PawnLastInventoryDate ,
  PawnPriorInventoryTotal ,
  PawnPriorInventoryDate ,
  PawnMaxStockValue ,
  InventoryLooseDiamonds ,
  InventoryWatchesLowValue ,
  InventoryWatchesHighValue ,
  InventoryHighValue ,
  InventoryLowValue ,
  InventoryScrap ,
  InventoryNonJewelry ,
  InventoryOther ,
  HasLuxuryBrandWatches ,
  HasWatchBlancpain ,
  HasWatchBreitling ,
  HasWatchCartier ,
  HasWatchOmega ,
  HasWatchPatekphilippe ,
  HasWatchRolex ,
  HasWatchOther ,
  WatchOtherExplanation ,
  IsOfficialRolexDealer ,
  ProtectionClassCode ,
  IsExclStkForSaleFromTheft ,
  ExclStkForSaleFromTheftPremium ,
  IsExclFromTheftExclBurglary ,
  IsExcludeNonJwlryInv ,
  IsExclSpecStkForSaleInv ,
  ExclSpecStkForSaleInvPropNotIncl ,
  IsJwlryPawnPledgedVal ,
  JwlryPawnPledgedValMethod ,
  JwlryPawnPledgedValOtherDesc ,
  JwlryPawnPledgedValMethodMultiplier ,
  IsJwlryPawnUnpledgedVal ,
  JwlryPawnUnpledgedValMethod ,
  JwlryPawnUnpledgedValOtherDesc ,
  JwlryPawnUnpledgedValMethodMultiplier ,
  IsUnrepInv ,
  UnrepInvDescValue ,
  IsStockLimitClsd ,
  StockLimitClsdOpenStockLimit ,
  StockLimitClsdClsdStockLimit ,
  EQTerritory ,
  EQZone ,
  EQZoneDesc ,
  FloodZone ,
  FloodZoneDesc ,
  FirmIndicator ,
  FloodInceptionDate ,
  OtherFloodInsurance ,
  OtherFloodInsuranceCarrier ,
  OtherFloodPolicyNumber ,
  OtherFloodPrimaryNFIP ,
  OtherFloodInformation ,
  OtherFloodInsUndWaiverJMPrimary ,
  bq_load_date
)
*/
CREATE OR REPLACE table `{project}.{dest_dataset}.CLRiskInLandMarineAttributes`
AS

SELECT
		RiskLocationIM.SourceSystem
		,PolicyTransaction.JobNumber
		,RiskLocationIM.PolicyTransactionKey
		,RiskLocationIM.RiskLocationKey
		,RiskStockIM.RiskStockKey
		,PolicyTransaction.TransactionStatus
		,RiskStockIM.IsTransactionSliceEffective AS IsTransactionSliceEffStock
		,RiskLocationIM.IsTransactionSliceEffective AS IsTransactionSliceEffLocn
		,RiskLocationIM.EffectiveDate AS LocationEffectiveDate
		,RiskLocationIM.ExpirationDate AS LocationExpirationDate
		,RiskStockIM.EffectiveDate AS StockEffectiveDate
		,RiskStockIM.ExpirationDate AS StockExpirationDate
	-------------------------------------------------------------- Location Attributes
		,RiskLocationIM.LocationNumber
		,RiskLocationIM.IsPrimaryLocation
		,RiskLocationIM.LocationFixedID
		,RiskLocationIM.LocationAddress1
		,RiskLocationIM.LocationAddress2
		,RiskLocationIM.LocationCity
		,RiskLocationIM.LocationState
		,RiskLocationIM.LocationStateCode
		,RiskLocationIM.LocationCountry
		,RiskLocationIM.LocationPostalCode
		,RiskLocationIM.LocationAddressStatus
		,RiskLocationIM.LocationCounty
		,RiskLocationIM.LocationCountyFIPS
		,RiskLocationIM.LocationCountyPopulation
		,RiskLocationIM.TerritoryCode
		,RiskLocationIM.Coastal
		,RiskLocationIM.CoastalZone
		,RiskLocationIM.SegmentationCode
		,RiskLocationIM.RetailSale
		,RiskLocationIM.RepairSale
		,RiskLocationIM.AppraisalSale
		,RiskLocationIM.WholesaleSale
		,RiskLocationIM.ManufacturingSale
		,RiskLocationIM.RefiningSale
		,RiskLocationIM.GoldBuyingSale
		,RiskLocationIM.PawnSale
		,RiskLocationIM.RatedAS
		,RiskLocationIM.FullTimeEmployees
		,RiskLocationIM.PartTimeEmployees
		,RiskLocationIM.Owners
		,RiskLocationIM.PublicProtection
		,RiskLocationIM.LocationTypeCode
		,RiskLocationIM.LocationTypeName
		,RiskLocationIM.LocationTypeOtherDescription
		,RiskLocationIM.AnnualSales
		,RiskLocationIM.AreSalesPerformedViaInternet
		,RiskLocationIM.InternetSalesAmount
		,RiskLocationIM.NormalBusinessHours
		,RiskLocationIM.TotalValueShippedInLast12Months
		,RiskLocationIM.ConstructionCode
		,RiskLocationIM.ConstructionType
		,RiskLocationIM.ConstructionYearBuilt
		,RiskLocationIM.NumberOfFloors
		,RiskLocationIM.FloorNumbersOccupied
		,RiskLocationIM.TotalBuildngAreaSQFT
		,RiskLocationIM.AreaOccupiedSQFT
		,RiskLocationIM.Basement
		,RiskLocationIM.SmokeDetectors
		,RiskLocationIM.PercentSprinklered
		,RiskLocationIM.BuildingClass
		,RiskLocationIM.LocationClassInsuredBusinessCode
		,RiskLocationIM.LocationClassInsuredBusinessClassification
		,RiskLocationIM.HeatingYear
		,RiskLocationIM.PlumbingYear
		,RiskLocationIM.RoofingYear
		,RiskLocationIM.WiringYear
		,RiskLocationIM.AdjacentOccupancies
		,RiskLocationIM.SharedPremises
		,RiskLocationIM.PremisesSharedWith
		,RiskLocationIM.HasShowCaseWindows
		,RiskLocationIM.NumberOfShowWindows
		,RiskLocationIM.WindowsEquippedWithLocks
		,RiskLocationIM.WindowsKeptLocked
		,RiskLocationIM.WindowsKeptUnlockedReason
		,RiskLocationIM.WindowsMaxValue

	-- T - Security Information
		,RiskLocationIM.LockedDoorBuzzer
		,RiskLocationIM.BarsonWindows
		,RiskLocationIM.SteelCurtainsGates
		,RiskLocationIM.MonitoredFireAlarm
		,RiskLocationIM.DoubleCylDeadBoltLocks
		,RiskLocationIM.SafetyBollardCrashProt
		,RiskLocationIM.GlassProtection
		,RiskLocationIM.ManTrapEntry
		,RiskLocationIM.RecordingCameras
		,RiskLocationIM.DaytimeArmedUniformGuard
		,RiskLocationIM.ElecTrackingRFTags
		,RiskLocationIM.MultipleAlarmSystems
		,RiskLocationIM.DualMonitoring
		,RiskLocationIM.SecuredBuilding
		,RiskLocationIM.AddlProtNotMentioned
		,RiskLocationIM.OperatingCameraSystem
		,RiskLocationIM.CameraCovPremExclRestrooms
		,RiskLocationIM.CameraOperRecording
		,RiskLocationIM.CameraBackUpRecOffSite
		,RiskLocationIM.CameraAccessRemotely
		,RiskLocationIM.IsCamerasOnExterior
		,RiskLocationIM.PremiseBurglarAlarmDetMotion
		,RiskLocationIM.CoverAreaMerchLeftOutSafeVault
		,RiskLocationIM.CoverAlarmControlPanel
		,RiskLocationIM.CoverSafeVaultArea
		,RiskLocationIM.HoldupAlarmSystem
		,RiskLocationIM.MobileDevicesUsed
		,RiskLocationIM.BurglarAlarmSysMonitored
		,RiskLocationIM.BurglarAlarmWhenClosed
		,RiskLocationIM.BurglarAlarmWhenClosedReason
		,RiskLocationIM.OpeningClosingMonitored
		,RiskLocationIM.OpeningClosingSupervised
		,RiskLocationIM.NbrOnCallInAlarmConditions
		,RiskLocationIM.RespondToAlarmConditions
		,RiskLocationIM.RespondToAlarmConditionsNoReason
		,RiskLocationIM.OtherEmployAbleDeactivateAlarm
		,RiskLocationIM.SafeScatteredOnPremiseOrInOneArea
		,RiskLocationIM.SafeVaultStkroomUsedByOther
		,RiskLocationIM.SafeVaultStkroomUsedByOtherReason
		,RiskLocationIM.AnySafeVaultStkroomsOnExtWall
		,RiskLocationIM.IsSafeOnExterior
		,RiskLocationIM.LeastNbrEmployOnPremiseBusHrs

	-- T - Hazard Categories
		,RiskLocationIM.YearsInBusiness
		,RiskLocationIM.JBTFinancial
		,RiskLocationIM.Inventory
		,RiskLocationIM.ClaimsFree
		,RiskLocationIM.PhysicalProtection
		,RiskLocationIM.ArmedUniformGuard
		,RiskLocationIM.MetalDetManTrap
		,RiskLocationIM.ElecTrackDevices
		,RiskLocationIM.HoldUpAlarm
		,RiskLocationIM.ElectronicProt
		,RiskLocationIM.PhysProtectedDoorWindow
		,RiskLocationIM.NoExtGrndFloorExposure
		,RiskLocationIM.LockedDoorBuzzSys
		,RiskLocationIM.SecuredBldg
		,RiskLocationIM.MonitoredFireAlarmSys
		,RiskLocationIM.ClosedBusStoragePractices
		,RiskLocationIM.CameraSystem
		,RiskLocationIM.DblCylDeadBoltLocks
		--,RiskLocationIM.AddlProtNotMentioned
		,RiskLocationIM.MltplAlarmsMonitoring

	-- T - Safes, Vaults, and Stockrooms
		,RiskLocationIM.TotalInStoragePercent
		,RiskLocationIM.BankVaultPercent
		,RiskLocationIM.OutOfStorageAmount

	-- T - Exclusions & Conditions
		,RiskLocationIM.BurglarylInclSpecProp
		,RiskLocationIM.BurglaryInclSpecPropValue
		,RiskLocationIM.ExclBurglary
		,RiskLocationIM.ExclBurglaryClosed
		,RiskLocationIM.ExclBurglaryClosedHoursOpen
		,RiskLocationIM.ExclBurglaryClosedTimeZone
		,RiskLocationIM.ExclBurglaryClosedTravelLimit
		,RiskLocationIM.ExclBurglaryJwlWtchSpecified
		,RiskLocationIM.ExclBurglaryJwlWtchSpecifiedValue
		,RiskLocationIM.ExclBurglaryJwlySpecifiedAmt
		,RiskLocationIM.ExclBurglaryJwlySpecifiedAmtValue
		,RiskLocationIM.ExclOutOfSafe
		,RiskLocationIM.ExclFireLightningSmoke
		,RiskLocationIM.HasSpecifiedBurglaryLimit
		,RiskLocationIM.SpecifiedBurglaryLimitValue
		,RiskLocationIM.AlarmSignalResponseReq
		,RiskLocationIM.BankVaultReq
		,RiskLocationIM.BankVaultReqOutOfSafeVaultPercent
		,RiskLocationIM.BankVaultReqInSafeVaultPercent
		,RiskLocationIM.BankVaultReqBankVaultPercent
		,RiskLocationIM.BurglaryDeductible
		,RiskLocationIM.BurglaryDeductibleValue
		,RiskLocationIM.EstimatedInventory
		,RiskLocationIM.IndivSafeMaxLimit
		,RiskLocationIM.IndivSafeMaxLimitInSafeVaultStkPercent
		,RiskLocationIM.IndivSafeMaxLimitMfg
		,RiskLocationIM.IndivSafeVaultMaxCap
		,RiskLocationIM.InnerSafeChest
		,RiskLocationIM.InnerSafeChestInSafePercent
		,RiskLocationIM.InSafePercent
		,RiskLocationIM.InSafePercentIndivSafeVaultMaxCapacity
		,RiskLocationIM.KeyedInvestigatorResponse
		,RiskLocationIM.KeyedInvestigatorResponseReq
		,RiskLocationIM.LockedCabinets
		,RiskLocationIM.LockedCabinetsPercentKept
		,RiskLocationIM.IsMaxDollarLimit
		,RiskLocationIM.MaxLimitOutOfSafeAmt
		,RiskLocationIM.MaxLimitBurglary
		,RiskLocationIM.MaxLimitBurglaryInSafeVaultStkPct
		,RiskLocationIM.MaxLimitBurglaryBurgLimit
		,RiskLocationIM.MaxLimitBurglaryAOPLimit
		,RiskLocationIM.MaxLimitFinishedMerch
		,RiskLocationIM.MaxLimitFinishedMerchOutOfSafeVaultAmt
		,RiskLocationIM.MaxLimitWarranty
		,RiskLocationIM.MaxLimitWarrantyOutOfSafeVaultAmt
		,RiskLocationIM.MaxStockValueOutWhenClosed
		,RiskLocationIM.MaxJwlryValueOutWhenClosed
		,RiskLocationIM.MaxNonJwlyValueOutWhenClosed
		,RiskLocationIM.MaxOutofSafeWhenClosed
		,RiskLocationIM.MaxOutWhenClosedMaxOutOfSafeVault
		,RiskLocationIM.MaxOutWhenClosedWithWarranty
		,RiskLocationIM.MaxOutWhenClosedWithWarrantyMaxOutOfSafeVault
		,RiskLocationIM.MaxOutOfLockedSafeVaultLimitSched
		,RiskLocationIM.MaxPerItemSafeVault
		,RiskLocationIM.MaxPerItemSafeVaultCostPerItem
		,RiskLocationIM.MaxPerItemSafeVaultStkroom
		,RiskLocationIM.MaxPerItemSafeVaultStkroomCostPerItem
		,RiskLocationIM.MaxValueInVault
		,RiskLocationIM.MaxValueInVaultInSafePercent
		,RiskLocationIM.MinMaxProportionInSafe
		,RiskLocationIM.MinMaxProportionInSafePercent
		,RiskLocationIM.MinNbrEmployeeCond
		,RiskLocationIM.MinNbrEmployeeCondNumber
		,RiskLocationIM.MinProportionValueSafeVault
		,RiskLocationIM.MinProportionValueSafeVaultInnerSafe
		,RiskLocationIM.MinProportionValueStkroom
		,RiskLocationIM.RobberyDeductible
		,RiskLocationIM.RobberyDeductibleValue
		,RiskLocationIM.SafeBurglaryDeductible
		,RiskLocationIM.SafeBurglaryDeductibleValue
		,RiskLocationIM.SafeMinLimit
		,RiskLocationIM.SafeVaultHurrWarningReq
		,RiskLocationIM.SaefVaultHurrWarningReqTX
		,RiskLocationIM.SafeVaultHurrWarningReqDeductible
		,RiskLocationIM.SafeVaultHurrWarningReqDeductibleValue
		,RiskLocationIM.ShowJewelryConditions
		,RiskLocationIM.SharedPremiseSecurity
		,RiskLocationIM.StkroomMaxDollarLimit
		,RiskLocationIM.StkroomMaxLimitOutOfSafeVaultAmt
		,RiskLocationIM.StkroomMaxLimit
		,RiskLocationIM.StkroomMaxLimitInSafePercent
		,RiskLocationIM.TheftProtection
		,RiskLocationIM.TheftProtectionDesc
		,RiskLocationIM.TheftProtectionGuardWarranty
		,RiskLocationIM.TheftProtectionSecurityDevice
		,RiskLocationIM.TheftProtectionSecurityDeviceDesc
		,RiskLocationIM.TotalPercentInSafe
		,RiskLocationIM.TotalPercentInSafeInSafeVaultStkrmPct
		,RiskLocationIM.TotalPercentInSafeNotToExceedAmt
		,RiskLocationIM.ShowcaseOrWindowCondition

	-- T - Location-Coverage Questions
		-- BOP/Inland Marine Loc Home Based Business Questions
		,RiskLocationIM.BusinessPremise
		,RiskLocationIM.InviteJewelryClients
		,RiskLocationIM.SignageAtHome
		,RiskLocationIM.AnimalsOnPremise
		,RiskLocationIM.AnimalSpecies
		,RiskLocationIM.SwimmingPool
		,RiskLocationIM.Trampoline
		,RiskLocationIM.MerchandiseAccessibleToChildren
		,RiskLocationIM.DescribeHowAccessRestricted
	-------------------------------------------------------------- Stock Attributes
	--T-Details
		--Inventory
		,RiskStockIM.IsCompleteInvAnnualOrMore
		,RiskStockIM.LastInventoryTotal
		,RiskStockIM.LastInventoryDate
		,RiskStockIM.InventoryType

		--Doyoukeepthefollowingrecords?
		,RiskStockIM.HasDetailedInvRecords
		,RiskStockIM.HasPurchaseInvoices
		,RiskStockIM.HasAwayListings
		,RiskStockIM.PriorInventoryTotal
		,RiskStockIM.PriorInventoryDate
		,RiskStockIM.MaxStockValue
		,RiskStockIM.MaxDailyScrapValue
		,RiskStockIM.IsCustPropertyRecorded
		,RiskStockIM.CustomerProperyAverage
		,RiskStockIM.ConsignmentPropertyAverage
		,RiskStockIM.InventoryPremiumBase
		,RiskStockIM.OutofSafeVltStkrmExposure

		--PawnedProperty
		,RiskStockIM.PawnPropertyHandled
		,RiskStockIM.PawnCoverageIncluded
		,RiskStockIM.PawnLastInventoryTotal
		,RiskStockIM.PawnLastInventoryDate
		,RiskStockIM.PawnPriorInventoryTotal
		,RiskStockIM.PawnPriorInventoryDate
		,RiskStockIM.PawnMaxStockValue

		--InventoryBreakdown
		,RiskStockIM.InventoryLooseDiamonds
		,RiskStockIM.InventoryWatchesLowValue
		,RiskStockIM.InventoryWatchesHighValue
		,RiskStockIM.InventoryHighValue
		,RiskStockIM.InventoryLowValue
		,RiskStockIM.InventoryScrap
		,RiskStockIM.InventoryNonJewelry
		,RiskStockIM.InventoryOther

		--WatchInformation
		,RiskStockIM.HasLuxuryBrandWatches
		,RiskStockIM.HasWatchBlancpain
		,RiskStockIM.HasWatchBreitling
		,RiskStockIM.HasWatchCartier
		,RiskStockIM.HasWatchOmega
		,RiskStockIM.HasWatchPatekphilippe
		,RiskStockIM.HasWatchRolex
		,RiskStockIM.HasWatchOther
		,RiskStockIM.WatchOtherExplanation
		,RiskStockIM.IsOfficialRolexDealer

		--PublicProtection
		,RiskStockIM.ProtectionClassCode

	--T-Exclusions&Conditions
		--Exclusions
		,RiskStockIM.IsExclStkForSaleFromTheft
		,RiskStockIM.ExclStkForSaleFromTheftPremium
		,RiskStockIM.IsExclFromTheftExclBurglary
		,RiskStockIM.IsExcludeNonJwlryInv
		,RiskStockIM.IsExclSpecStkForSaleInv
		,RiskStockIM.ExclSpecStkForSaleInvPropNotIncl

		--PolicyConditions
		,RiskStockIM.IsJwlryPawnPledgedVal
		,RiskStockIM.JwlryPawnPledgedValMethod
		,RiskStockIM.JwlryPawnPledgedValOtherDesc
		,RiskStockIM.JwlryPawnPledgedValMethodMultiplier
		,RiskStockIM.IsJwlryPawnUnpledgedVal
		,RiskStockIM.JwlryPawnUnpledgedValMethod
		,RiskStockIM.JwlryPawnUnpledgedValOtherDesc
		,RiskStockIM.JwlryPawnUnpledgedValMethodMultiplier
		,RiskStockIM.IsUnrepInv
		,RiskStockIM.UnrepInvDescValue
		,RiskStockIM.IsStockLimitClsd
		,RiskStockIM.StockLimitClsdOpenStockLimit
		,RiskStockIM.StockLimitClsdClsdStockLimit

	--T-Earthquake&FloodInformation
		--EarthquakeInformation
		,RiskStockIM.EQTerritory

		--,RiskStockIM.EarthquakeZone_JMIC
		,RiskStockIM.EQZone
		,RiskStockIM.EQZoneDesc

		--FloodInformation
		,RiskStockIM.FloodZone
		,RiskStockIM.FloodZoneDesc
		,RiskStockIM.FirmIndicator
		,RiskStockIM.FloodInceptionDate
		,RiskStockIM.OtherFloodInsurance
		,RiskStockIM.OtherFloodInsuranceCarrier
		,RiskStockIM.OtherFloodPolicyNumber
		,RiskStockIM.OtherFloodPrimaryNFIP
		,RiskStockIM.OtherFloodInformation
		,RiskStockIM.OtherFloodInsUndWaiverJMPrimary
		,DATE({partition_date}) AS bq_load_Date

	FROM `{project}.{core_dataset}.RiskLocationIM` AS RiskLocationIM

		INNER JOIN `{project}.{core_dataset}.RiskStockIM` AS RiskStockIM
			ON RiskStockIM.RiskLocationKey = RiskLocationIM.RiskLocationKey
			AND RiskStockIM.PolicyTransactionKey = RiskLocationIM.PolicyTransactionKey
			
		LEFT JOIN (SELECT * FROM `{project}.{core_dataset}.PolicyTransaction` WHERE bq_load_date = DATE({partition_date})) PolicyTransaction
			ON PolicyTransaction.PolicyTransactionKey = RiskLocationIM.PolicyTransactionKey

	WHERE 	1 = 1
		AND RiskLocationIM.bq_load_date = DATE({partition_date})
		AND RiskStockIM.bq_load_date = DATE({partition_date})
		AND RiskLocationIM.FixedLocationRank = 1
		AND RiskStockIM.FixedStockRank = 1
		--AND RiskStockIM.IsTransactionSliceEffective = 1
		--AND RiskLocationIM.IsTransactionSliceEffective = 1