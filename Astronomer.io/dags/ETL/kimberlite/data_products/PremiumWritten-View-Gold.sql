-- tag: v_PremiumWritten - tag ends/
/**********************************************************************************************************************************
 ***** Kimberlite Data Product ******
		v_PremiumWritten.sql
**********************************************************************************************************************************/
/*
----------------------------------------------------------------------------------------------------------------------------------
 *****  Change History  *****

    05/31/2022  DROBAK      Initial create in gld_kimberlite dataset
	11/14/2022	DROBAK		Incorporate TVerner's changes to PremiumWritten; updated table description
	
-----------------------------------------------------------------------------------------------------------------------------------
*/	
/**********************************************************************************************************************************/

	--DROP VIEW `qa-edl.B_QA_gld_kimberlite.v_PremiumWritten`
    --CREATE OR REPLACE VIEW `qa-edl.B_QA_gld_kimberlite.v_PremiumWritten`
	CREATE OR REPLACE VIEW `{project}.{dp_dataset}.v_PremiumWritten`
    (
      SourceSystem,
      FinancialTransactionKey,
      PolicyTransactionKey,
      RiskLocationKey,
      RiskBuildingKey,
      RiskStockKey,
      RiskPAJewelryKey,
      RiskJewelryItemKey,
      CoverageKey,
      AccountingDate,
      AccountingYear,
      AccountingQuarter,
      AccountingMonth,
      BusinessUnit,
      ProductCode,
	  ProductType,
      AccountSegment,
      JobNumber,
      PolicyNumber,
      PolicyExpirationDate,
      PolicyTransEffDate,
      ItemNumber,
      TermNumber,				--new
      ModelNumber,
      CoverageCode,
      TransactionType,
	  LocationCountry,			--new
      TransactionCount,
      PolicyCoverageRank,
      WrittenPremium,
      bq_load_date
    ) 
    OPTIONS(description="selects from buildingblock table ref_kimberlite.PremiumWritten")
    AS
    SELECT
      SourceSystem,
      FinancialTransactionKey,
      PolicyTransactionKey,
      RiskLocationKey,
      RiskBuildingKey,
      RiskStockKey,
      RiskPAJewelryKey,
      RiskJewelryItemKey,
      CoverageKey,
      AccountingDate,
      AccountingYear,
      AccountingQuarter,
      AccountingMonth,
      BusinessUnit,
      ProductCode,
	  ProductType,
      AccountSegment,
      JobNumber,
      PolicyNumber,
      PolicyExpirationDate,
      PolicyTransEffDate,
      ItemNumber,
	  TermNumber,
      ModelNumber,
      CoverageCode,
      TransactionType,
	  LocationCountry,
      TransactionCount,
      PolicyCoverageRank,
      WrittenPremium,
      bq_load_date
    FROM
      --`qa-edl.B_QA_ref_kimberlite.PremiumWritten_bb` 
	  `{project}.{dest_dataset}.PremiumWritten`
    WHERE bq_load_date = 
      (
        SELECT MAX(PARSE_DATE("%Y%m%d",partition_id))
        FROM `{project}.{dest_dataset}.INFORMATION_SCHEMA.PARTITIONS`
		--FROM `qa-edl.B_QA_ref_kimberlite.INFORMATION_SCHEMA.PARTITIONS`
        WHERE table_name = 'PremiumWritten'
        AND total_rows > 0
      );