#StandardSQL
SELECT
    offers.* EXCEPT (
      EXPERIENCE_TIER_FACTOR
      ,TOTAL_PREMIUM
      ,INVENTORY_PREMIUM_BASE
      ,STOCK_LIMIT
      ,STOCK_DEDUCTIBLE
      ,BPP_LIMIT
      ,BPP_DEDUCTIBLE
      ,BLDG_LIMIT
      ,BLDG_DEDUCTIBLE
      ,ROW_LIMIT
      ,POLICY_LIMIT
      ,PROPORTION
      ,Allocated_Premium
      ,TRAVEL_LIMIT
      ,BPP_PAWN_LIMIT
      ,OUT_OF_SAFE_LIMIT
      ,StockInventoryLooseDiamonds
      ,StockInventoryLowValue
      ,StockInventoryHighValue
      ,StockInventoryLowValueWatches
      ,StockInventoryHighValueWatches
      ,StockInventoryScrapPreciousMetals
      ,StockInventoryNonJewelryPreciousMetals
      ,StockInventoryOther 
      ,BOPIRPM
      ,JBIRPM
      ,JSIRPM
      ,MaxTotLocInSafeVaultStkrm
      ,JB_JS_Out_of_Safe_Limit
      ,Estimated_Location_Premium
      ,Perc_of_Stock_Limit_Out_of_Safe
      )
      ,CAST(EXPERIENCE_TIER_FACTOR AS float64) AS Experience_Tier_Factor
      ,CAST(TOTAL_PREMIUM AS float64) AS Total_Premium
      ,CAST(INVENTORY_PREMIUM_BASE AS float64) AS Inventory_Premium_Base
      ,CAST(STOCK_LIMIT AS float64) AS Stock_Limit
      ,CAST(STOCK_DEDUCTIBLE AS float64) AS Stock_Deductible
      ,CAST(BPP_LIMIT AS float64) AS BPP_Limit
      ,CAST(BPP_DEDUCTIBLE AS float64) AS BPP_Deductible
      ,CAST(BLDG_LIMIT AS float64) AS BLDG_Limit
      ,CAST(BLDG_DEDUCTIBLE AS float64) AS BLDG_Deductible
      ,CAST(ROW_LIMIT AS float64) AS Row_Limit
      ,CAST(POLICY_LIMIT AS float64) AS Policy_Limit
      ,CAST(PROPORTION AS float64) AS Proportion
      ,CAST(Allocated_Premium AS float64) AS Allocated_Premium
      ,CAST(TRAVEL_LIMIT AS float64) AS Travel_Limit
      ,CAST(BPP_PAWN_LIMIT AS float64) AS BPP_Pawn_Limit
      ,CAST(OUT_OF_SAFE_LIMIT AS float64) AS Out_Of_Safe_Limit
      ,CAST(StockInventoryLooseDiamonds AS float64) AS StockInventoryLooseDiamonds
      ,CAST(StockInventoryLowValue AS float64) AS StockInventoryLowValue
      ,CAST(StockInventoryHighValue AS float64) AS StockInventoryHighValue
      ,CAST(StockInventoryLowValueWatches AS float64) AS StockInventoryLowValueWatches
      ,CAST(StockInventoryHighValueWatches AS float64) AS StockInventoryHighValueWatches
      ,CAST(StockInventoryScrapPreciousMetals AS float64) AS StockInventoryScrapPreciousMetals
      ,CAST(StockInventoryNonJewelryPreciousMetals AS float64) AS StockInventoryNonJewelryPreciousMetals
      ,CAST(StockInventoryOther AS float64) AS StockInventoryOther
      ,CAST(BOPIRPM AS float64) AS BOPIRPM
      ,CAST(JBIRPM AS float64) AS JBIRPM
      ,CAST(JSIRPM AS float64) AS JSIRPM
      ,CAST(MaxTotLocInSafeVaultStkrm AS float64) AS MaxTotLocInSafeVaultStkrm
      ,CAST(JB_JS_Out_of_Safe_Limit AS float64) AS JB_JS_Out_of_Safe_Limit
      ,CAST(Estimated_Location_Premium AS float64) AS Estimated_Location_Premium
      ,CAST(Perc_of_Stock_Limit_Out_of_Safe AS float64) AS Perc_of_Stock_Limit_Out_of_Safe
      ,CASE
        WHEN offers.Agency_Code IN ('332', 'MJS')
        THEN agency_codes.Agency_Filter
        ELSE COALESCE(master_agency.Agency_Filter, 'Non-Top 40')
      END AS Agency_Filter
FROM `{project}.{dataset}.wip_cl_offers` AS offers
LEFT JOIN `semi-managed-reporting.data_products_t1_quotes_offers.nonpromoted_agency_filter` AS agency_codes
    ON offers.Agency_Code = agency_codes.Agency_Code
LEFT JOIN `semi-managed-reporting.data_products_t1_quotes_offers.nonpromoted_agency_filter` AS master_agency
    ON offers.Agency_Master_Code = master_agency.Agency_Code