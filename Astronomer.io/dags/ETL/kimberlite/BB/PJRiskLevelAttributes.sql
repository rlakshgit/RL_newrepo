	--MANUAL TESTING--
	/*
	DECLARE DATE({partition_date}) DATE;
	DECLARE vItemPublicID_Length STRING; DECLARE vItemPublicID_SideStone STRING; DECLARE vItemPublicID_CenterStone STRING; 
	DECLARE vItemPublicID_WatchMounting STRING; DECLARE vItemPublicID_Other STRING; DECLARE vItemPublicID_Mounting STRING; 
	DECLARE vItemPublicID_ModelNo STRING; DECLARE vItemPublicID_SerialNo STRING; DECLARE vItemPublicID_Grams STRING; 
	DECLARE vItemPublicID_PreOwned STRING; DECLARE vItemPublicID_Pearl STRING; DECLARE vItemPublicID_MilliMeter STRING; 
	SET vItemPublicID_Length = 'pc:13773641';
	SET vItemPublicID_SideStone = 'pc:1000002';
	SET vItemPublicID_CenterStone = 'pc:12569652';
	------------Pearl: pc:1139613
	------------Other: pc:869288
	------------Diamond: pc:12569652
	------------Mystery: pc:12235271
	SET vItemPublicID_WatchMounting = 'pc:26514933';
	SET vItemPublicID_Other = 'pc:10970259';
	SET vItemPublicID_Mounting = 'pc:12216750';
	SET vItemPublicID_ModelNo = 'pc:32929198';
	SET vItemPublicID_SerialNo = 'pc:32096760';
	SET vItemPublicID_Grams = 'pc:1115152';
	SET vItemPublicID_PreOwned = 'pc:2129332';
	SET vItemPublicID_Pearl = 'pc:9436701';
	SET vItemPublicID_MilliMeter = 'pc:9841302';
	SET DATE({partition_date}) = "2021-04-15";
*/
-----------------------------------------------------------------------------------------------

/*
	Kimberlite - Building Blocks
	PJRiskLevelAttributes - Personal Jewelry Item Attributes and Feature Details
	------------------------------------------------------------------------------
	--Change Log--
	------------------------------------------------------------------------------
	05/05/2021	DROBAK		Init
	07/15/2021	DROBAK		Added PolicyNumber
	08/27/2021	DROBAK		Added IsTransactionSliceEffective

	------------------------------------------------------------------------------
*/

-----------------------------------------------------------------------------------------------

/***********	Length	*******************************/

CREATE OR REPLACE TEMP TABLE RiskJewelryItemPivot
AS
SELECT  
    RiskJewelryItem.RiskJewelryItemKey
	,CONCAT( CONCAT('{lbracket}"Length"',':[',STRING_AGG(TO_JSON_STRING(RiskJewelryItemFeature.Length)), '],')
			,CONCAT('"FeatureNotes"',':[',STRING_AGG(TO_JSON_STRING(FeatureNotes)), ']{rbracket}')          
		) AS LengthFeatureNotes
    ,RiskJewelryItemFeature.FeatureDetailType

FROM `{project}.{core_dataset}.RiskJewelryItem` RiskJewelryItem
INNER JOIN `{project}.{core_dataset}.RiskJewelryItemFeature` RiskJewelryItemFeature
    ON RiskJewelryItem.RiskJewelryItemKey = RiskJewelryItemFeature.RiskJewelryItemKey
WHERE RiskJewelryItem.bq_load_date = DATE({partition_date})
    AND RiskJewelryItemFeature.bq_load_date = DATE({partition_date})
    --AND RiskJewelryItem.ItemPublicID = vItemPublicID_Length
    AND RiskJewelryItemFeature.FeatureDetailType = 'Length'
GROUP BY RiskJewelryItem.RiskJewelryItemKey
        ,RiskJewelryItemFeature.FeatureDetailType
;
CALL `{project}.custom_functions.sp_pivot`(
  'RiskJewelryItemPivot'	--# source table
  ,'{project}.{core_dataset}.t_JewelryItem_LengthPivot' --# destination table
  , ['RiskJewelryItemKey']	--# row_ids
  , 'FeatureDetailType'		--# pivot_col_name
  , 'LengthFeatureNotes'	--# pivot_col_value
  , 1						--# max_columns
  , 'MIN'					--# aggregation
  , ''						--# optional_limit
);

/***********	SideStone	*******************************/
CREATE OR REPLACE TEMP TABLE RiskJewelryItemPivot
AS
SELECT  
    RiskJewelryItem.RiskJewelryItemKey   
	,CONCAT( CONCAT('{lbracket}"NumberOfSideStones"',':[',STRING_AGG(TO_JSON_STRING(NumberOfSideStones)), '],')
			,CONCAT('"SideStoneWeight"',':[',STRING_AGG(TO_JSON_STRING(SideStoneWeight)), '],')
			,CONCAT('"SideStoneMilliMeter"',':[',STRING_AGG(TO_JSON_STRING(SideStoneMilliMeter)), '],')
			,CONCAT('"SideStoneCut"',':[', STRING_AGG(TO_JSON_STRING(SideStoneCut)), '],' )
			,CONCAT('"SideStoneCutOtherDesc"',':[', STRING_AGG(TO_JSON_STRING(SideStoneCutOtherDesc)), '],' )
            ,CONCAT('"SideStoneType"',':[',STRING_AGG(TO_JSON_STRING(SideStoneType)), '],')
            ,CONCAT('"SideStoneOtherDesc"',':[',STRING_AGG(TO_JSON_STRING(SideStoneOtherDesc)), '],')
            ,CONCAT('"SideStonePearlType"',':[',STRING_AGG(TO_JSON_STRING(SideStonePearlType)), '],')
			,CONCAT('"ColorofSideStone"',':[',STRING_AGG(TO_JSON_STRING(ColorofSideStone)), '],')
			,CONCAT('"SideStoneClarity"',':[',STRING_AGG(TO_JSON_STRING(SideStoneClarity)), '],')
            ,CONCAT('"SideStoneClarityEnhancedType"',':[',STRING_AGG(TO_JSON_STRING(SideStoneClarityEnhancedType)), '],')
            ,CONCAT('"SideGemCert"',':[',STRING_AGG(TO_JSON_STRING(SideGemCert)), '],')
            ,CONCAT('"CertNo"',':[',STRING_AGG(TO_JSON_STRING(CertNo)), '],')
            ,CONCAT('"FeatureNotes"',':[',STRING_AGG(TO_JSON_STRING(FeatureNotes)), ']{rbracket}')
            
        ) AS SideStoneFeatures
    ,RiskJewelryItemFeature.FeatureDetailType
	--Do we want to add fields like this?
	--,COUNT(NumberOfSideStones) AS NbrSideStones
				
FROM `{project}.{core_dataset}.RiskJewelryItem` RiskJewelryItem
INNER JOIN `{project}.{core_dataset}.RiskJewelryItemFeature` RiskJewelryItemFeature
    ON RiskJewelryItem.RiskJewelryItemKey = RiskJewelryItemFeature.RiskJewelryItemKey
WHERE RiskJewelryItem.bq_load_date = DATE({partition_date})
    AND RiskJewelryItemFeature.bq_load_date = DATE({partition_date})
    --AND RiskJewelryItem.ItemPublicID = vItemPublicID_SideStone
    AND RiskJewelryItemFeature.FeatureDetailType = 'SideStone'
GROUP BY RiskJewelryItem.RiskJewelryItemKey
        ,RiskJewelryItemFeature.FeatureDetailType
;
CALL `{project}.custom_functions.sp_pivot`(
  'RiskJewelryItemPivot'	--# source table
  ,'{project}.{core_dataset}.t_JewelryItem_SideStonePivot' --# destination table
  , ['RiskJewelryItemKey']	--# row_ids
  , 'FeatureDetailType'		--# pivot_col_name
  , 'SideStoneFeatures'		--# pivot_col_value
  , 1						--# max_columns
  , 'MIN'					--# aggregation
  , ''						--# optional_limit
);

/***********	Center Stone	*******************************/
CREATE OR REPLACE TEMP TABLE RiskJewelryItemPivot
AS
SELECT  		 
    RiskJewelryItem.RiskJewelryItemKey   
	,CONCAT( CONCAT('{lbracket}"NumberOfCntrStones"',':[',STRING_AGG(TO_JSON_STRING(NumberOfCntrStones)), '],')
			,CONCAT('"CntrStoneWeight"',':[',STRING_AGG(TO_JSON_STRING(CntrStoneWeight)), '],')
			,CONCAT('"CntrStoneMilliMeter"',':[',STRING_AGG(TO_JSON_STRING(CntrStoneMilliMeter)), '],')
			,CONCAT('"CntrStoneCut"',':[', STRING_AGG(TO_JSON_STRING(CntrStoneCut)), '],' )
			,CONCAT('"CntrStoneCutOtherDesc"',':[', STRING_AGG(TO_JSON_STRING(CntrStoneCutOtherDesc)), '],' )
            ,CONCAT('"CntrStoneType"',':[',STRING_AGG(TO_JSON_STRING(CntrStoneType)), '],')
            ,CONCAT('"CntrStoneOtherDesc"',':[',STRING_AGG(TO_JSON_STRING(CntrStoneOtherDesc)), '],')
            ,CONCAT('"CntrStonePearlType"',':[',STRING_AGG(TO_JSON_STRING(CntrStonePearlType)), '],')
			,CONCAT('"ColorofCntrStone"',':[',STRING_AGG(TO_JSON_STRING(ColorofCntrStone)), '],')
			,CONCAT('"CntrStoneClarity"',':[',STRING_AGG(TO_JSON_STRING(CntrStoneClarity)), '],')
            ,CONCAT('"CntrStoneClarityEnhancedType"',':[',STRING_AGG(TO_JSON_STRING(CntrStoneClarityEnhancedType)), '],')
            ,CONCAT('"CntrGemCert"',':[',STRING_AGG(TO_JSON_STRING(CntrGemCert)), '],')
            ,CONCAT('"CertNo"',':[',STRING_AGG(TO_JSON_STRING(CertNo)), '],')
            ,CONCAT('"FeatureNotes"',':[',STRING_AGG(TO_JSON_STRING(FeatureNotes)), ']{rbracket}')
            
        ) AS CenterStoneFeatures
    ,RiskJewelryItemFeature.FeatureDetailType
	--Do we want to add fields like this?
	--,COUNT(NumberOfCntrStones) AS NbrCntrStones
				
FROM `{project}.{core_dataset}.RiskJewelryItem` RiskJewelryItem
INNER JOIN `{project}.{core_dataset}.RiskJewelryItemFeature` RiskJewelryItemFeature
    ON RiskJewelryItem.RiskJewelryItemKey = RiskJewelryItemFeature.RiskJewelryItemKey
WHERE RiskJewelryItem.bq_load_date = DATE({partition_date})
    AND RiskJewelryItemFeature.bq_load_date = DATE({partition_date})
   -- AND RiskJewelryItem.ItemPublicID = vItemPublicID_CenterStone
    AND RiskJewelryItemFeature.FeatureDetailType = 'Center Stone'
GROUP BY RiskJewelryItem.RiskJewelryItemKey
        ,RiskJewelryItemFeature.FeatureDetailType
;
CALL `{project}.custom_functions.sp_pivot`(
  'RiskJewelryItemPivot'	--# source table
  ,'{project}.{core_dataset}.t_JewelryItem_CenterStonePivot' --# destination table
  , ['RiskJewelryItemKey']	--# row_ids
  , 'FeatureDetailType'		--# pivot_col_name
  , 'CenterStoneFeatures'	--# pivot_col_value
  , 1						--# max_columns
  , 'MIN'					--# aggregation
  , ''						--# optional_limit
);

/***********	Watch Mounting	*******************************/
CREATE OR REPLACE TEMP TABLE RiskJewelryItemPivot
AS
SELECT  	
    RiskJewelryItem.RiskJewelryItemKey
	,CONCAT( CONCAT('{lbracket}"WatchMountingType"',':[',STRING_AGG(TO_JSON_STRING(WatchMountingType)), '],')
			,CONCAT('"WatchMountingOtherDesc"',':[',STRING_AGG(TO_JSON_STRING(WatchMountingOtherDesc)), '],')
	        ,CONCAT('"FeatureNotes"',':[',STRING_AGG(TO_JSON_STRING(FeatureNotes)), ']{rbracket}') 
		) AS WatchMountingFeatures
    ,RiskJewelryItemFeature.FeatureDetailType

FROM `{project}.{core_dataset}.RiskJewelryItem` RiskJewelryItem
INNER JOIN `{project}.{core_dataset}.RiskJewelryItemFeature` RiskJewelryItemFeature
    ON RiskJewelryItem.RiskJewelryItemKey = RiskJewelryItemFeature.RiskJewelryItemKey
WHERE RiskJewelryItem.bq_load_date = DATE({partition_date})
    AND RiskJewelryItemFeature.bq_load_date = DATE({partition_date})
    --AND RiskJewelryItem.ItemPublicID = vItemPublicID_WatchMounting
    AND RiskJewelryItemFeature.FeatureDetailType = 'Watch Mounting'
GROUP BY RiskJewelryItem.RiskJewelryItemKey
        ,RiskJewelryItemFeature.FeatureDetailType
;
CALL `{project}.custom_functions.sp_pivot`(
  'RiskJewelryItemPivot'		--# source table
  ,'{project}.{core_dataset}.t_JewelryItem_WatchMountingPivot' --# destination table
  , ['RiskJewelryItemKey']		--# row_ids
  , 'FeatureDetailType'			--# pivot_col_name
  , 'WatchMountingFeatures'		--# pivot_col_value
  , 1							--# max_columns
  , 'MIN'						--# aggregation
  , ''							--# optional_limit
);

/***********	Other	*******************************/
CREATE OR REPLACE TEMP TABLE RiskJewelryItemPivot
AS
SELECT  	
    RiskJewelryItem.RiskJewelryItemKey
	,CONCAT( CONCAT('{lbracket}"DescOfOther"',':[',STRING_AGG(TO_JSON_STRING(DescOfOther)), '],')
	        ,CONCAT('"FeatureNotes"',':[',STRING_AGG(TO_JSON_STRING(FeatureNotes)), ']{rbracket}') 
		) AS OtherFeatures
    ,RiskJewelryItemFeature.FeatureDetailType

FROM `{project}.{core_dataset}.RiskJewelryItem` RiskJewelryItem
INNER JOIN `{project}.{core_dataset}.RiskJewelryItemFeature` RiskJewelryItemFeature
    ON RiskJewelryItem.RiskJewelryItemKey = RiskJewelryItemFeature.RiskJewelryItemKey
WHERE RiskJewelryItem.bq_load_date = DATE({partition_date})
    AND RiskJewelryItemFeature.bq_load_date = DATE({partition_date})
    --AND RiskJewelryItem.ItemPublicID = vItemPublicID_Other
    AND RiskJewelryItemFeature.FeatureDetailType = 'Other'
GROUP BY RiskJewelryItem.RiskJewelryItemKey
        ,RiskJewelryItemFeature.FeatureDetailType
;
CALL `{project}.custom_functions.sp_pivot`(
  'RiskJewelryItemPivot'		--# source table
  ,'{project}.{core_dataset}.t_JewelryItem_OtherPivot' --# destination table
  , ['RiskJewelryItemKey']		--# row_ids
  , 'FeatureDetailType'			--# pivot_col_name
  , 'OtherFeatures'				--# pivot_col_value
  , 1							--# max_columns
  , 'MIN'						--# aggregation
  , ''							--# optional_limit
);

/***********	Mounting	*******************************/
CREATE OR REPLACE TEMP TABLE RiskJewelryItemPivot
AS
SELECT  	
    RiskJewelryItem.RiskJewelryItemKey
	,CONCAT( CONCAT('{lbracket}"MountingType"',':[',STRING_AGG(TO_JSON_STRING(MountingType)), '],')
	        ,CONCAT('"FeatureNotes"',':[',STRING_AGG(TO_JSON_STRING(FeatureNotes)), ']{rbracket}') 
		) AS MountingFeatures
    ,RiskJewelryItemFeature.FeatureDetailType

FROM `{project}.{core_dataset}.RiskJewelryItem` RiskJewelryItem
INNER JOIN `{project}.{core_dataset}.RiskJewelryItemFeature` RiskJewelryItemFeature
    ON RiskJewelryItem.RiskJewelryItemKey = RiskJewelryItemFeature.RiskJewelryItemKey
WHERE RiskJewelryItem.bq_load_date = DATE({partition_date})
    AND RiskJewelryItemFeature.bq_load_date = DATE({partition_date})
    --AND RiskJewelryItem.ItemPublicID = vItemPublicID_Mounting
    AND RiskJewelryItemFeature.FeatureDetailType = 'Mounting'
GROUP BY RiskJewelryItem.RiskJewelryItemKey
        ,RiskJewelryItemFeature.FeatureDetailType
;
CALL `{project}.custom_functions.sp_pivot`(
  'RiskJewelryItemPivot'		--# source table
  ,'{project}.{core_dataset}.t_JewelryItem_MountingPivot' --# destination table
  , ['RiskJewelryItemKey']		--# row_ids
  , 'FeatureDetailType'			--# pivot_col_name
  , 'MountingFeatures'			--# pivot_col_value
  , 1							--# max_columns
  , 'MIN'						--# aggregation
  , ''							--# optional_limit
);

/***********	Model No	*******************************/
CREATE OR REPLACE TEMP TABLE RiskJewelryItemPivot
AS
SELECT  	
    RiskJewelryItem.RiskJewelryItemKey
	,CONCAT( CONCAT('{lbracket}"ModelNo"',':[',STRING_AGG(TO_JSON_STRING(ModelNo)), '],')
	        ,CONCAT('"FeatureNotes"',':[',STRING_AGG(TO_JSON_STRING(FeatureNotes)), ']{rbracket}') 
		) AS ModelNoFeatures
    ,RiskJewelryItemFeature.FeatureDetailType

FROM `{project}.{core_dataset}.RiskJewelryItem` RiskJewelryItem
INNER JOIN `{project}.{core_dataset}.RiskJewelryItemFeature` RiskJewelryItemFeature
    ON RiskJewelryItem.RiskJewelryItemKey = RiskJewelryItemFeature.RiskJewelryItemKey
WHERE RiskJewelryItem.bq_load_date = DATE({partition_date})
    AND RiskJewelryItemFeature.bq_load_date = DATE({partition_date})
    --AND RiskJewelryItem.ItemPublicID = vItemPublicID_ModelNo
    AND RiskJewelryItemFeature.FeatureDetailType = 'Model No'
GROUP BY RiskJewelryItem.RiskJewelryItemKey
        ,RiskJewelryItemFeature.FeatureDetailType
;
CALL `{project}.custom_functions.sp_pivot`(
  'RiskJewelryItemPivot'		--# source table
  ,'{project}.{core_dataset}.t_JewelryItem_ModelNoPivot' --# destination table
  , ['RiskJewelryItemKey']		--# row_ids
  , 'FeatureDetailType'			--# pivot_col_name
  , 'ModelNoFeatures'			--# pivot_col_value
  , 1							--# max_columns
  , 'MIN'						--# aggregation
  , ''							--# optional_limit
);

/***********	SerialNo	*******************************/
CREATE OR REPLACE TEMP TABLE RiskJewelryItemPivot
AS
SELECT  	
    RiskJewelryItem.RiskJewelryItemKey
	,CONCAT( CONCAT('{lbracket}"SerialNo"',':[',STRING_AGG(TO_JSON_STRING(SerialNo)), '],')
	        ,CONCAT('"FeatureNotes"',':[',STRING_AGG(TO_JSON_STRING(FeatureNotes)), ']{rbracket}') 
		) AS SerialNoFeatures
    ,RiskJewelryItemFeature.FeatureDetailType

FROM `{project}.{core_dataset}.RiskJewelryItem` RiskJewelryItem
INNER JOIN `{project}.{core_dataset}.RiskJewelryItemFeature` RiskJewelryItemFeature
    ON RiskJewelryItem.RiskJewelryItemKey = RiskJewelryItemFeature.RiskJewelryItemKey
WHERE RiskJewelryItem.bq_load_date = DATE({partition_date})
    AND RiskJewelryItemFeature.bq_load_date = DATE({partition_date})
    --AND RiskJewelryItem.ItemPublicID = vItemPublicID_SerialNo
    AND RiskJewelryItemFeature.FeatureDetailType = 'SerialNo'
GROUP BY RiskJewelryItem.RiskJewelryItemKey
        ,RiskJewelryItemFeature.FeatureDetailType
;
CALL `{project}.custom_functions.sp_pivot`(
  'RiskJewelryItemPivot'		--# source table
  ,'{project}.{core_dataset}.t_JewelryItem_SerialNoPivot' --# destination table
  , ['RiskJewelryItemKey']		--# row_ids
  , 'FeatureDetailType'			--# pivot_col_name
  , 'SerialNoFeatures'			--# pivot_col_value
  , 1							--# max_columns
  , 'MIN'						--# aggregation
  , ''							--# optional_limit
);

/***********	Grams	*******************************/
CREATE OR REPLACE TEMP TABLE RiskJewelryItemPivot
AS
SELECT  	
    RiskJewelryItem.RiskJewelryItemKey
	,CONCAT( CONCAT('{lbracket}"GramsOrDWT"',':[',STRING_AGG(TO_JSON_STRING(GramsOrDWT)), '],')
	        ,CONCAT('"FeatureNotes"',':[',STRING_AGG(TO_JSON_STRING(FeatureNotes)), ']{rbracket}') 
		) AS GramsFeatures
    ,RiskJewelryItemFeature.FeatureDetailType

FROM `{project}.{core_dataset}.RiskJewelryItem` RiskJewelryItem
INNER JOIN `{project}.{core_dataset}.RiskJewelryItemFeature` RiskJewelryItemFeature
    ON RiskJewelryItem.RiskJewelryItemKey = RiskJewelryItemFeature.RiskJewelryItemKey
WHERE RiskJewelryItem.bq_load_date = DATE({partition_date})
    AND RiskJewelryItemFeature.bq_load_date = DATE({partition_date})
   -- AND RiskJewelryItem.ItemPublicID = vItemPublicID_Grams
    AND RiskJewelryItemFeature.FeatureDetailType = 'Grams'
GROUP BY RiskJewelryItem.RiskJewelryItemKey
        ,RiskJewelryItemFeature.FeatureDetailType
;
CALL `{project}.custom_functions.sp_pivot`(
  'RiskJewelryItemPivot'		--# source table
  ,'{project}.{core_dataset}.t_JewelryItem_GramsPivot' --# destination table
  , ['RiskJewelryItemKey']		--# row_ids
  , 'FeatureDetailType'			--# pivot_col_name
  , 'GramsFeatures'			--# pivot_col_value
  , 1							--# max_columns
  , 'MIN'						--# aggregation
  , ''							--# optional_limit
);

/***********	PreOwned	*******************************/
CREATE OR REPLACE TEMP TABLE RiskJewelryItemPivot
AS
SELECT  	
    RiskJewelryItem.RiskJewelryItemKey
	,CONCAT( CONCAT('{lbracket}"PreOwned"',':[',STRING_AGG(TO_JSON_STRING(PreOwned)), '],')
	        ,CONCAT('"FeatureNotes"',':[',STRING_AGG(TO_JSON_STRING(FeatureNotes)), ']{rbracket}') 
		) AS PreOwnedFeatures
    ,RiskJewelryItemFeature.FeatureDetailType

FROM `{project}.{core_dataset}.RiskJewelryItem` RiskJewelryItem
INNER JOIN `{project}.{core_dataset}.RiskJewelryItemFeature` RiskJewelryItemFeature
    ON RiskJewelryItem.RiskJewelryItemKey = RiskJewelryItemFeature.RiskJewelryItemKey
WHERE RiskJewelryItem.bq_load_date = DATE({partition_date})
    AND RiskJewelryItemFeature.bq_load_date = DATE({partition_date})
    --AND RiskJewelryItem.ItemPublicID = vItemPublicID_PreOwned
    AND RiskJewelryItemFeature.FeatureDetailType = 'PreOwned'
GROUP BY RiskJewelryItem.RiskJewelryItemKey
        ,RiskJewelryItemFeature.FeatureDetailType
;
CALL `{project}.custom_functions.sp_pivot`(
  'RiskJewelryItemPivot'		--# source table
  ,'{project}.{core_dataset}.t_JewelryItem_PreOwnedPivot' --# destination table
  , ['RiskJewelryItemKey']		--# row_ids
  , 'FeatureDetailType'			--# pivot_col_name
  , 'PreOwnedFeatures'			--# pivot_col_value
  , 1							--# max_columns
  , 'MIN'						--# aggregation
  , ''							--# optional_limit
);

/***********	Pearl	*******************************/
CREATE OR REPLACE TEMP TABLE RiskJewelryItemPivot
AS
SELECT
    RiskJewelryItem.RiskJewelryItemKey
	,CONCAT( CONCAT('{lbracket}"NumberOfPearls"',':[',STRING_AGG(TO_JSON_STRING(NumberOfPearls)), '],')
			,CONCAT('"PearlType"',':[',STRING_AGG(TO_JSON_STRING(PearlType)), '],')
			,CONCAT('"PearlTypeOtherDesc"',':[',STRING_AGG(TO_JSON_STRING(PearlTypeOtherDesc)), '],')
	        ,CONCAT('"FeatureNotes"',':[',STRING_AGG(TO_JSON_STRING(FeatureNotes)), ']{rbracket}') 
		) AS PearlFeatures
    ,RiskJewelryItemFeature.FeatureDetailType

FROM `{project}.{core_dataset}.RiskJewelryItem` RiskJewelryItem
INNER JOIN `{project}.{core_dataset}.RiskJewelryItemFeature` RiskJewelryItemFeature
    ON RiskJewelryItem.RiskJewelryItemKey = RiskJewelryItemFeature.RiskJewelryItemKey
WHERE RiskJewelryItem.bq_load_date = DATE({partition_date})
    AND RiskJewelryItemFeature.bq_load_date = DATE({partition_date})
    --AND RiskJewelryItem.ItemPublicID = vItemPublicID_Pearl
    AND RiskJewelryItemFeature.FeatureDetailType = 'Pearl'
GROUP BY RiskJewelryItem.RiskJewelryItemKey
        ,RiskJewelryItemFeature.FeatureDetailType
;
CALL `{project}.custom_functions.sp_pivot`(
  'RiskJewelryItemPivot'		--# source table
  ,'{project}.{core_dataset}.t_JewelryItem_PearlPivot' --# destination table
  , ['RiskJewelryItemKey']		--# row_ids
  , 'FeatureDetailType'			--# pivot_col_name
  , 'PearlFeatures'				--# pivot_col_value
  , 1							--# max_columns
  , 'MIN'						--# aggregation
  , ''							--# optional_limit
);

/***********	MilliMeter	*******************************/
CREATE OR REPLACE TEMP TABLE RiskJewelryItemPivot
AS
SELECT
    RiskJewelryItem.RiskJewelryItemKey
	,CONCAT( CONCAT('{lbracket}"MilliMeter"',':[',STRING_AGG(TO_JSON_STRING(MilliMeter)), '],')
	        ,CONCAT('"FeatureNotes"',':[',STRING_AGG(TO_JSON_STRING(FeatureNotes)), ']{rbracket}') 
		) AS MilliMeterFeatures
    ,RiskJewelryItemFeature.FeatureDetailType

FROM `{project}.{core_dataset}.RiskJewelryItem` RiskJewelryItem
INNER JOIN `{project}.{core_dataset}.RiskJewelryItemFeature` RiskJewelryItemFeature
    ON RiskJewelryItem.RiskJewelryItemKey = RiskJewelryItemFeature.RiskJewelryItemKey
WHERE RiskJewelryItem.bq_load_date = DATE({partition_date})
    AND RiskJewelryItemFeature.bq_load_date = DATE({partition_date})
    --AND RiskJewelryItem.ItemPublicID = vItemPublicID_MilliMeter
    AND RiskJewelryItemFeature.FeatureDetailType = 'MilliMeter'
GROUP BY RiskJewelryItem.RiskJewelryItemKey
        ,RiskJewelryItemFeature.FeatureDetailType
;
CALL `{project}.custom_functions.sp_pivot`(
  'RiskJewelryItemPivot'		--# source table
  ,'{project}.{core_dataset}.t_JewelryItem_MilliMeterPivot' --# destination table
  , ['RiskJewelryItemKey']		--# row_ids
  , 'FeatureDetailType'			--# pivot_col_name
  , 'MilliMeterFeatures'		--# pivot_col_value
  , 1							--# max_columns
  , 'MIN'						--# aggregation
  , ''							--# optional_limit
);

DELETE `{project}.{dest_dataset}.PJRiskLevelAttributes` WHERE bq_load_date = DATE({partition_date});

INSERT INTO `{project}.{dest_dataset}.PJRiskLevelAttributes` 
	(   
	SourceSystem
	,RiskJewelryItemKey
	,PolicyTransactionKey
	,IsTransactionSliceEffective
	,PolicyNumber
	,JobNumber
	,RiskLevel
	,ItemNumber
	,ItemEffectiveDate
	,ItemExpirationDate
	,ItemDescription
	,ItemClassOtherDescText
	,ItemBrand
	,ItemStyle
	,ItemStyleOtherDescText
	,ItemInitialValue
	,ItemDescriptionDate
	,ItemAppraisalReceived
	,ItemAppraisalDocType
	,ItemAppraisalViewEntireDoc
	,ItemIVADate
	,ItemIVAPercentage
	,ItemHasIVAApplied
	,ItemUseInitialLimit
	,ItemPremiumDiffForIVA
	,ItemJewelerAppraiser
	,ItemValuationType
	,ItemBankVault
	,ItemDamage
	,ItemDamagaeDescText
	,ItemStored
	,ItemPLSafe
	,ItemSafe
	,ItemExpressDescText
	,ItemExpressDescIsAppraisal
	,IsItemInactive
	,InactiveReason
    ,Length
    ,SideStone
    ,CenterStone
    ,WatchMounting
    ,Other
    ,Mounting
    ,ModelNo
    ,SerialNo
    ,Grams
    ,PreOwned
    ,Pearl
    ,MilliMeter
	,bq_load_date

)
/***********	Final Select	*******************************/
SELECT
	RiskJewelryItem.SourceSystem
	,RiskJewelryItem.RiskJewelryItemKey
	,RiskJewelryItem.PolicyTransactionKey
	,RiskJewelryItem.IsTransactionSliceEffective
	,RiskJewelryItem.PolicyNumber
	,RiskJewelryItem.JobNumber
	,RiskJewelryItem.RiskLevel
	,RiskJewelryItem.ItemNumber
	,TIMESTAMP_TRUNC(RiskJewelryItem.ItemEffectiveDate, SECOND)
	,TIMESTAMP_TRUNC(RiskJewelryItem.ItemExpirationDate, SECOND)
	,RiskJewelryItem.ItemDescription
	,RiskJewelryItem.ItemClassOtherDescText
	,RiskJewelryItem.ItemBrand
	,RiskJewelryItem.ItemStyle
	,RiskJewelryItem.ItemStyleOtherDescText
	,RiskJewelryItem.ItemInitialValue
	,TIMESTAMP_TRUNC(RiskJewelryItem.ItemDescriptionDate, SECOND)
	,RiskJewelryItem.ItemAppraisalReceived
	,RiskJewelryItem.ItemAppraisalDocType
	,RiskJewelryItem.ItemAppraisalViewEntireDoc
	,TIMESTAMP_TRUNC(RiskJewelryItem.ItemIVADate, SECOND)
	,RiskJewelryItem.ItemIVAPercentage
	,RiskJewelryItem.ItemHasIVAApplied
	,RiskJewelryItem.ItemUseInitialLimit
	,RiskJewelryItem.ItemPremiumDiffForIVA
	,RiskJewelryItem.ItemJewelerAppraiser
	,RiskJewelryItem.ItemValuationType
	,RiskJewelryItem.ItemBankVault
	,RiskJewelryItem.ItemDamage
	,RiskJewelryItem.ItemDamagaeDescText
	,RiskJewelryItem.ItemStored
	,RiskJewelryItem.ItemPLSafe
	,RiskJewelryItem.ItemSafe
	,RiskJewelryItem.ItemExpressDescText
	,RiskJewelryItem.ItemExpressDescIsAppraisal
	,RiskJewelryItem.IsItemInactive
	,RiskJewelryItem.InactiveReason
	
	--Summary fields - just some examples for now
	--,t_JewelryItem_SideStonePivot.NbrSideStones
	--,t_JewelryItem_CenterStonePivot.NbrCntrStones

	--array fields only
    ,t_JewelryItem_LengthPivot.Length
    ,t_JewelryItem_SideStonePivot.SideStone
    ,t_JewelryItem_CenterStonePivot.CenterStone
    ,t_JewelryItem_WatchMountingPivot.WatchMounting
    ,t_JewelryItem_OtherPivot.Other
    ,t_JewelryItem_MountingPivot.Mounting
    ,t_JewelryItem_ModelNoPivot.ModelNo
    ,t_JewelryItem_SerialNoPivot.SerialNo
    ,t_JewelryItem_GramsPivot.Grams
    ,t_JewelryItem_PreOwnedPivot.PreOwned
    ,t_JewelryItem_PearlPivot.Pearl
    ,t_JewelryItem_MilliMeterPivot.MilliMeter
	,DATE({partition_date}) as bq_load_date
/*
	--Feature Detail Fields
	---Length
	,JSON_EXTRACT_ARRAY(Length, "$.Length") AS ItemLength
    ,JSON_EXTRACT_ARRAY(SideStone, "$.FeatureNotes") AS ItemFeatureNotes
	---Side Stone
    ,JSON_EXTRACT_ARRAY(SideStone, "$.NumberOfSideStones") AS NumberOfSideStones
    ,JSON_EXTRACT_ARRAY(SideStone, "$.SideStoneWeight") AS SideStoneWeight
    ,JSON_EXTRACT_ARRAY(SideStone, "$.SideStoneMilliMeter") AS SideStoneMilliMeter
    ,JSON_EXTRACT_ARRAY(SideStone, "$.SideStoneCut") AS SideStoneCut
	,JSON_EXTRACT_ARRAY(SideStone, "$.SideStoneCutOtherDesc") AS SideStoneCutOtherDesc
    ,JSON_EXTRACT_ARRAY(SideStone, "$.SideStoneType") AS SideStoneType
	,JSON_EXTRACT_ARRAY(SideStone, "$.SideStoneOtherDesc") AS SideStoneOtherDesc
	,JSON_EXTRACT_ARRAY(SideStone, "$.SideStonePearlType") AS SideStonePearlType
    ,JSON_EXTRACT_ARRAY(SideStone, "$.ColorofSideStone") AS ColorofSideStone
    ,JSON_EXTRACT_ARRAY(SideStone, "$.SideStoneClarity") AS SideStoneClarity
    ,JSON_EXTRACT_ARRAY(SideStone, "$.SideStoneClarityEnhancedType") AS SideStoneClarityEnhancedType
    ,JSON_EXTRACT_ARRAY(SideStone, "$.SideGemCert") AS SideGemCert
    ,JSON_EXTRACT_ARRAY(SideStone, "$.CertNo") AS SideStoneCertNo
    ,JSON_EXTRACT_ARRAY(SideStone, "$.FeatureNotes") AS SideStoneFeatureNotes
	---Center Stone
    ,JSON_EXTRACT_ARRAY(CenterStone, "$.NumberOfCntrStones") AS NumberOfCntrStones
    ,JSON_EXTRACT_ARRAY(CenterStone, "$.CntrStoneWeight") AS CntrStoneWeight
	,JSON_EXTRACT_ARRAY(CenterStone, "$.CntrStoneMilliMeter") AS CntrStoneMilliMeter
	,JSON_EXTRACT_ARRAY(CenterStone, "$.CntrStoneCut") AS CntrStoneCut
	,JSON_EXTRACT_ARRAY(CenterStone, "$.CntrStoneCutOtherDesc") AS CntrStoneCutOtherDesc
	,JSON_EXTRACT_ARRAY(CenterStone, "$.CntrStoneType") AS CntrStoneType
	,JSON_EXTRACT_ARRAY(CenterStone, "$.CntrStoneOtherDesc") AS CntrStoneOtherDesc
	,JSON_EXTRACT_ARRAY(CenterStone, "$.CntrStonePearlType") AS CntrStonePearlType
	,JSON_EXTRACT_ARRAY(CenterStone, "$.ColorofCntrStone") AS ColorofCntrStone
	,JSON_EXTRACT_ARRAY(CenterStone, "$.CntrStoneClarity") AS CntrStoneClarity
	,JSON_EXTRACT_ARRAY(CenterStone, "$.CntrStoneClarityEnhancedType") AS CntrStoneClarityEnhancedType
	,JSON_EXTRACT_ARRAY(CenterStone, "$.CntrGemCert") AS CntrGemCert
	,JSON_EXTRACT_ARRAY(CenterStone, "$.CertNo") AS CntrStoneCertNo
    ,JSON_EXTRACT_ARRAY(CenterStone, "$.FeatureNotes") AS CntrStoneFeatureNotes
	---Watch Mounting
	,JSON_EXTRACT_ARRAY(CenterStone, "$.WatchMountingType") AS WatchMountingType
	,JSON_EXTRACT_ARRAY(CenterStone, "$.WatchMountingOtherDesc") AS WatchMountingOtherDesc
    ,JSON_EXTRACT_ARRAY(CenterStone, "$.FeatureNotes") AS WatchMountingFeatureNotes
	---Other
	,JSON_EXTRACT_ARRAY(CenterStone, "$.DescOfOther") AS DescOfOther
    ,JSON_EXTRACT_ARRAY(CenterStone, "$.FeatureNotes") AS OtherFeatureNotes
	---Mounting
	,JSON_EXTRACT_ARRAY(CenterStone, "$.MountingType") AS MountingType
    ,JSON_EXTRACT_ARRAY(CenterStone, "$.FeatureNotes") AS MountingFeatureNotes
	---ModelNo
	,JSON_EXTRACT_ARRAY(CenterStone, "$.ModelNo") AS ModelNo
    ,JSON_EXTRACT_ARRAY(CenterStone, "$.FeatureNotes") AS ModelNoFeatureNotes
	---SerialNo
	,JSON_EXTRACT_ARRAY(CenterStone, "$.SerialNo") AS SerialNo
    ,JSON_EXTRACT_ARRAY(CenterStone, "$.FeatureNotes") AS SerialNoFeatureNotes
	---Grams
	,JSON_EXTRACT_ARRAY(CenterStone, "$.GramsOrDWT") AS GramsOrDWT
    ,JSON_EXTRACT_ARRAY(CenterStone, "$.FeatureNotes") AS GramsFeatureNotes
	---PreOwned
	,JSON_EXTRACT_ARRAY(CenterStone, "$.PreOwned") AS PreOwned
    ,JSON_EXTRACT_ARRAY(CenterStone, "$.FeatureNotes") AS PreOwnedFeatureNotes
	---Pearl
	,JSON_EXTRACT_ARRAY(CenterStone, "$.NumberOfPearls") AS NumberOfPearls
	,JSON_EXTRACT_ARRAY(CenterStone, "$.PearlType") AS PearlType
	,JSON_EXTRACT_ARRAY(CenterStone, "$.PearlTypeOtherDesc") AS PearlTypeOtherDesc
    ,JSON_EXTRACT_ARRAY(CenterStone, "$.FeatureNotes") AS PearlFeatureNotes
	---MilliMeter
	,JSON_EXTRACT_ARRAY(CenterStone, "$.MilliMeter") AS MilliMeter
	,JSON_EXTRACT_ARRAY(CenterStone, "$.FeatureNotes") AS MilliMeterFeatureNotes	
*/
FROM `{project}.{core_dataset}.RiskJewelryItem` RiskJewelryItem
LEFT JOIN `{project}.{core_dataset}.t_JewelryItem_LengthPivot` t_JewelryItem_LengthPivot
    ON RiskJewelryItem.RiskJewelryItemKey = t_JewelryItem_LengthPivot.RiskJewelryItemKey

LEFT JOIN `{project}.{core_dataset}.t_JewelryItem_SideStonePivot` t_JewelryItem_SideStonePivot
    ON RiskJewelryItem.RiskJewelryItemKey = t_JewelryItem_SideStonePivot.RiskJewelryItemKey

LEFT JOIN `{project}.{core_dataset}.t_JewelryItem_CenterStonePivot` t_JewelryItem_CenterStonePivot
    ON RiskJewelryItem.RiskJewelryItemKey = t_JewelryItem_CenterStonePivot.RiskJewelryItemKey

LEFT JOIN `{project}.{core_dataset}.t_JewelryItem_WatchMountingPivot` t_JewelryItem_WatchMountingPivot
    ON RiskJewelryItem.RiskJewelryItemKey = t_JewelryItem_WatchMountingPivot.RiskJewelryItemKey

LEFT JOIN `{project}.{core_dataset}.t_JewelryItem_OtherPivot` t_JewelryItem_OtherPivot
    ON RiskJewelryItem.RiskJewelryItemKey = t_JewelryItem_OtherPivot.RiskJewelryItemKey

LEFT JOIN `{project}.{core_dataset}.t_JewelryItem_MountingPivot` t_JewelryItem_MountingPivot
    ON RiskJewelryItem.RiskJewelryItemKey = t_JewelryItem_MountingPivot.RiskJewelryItemKey

LEFT JOIN `{project}.{core_dataset}.t_JewelryItem_ModelNoPivot` t_JewelryItem_ModelNoPivot
    ON RiskJewelryItem.RiskJewelryItemKey = t_JewelryItem_ModelNoPivot.RiskJewelryItemKey

LEFT JOIN `{project}.{core_dataset}.t_JewelryItem_SerialNoPivot` t_JewelryItem_SerialNoPivot
    ON RiskJewelryItem.RiskJewelryItemKey = t_JewelryItem_SerialNoPivot.RiskJewelryItemKey

LEFT JOIN `{project}.{core_dataset}.t_JewelryItem_GramsPivot` t_JewelryItem_GramsPivot
    ON RiskJewelryItem.RiskJewelryItemKey = t_JewelryItem_GramsPivot.RiskJewelryItemKey

LEFT JOIN `{project}.{core_dataset}.t_JewelryItem_PreOwnedPivot` t_JewelryItem_PreOwnedPivot
    ON RiskJewelryItem.RiskJewelryItemKey = t_JewelryItem_PreOwnedPivot.RiskJewelryItemKey

LEFT JOIN `{project}.{core_dataset}.t_JewelryItem_PearlPivot` t_JewelryItem_PearlPivot
    ON RiskJewelryItem.RiskJewelryItemKey = t_JewelryItem_PearlPivot.RiskJewelryItemKey

LEFT JOIN `{project}.{core_dataset}.t_JewelryItem_MilliMeterPivot` t_JewelryItem_MilliMeterPivot
    ON RiskJewelryItem.RiskJewelryItemKey = t_JewelryItem_MilliMeterPivot.RiskJewelryItemKey

WHERE RiskJewelryItem.bq_load_date = DATE({partition_date})
--AND IsTransactionSliceEffective = 1
--AND RiskJewelryItem.ItemPublicID = 'pc:13773641'
;


/******************* UNIT TESTS ****************************************************
SELECT 'MISSING KEY' AS TestCase, SourceSystem, RiskJewelryItemKey,PolicyTransactionKey
FROM `{dest_dataset}.dave_PJRiskLevelAttributes` dave_PJRiskLevelAttributes
WHERE (RiskJewelryItemKey IS NULL OR PolicyTransactionKey IS NULL)

SELECT 'Item Key DUPES' AS TestCase, SourceSystem, RiskJewelryItemKey
FROM `{dest_dataset}.dave_PJRiskLevelAttributes` dave_PJRiskLevelAttributes
GROUP BY SourceSystem, RiskJewelryItemKey
HAVING COUNT(RiskJewelryItemKey) > 1


--Not sure this is really needed since we are selecting straight from RiskJewelryItem without restriction
SELECT 'MISSING ITEMS' AS TestCase, RiskJewelryItem.SourceSystem, RiskJewelryItem.RiskJewelryItemKey, RiskJewelryItem.PolicyTransactionKey
FROM `{project}.{dest_dataset}.RiskJewelryItem` RiskJewelryItem
WHERE RiskJewelryItem.bq_load_date = "2021-04-15"
AND RiskJewelryItem.RiskJewelryItemKey NOT IN 
(
    SELECT dave_PJRiskLevelAttributes.RiskJewelryItemKey FROM `{dest_dataset}.dave_PJRiskLevelAttributes` dave_PJRiskLevelAttributes
)
*/