/*Creating RiskJPAFeaturesConfig*/

CREATE OR REPLACE TABLE  `{project}.{dataset}.RiskJPAFeaturesConfig`
	(
		Key STRING,
		Value STRING
	);
	INSERT INTO `{project}.{dataset}.RiskJPAFeaturesConfig`
		VALUES	
			('SourceSystem','GW')
			,('HashKeySeparator','_')
			,('HashingAlgorithm','SHA2_256')
			,('PJALineCode','PersonalArtclLine_JM')
			,('PJALevelRisk','PersonalArticleJewelry')
			,('PAType', 'JewelryItem_JM');

############################################################################################

/*Creating FinBOPCededConfig*/

CREATE OR REPLACE TABLE `{project}.{dataset}.FinBOPCededConfig`
	(
		Key STRING,
		Value STRING
	);
	INSERT INTO `{project}.{dataset}.FinBOPCededConfig`
		VALUES	
			('SourceSystem','GW')
			,('HashKeySeparator','_')
			,('HashAlgorithm', 'SHA2_256')
			,('LineCode','BusinessOwnersLine')
			,('BusinessType', 'Ceded')
			/*CoverageLevel Values */
			,('LineLevelCoverage','Line')
			,('SubLineLevelCoverage','SubLine')
			,('LocationLevelCoverage','Location')
			,('SubLocLevelCoverage','SubLoc')
			,('BuildingLevelCoverage', 'Building')
			,('OneTimeCreditCustomCoverage','BOPOneTimeCredit_JMIC')
			,('AdditionalInsuredCustomCoverage','Additional_Insured_JMIC')
			,('NoCoverage','NoCoverage')
			/*Ceded Coverable Types */
			,('CededCoverable', 'DefaultCov')
			,('CededCoverableBOP', 'BOPCov')
			,('CededCoverableBOPBldg', 'BOPBuildingCov')
			/* Risk Key Values */
			,('LocationLevelRisk', 'BusinessOwnersLocation')
			,('BuildingLevelRisk', 'BusinessOwnersBuilding');

############################################################################################
/*Creating PJDirectFinancialsConfig*/
CREATE OR REPLACE TABLE  `{project}.{dataset}.PJDirectFinancialsConfig`
	(
		key STRING,
		value STRING
	);
	INSERT INTO 
			`{project}.{dataset}.PJDirectFinancialsConfig`
		VALUES	
			('SourceSystem','GW')
			,('HashKeySeparator','_')
			,('HashAlgorithm', 'SHA2_256') 
			,('LineCode','PersonalJewelryLine_JMIC_PL')
			,('BusinessType', 'Direct')
			/* CoverageLevel Values */
			,('ScheduledCoverage','ScheduledCov')
			,('UnScheduledCoverage','UnScheduledCov')
			,('NoCoverage','NoCoverage')
			/* Risk Key Values */
			,('ScheduledItemRisk', 'JewelryItem');
			
###############################################################################################

/*Creating PJCededConfig*/
CREATE OR REPLACE TABLE  `{project}.{dataset}.PJCededConfig`
	(
		Key STRING,
		Value STRING
	);
	INSERT INTO `{project}.{dataset}.PJCededConfig`
		VALUES
			('SourceSystem','GW')
			,('HashKeySeparator','_')
			,('HashAlgorithm', 'SHA2_256')
			,('LineCode','PersonalJewelryLine_JMIC_PL')
			,('BusinessType', 'Ceded')
			/* CoverageLevel Values */
			,('ScheduledCoverage','ScheduledCov')
			,('UnScheduledCoverage','UnScheduledCov')
			,('NoCoverage','NoCoverage')
			/* Risk Key Values */
			,('ScheduledItemRisk', 'JewelryItem');

###############################################################################################