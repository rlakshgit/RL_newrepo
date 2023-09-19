/**** SourceRecordExceptions.sql ********
	Kimberlite Data Quality Checks Exceptions

 *****  Change History  *****

	02/11/2022	DROBAK		Initial List added
	04/13/2022	DROBAK		Additional rows added - CLRiskInLandMarineAttributes
	04/25/2022	DROBAK		Additional rows added - CLCoverageLevelAttributes
	05/27/2022	DROBAK		Additional rows added - RiskJewelryItemFeature
	03/01/2023	DROBAK		Additional rows added - RiskLocationIM
	03/21/2023	DROBAK		Additional rows added - ClaimFinancialTransactionLinePJDirect
	--05/09/2023	DROBAK		Converted to a Merge Script style; added ID field (Use Excel sheet to maintain)
	06/21/2023	DROBAK		Additional rows added - FinancialTransactionBOPDirect
	07/12/2023	DROBAK		Additional rows added - ClaimFinancialTransactionLinePJDirect
-------------------------------------------------------------------------------------------------------------
*/
-------------------------------------------------------------------------------------------------------------
--- Create DQ Exception Table ---
-------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `{project}.{dest_dataset}.source_record_exceptions`
--CREATE TABLE IF NOT EXISTS `qa-edl.B_QA_ref_kimberlite_DQ.source_record_exceptions`
(
	 SourceSystem	STRING
    ,TableName	    STRING
    ,DQTest         STRING
    ,KeyCol1	    STRING
    ,KeyCol1Name    STRING
    ,Reason         STRING
)
OPTIONS(
  description="table used to capture source system DQ exceptions so they do not appear as DQ Check failures in Kimberlite"
);
-------------------------------------------------------------------------------------------------------------
--- Initial List of DQ Values ---
-------------------------------------------------------------------------------------------------------------
INSERT `{project}.{dest_dataset}.source_record_exceptions`
VALUES
('GW',	'CoverageIM',	'MISSING RISKS',	'1765087',	'JobNumber', 'Missing RiskLocationKey'),
('GW',	'CoverageIM',	'MISSING RISKS',	'3243339',	'JobNumber', 'Missing RiskLocationKey'),
('GW',	'CoverageIM',	'MISSING RISKS',	'3716235',	'JobNumber', 'Missing RiskLocationKey'),
('GW',	'CoverageIM',	'MISSING RISKS',	'4362070',	'JobNumber', 'Missing RiskLocationKey'),
('GW',	'CoverageIM',	'MISSING RISKS',	'4670767',	'JobNumber', 'Missing RiskLocationKey'),
('GW',	'CoverageIM',	'MISSING RISKS',	'7543854',	'JobNumber', 'Missing RiskLocationKey'),
('GW',	'RiskStockIM',	'DUPE by Tran-Loc',	'6370612',	'JobNumber', 'LocationPublicID and LocationNumber with > 1 StockPublicID'),
('GW',	'RiskStockIM',	'DUPE by Tran-Loc',	'6669024',	'JobNumber', 'LocationPublicID and LocationNumber with > 1 StockPublicID'),
('GW',	'RiskStockIM',	'DUPE by Tran-Loc',	'4767028',	'JobNumber', 'LocationPublicID and LocationNumber with > 1 StockPublicID'),
('GW',	'RiskStockIM',	'DUPE by Tran-Loc',	'6524585',	'JobNumber', 'LocationPublicID and LocationNumber with > 1 StockPublicID'),
('GW',	'CoverageJewelryItem',	'MISSING',	'3217996',	'JobNumber', 'Single Policy (24-116031) has missing coverage info; GW issue or something else?'),
('GW',	'CoverageJewelryItem',	'MISSING',	'9636943',	'JobNumber', 'Single Policy (24-116031) has missing coverage info; GW issue or something else?'),
('GW',	'CoverageJewelryItem',	'MISSING',	'3964430',	'JobNumber', 'Single Policy (24-116031) has missing coverage info; GW issue or something else?'),
('GW',	'CoverageJewelryItem',	'MISSING',	'2395875',	'JobNumber', 'Single Policy (24-116031) has missing coverage info; GW issue or something else?'),
('GW',	'CoverageJewelryItem',	'MISSING',	'6531934',	'JobNumber', 'Single Policy (24-116031) has missing coverage info; GW issue or something else?'),
('GW',	'CoverageJewelryItem',	'MISSING',	'4226035',	'JobNumber', 'Single Policy (24-116031) has missing coverage info; GW issue or something else?'),
('GW',	'CoverageJewelryItem',	'MISSING',	'7998004',	'JobNumber', 'Single Policy (24-116031) has missing coverage info; GW issue or something else?'),
('GW',	'CoverageJewelryItem',	'MISSING',	'5212903',	'JobNumber', 'Single Policy (24-116031) has missing coverage info; GW issue or something else?'),
('GW',	'CoverageJewelryItem',	'MISSING',	'7457021',	'JobNumber', 'Single Policy (24-116031) has missing coverage info; GW issue or something else?'),
('GW',	'RiskPAJewelryFeature',	'Missing Article Key',	'8044463',	'JobNumber', NULL),
('GW',	'CoverageBOP',	'MISSING RISKS',	'6277127',	'JobNumber', 'Missing RiskLocation and RiskBuildingkey');
-------------------------------------------------------------------------------------------------------------
--- List of DQ Values From 2022-04-13 ---
-------------------------------------------------------------------------------------------------------------
INSERT `{project}.{dest_dataset}.source_record_exceptions`
VALUES
('GW',	'CLRiskInLandMarineAttributes', 'IM Location with > 1 Stock', '9059680', 'JobNumber', 'BOUND Policy has duplicate LocationNumber for same JobNumber'), 
('GW',	'CLRiskInLandMarineAttributes', 'IM Location with > 1 Stock', '6370612', 'JobNumber', 'Due to duplicate JobNumber found in RiskStockIM'), 
('GW',	'CLRiskInLandMarineAttributes', 'IM Location with > 1 Stock', '6669024', 'JobNumber', 'Due to duplicate JobNumber found in RiskStockIM'), 
('GW',	'CLRiskInLandMarineAttributes', 'IM Location with > 1 Stock', '4767028', 'JobNumber', 'Due to duplicate JobNumber found in RiskStockIM'), 
('GW',	'CLRiskInLandMarineAttributes', 'IM Location with > 1 Stock', '6524585', 'JobNumber', 'Due to duplicate JobNumber found in RiskStockIM');
-------------------------------------------------------------------------------------------------------------
--- List of DQ Values From 2022-04-25 ---
-------------------------------------------------------------------------------------------------------------
INSERT `{project}.{dest_dataset}.source_record_exceptions`
VALUES
('GW',	'CLCoverageLevelAttributes',	'MissingByX',	'1765087',	'JobNumber', 'Due to CoverageIM MISSING RISKS'),
('GW',	'CLCoverageLevelAttributes',	'MissingByX',	'3243339',	'JobNumber', 'Due to CoverageIM MISSING RISKS'),
('GW',	'CLCoverageLevelAttributes',	'MissingByX',	'3716235',	'JobNumber', 'Due to CoverageIM MISSING RISKS'),
('GW',	'CLCoverageLevelAttributes',	'MissingByX',	'4362070',	'JobNumber', 'Due to CoverageIM MISSING RISKS'),
('GW',	'CLCoverageLevelAttributes',	'MissingByX',	'4670767',	'JobNumber', 'Due to CoverageIM MISSING RISKS'),
('GW',	'CLCoverageLevelAttributes',	'MissingByX',	'7543854',	'JobNumber', 'Due to CoverageIM MISSING RISKS');
-------------------------------------------------------------------------------------------------------------
--- List of DQ Values From 2022-05-27 ---
-------------------------------------------------------------------------------------------------------------
INSERT `{project}.{dest_dataset}.source_record_exceptions`
VALUES
('GW',	'RiskJewelryItemFeature',	'DUPES',	'9728929',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'9850848',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'9916819',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'9850842',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'9185775',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'8275068',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'6842246',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'6796691',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'6796703',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'6842122',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'6842134',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'6599009',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'6619245',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'6557950',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'6442072',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'6557920',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'6557977',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'6558016',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'6596858',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'6075370',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'6075395',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'6197131',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'6211454',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'5648350',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'3778151',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'3791836',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'3333604',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'2913202',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'3036621',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'2932934',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'2935348',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'2935349',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward'),
('GW',	'RiskJewelryItemFeature',	'DUPES',	'3095868',	'JobNumber', 'Duplicate due to undetermined GW issue that is copied forward');
-------------------------------------------------------------------------------------------------------------
--- List of DQ Values From 2023-03-01 ---
-------------------------------------------------------------------------------------------------------------
INSERT `{project}.{dest_dataset}.source_record_exceptions`
VALUES
('GW',	'RiskLocationIM',	'MISSING RISKS',	'11188782',	'JobNumber', 'Missing Location fro PolicyLocation table due to mismatch on FixedID to PolicyLine');
-------------------------------------------------------------------------------------------------------------
--- List of DQ Values From 2023-03-21 ---
-------------------------------------------------------------------------------------------------------------
INSERT `{project}.{dest_dataset}.source_record_exceptions`
VALUES
('GW',	'ClaimFinancialTransactionLinePJDirect',	'MISSING RISKS',	'pc:12206481',	'CoveragePublicID', 'Missing RiskJewelryItemKey likely due to data and process issue in GW'),
('GW',	'ClaimFinancialTransactionLinePJDirect',	'MISSING RISKS',	'pc:3218833',	  'CoveragePublicID', 'Missing RiskJewelryItemKey likely due to data and process issue in GW'),
('GW',	'ClaimFinancialTransactionLinePJDirect',	'MISSING RISKS',	'pc:10555728',	'CoveragePublicID', 'Missing RiskJewelryItemKey likely due to data and process issue in GW'),
('GW',	'ClaimFinancialTransactionLinePJDirect',	'MISSING RISKS',	'pc:24293362',	'CoveragePublicID', 'Missing RiskJewelryItemKey likely due to data and process issue in GW');
-------------------------------------------------------------------------------------------------------------
--- List of DQ Values From 2023-06-21 ---
-------------------------------------------------------------------------------------------------------------
INSERT `{project}.{dest_dataset}.source_record_exceptions`
VALUES
('GW',	'FinancialTransactionBOPDirect',	'MISSING RISKS',	'pc:5399069',	'TransactionPublicID', 'Missing RiskLocationKey from pc_boplocation for NoCoverage section; may be caused by out-of-sequence transactions in GW'),
('GW',	'FinancialTransactionBOPDirect',	'MISSING RISKS',	'pc:5399070',	'TransactionPublicID', 'Missing RiskLocationKey from pc_boplocation for NoCoverage section; may be caused by out-of-sequence transactions in GW');
-------------------------------------------------------------------------------------------------------------
--- List of DQ Values From 2023-07-12 ---
-------------------------------------------------------------------------------------------------------------
INSERT `{project}.{dest_dataset}.source_record_exceptions`
VALUES
('GW',	'ClaimFinancialTransactionLinePJDirect',	'MISSING RISKS',	'pc:46335169',	'CoveragePublicID', 'Missing RiskJewelryItemKey due to Jwlry item being added after loss date; claim denied');
