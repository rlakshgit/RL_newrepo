/* Building Blocks Extract Query
    PolicyLevelAttributes
----------------------------------------------------------------------------------------------------

	-- CHANGE HISTORY --
	07/15/2021	DROBAK		Add TransEffDate; Change InsuranceProductCode to ProductType
	12/07/2021	DROBAK		Add Product and ProductType, PolicyTenure
	02/08/2022	DROBAK		Calculate PolicyTenure (move calc from Kimberlite PolicyTransaction to BB)
	05/23/2022	DROBAK		Add ProductCode to PolicyTransactionProduct field, modify PolicyTenure calc

----------------------------------------------------------------------------------------------------
*/

DELETE `{project}.{dest_dataset}.PolicyLevelAttributes` WHERE bq_load_date = DATE({partition_date});

INSERT INTO `{project}.{dest_dataset}.PolicyLevelAttributes` 
	(  
		SourceSystem
		,PolicyTransactionKey
		,PolicyTenure
		,TransEffDate
		,WrittenDate
		,SubmissionDate
		,JobCloseDate
		,DeclineDate
		,TermNumber
		,ModelNumber
		,TransactionStatus
		,TransactionCostRPT
		,TotalCostRPT
		,EstimatedPremium
		,TotalMinimumPremiumRPT
		,TotalMinimumPremiumRPTft
		,TotalPremiumRPT
		,TotalSchedPremiumRPT
		,TotalUnschedPremiumRPT
		,NotTakenReason
		,NotTakenExplanation
		,PolicyChangeReason
		,CancelSource
		,CancelType
		,CancelReason
		,CancelReasonDescription
		,CancelEffectiveDate
		,ReinstReason
		,RewriteType
		,RenewalCode
		,PreRenewalDirection
		,NonRenewReason
		,NonRenewExplanation
		,IsConditionalRenewal
		,IsStraightThrough
		,AccountingDate
		,PolicyTransactionProduct
		,bq_load_date
	)
	
SELECT
    PolicyTransaction.SourceSystem
    ,PolicyTransaction.PolicyTransactionKey
    --,NumberOfYearsInsured AS PolicyTenure
	,ROUND((TIMESTAMP_DIFF(PolicyTransaction.PeriodEffDate, PolicyTransaction.OriginalPolicyEffDate, DAY))/365)	AS PolicyTenure
	,TransEffDate
    ,WrittenDate
    ,SubmissionDate
    ,JobCloseDate
    ,DeclineDate
    ,TermNumber
    ,ModelNumber
    ,TransactionStatus
    ,TransactionCostRPT
    ,TotalCostRPT
    ,EstimatedPremium
    ,TotalMinimumPremiumRPT
    ,TotalMinimumPremiumRPTft
    ,TotalPremiumRPT
    ,TotalSchedPremiumRPT
    ,TotalUnschedPremiumRPT
    ,NotTakenReason
    ,NotTakenExplanation
    ,PolicyChangeReason
    ,CancelSource
    ,CancelType
    ,CancelReason
    ,CancelReasonDescription
    ,CancelEffectiveDate
    ,ReinstReason
    ,RewriteType
    ,RenewalCode
    ,PreRenewalDirection
    ,NonRenewReason
    ,NonRenewExplanation
    ,IsConditionalRenewal
    ,IsStraightThrough
    ,CAST(CASE WHEN IFNULL(TransEffDate,'1990-01-01') >= IFNULL(JobCloseDate, '1990-01-01') 
            THEN TransEffDate ELSE JobCloseDate END AS TIMESTAMP) AS AccountingDate
    --Future: agency, insured info
    --Future: limits, counts (item, locn)
    ,CONCAT( CONCAT('{lbracket}"PolicyTransactionProductKey"',':[', STRING_AGG(TO_JSON_STRING(PolicyTransactionProductKey)), '],' )
            ,CONCAT('"ProductCode"',':[',STRING_AGG(TO_JSON_STRING(
                CASE 
                    WHEN PolicyTransactionProduct.PolicyLineCode = 'BusinessOwnersLine' THEN 'BOP'
                    WHEN PolicyTransactionProduct.PolicyLineCode = 'UmbrellaLine_JMIC' THEN 'UMB'
                    WHEN PolicyTransactionProduct.PolicyLineCode = 'PersonalJewelryLine_JMIC_PL' THEN 'PJ'
                    WHEN PolicyTransactionProduct.PolicyLineCode = 'PersonalArtclLine_JM' THEN 'PA'
                    WHEN PolicyTransactionProduct.PolicyLineCode IN ('ILMLine_JMIC', 'InlandMarineLine') 
					THEN CASE 
							WHEN PolicyTransaction.OfferingCode IN ('JB', 'JBP') THEN 'JB'
							WHEN PolicyTransaction.OfferingCode IN ('JS', 'JSP') THEN 'JS'
						END
					ELSE PolicyTransactionProduct.PolicyLineCode
                END)), '],' ) 
            ,CONCAT('"ProductType"',':[',STRING_AGG(TO_JSON_STRING(
                CASE
                    WHEN PolicyTransactionProduct.PolicyLineCode = 'BusinessOwnersLine' THEN 'CMP Liability and Non-Liability'
                    WHEN PolicyTransactionProduct.PolicyLineCode = 'UmbrellaLine_JMIC' THEN 'Other Liability'
                    WHEN PolicyTransactionProduct.PolicyLineCode IN ('PersonalJewelryLine_JMIC_PL', 'PersonalArtclLine_JM') THEN 'Other Personal Lines Inland Marine'
                    WHEN PolicyTransactionProduct.PolicyLineCode IN ('ILMLine_JMIC', 'InlandMarineLine') THEN 'Other Commercial Inland Marine'
					ELSE PolicyTransactionProduct.PolicyLineCode
                END)), "]{rbracket}") 
    ) AS PolicyTransactionProduct
	,DATE('{date}') AS bq_load_date										
	
FROM (SELECT * FROM `{project}.{core_dataset}.PolicyTransaction` WHERE bq_load_date = DATE({partition_date})) PolicyTransaction
LEFT JOIN (SELECT * FROM `{project}.{core_dataset}.PolicyTransactionProduct` WHERE bq_load_date = DATE({partition_date}) ) PolicyTransactionProduct 
    ON PolicyTransactionProduct.PolicyTransactionKey = PolicyTransaction.PolicyTransactionKey
/* TEST */
--WHERE PolicyTransaction.PolicyNumber = '55-008520'
GROUP BY 
    PolicyTransaction.PolicyTransactionKey
    ,PolicyTransaction.SourceSystem
    ,PolicyTransaction.PeriodEffDate
	,PolicyTransaction.OriginalPolicyEffDate
    -- ,NumberOfYearsInsured
    ,WrittenDate
    ,SubmissionDate
    ,JobCloseDate
    ,DeclineDate
    ,TermNumber
    ,ModelNumber
    ,TransactionStatus
    ,TransactionCostRPT
    ,TotalCostRPT
    ,EstimatedPremium
    ,TotalMinimumPremiumRPT
    ,TotalMinimumPremiumRPTft
    ,TotalPremiumRPT
    ,TotalSchedPremiumRPT
    ,TotalUnschedPremiumRPT
    ,NotTakenReason
    ,NotTakenExplanation
    ,PolicyChangeReason
    ,CancelSource
    ,CancelType
    ,CancelReason
    ,CancelReasonDescription
    ,CancelEffectiveDate
    ,ReinstReason
    ,RewriteType
    ,RenewalCode
    ,PreRenewalDirection
    ,NonRenewReason
    ,NonRenewExplanation
    ,IsConditionalRenewal
    ,IsStraightThrough
    ,TransEffDate
    ,JobCloseDate

/*
SELECT JSON_EXTRACT(PolTransProduct, "$.PolicyTransactionProductKey[0]"), JSON_EXTRACT(PolTransProduct, "$.InsuranceProductCode[0]") AS parsed_type from `{project}.bl_kimberlite.Dave2` t
SELECT JSON_EXTRACT_ARRAY(PolTransProduct, "$.PolicyTransactionProductKey"), JSON_EXTRACT_ARRAY(PolTransProduct, "$.InsuranceProductCode") AS parsed_type from `{project}.bl_kimberlite.Dave2` t

-- Test #1 for Missing Transactions 
SELECT PolicyTransaction.PolicyTransactionKey, PolicyNumber, JobNumber, PolicyPeriodPublicID
FROM `{project}.{dest_dataset}.PolicyTransaction` PolicyTransaction
WHERE PolicyTransaction.bq_load_date = {partition_date}" 
AND PolicyTransactionKey NOT IN (SELECT t.PolicyTransactionKey FROM `{project}.{dest_dataset}.PolicyTransactionProduct` t WHERE DATE(t.bq_load_date) = {partition_date}" )

-- Test #2 for Missing Transactions 
SELECT PolicyTransactionProduct.PolicyTransactionKey, PolicyPeriodPublicID, InsuranceProductCode
FROM `{project}.{dest_dataset}.PolicyTransactionProduct` PolicyTransactionProduct
WHERE PolicyTransactionProduct.bq_load_date = {partition_date}" 
AND PolicyTransactionKey NOT IN (SELECT t.PolicyTransactionKey FROM `{project}.{dest_dataset}.PolicyTransaction` t WHERE t.bq_load_date = {partition_date}" )
*/
