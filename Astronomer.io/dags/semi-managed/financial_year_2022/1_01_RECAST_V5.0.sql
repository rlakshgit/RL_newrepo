SET NOCOUNT ON;

DECLARE @BEGDATE	AS DATE = '1/1/2014'
DECLARE @ENDDATE	AS DATE = DATEADD(DAY, -1, GETDATE())
DECLARE @BEGDATEKEY AS INT = DW_DDS_CURRENT.bief_dds.fn_GetDateKeyFromDate(@BEGDATE)
DECLARE @ENDDATEKEY AS INT = DW_DDS_CURRENT.bief_dds.fn_GetDateKeyFromDate(@ENDDATE)

/******************************************************************************************************************************************************
NBMasterList temp table: Submission policies using FactPremiumWritten
******************************************************************************************************************************************************/

SELECT
		fWP.AccountingDateKey
		,dAccount.AccountNumber
		,dPol.ConformedPolicyNumber
		,dPol.JewelersCutSubmissionID												AS SubmissionID
		,CAST(CAST(dPol.PolicyEffectiveDate AS varchar(10)) AS date)				AS PolicyEffectiveDate
		,CAST(CAST(dPol.JobCloseDate AS varchar(10)) AS date)						AS JobCloseDate
		,CASE	WHEN dPol.PolicyBillingMethod = 'List Bill' THEN 'Cap'
				WHEN dPol.SourceOfBusiness = '?' THEN 'Other'
				ELSE dPol.SourceOfBusiness
		 END																		AS RecastSourceOfBusiness
		,dPol.ApplicationTakenBy													AS ApplicationTakenBy
		,COALESCE(dPIContact.PrimaryEmailAddress,'')								AS InsuredEmail
		,COALESCE(REPLACE(REPLACE(REPLACE(dAJContact.FullName
											,',','')
									,'''','')
							,CHAR(13) + CHAR(10), '')
					,'')															AS JewelerName
		,dAgency.AgencyMasterCode													AS AgencyMasterCode
		,dAgency.AgencySubCode														AS AgencySubCode
		,dAValue.Value5																AS GeicoSubCodeDesc
		,REPLACE(REPLACE(dSPValue.Value1,',',''),'.','')							AS SP_Source
		,SUM(fWP.PremiumWritten)													AS WP

	INTO #NBMasterList
	FROM DW_DDS_CURRENT.bi_dds.FactPremiumWritten AS fWP

		INNER JOIN DW_DDS_CURRENT.bi_dds.DimBusinessType AS dBType
		ON dBType.BusinessTypeKey = fWP.BusinessTypeKey

		INNER JOIN DW_DDS_CURRENT.bi_dds.DimLineOfBusiness AS dLOB
		ON dLOB.LineOfBusinessKey = fWP.LineOfBusinessKey

		INNER JOIN DW_DDS_CURRENT.bi_dds.DimPolicyTransactionType AS dTType
		ON dTType.PolicyTransactionTypeKey = fWP.PolicyTransactionTypeKey

		INNER JOIN DW_DDS_CURRENT.bi_dds.DimPolicy AS dPol
		ON dPol.PolicyKey = fWP.PolicyKey

		INNER JOIN DW_DDS_CURRENT.bi_dds.DimContact AS dPIContact
		ON dPIContact.ContactKey = dPol.PolicyInsuredContactKey

		INNER JOIN DW_DDS_CURRENT.bi_dds.DimContact AS dAJContact
		ON dAJContact.ContactKey = fWP.AccountJewelerContactKey

		INNER JOIN DW_DDS_CURRENT.bi_dds.DimAccount AS dAccount
		ON dAccount.AccountKey = dPol.AccountKey

		INNER JOIN DW_DDS_CURRENT.bi_dds.DimAgency AS dAgency
		ON dAgency.AgencyKey = fWP.AgencyKey

		LEFT JOIN DW_SOURCE.bief_src.NameValue AS dAValue
		ON dAValue.Name = dAgency.AgencySubCode
		AND dAValue.Category = 'ProducerCode'

		LEFT JOIN DW_SOURCE.bief_src.NameValue AS dSPValue
		ON dSPValue.Name = dAgency.AgencyMasterCode
		AND dSPValue.Category = 'StrategicPartner'

	WHERE
			    fWP.AccountingDateKey >= @BEGDATEKEY
            AND fWP.AccountingDateKey <= @ENDDATEKEY
			AND dBType.BusinessTypeDesc = 'Direct'
			AND dLOB.LOBProductLineCode = 'PL'
			AND dTType.ConformedPolicyTransactionDesc IN ('Submission')
			--AND dPol.ConformedPolicyNumber = '24-444981'

	GROUP BY
		fWP.AccountingDateKey
		,dAccount.AccountNumber
		,dPol.ConformedPolicyNumber
		,dPol.JewelersCutSubmissionID
		,CAST(CAST(dPol.PolicyEffectiveDate AS varchar(10)) AS date)
		,CAST(CAST(dPol.JobCloseDate AS varchar(10)) AS date)
		,CASE	WHEN dPol.PolicyBillingMethod = 'List Bill' THEN 'Cap'
				WHEN dPol.SourceOfBusiness = '?' THEN 'Other'
				ELSE dPol.SourceOfBusiness
		 END
		,dPol.ApplicationTakenBy
		,COALESCE(dPIContact.PrimaryEmailAddress,'')
		,COALESCE(REPLACE(REPLACE(REPLACE(dAJContact.FullName
											,',','')
									,'''','')
							,CHAR(13) + CHAR(10), '')
					,'')
		,dAgency.AgencyMasterCode
		,dAgency.AgencySubCode
		,dAValue.Value5
		,REPLACE(REPLACE(dSPValue.Value1,',',''),'.','')

/******************************************************************************************************************************************************
Jewelers Cut
******************************************************************************************************************************************************/
;WITH JCData AS
(
    SELECT *
    FROM (
    	SELECT
    		cNB.ConformedPolicyNumber
    		,cNB.PolicyEffectiveDate
    		,pPDesc.ProgramDescription													AS JC_ProgramDesc
    		,pSub.DateReceived
    		,pSub.QuoteDate
    		,CASE	WHEN pSub.SubmissionId = cNB.SubmissionID THEN 1
    				WHEN (pSub.DateReceived <= cNB.JobCloseDate
    						OR pSub.QuoteDate <= cNB.JobCloseDate)
    						AND pSub.QuoteResultId = 3 THEN 2
    				WHEN (pSub.DateReceived <= cNB.JobCloseDate
    						OR pSub.QuoteDate <= cNB.JobCloseDate)
    						AND pSub.QuoteResultId <> 3 THEN 3
    				WHEN pSub.DateReceived > cNB.JobCloseDate
    						AND pSub.QuoteResultId = 3 THEN 4
    				WHEN pSub.DateReceived > cNB.JobCloseDate
    						AND pSub.QuoteResultId <> 3 THEN 99
    		 END																		AS JC_Flag

    	FROM PLEcom.JewelersCut.Submission AS pSub

    		INNER JOIN PLEcom.JewelersCut.Jeweler AS pJlr
    		ON pJlr.JewelerId = pSub.JewelerId

    		INNER JOIN PLEcom.JewelersCut.ProgramDescription AS pPDesc
    		ON pPDesc.ProgramDescriptionId = pSub.ProgramDescriptionId

    		INNER JOIN DW_DDS_CURRENT.bi_dds.DimAccount AS pOAccount
    		ON pOAccount.AccountNumber = LTRIM(pSub.PJPolicyNumber) COLLATE database_default

    		INNER JOIN DW_DDS_CURRENT.bi_dds.DimAccount AS pCAccount
    		ON pCAccount.AccountKey = pOAccount.CurrentAccountKey

    		INNER JOIN #NBMasterList AS cNB
    		ON pSub.SubmissionId = cNB.SubmissionID


    	WHERE
    			pSub.PJPolicyNumber IS NOT NULL

    	UNION ALL

    	SELECT
    		cNB.ConformedPolicyNumber
    		,cNB.PolicyEffectiveDate
    		,pPDesc.ProgramDescription													AS JC_ProgramDesc
    		,pSub.DateReceived
    		,pSub.QuoteDate
    		,CASE	WHEN pSub.SubmissionId = cNB.SubmissionID THEN 1
    				WHEN (pSub.DateReceived <= cNB.JobCloseDate
    						OR pSub.QuoteDate <= cNB.JobCloseDate)
    						AND pSub.QuoteResultId = 3 THEN 2
    				WHEN (pSub.DateReceived <= cNB.JobCloseDate
    						OR pSub.QuoteDate <= cNB.JobCloseDate)
    						AND pSub.QuoteResultId <> 3 THEN 3
    				WHEN pSub.DateReceived > cNB.JobCloseDate
    						AND pSub.QuoteResultId = 3 THEN 4
    				WHEN pSub.DateReceived > cNB.JobCloseDate
    						AND pSub.QuoteResultId <> 3 THEN 99
    		 END																		AS JC_Flag

    	FROM PLEcom.JewelersCut.Submission AS pSub

    		INNER JOIN PLEcom.JewelersCut.Jeweler AS pJlr
    		ON pJlr.JewelerId = pSub.JewelerId

    		INNER JOIN PLEcom.JewelersCut.ProgramDescription AS pPDesc
    		ON pPDesc.ProgramDescriptionId = pSub.ProgramDescriptionId

    		INNER JOIN DW_DDS_CURRENT.bi_dds.DimAccount AS pOAccount
    		ON pOAccount.AccountNumber = LTRIM(pSub.PJPolicyNumber) COLLATE database_default

    		INNER JOIN DW_DDS_CURRENT.bi_dds.DimAccount AS pCAccount
    		ON pCAccount.AccountKey = pOAccount.CurrentAccountKey

    		INNER JOIN #NBMasterList AS cNB
    		ON LTRIM(pCAccount.AccountNumber) = cNB.AccountNumber


    	WHERE
    			pSub.PJPolicyNumber IS NOT NULL

    	UNION ALL

    	SELECT
    		cNB.ConformedPolicyNumber
    		,cNB.PolicyEffectiveDate
    		,pPDesc.ProgramDescription													AS JC_ProgramDesc
    		,pSub.DateReceived
    		,pSub.QuoteDate
    		,CASE	WHEN pSub.SubmissionId = cNB.SubmissionID THEN 1
    				WHEN (pSub.DateReceived <= cNB.JobCloseDate
    						OR pSub.QuoteDate <= cNB.JobCloseDate)
    						AND pSub.QuoteResultId = 3 THEN 2
    				WHEN (pSub.DateReceived <= cNB.JobCloseDate
    						OR pSub.QuoteDate <= cNB.JobCloseDate)
    						AND pSub.QuoteResultId <> 3 THEN 3
    				WHEN pSub.DateReceived > cNB.JobCloseDate
    						AND pSub.QuoteResultId = 3 THEN 4
    				WHEN pSub.DateReceived > cNB.JobCloseDate
    						AND pSub.QuoteResultId <> 3 THEN 99
    		 END																		AS JC_Flag

    	FROM PLEcom.JewelersCut.Submission AS pSub

    		INNER JOIN PLEcom.JewelersCut.Jeweler AS pJlr
    		ON pJlr.JewelerId = pSub.JewelerId

    		INNER JOIN PLEcom.JewelersCut.ProgramDescription AS pPDesc
    		ON pPDesc.ProgramDescriptionId = pSub.ProgramDescriptionId

    		INNER JOIN DW_DDS_CURRENT.bi_dds.DimAccount AS pOAccount
    		ON pOAccount.AccountNumber = LTRIM(pSub.PJPolicyNumber) COLLATE database_default

    		INNER JOIN DW_DDS_CURRENT.bi_dds.DimAccount AS pCAccount
    		ON pCAccount.AccountKey = pOAccount.CurrentAccountKey

    		INNER JOIN #NBMasterList AS cNB
    		ON LTRIM(pSub.PJPolicyNumber) = cNB.AccountNumber

    	WHERE
    			pSub.PJPolicyNumber IS NOT NULL
    ) AS plecom
    GROUP BY
        ConformedPolicyNumber
        ,PolicyEffectiveDate
        ,JC_ProgramDesc
        ,DateReceived
        ,QuoteDate
        ,JC_Flag
)

,JCData_01 AS
(
	SELECT
		cJCD.ConformedPolicyNumber
		,cJCD.PolicyEffectiveDate
		,CASE	WHEN cJCD.JC_Flag IN (1,2) THEN 1
				ELSE 0
		 END																		AS JC_ProgramFlag
		,cJCD.JC_ProgramDesc														AS JC_ProgramDesc
		,CASE	WHEN cJCD.JC_Flag = 3 THEN 'Had quote before eff'
				WHEN cJCD.JC_Flag = 4 THEN 'Had quote after eff'
				WHEN cJCD.JC_Flag = 99 THEN 'Had other quotes after eff'
		 END																		AS JC_QuoteFlag
		,DENSE_RANK() OVER(PARTITION BY cJCD.ConformedPolicyNumber
										,cJCD.PolicyEffectiveDate
						 ORDER BY		cJCD.JC_Flag ASC
										,cJCD.DateReceived ASC
										,cJCD.QuoteDate ASC
						 )														AS JC_Rank
	FROM JCData AS cJCD
) --SELECT * FROM JCData_01

/******************************************************************************************************************************************************
Platinum Points
******************************************************************************************************************************************************/
,PPData AS
(
	SELECT
		cNB.ConformedPolicyNumber
		,cNB.PolicyEffectiveDate
		,CASE	WHEN CAST(jPromo.SubmissionDateTime AS date) <= cNB.JobCloseDate THEN 1
				ELSE 0
		 END																		AS JMS_PlatinumFlag
		,DENSE_RANK() OVER(PARTITION BY cNB.ConformedPolicyNumber
										,cNB.PolicyEffectiveDate
						 ORDER BY		CASE WHEN CAST(jPromo.SubmissionDateTime AS date) <= cNB.JobCloseDate THEN 1 ELSE 0 END DESC
										,jPromo.SubmissionDateTime ASC
						 )														AS JMS_Rank

	FROM JMServices.PlatinumPoints.jm_Jeweler_tb AS jJlr

		INNER JOIN JMServices.PlatinumPoints.jm_JewelerPoints_tb AS jJlrPoints
		ON jJlrPoints.JewelerId = jJlr.JewelerId

		INNER JOIN JMServices.PlatinumPoints.jm_PromotionProspect AS jPromo
		ON jPromo.Id = jJlrPoints.ProspectId

		INNER JOIN #NBMasterList AS cNB
		ON jPromo.Email = cNB.InsuredEmail

	WHERE
			    jPromo.PromotionId = 3 -- Jewelers Incentive
			AND cNB.InsuredEmail <> ''
) --SELECT * FROM PPData

,GENERAL AS
(
	SELECT
		cNB.AccountNumber
		,cNB.ConformedPolicyNumber
		,cNB.RecastSourceOfBusiness
		,cNB.JewelerName
		,cNB.GeicoSubCodeDesc
		,cJCD.JC_ProgramFlag
		,cJCD.JC_QuoteFlag
		,cJCD.JC_ProgramDesc
		,cPPD.JMS_PlatinumFlag
		,cNB.SP_Source
		,cNB.AgencyMasterCode
		,CASE	WHEN cNB.SP_Source = 'GEICO Insurance Agency Inc' THEN 'SP - GEICO'
				WHEN cNB.SP_Source = 'Kraft Lake Insurance Agency Inc' THEN 'SP - Kraft Lake'
				WHEN cNB.SP_Source = 'Ivantage Select Agency Inc' THEN 'SP - Ivantage'
				WHEN cNB.SP_Source = 'Liberty Mutual Group Inc' THEN 'SP - Helmsman'
				WHEN cNB.SP_Source = 'NBS Insurance Agency Inc' THEN 'SP - Nationwide'
				WHEN cNB.SP_Source = 'Orchid Insurance' THEN 'SP - Orchid'
				WHEN cNB.SP_Source = 'Goosehead Insurance' THEN 'SP - Goosehead'
				WHEN cNB.SP_Source <> '' THEN 'SP - Other'
				WHEN cJCD.JC_ProgramDesc = 'LINK' AND cNB.AgencyMasterCode = 'LUX' THEN 'JP - Luxsurance' -- Added 1/19/2022
				WHEN cNB.RecastSourceOfBusiness = 'LINK' AND cNB.AgencyMasterCode = 'LUX' THEN 'JP - Luxsurance' -- Added 1/19/2022
				WHEN cJCD.JC_ProgramFlag = 1 THEN 'Jeweler Programs' -- Renamed JP - In Store to Jeweler Programs 1/19/2022
				WHEN cNB.RecastSourceOfBusiness = 'LINK' THEN 'Jeweler Programs' -- Renamed JP - In Store to Jeweler Programs 1/19/2022
				WHEN cNB.RecastSourceOfBusiness IN ('Jewelers Cut','Cap') THEN 'Jeweler Programs' -- Renamed JP - In Store to Jeweler Programs 1/19/2022
				--WHEN cNB.RecastSourceOfBusiness = 'Point of Sale' THEN 'Jeweler Programs' -- Added 08/24/2021 -- Renamed JP - In Store to Jeweler Programs 1/19/2022
				WHEN cNB.RecastSourceOfBusiness in ( 'Point of Sale', 'JMCC') THEN 'Jeweler Programs' -- Updated per Ann following Angela Vogel's request 2022-03-25
				WHEN cPPD.JMS_PlatinumFlag = 1 THEN 'Jeweler Programs' -- Renamed JP - In Store to Jeweler Programs 1/19/2022
				WHEN (cNB.RecastSourceOfBusiness = 'Appraisal') AND cNB.AgencyMasterCode = 'DIR' THEN 'JP - Appraisal'
				WHEN (cNB.RecastSourceOfBusiness = 'Web' OR cNB.ApplicationTakenBy = 'Web') AND cNB.AgencyMasterCode = 'DIR' THEN 'eCommerce'
				WHEN (cNB.RecastSourceOfBusiness = 'Phone' OR cNB.ApplicationTakenBy = 'Customer Care') AND cNB.AgencyMasterCode = 'DIR' THEN 'Customer Care'
				WHEN (cNB.RecastSourceOfBusiness = 'Express') AND cNB.JewelerName = 'PL Customer Brochure' THEN 'eCommerce' -- Added 09/10/2021
				WHEN (cNB.RecastSourceOfBusiness = 'Express') AND cNB.AgencyMasterCode = 'DIR' THEN 'Express'
				WHEN (cNB.RecastSourceOfBusiness = 'Agency Express') AND cNB.AgencyMasterCode = 'DIR' THEN 'eCommerce'
				WHEN (cNB.RecastSourceOfBusiness = 'Agency') AND cNB.AgencyMasterCode = 'DIR' THEN 'eCommerce'
				WHEN cNB.RecastSourceOfBusiness = 'Agency' THEN 'Agency'
				WHEN cNB.RecastSourceOfBusiness = 'Other' THEN 'Paper'
				WHEN cNB.AgencyMasterCode <> 'DIR' THEN 'Agency'
		 END AS Recast

	FROM #NBMasterList AS cNB

		LEFT JOIN JCData_01 AS cJCD
		ON cJCD.ConformedPolicyNumber = cNB.ConformedPolicyNumber
		AND cJCD.PolicyEffectiveDate = cNB.PolicyEffectiveDate
		AND cJCD.JC_Rank = 1

		LEFT JOIN PPData AS cPPD
		ON cPPD.ConformedPolicyNumber = cNB.ConformedPolicyNumber
		AND cPPD.PolicyEffectiveDate = cNB.PolicyEffectiveDate
		AND cPPD.JMS_Rank = 1
) --SELECT * INTO #GENERAL FROM GENERAL

SELECT
	cG.AccountNumber AS AccountNumber
	,cG.ConformedPolicyNumber AS PolicyNumber
	,CASE	WHEN cG.Recast = 'SP - GEICO' AND cG.GeicoSubCodeDesc = 'Call Center - Honolulu' THEN 'SP - Other' ELSE cG.Recast END AS Recast -- Express rename removed 1/19/2022 -- Geico HI callcenter added 09/14/2022
	,CASE	WHEN cG.Recast = 'SP - GEICO' THEN cG.GeicoSubCodeDesc
			WHEN cG.Recast like 'SP%' THEN cG.SP_Source
			WHEN cG.Recast = 'JP - Luxsurance' THEN 'Luxsurance' -- Added 1/19/2022
			WHEN cG.Recast = 'Jeweler Programs' THEN  -- Renamed JP - In Store to Jeweler Programs 1/19/2022
					CASE	WHEN cG.JC_ProgramDesc IN ('Jeweler''s Cut','JC CAP') THEN 'JC Paper & CAP'
							WHEN cG.JC_ProgramDesc = 'JC EDGE' THEN 'JC EDGE'
							--WHEN cG.JC_ProgramDesc = 'LINK' AND cG.AgencyMasterCode = 'LUX' THEN 'Luxsurance' -- Removed 1/19/2022
							WHEN cG.JC_ProgramDesc = 'LINK' AND cG.AgencyMasterCode <> 'LUX' THEN 'JC LINK'
							--WHEN cG.RecastSourceOfBusiness = 'LINK' AND cG.AgencyMasterCode = 'LUX' THEN 'Luxsurance' -- Removed 1/19/2022
							WHEN cG.RecastSourceOfBusiness = 'LINK' AND cG.AgencyMasterCode <> 'LUX' THEN 'JC LINK'
							WHEN cG.RecastSourceOfBusiness IN ('Jewelers Cut','Cap') THEN 'JC Paper & CAP'
							--WHEN cG.RecastSourceOfBusiness = 'Point of Sale' THEN 'Customer Connect' -- Added 08/24/2021 -- name change 09/30/2021 -- Renamed Point of sale to Customer Connect 1/19/2022
							WHEN cG.RecastSourceOfBusiness in ( 'Point of Sale', 'JMCC') THEN 'Customer Connect' -- Updated per Ann following Angela Vogel's request 2022-03-25
							WHEN cG.JMS_PlatinumFlag = 1 THEN 'Platinum Points'
					END
			WHEN cG.Recast = 'JP - Appraisal' THEN 'Appraisal'
			WHEN cG.Recast = 'Express' THEN
					CASE	WHEN cG.JewelerName LIKE '%BLUE NILE%' THEN 'Blue Nile'
							WHEN cG.JewelerName LIKE '%JAMES ALLEN%' THEN 'James Allen'
							WHEN cG.JewelerName LIKE '%DOTDASH%' THEN 'DotDash'
							ELSE 'All Other Jewelers'
					END
			WHEN cG.Recast = 'eCommerce' AND cG.JewelerName = 'PL Customer Brochure' THEN 'Brochure' -- Added 09/10/2021
			ELSE cG.Recast
		END AS SubRecast

FROM GENERAL AS cG

GROUP BY
	cG.AccountNumber
	,cG.ConformedPolicyNumber
	,CASE	WHEN cG.Recast = 'SP - GEICO' AND cG.GeicoSubCodeDesc = 'Call Center - Honolulu' THEN 'SP - Other' ELSE cG.Recast END -- Express rename removed 1/19/2022 -- Geico HI callcenter added 09/14/2022
	,CASE	WHEN cG.Recast = 'SP - GEICO' THEN cG.GeicoSubCodeDesc
			WHEN cG.Recast like 'SP%' THEN cG.SP_Source
			WHEN cG.Recast = 'JP - Luxsurance' THEN 'Luxsurance' -- Added 1/19/2022
			WHEN cG.Recast = 'Jeweler Programs' THEN  -- Renamed JP - In Store to Jeweler Programs 1/19/2022
					CASE	WHEN cG.JC_ProgramDesc IN ('Jeweler''s Cut','JC CAP') THEN 'JC Paper & CAP'
							WHEN cG.JC_ProgramDesc = 'JC EDGE' THEN 'JC EDGE'
							--WHEN cG.JC_ProgramDesc = 'LINK' AND cG.AgencyMasterCode = 'LUX' THEN 'Luxsurance' -- Removed 1/19/2022
							WHEN cG.JC_ProgramDesc = 'LINK' AND cG.AgencyMasterCode <> 'LUX' THEN 'JC LINK'
							--WHEN cG.RecastSourceOfBusiness = 'LINK' AND cG.AgencyMasterCode = 'LUX' THEN 'Luxsurance' -- Removed 1/19/2022
							WHEN cG.RecastSourceOfBusiness = 'LINK' AND cG.AgencyMasterCode <> 'LUX' THEN 'JC LINK'
							WHEN cG.RecastSourceOfBusiness IN ('Jewelers Cut','Cap') THEN 'JC Paper & CAP'
							--WHEN cG.RecastSourceOfBusiness = 'Point of Sale' THEN 'Customer Connect' -- Added 08/24/2021 -- name change 09/30/2021 -- Renamed Point of sale to Customer Connect 1/19/2022
							WHEN cG.RecastSourceOfBusiness in ( 'Point of Sale', 'JMCC') THEN 'Customer Connect' -- Updated per Ann following Angela Vogel's request 2022-03-25
							WHEN cG.JMS_PlatinumFlag = 1 THEN 'Platinum Points'
					END
			WHEN cG.Recast = 'JP - Appraisal' THEN 'Appraisal'
			WHEN cG.Recast = 'Express' THEN
					CASE	WHEN cG.JewelerName LIKE '%BLUE NILE%' THEN 'Blue Nile'
							WHEN cG.JewelerName LIKE '%JAMES ALLEN%' THEN 'James Allen'
							WHEN cG.JewelerName LIKE '%DOTDASH%' THEN 'DotDash'
							ELSE 'All Other Jewelers'
					END
			WHEN cG.Recast = 'eCommerce' AND cG.JewelerName = 'PL Customer Brochure' THEN 'Brochure' -- Added 09/10/2021
			ELSE cG.Recast
		END

UNION ALL

SELECT DISTINCT
	NULL															AS AccountNumber
	,LEFT(PolicyNumber,CHARINDEX('-', PolicyNumber,4)-1)			AS PolicyNumber
	,'Assumed'														AS Recast
	,CASE WHEN Source = 'FedNat' THEN 'Fed Nat' ELSE 'Other' END	AS SubRecast

FROM GW_Reporting.PartnerData.Earnings