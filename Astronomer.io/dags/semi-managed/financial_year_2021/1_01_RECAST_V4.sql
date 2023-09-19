SET NOCOUNT ON;
USE DW_DDS_CURRENT;

DECLARE @BEGDATE	AS DATE = '1/1/2014'
DECLARE @ENDDATE	AS DATE = DATEADD(DAY, -1, GETDATE())
DECLARE @BEGDATEKEY AS INT = DW_DDS_CURRENT.bief_dds.fn_GetDateKeyFromDate(@BEGDATE)
DECLARE @ENDDATEKEY AS INT = DW_DDS_CURRENT.bief_dds.fn_GetDateKeyFromDate(@ENDDATE)

/******************************************************************************************************************************************************
NBMasterList temp table
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
		,CASE	WHEN dAValue.Value5	IS NOT NULL THEN dAValue.Value5
				WHEN RIGHT(dAgency.AgencySubCode,3) = 'D10' THEN 'JM Call Center'
				ELSE 'Web'
		 END																		AS GeicoSubCodeDesc
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

	WHERE
			    fWP.AccountingDateKey >= @BEGDATEKEY
            AND fWP.AccountingDateKey <= @ENDDATEKEY
			AND dBType.BusinessTypeDesc = 'Direct'
			AND dLOB.LOBProductLineCode = 'PL'
			AND dTType.ConformedPolicyTransactionDesc IN ('Submission')

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
		,CASE	WHEN dAValue.Value5	IS NOT NULL THEN dAValue.Value5
				WHEN RIGHT(dAgency.AgencySubCode,3) = 'D10' THEN 'JM Call Center'
				ELSE 'Web'
		 END

/******************************************************************************************************************************************************
Strategic partner agency codes
******************************************************************************************************************************************************/
;WITH SPCodes AS
(
	SELECT DISTINCT
		dAgency.AgencyMasterCode													AS SP_Code
		,CASE	WHEN dAgency.AgencyMasterCode = 'Z100' THEN 'GEICO'
				WHEN dAgency.AgencyMasterCode = 'KL001' THEN 'Kraft Lake'
				WHEN dAgency.AgencyMasterCode = 'ISA001' THEN 'Ivantage'
				WHEN dAgency.AgencyMasterCode = 'HE001' THEN 'Helmsman'
				WHEN dAgency.AgencyMasterCode = 'NBS' THEN 'Nationwide'
				WHEN dAgency.AgencyMasterCode = 'RC001' THEN 'Orchid'
				WHEN dAgency.AgencyMasterCode = 'GH001' THEN 'Goosehead'
				WHEN dAgency.AgencyMasterCode = 'U10' THEN 'McGriff'
				WHEN dAgency.AgencyMasterCode = 'Z400' THEN 'Big I'
				WHEN dAgency.AgencyMasterCode = 'TWFG' THEN 'TWFG'
				WHEN dAgency.AgencyMasterCode = 'B99' THEN 'Bolt'
				WHEN dAgency.AgencyMasterCode = 'BRP001' THEN 'BRP Guided Baldwin Risk' 
				WHEN dAgency.AgencyMasterCode = 'BKS001' THEN 'Baldwin Krystyn Sherman'
				WHEN dAgency.AgencyMasterCode = 'YNG001' THEN 'Young Alfred'
				WHEN dAgency.AgencyMasterCode = 'PGI001' THEN 'Premier Group Insurance'
				WHEN dAgency.AgencyMasterCode = 'INS001' THEN 'Banc Ins Agy Insuritas'
				WHEN dAgency.AgencyMasterCode = 'AIB001' THEN 'Atlas Insurance Brokers'
				WHEN dAgency.AgencyMasterCode = 'MAT001' THEN 'Matic Insurance Services' -- Added 09/24/2021
				WHEN dAgency.AgencyMasterCode = 'Z41' THEN 'We Insure' -- Added 09/24/2021
				WHEN dAgency.AgencyMasterCode = 'AAS001' THEN 'Agents Alliance Services' -- Added 10/12/2021
				WHEN dAgency.AgencyMasterCode = 'AOH001' THEN 'AAA of Ohio' -- Added 12/01/2021
				WHEN dAgency.AgencyMasterCode = 'USAA001' THEN 'USAA' -- Added 12/01/2021
				ELSE ''
		END AS SP_Source

	FROM DW_DDS_CURRENT.bi_dds.DimPolicy AS dPol

		INNER JOIN DW_DDS_CURRENT.bi_dds.DimAgency AS dAgency
		ON dAgency.AgencyKey = dPol.AgencyKey

	WHERE	1 = 1
			AND dPol.PolicyStatus = 'Bound'
			AND dPol.SourceOfBusiness = 'Agency Express'
)

/******************************************************************************************************************************************************
Submission policies using FactPremiumWritten
******************************************************************************************************************************************************/


/******************************************************************************************************************************************************
Jewelers Cut
******************************************************************************************************************************************************/
,JCData AS
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
		,cSP.SP_Source
		,CASE	WHEN cSP.SP_Source = 'GEICO' THEN 'SP - GEICO'
				WHEN cSP.SP_Source = 'Kraft Lake' THEN 'SP - Kraft Lake'
				WHEN cSP.SP_Source = 'Helmsman' THEN 'SP - Helmsman'
				WHEN cSP.SP_Source = 'Ivantage' THEN 'SP - Ivantage'
				WHEN cSP.SP_Source = 'Nationwide' THEN 'SP - Nationwide'
				WHEN cSP.SP_Source = 'Orchid' THEN 'SP - Orchid'
				WHEN cSP.SP_Source = 'Goosehead' THEN 'SP - Goosehead'
				WHEN cSP.SP_Source <> '' THEN 'SP - Other'
				WHEN cJCD.JC_ProgramFlag = 1 THEN 'Jeweler Programs'
				WHEN cNB.RecastSourceOfBusiness = 'LINK' THEN 'Jeweler Programs'
				WHEN cNB.RecastSourceOfBusiness IN ('Jewelers Cut','Cap') THEN 'Jeweler Programs'
				WHEN cPPD.JMS_PlatinumFlag = 1 THEN 'Jeweler Programs'
				WHEN (cNB.RecastSourceOfBusiness = 'Appraisal') AND cNB.AgencyMasterCode = 'DIR' THEN 'Jeweler Programs' -- Added 12/03/2020
				WHEN (cNB.RecastSourceOfBusiness = 'Web' OR cNB.ApplicationTakenBy = 'Web') AND cNB.AgencyMasterCode = 'DIR' THEN 'eCommerce'
				WHEN (cNB.RecastSourceOfBusiness = 'Phone' OR cNB.ApplicationTakenBy = 'Customer Care') AND cNB.AgencyMasterCode = 'DIR' THEN 'Customer Care'
				WHEN (cNB.RecastSourceOfBusiness = 'Express') AND cNB.JewelerName = 'PL Customer Brochure' THEN 'eCommerce' -- Added 09/10/2021
				WHEN (cNB.RecastSourceOfBusiness = 'Express') AND cNB.AgencyMasterCode = 'DIR' THEN 'Express'
				WHEN (cNB.RecastSourceOfBusiness = 'Agency Express') AND cNB.AgencyMasterCode = 'DIR' THEN 'eCommerce'
				WHEN (cNB.RecastSourceOfBusiness = 'Agency') AND cNB.AgencyMasterCode = 'DIR' THEN 'eCommerce'
				WHEN cNB.RecastSourceOfBusiness = 'Agency' THEN 'Agency'
				WHEN cNB.RecastSourceOfBusiness = 'Other' THEN 'Paper'
				WHEN cNB.RecastSourceOfBusiness = 'Point of Sale' THEN 'Jeweler Programs' -- Added 09/10/2021
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

		LEFT JOIN SPCodes AS cSP
		ON cSP.SP_Code = cNB.AgencyMasterCode
) --SELECT * INTO #GENERAL FROM GENERAL

SELECT
	cG.AccountNumber AS AccountNumber
	,cG.ConformedPolicyNumber AS PolicyNumber
	,cG.Recast AS Recast
	,CASE	WHEN cG.Recast = 'SP - GEICO' THEN cG.GeicoSubCodeDesc
			WHEN cG.Recast like 'SP%' THEN cG.SP_Source
			WHEN cG.Recast = 'Jeweler Programs' THEN
					CASE	WHEN cG.JC_ProgramDesc IN ('Jeweler''s Cut','JC CAP') THEN 'JC Paper & CAP'
							WHEN cG.JC_ProgramDesc = 'JC EDGE' THEN 'JC EDGE'
							WHEN cG.JC_ProgramDesc = 'LINK' THEN 'JC LINK'
							WHEN cG.RecastSourceOfBusiness = 'LINK' THEN 'JC LINK'
							WHEN cG.RecastSourceOfBusiness IN ('Jewelers Cut','Cap') THEN 'JC Paper & CAP'
							WHEN cG.RecastSourceOfBusiness = 'Appraisal' THEN 'Appraisal' -- Added 12/03/2020
							WHEN cG.RecastSourceOfBusiness = 'Point of Sale' THEN 'Point of Sale' -- Added 09/10/2021 -- name change 09/30/2021
							WHEN cG.JMS_PlatinumFlag = 1 THEN 'Platinum Points'
					END
			WHEN cG.Recast = 'Express' THEN
					CASE	WHEN cG.JewelerName LIKE '%BLUE NILE%' THEN 'Blue Nile'
							WHEN cG.JewelerName LIKE '%JAMES ALLEN%' THEN 'James Allen'
							ELSE 'All Other Jewelers'
					END
			WHEN cG.Recast = 'eCommerce' AND cG.JewelerName = 'PL Customer Brochure' THEN 'Brochure' -- Added 09/10/2021
			ELSE cG.Recast
		END AS SubRecast
	--,CASE WHEN cG.Recast = 'Jeweler Programs' AND cG.JC_ProgramDesc = 'JC CAP' THEN 'CAP' ELSE 'Other' END AS CapInd

FROM GENERAL AS cG

GROUP BY
	cG.AccountNumber
	,cG.ConformedPolicyNumber
	,cG.Recast
	,CASE	WHEN cG.Recast = 'SP - GEICO' THEN cG.GeicoSubCodeDesc
			WHEN cG.Recast like 'SP%' THEN cG.SP_Source
			WHEN cG.Recast = 'Jeweler Programs' THEN
					CASE	WHEN cG.JC_ProgramDesc IN ('Jeweler''s Cut','JC CAP') THEN 'JC Paper & CAP'
							WHEN cG.JC_ProgramDesc = 'JC EDGE' THEN 'JC EDGE'
							WHEN cG.JC_ProgramDesc = 'LINK' THEN 'JC LINK'
							WHEN cG.RecastSourceOfBusiness = 'LINK' THEN 'JC LINK'
							WHEN cG.RecastSourceOfBusiness IN ('Jewelers Cut','Cap') THEN 'JC Paper & CAP'
							WHEN cG.RecastSourceOfBusiness = 'Appraisal' THEN 'Appraisal' -- Added 12/03/2020
							WHEN cG.RecastSourceOfBusiness = 'Point of Sale' THEN 'Point of Sale' -- Added 09/10/2020 -- name change 09/30/2021
							WHEN cG.JMS_PlatinumFlag = 1 THEN 'Platinum Points'
					END
			WHEN cG.Recast = 'Express' THEN
					CASE	WHEN cG.JewelerName LIKE '%BLUE NILE%' THEN 'Blue Nile'
							WHEN cG.JewelerName LIKE '%JAMES ALLEN%' THEN 'James Allen'
							ELSE 'All Other Jewelers'
					END
			WHEN cG.Recast = 'eCommerce' AND cG.JewelerName = 'PL Customer Brochure' THEN 'Brochure' -- Added 09/10/2020
			ELSE cG.Recast
		END
