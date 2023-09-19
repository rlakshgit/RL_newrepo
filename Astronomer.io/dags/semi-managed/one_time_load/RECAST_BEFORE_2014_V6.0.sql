;WITH PolicyOrig AS
(
	SELECT
		dPol.ConformedPolicyNumber
		,MIN(dPol.PolicyEffectiveDate) AS ORIG_DATE
	FROM DW_DDS_CURRENT.bi_dds.DimPolicy AS dPol
	WHERE	1 = 1
			AND (( dPol.SourceSystem = 'PAS' 
								AND dPol.PolicyIssueDate <> -1 
								AND dPol.PolicyStatus NOT IN ('V','IT','DE','DA','DC')) 
						OR (dPol.SourceSystem = 'GW' 
								AND dPol.PolicyStatus = 'Bound'
								AND dPol.JobCloseDate < 20140101
								AND dPol.TransactionEffectiveDate < 20140101))
			AND dPol.PolicyVersion <> 0
			AND dPol.AccountSegment = 'Personal Lines'
	GROUP BY 
		dPol.ConformedPolicyNumber
)
SELECT DISTINCT
	2023 AS PlanYear
	,t.AccountNumber AS AccountNumber
	,t.ConformedPolicyNumber AS PolicyNumber
	,t.RecastBefore2014 AS Recast
	,CASE	WHEN t.RecastBefore2014 = 'Jeweler Programs' THEN 'JC Paper & CAP'
			WHEN t.RecastBefore2014 = 'Express' THEN 
					CASE	WHEN t.JewelerName LIKE '%BLUE NILE%' THEN 'Blue Nile'
							WHEN t.JewelerName LIKE '%JAMES ALLEN%' THEN 'James Allen'
							ELSE 'All Other Jewelers'
					END 
			ELSE t.RecastBefore2014
		END AS SubRecast
FROM
	(
		SELECT DISTINCT
			cPO.ConformedPolicyNumber
			,dAcct.AccountNumber
			, dPol.SourceOfBusiness
			,CASE	WHEN dPol.SourceOfBusiness IN ('Jewelers Cut','LINK') THEN 'Jeweler Programs'
					WHEN dPol.SourceOfBusiness = 'Web' AND dAgency.AgencyMasterCode IN ('DIR', '?') THEN 'eCommerce'
					WHEN dPol.SourceOfBusiness = 'Phone' AND dAgency.AgencyMasterCode IN ('DIR', '?') THEN 'Customer Care'
					WHEN dPol.SourceOfBusiness = 'Express' AND dAgency.AgencyMasterCode IN ('DIR', '?') THEN 'Express'
					WHEN dPol.SourceOfBusiness = 'Agency Express' AND dAgency.AgencyMasterCode IN ('DIR', '?') THEN 'eCommerce'
					WHEN dPol.SourceOfBusiness = 'Agency Express' AND dAgency.AgencyMasterCode <> 'DIR' THEN 'Agency'
					WHEN dPol.SourceOfBusiness = 'Agency' AND dAgency.AgencyMasterCode IN ('DIR', '?') THEN 'eCommerce'
					WHEN dPol.SourceOfBusiness = 'Agency' THEN 'Agency'
					WHEN dPol.SourceOfBusiness IN ('Other','?') THEN 'Paper'
					WHEN dAgency.AgencyMasterCode <> 'DIR' THEN 'Agency'
				END AS RecastBefore2014
			,cPO.ORIG_DATE
			,RANK() OVER(PARTITION BY cPO.ConformedPolicyNumber
							ORDER BY dPol.PolicyGroup
									,dPol.PolicyVersion ASC
									,dPol.Policykey ASC
							) AS rnk
			,COALESCE(REPLACE(REPLACE(REPLACE(dAJCont.FullName,',',''),'''',''),CHAR(13) + CHAR(10), ''),'') AS JewelerName

		FROM PolicyOrig AS cPO

		INNER JOIN DW_DDS_CURRENT.bi_dds.DimPolicy AS dPol
		ON dPol.ConformedPolicyNumber = cPO.ConformedPolicyNumber
		AND dPol.PolicyEffectiveDate = cPO.ORIG_DATE

		INNER JOIN DW_DDS_CURRENT.bi_dds.DimAccount AS dAcct
		ON dAcct.AccountKey = dPol.AccountKey

		INNER JOIN DW_DDS_CURRENT.bi_dds.DimContact AS dAJCont
		ON dAJCont.ContactKey = dAcct.AccountJewelerContactKey

		INNER JOIN DW_DDS_CURRENT.bi_dds.DimAgency AS dAgency
		ON dAgency.AgencyKey = dPol.ProducerOfRecordAgencyKey

	) AS t
WHERE rnk = 1
