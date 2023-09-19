
SELECT 
	PolicyNumber
	,JobNumber
	,SourceOfBusiness	
	,InsuredAge
	,InsuredGender
	,InsuredState
	,InsuredCountry
	,RecordAgencyName
	,RecordAgencySubName
	,ServiceAgencyName
	,ServiceAgencySubName
	,AccountJewelerName
FROM 
(
	SELECT DISTINCT
		dPol.ConformedPolicyNumber																				AS PolicyNumber
		,'J_' + dPol.JobNumber																					AS JobNumber
		,dPol.SourceOfBusiness																					AS SourceOfBusiness	
		,COALESCE(DATEDIFF(YEAR
							,dPCont.BirthDate
							,DW_DDS_CURRENT.bief_dds.fn_GetDateFromDateKey(dPol.PolicyEffectiveDate))
				  ,'')																							AS InsuredAge
		,CASE	WHEN dPCont.Gender = '1' THEN 'FEMALE'
				WHEN dPCont.Gender = '2' THEN 'MALE'
				ELSE 'Unknown' 
		 END																									AS InsuredGender
		,COALESCE(dPGeo.GeographyStateCode,'Unknown') 															AS InsuredState
		,COALESCE(dPGeo.GeographyCountryCode,'Unknown') 														AS InsuredCountry
		,dPRAgency.AgencyMasterName																				AS RecordAgencyName
		,dPRAgency.AgencySubName																				AS RecordAgencySubName
		,dPSAgency.AgencyMasterName																				AS ServiceAgencyName
		,dPSAgency.AgencySubName																				AS ServiceAgencySubName
		,COALESCE(REPLACE(REPLACE(REPLACE(dJlrCont.FullName,',',''),'''',''),CHAR(13) + CHAR(10), ''),'')		AS AccountJewelerName

		,DENSE_RANK() OVER(PARTITION BY	dPol.JobNumber
							ORDER BY	dPol.PolicyInforceFromDate ASC	
										,dPol.PolicyKey DESC
										,dPol.ProducerOfRecordAgencyKey DESC
							)																					AS PolRank

	FROM DW_DDS_CURRENT.bi_dds.DimPolicy AS dPol

		INNER JOIN DW_DDS_CURRENT.bi_dds.DimContact AS dPCont
		ON dPCont.ContactKey = dPol.PolicyInsuredContactKey

		INNER JOIN DW_DDS_CURRENT.bi_dds.DimGeography AS dPGeo
		ON dPGeo.GeographyKey = dPCont.PrimaryAddressGeographyKey

		INNER JOIN DW_DDS_CURRENT.bi_dds.DimAccount AS dAcct
		ON dAcct.AccountKey = dPol.AccountKey

		LEFT JOIN DW_DDS_CURRENT.bi_dds.DimAgency AS dPRAgency
		ON dPRAgency.AgencyKey = dPol.ProducerOfRecordAgencyKey
		AND dPol.ProducerOfRecordAgencyKey <> -1
	
		INNER JOIN DW_DDS_CURRENT.bi_dds.DimAgency AS dPSAgency
		ON dPSAgency.AgencyKey = dPol.AgencyKey

		LEFT JOIN DW_DDS_CURRENT.bi_dds.DimContact AS dJlrCont
		ON dJlrCont.ContactKey = dAcct.AccountJewelerContactKey

		INNER JOIN DW_DDS_CURRENT.bi_keymap.KeymapDimPolicy KDP
		ON dPol.PolicyKey = KDP.PolicyKey
	
	
	WHERE	dPol.AccountSegment = 'Personal Lines'
			AND dPol.SourceSystem = 'GW' AND dPol.PolicyStatus = 'Bound'
			AND dPol.ConformedPolicyNumber <> 'Unassigned'
			AND dPol.PolicyVersion <> 0
			AND dPol.PolicyType = 'PJ'
			AND LEFT(dPol.PolicyEffectiveDate,4) >= 2017
			--AND LEFT(dPol.PolicyEffectiveDate,6) = 202201
			AND CASE	WHEN dPol.SourceSystem = 'PAS' THEN 1
						WHEN CAST(KDP.gwSubEffectiveDate AS DATE) = CAST(CAST(dPol.TransactionEffectiveDate AS VARCHAR(10)) AS DATE) THEN 1 
						ELSE 0 
				 END = 1
)t
WHERE PolRank = 1

