#StandardSQL
SELECT
    cl.Master_Code
    ,cl.Agency_Code
    ,cl.PolicyNumber
    ,cl.AccountNumber
    ,cl.Jeweler
    ,COALESCE(claims.Jeweler_Display_Names, cl.Jeweler) AS Jeweler_Display_Names
    ,cl.Number_of_Employees
    ,cl.Number_of_Locations
    ,PARSE_DATE('%m/%d/%Y', cl.Policy_Effective_Date) AS Policy_Effective_Date
    ,PARSE_DATE('%m/%d/%Y', cl.Policy_Expiration_Date) AS Policy_Expiration_Date
    ,PARSE_DATE('%m/%d/%Y', cl.Original_Effective_Date) AS Original_Effective_Date
    ,cl.JC_YTD_Total
    ,cl.JC_Paid_Amount
    ,cl.JC_Enrollment
    ,cl.PP_YTD_Total
    ,cl.PP_Paid_Amount
    ,cl.PP_Enrollment
    ,COALESCE(claims.WorkedClaims5yrs, 0) AS WorkedClaims5yrs
    ,COALESCE(claims.WorkedClaimPayments5yrs, 0) AS WorkedClaimPayments5yrs
    ,COALESCE(claims.WorkedClaimsCurrent, 0) AS WorkedClaimsCurrent
    ,COALESCE(claims.WorkedClaimsPaidCurrent, 0) AS WorkedClaimsPaidCurrent
    ,cl.JSA_JVC_Membership_Fees
    ,cl.Years_with_JM
    ,cl.JSA_JVC_Lifetime_Fees
    
FROM `{project}.{dataset}.nonpromoted_mbs_cl_attributes` AS cl

LEFT JOIN (
    SELECT
        claims.Jeweler_Key
        ,STRING_AGG(claims.AccountJewelerName, ", ") AS Jeweler_Display_Names
        --,claims.GeographyCountryDesc
        ,SUM(claims.WorkedClaims5yrs) AS WorkedClaims5yrs
        ,SUM(claims.WorkedClaimPayments5yrs) AS WorkedClaimPayments5yrs
        ,SUM(claims.WorkedClaimsCurrent) AS WorkedClaimsCurrent
        ,SUM(claims.WorkedClaimsPaidCurrent) AS WorkedClaimsPaidCurrent

    FROM `{project}.{dataset}.nonpromoted_mbs_claims` AS claims

GROUP BY
        claims.Jeweler_Key
        --,claims.GeographyCountryDesc
        --,claims.LOBProductLineDescription
) AS claims
ON cl.Jeweler_Key = claims.Jeweler_Key;
