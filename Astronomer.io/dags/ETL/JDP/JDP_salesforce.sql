SELECT
    SFAccountNumber
    ,SalesforceID
	,DUNS
    ,Name
    ,PrimaryAddressStreet
    ,PrimaryAddressCity
    ,PrimaryAddressState
    ,PrimaryAddressPostalCode
	,PrimaryAddressCountry
    ,PrimaryPhone
    ,LocationNumber
    ,PrimaryLocation
    ,_DateCreated

    ,Agency_Code_Jeweler__c
    ,Agency_Sub_Group__c
    ,Agency__c
    ,Master_Agency_Code_Jeweler__c
    ,Producer_with_code__c
    ,Type
    ,TypeOfBusiness
	,SalesforceID as Trade_Association__c
	,DATE("{{{{ ds }}}}") as bq_load_date
FROM (
    SELECT  
    RTRIM(REGEXP_REPLACE(CL_Account_Number__c, r'None', ''), '.0') AS SFAccountNumber
    ,REGEXP_REPLACE(Id, r'None', '') AS SalesforceID
    ,RTRIM(REGEXP_REPLACE(DNBoptimizer__DNB_D_U_N_S_Number__c, r'None', ''), '.0') AS DUNS
    --,Name AS Name /* alternative name source below */
	,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(Agency_Name_and_Producer_Code__c, 'Ã©', 'è'), '%2B', '+'), '%20', ' '), '%26', '&'), '%2C', ', '), '%23', '$'), '%0D', ''), '%28', '('), '%29', ')') AS Name
	,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(BillingStreet , 'Ã©', 'è'), '%2B', '+'), '%20', ' '), '%26', '&'), '%2C', ', '),  '%23', '$'), '%0D', ''), '%28', '('), '%29', ')') AS PrimaryAddressStreet
    ,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(BillingCity, 'Ã©', 'è'), '%2B', '+'), '%20', ' '), '%26', '&'), '%2C', ', '), '%23', '$'), '%0D', ''), '%28', '('), '%29', ')') AS PrimaryAddressCity
    ,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(BillingState, 'Ã©', 'è'), '%2B', '+'), '%20', ' '), '%26', '&'), '%2C', ', '), '%23', '$'), '%0D', ''), '%28', '('), '%29', ')')  AS PrimaryAddressState
    ,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(BillingPostalCode, 'Ã©', 'è'), '%2B', '+'), '%20', ' '), '%26', '&'), '%2C', ', '), '%23', '$'), '%0D', ''), '%28', '('), '%29', ')')  AS PrimaryAddressPostalCode
	,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(BillingCountry, 'Ã©', 'è'), '%2B', '+'), '%20', ' '), '%26', '&'), '%2C', ', '), '%23', '$'), '%0D', ''), '%28', '('), '%29', ')')  AS PrimaryAddressCountry
	,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(Phone, 'Ã©', 'è'), '%2B', '+'), '%20', ' '), '%26', '&'), '%2C', ', '), '%23', '$'), '%0D', ''), '%28', '('), '%29', ')') AS PrimaryPhone
    ,Location_Number__c AS LocationNumber
    ,Primary_Location__c AS PrimaryLocation
    ,data_land_date AS _DateCreated
    ,Agency_Code_Jeweler__c
    ,Agency_Sub_Group__c
    ,Agency__c
	,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(Master_Agency_Code_Jeweler__c, 'Ã©', 'è'), '%2B', '+'), '%20', ' '), '%26', '&'), '%2C', ', '), '%23', '$'), '%0D', ''), '%28', '('), '%29', ')') as Master_Agency_Code_Jeweler__c
	,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(Producer_with_code__c, 'Ã©', 'è'), '%2B', '+'), '%20', ' '), '%26', '&'), '%2C', ', '), '%23', '$'), '%0D', ''), '%28', '('), '%29', ')') as Producer_with_code__c
    ,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(Type, 'Ã©', 'è'), '%2B', '+'), '%20', ' '), '%26', '&'), '%2C', ', '), '%23', '$'), '%0D', ''), '%28', '('), '%29', ')') as Type
    ,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(Type_of_Business__c, 'Ã©', 'è'), '%2B', '+'), '%20', ' '), '%26', '&'), '%2C', ', '), '%23', '$'), '%0D', ''), '%28', '('), '%29', ')') AS TypeOfBusiness
    ------------------------------------------------
    ,ROW_NUMBER() 
      OVER (PARTITION BY Id
      ORDER BY _PARTITIONTIME DESC) AS record_order
    FROM `{project}.{dataset}.sf_account` 
    where 1=1 
    AND Primary_Location__c = 'True'
 --   AND ID IN (
 --         SELECT DISTINCT sf_trade_associations__c.Jeweler__c
 --         FROM `{project}.{dataset}.sf_account` AS sf_account
 --         INNER JOIN `{project}.{dataset}.sf_trade_associations__c` AS sf_trade_associations__c
 --         ON sf_account.ID = sf_trade_associations__c.Trade_Association__c
 --         )
  ) 
WHERE record_order = 1     /* get record from the latest partitiontime */
ORDER BY SalesforceID