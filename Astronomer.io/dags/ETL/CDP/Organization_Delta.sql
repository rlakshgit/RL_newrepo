/********************************************/
/**********   CDP - Organization   **********/
/********************************************/
SELECT  
	pc_uwcompany.PublicID AS SourceOrganizationNumber
	,pc_uwcompany.Name
	--, AS ParentSourceOrganizationNumber
	,pc_address.AddressLine1
	,pc_address.AddressLine2
	,pc_address.City
	,pc_address.PostalCode AS Zip
	,pctl_state.NAME AS State
	,pctl_country.NAME AS Country
	,pctl_uwcompanystatus.NAME AS Status
--	,pc_contact.Subtype
	
FROM {prefix}{pc_dataset}.pc_uwcompany pc_uwcompany
INNER JOIN {prefix}{pc_dataset}.pctl_uwcompanycode
	ON pc_uwcompany.Code = pctl_uwcompanycode.ID
	AND pctl_uwcompanycode.Retired=false
  AND DATE(pc_uwcompany._PARTITIONTIME) = DATE('{date}')
INNER JOIN {prefix}{pc_dataset}.pctl_uwcompanystatus
	ON pctl_uwcompanystatus.ID = pc_uwcompany.Status
INNER JOIN {prefix}{pc_dataset}.pc_contact
	ON 7 = pc_contact.ID AND DATE(pc_contact._PARTITIONTIME) = DATE('{date}')

INNER JOIN {prefix}{pc_dataset}.pc_address
	ON pc_contact.PrimaryAddressID = pc_address.ID
AND DATE(pc_address._PARTITIONTIME) = DATE('{date}')
INNER JOIN {prefix}{pc_dataset}.pctl_state
	ON pc_address.State = pctl_state.ID
INNER JOIN {prefix}{pc_dataset}.pctl_country
	ON pctl_country.ID = pc_address.Country