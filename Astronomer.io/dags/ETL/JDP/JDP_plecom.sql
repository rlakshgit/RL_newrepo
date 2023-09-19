SELECT distinct 
	JewelerId,
	Name JewelerName,
	StreetAddress,
	city,
	PostalZipCode,
	State.StateCode StateCode,
	State.State State,
	Country.CountryCode CountryCode ,
	Country.CountryDescription  Country,
	DATE("{{{{ ds }}}}") as bq_load_date
FROM `{project}.{dataset}.Jeweler` Jeweler
Left join `{project}.{dataset}.State` State on Jeweler.StateId  = State.StateId 
Left join  `{project}.{dataset}.Country` Country on State.CountryId = Country.CountryId 