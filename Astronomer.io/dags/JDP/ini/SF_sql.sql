with recent_records as
			(
			select * from `{source_project}.{source_dataset}.{source_table}`
			)
                           SELECT SFAccountNumber as SF_AccountNumber
                                , SalesforceID
                                , DUNS
                                , '' as CompanyRecord
                                , '' as GlobalUltimateBusinessName
                                , '' as GlobalUltimateDUNS
                                , TO_BASE64(SHA256(ARRAY_TO_STRING( [
                                                                    Name
                                                                  , PrimaryAddressStreet
                                                                  , PrimaryAddressCity
                                                                  , PrimaryAddressState
                                                                  , PrimaryAddressPostalCode], ' '))) as SF_LocationKey
                                , ARRAY_TO_STRING( [
                                                    Name
                                                  , PrimaryAddressStreet
                                                  , PrimaryAddressCity
                                                  , PrimaryAddressState
                                                  , SPLIT(PrimaryAddressPostalCode, '-')[OFFSET(0)]], ' ') as PlaceID_Lookup
                                , ARRAY_TO_STRING( [
                                                    Name
                                                  , PrimaryAddressStreet
                                                  , PrimaryAddressCity
                                                  , PrimaryAddressState
                                                  , SPLIT(PrimaryAddressPostalCode, '-')[OFFSET(0)]], '||') as ExperianBIN_Lookup
                                , Name
                                , Name as input_name
                                , ARRAY_TO_STRING( [
                                                    PrimaryAddressStreet
                                                  , PrimaryAddressCity
                                                  , PrimaryAddressState
                                                  , SPLIT(PrimaryAddressPostalCode, '-')[OFFSET(0)]], ' ') as input_address
                                , PrimaryAddressStreet as PrimaryAddress_street
                                , PrimaryAddressCity as PrimaryAddress_city
                                , PrimaryAddressState as PrimaryAddress_state
                                , PrimaryAddressPostalCode as PrimaryAddress_postalCode
                                , PrimaryAddressCountry as PrimaryAddress_country
                                , LocationNumber as Location_Number
                                , PrimaryLocation as Primary_Location
                                , _DateCreated as data_land_date
                                , DATE( _DateCreated) as date_created 
                           FROM recent_records 
                          #WHERE record_order = 1
                          WHERE DATE(_DateCreated) = DATE("{{{{ ds }}}}")