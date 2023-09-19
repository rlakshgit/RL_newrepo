SELECT DISTINCT     J.*
               ,JP.* except(JewelerId,bq_load_date)
							 ,JS.StatusName as StatusName
							 ,pp.PromotionId as PromotionProspectPromotionId
							 ,pp.Id  as PlatinumPointsID
      ,pp.NameId as PromotionProspectNameId
      ,pp.FirstName as PromotionProspectFirstName
      ,pp.LastName as PromotionProspectLastName
      ,pp.State as PromotionProspectState
      ,pp.Email as PromotionProspectEmail
      ,pp.Phone as PromotionProspectPhone
      ,pp.ConfirmEMailOptIn as PromotionProspectConfirmEMailOptIn
      ,pp.AnnualPremium as PromotionProspectAnnualPremium
      ,pp.ItemValues as PromotionProspectItemValues
	  ,pp.SubmissionDateTime as PromotionProspectSubmissionDateTime
	  ,p.PromotionDesc as PromotionDesc
                             ,Address.State as State
							 ,Address.Street1 as Street1
							 ,Address.Street2 as Street2
							 ,Address.City as City
							 ,Address.Zip as Zip

                                  FROM  `{project}.{dataset}.jm_Jeweler_tb` AS J
                                  INNER JOIN `{project}.{dataset}.jm_JewelerPoints_tb` AS JP
                                      ON J.JewelerId = JP.JewelerId
                                      and DATE(J._PARTITIONTIME) = DATE("{{{{ ds }}}}") and DATE(JP._PARTITIONTIME) = DATE("{{{{ ds }}}}")
                                  INNER JOIN `{project}.{dataset}.jm_JewelerStatus_tb` AS JS
                                      ON J.StatusId = JS.StatusId
                                      and DATE( JS._PARTITIONTIME) = DATE("{{{{ ds }}}}")
                                  INNER JOIN `{project}.{dataset}.jm_PromotionProspect` AS PP
                                      ON JP.ProspectId = PP.Id
                                      and DATE(PP._PARTITIONTIME) = DATE("{{{{ ds }}}}")
                                  INNER JOIN `{project}.{dataset}.jm_Address_tb` AS Address
                                      ON J.AddressId = Address.AddressId
                                      and DATE(Address._PARTITIONTIME) = DATE("{{{{ ds }}}}")
								  INNER JOIN `{project}.{dataset}.jm_Promotion` as p
								         on pp.PromotionId  = p.PromotionId