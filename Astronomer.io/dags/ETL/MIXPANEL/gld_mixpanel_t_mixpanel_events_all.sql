SELECT  * EXCEPT(item_types,item_values,item_deductibles,acquisition_source),
	`{project}.custom_functions.fn_quoteSpaceClean`(item_types) as item_types,
	`{project}.custom_functions.fn_quoteSpaceClean`(item_values) as item_values,
	`{project}.custom_functions.fn_quoteSpaceClean`(item_deductibles) as item_deductibles,
	CASE 
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "JamesAllen" THEN "James Allen"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "jamesallen" THEN "James Allen"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "BlueNile" THEN "Blue Nile"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "bluenile" THEN "Blue Nile"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "allurez" THEN "Allurez"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "bestbrilliance" THEN "Best Brillance"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "apresjewelry" THEN "Apres Jewelry"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "asdgems" THEN "ASD Gems"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "adiamor" THEN "Adiamor"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "anyedesigns" THEN "Any EDesigns"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "briangavindiamonds" THEN "Brian Gavin Diamonds"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "crownandcaliber" THEN "Crown and Caliber"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "CrownandCaliber" THEN "Crown and Caliber"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "danielsjewelers" THEN "Daniels Jewelers"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "davidyurman" THEN "David Yurman"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "dercofinejewelers" THEN "Derco Fine Jewelers"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "forgejewelryworks" THEN "Forge Jewelry Works"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "gemjewel" THEN "Gem Jewel"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "gemprint" THEN "Gem Print"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "heartsonfire" THEN "Hearts On Fire"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "hemmingplazajewelers" THEN "Hemming Plaza Jewelers"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "heritageappraisers" THEN "Heritage Appraisers"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "hydepark" THEN "Hyde Park"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "josephschubachjewelers" THEN "Joseph Schubach Jewelers"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "kendanadesign" THEN "Kendana Design"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "link" THEN "LINK"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "loverly" THEN "Loverly"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "mydiamond" THEN "My Diamond"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "oliveave" THEN "Olive Ave"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "peterindorfdesigns" THEN "Peter Indorf Designs"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "pointnopointstudio" THEN "Point No Point Studio"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "prestigetime" THEN "Prestige Time"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "pricescope" THEN "Price Scope"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "princessbridediamonds" THEN "Princess Bride Diamonds"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "raleighdiamond" THEN "Raleigh Diamond"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "ringwraps" THEN "Ring Wraps"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "rockher" THEN "Rock Her"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "summitdiamondcorp" THEN "Summit Diamond Corp"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "whiteflash" THEN "White Flash"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "taylorandhart" THEN "Taylor And Hart"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "thatspecificcompanyinc" THEN "That Specific Company Inc"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "tiffanyco" THEN "Tiffany & Co"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "trumpethorn" THEN "Trumpet & Horn"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "verifymydiamond" THEN "Verify My Diamond"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "veleska" THEN "Veleska"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "vraioro" THEN "Vraioro"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "zoara" THEN "Zoara"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "zola" THEN "Zola"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) = "BrianGavinDiamonds" THEN "Brian Gavin Diamonds"
	  WHEN `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source) is Null THEN "None"
	  ELSE `{project}.custom_functions.fn_quoteSpaceClean`(acquisition_source)
	END as acquisition_source
	
FROM `{source_project_prefix}{base_dataset}.t_mixpanel_events_all` 
WHERE DATE(_PARTITIONTIME) = DATE("{{{{ ds }}}}")