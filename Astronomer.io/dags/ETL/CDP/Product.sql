/*  Products

20201113 Needs:
[ ] - PA products are just a shortcut, need to pull in a more reliable way

v2 - 
[X] Trimmed ItemClassDescription to account for " Loose stone" in the data

v3 -
[X]	Convert from DW source

*/
SELECT 
	JMProduct
	,ItemClassDescription
	,ItemClassGender
FROM (
	-- Personal Jewelry
	SELECT 
		'Personal Jewelry' AS JMProduct
		,NULLIF(LTRIM(RTRIM(REPLACE(REPLACE (pctl_classcodetype_jmic_pl.NAME, 'Gents ', ''), 'Ladies ', ''))), '?') AS ItemClassDescription
		,CASE WHEN pctl_classcodetype_jmic_pl.NAME LIKE 'Gents%' THEN 'Gents' 
			WHEN pctl_classcodetype_jmic_pl.NAME LIKE 'Ladies%' THEN 'Ladies' 
			WHEN pctl_classcodetype_jmic_pl.NAME IS NOT NULL THEN NULL END
			AS ItemClassGender
	FROM {prefix}{pc_dataset}.pcx_jewelryitem_jmic_pl AS pcx_jewelryitem_jmic_pl	
	LEFT JOIN {prefix}{pc_dataset}.pctl_classcodetype_jmic_pl AS pctl_classcodetype_jmic_pl
	ON pctl_classcodetype_jmic_pl.ID = pcx_jewelryitem_jmic_pl.ClassCodeType

	UNION ALL

	-- Personal Article  
	SELECT 
		'Personal Articles' AS JMProduct
		,LTRIM(RTRIM(pctl_jpaitemtype_jm.NAME)) AS ItemClassDescription
		,LTRIM(RTRIM(pctl_jpaitemgendertype_jm.NAME)) AS ItemClassGender
	FROM {prefix}{pc_dataset}.pcx_personalarticle_jm
	INNER JOIN {prefix}{pc_dataset}.pctl_personalarticle_jm
		ON pcx_personalarticle_jm.Subtype = pctl_personalarticle_jm.ID	
		AND pctl_Personalarticle_jm.TYPECODE = 'JewelryItem_JM'
		AND DATE(pcx_personalarticle_jm._PARTITIONTIME) = DATE('{date}')
	LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_jpaitemtype_jm --ITEM TYPE
		ON pcx_personalarticle_jm.ItemType = pctl_jpaitemtype_jm.ID
		AND DATE(pcx_personalarticle_jm._PARTITIONTIME) = DATE('{date}')
	LEFT OUTER JOIN {prefix}{pc_dataset}.pctl_jpaitemgendertype_jm --GENDER TYPE
		ON pcx_personalarticle_jm.ItemGenderType= pctl_jpaitemgendertype_jm.ID
		AND DATE(pcx_personalarticle_jm._PARTITIONTIME) = DATE('{date}')
	) Products
GROUP BY 
	JMProduct
	,ItemClassDescription
	,ItemClassGender
