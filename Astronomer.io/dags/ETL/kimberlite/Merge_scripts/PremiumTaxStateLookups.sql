--SELECT '(''' + [State] + ''',''' + [ASL] + ''',''' + [StateTaxType] + ''',' + CAST([TaxPercent] AS VARCHAR(10)) + ',' + CAST([SubjectPercent] AS VARCHAR(10)) + ',''' + [IncludeServiceCharge] + ''',''' + [IncludeDividend] + '''),'
--	FROM [bi_lookup].[PremiumTaxStateLookups] ORDER BY [State], [ASL], [StateTaxType]

CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.PremiumTaxStateLookups`
(
	State STRING, 
	ASL STRING, 
	StateTaxType STRING, 
	TaxPercent FLOAT64, 
	SubjectPercent FLOAT64, 
	IncludeServiceCharge STRING, 
	IncludeDividend STRING
);

MERGE INTO `{project}.{dest_dataset}.PremiumTaxStateLookups` AS Target 
USING (
		SELECT 'AL' as State,'051' as ASL,'General' as StateTaxType,4.00 as TaxPercent,100.00 as SubjectPercent,'YES' as IncludeServiceCharge,'NO' as IncludeDividend UNION ALL
		SELECT 'AL','052','General',1.00,100.00,'YES','NO' UNION ALL
		SELECT 'AL','090','General',4.00,100.00,'YES','NO' UNION ALL
		SELECT 'DE','051','General',0.00,100.00,'NO','NO' UNION ALL
		SELECT 'DE','052','General',0.00,0.00,'NO','NO' UNION ALL
		SELECT 'DE','090','General',0.00,100.00,'NO','NO' UNION ALL
		SELECT 'FL','051','FireFighters',1.85,70.00,'YES','NO' UNION ALL
		SELECT 'FL','051','Police',0.85,30.00,'YES','NO' UNION ALL
		SELECT 'FL','052','FireFighters',1.85,70.00,'YES','NO' UNION ALL
		SELECT 'FL','052','Police',0.85,30.00,'YES','NO' UNION ALL
		SELECT 'FL','090','FireFighters',1.85,0.00,'YES','NO' UNION ALL
		SELECT 'FL','090','Police',0.85,30.00,'YES','NO' UNION ALL
		SELECT 'IL','051','General',2.00,50.00,'NO','NO' UNION ALL
		SELECT 'IL','052','General',2.00,50.00,'NO','NO' UNION ALL
		SELECT 'IL','090','General',2.00,15.00,'NO','NO' UNION ALL
		SELECT 'KY','051','General',0.00,100.00,'NO','NO' UNION ALL
		SELECT 'KY','052','General',0.00,100.00,'NO','NO' UNION ALL
		SELECT 'KY','090','General',0.00,100.00,'NO','NO' UNION ALL
		SELECT 'LA','051','General',0.00,100.00,'YES','NO' UNION ALL
		SELECT 'LA','052','General',0.00,100.00,'YES','NO' UNION ALL
		SELECT 'LA','090','General',0.00,100.00,'YES','NO' UNION ALL
		SELECT 'MS','051','General',0.50,45.00,'YES','YES' UNION ALL
		SELECT 'MS','052','General',0.50,0.00,'YES','YES' UNION ALL
		SELECT 'MS','090','General',0.50,45.00,'YES','YES' UNION ALL
		SELECT 'NC','051','General',0.50,45.00,'NO','NO' UNION ALL
		SELECT 'NC','052','General',0.50,0.00,'NO','NO' UNION ALL
		SELECT 'NC','090','General',0.50,28.00,'NO','NO' UNION ALL
		SELECT 'ND','051','General',0.00,100.00,'NO','NO' UNION ALL
		SELECT 'ND','052','General',0.00,0.00,'NO','NO' UNION ALL
		SELECT 'ND','090','General',0.00,0.00,'NO','NO' UNION ALL
		SELECT 'NJ','051','General',2.00,45.00,'NO','YES' UNION ALL
		SELECT 'NJ','052','General',2.00,0.00,'NO','YES' UNION ALL
		SELECT 'NJ','090','General',2.00,0.00,'NO','YES' UNION ALL
		SELECT 'NY','051','General',2.00,100.00,'YES','YES' UNION ALL
		SELECT 'NY','052','General',2.00,0.00,'YES','YES' UNION ALL
		SELECT 'NY','090','General',2.00,100.00,'YES','YES' UNION ALL
		SELECT 'SC','051','General',2.00,41.00,'NO','YES' UNION ALL
		SELECT 'SC','052','General',2.00,0.00,'NO','YES' UNION ALL
		SELECT 'SC','090','General',2.00,20.00,'NO','YES'
	) 
AS Source -- ([State], [ASL], [StateTaxType], [TaxPercent], [SubjectPercent], [IncludeServiceCharge], [IncludeDividend]) 
ON Target.State = Source.State
	AND Target.ASL = Source.ASL
	AND Target.StateTaxType = Source.StateTaxType
WHEN MATCHED THEN UPDATE
SET TaxPercent = Source.TaxPercent
	,SubjectPercent = Source.SubjectPercent
	,IncludeServiceCharge = Source.IncludeServiceCharge
	,IncludeDividend = Source.IncludeDividend
WHEN NOT MATCHED BY TARGET THEN 
	INSERT (State, ASL, StateTaxType, TaxPercent, SubjectPercent, IncludeServiceCharge, IncludeDividend) 
	VALUES (State, ASL, StateTaxType, TaxPercent, SubjectPercent, IncludeServiceCharge, IncludeDividend) 
WHEN NOT MATCHED BY SOURCE THEN 
	DELETE;