-- Poplulate ConstructionCode Lookup Table    ---------------------------------------
--SET IDENTITY_INSERT [StatsRpt].[ConstructionCode] ON;
CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.ConstructionCode`
(
	ID  INT64, 
	BuildingConstructionDescription STRING, 
	NISSBOPStatCode STRING, 
	ISOBOPStatCode STRING
);
MERGE `{project}.{dest_dataset}.ConstructionCode` AS t
USING ( 
-- Original List
SELECT 2 as ID, 'Not Applicable' as BuildingConstructionDescription, '0' as NISSBOPStatCode, '00' as ISOBOPStatCode UNION ALL
SELECT 3, 'Frame', '1', '10' UNION ALL
SELECT 4, 'Joisted Masonry (reinforced)', '2', '20' UNION ALL
SELECT 5, 'Non-Combustible (light steel)', '3', '30' UNION ALL
SELECT 6, 'Masonry Non-Combustible (not reinforced)', '4', '40' UNION ALL
SELECT 7, 'Modified Fire Resistive', '5', '50' UNION ALL
SELECT 8, 'Fire Resistive', '6', '60' UNION ALL
-- Added R2 2014-02		
SELECT 9, 'Joisted Masonry', '2', '20' UNION ALL
SELECT 10, 'Non-Combustible', '3', '30' UNION ALL
SELECT 11, 'Masonry Non-Combustible', '4', '40' UNION ALL
SELECT 12, 'Masonry Non-Combustible (reinforced)', '4', '40' UNION ALL
SELECT 13, 'Fire Resistive (light steel/reinforced)', '6', '60' UNION ALL
-- Add R2 2014-08 US21353
SELECT 14, 'Masonry NonCombustible', '4', '40' 
) as s
--([ID], [BuildingConstructionDescription], [NISSBOPStatCode], [ISOBOPStatCode])
ON ( t.ID = s.ID )
WHEN MATCHED THEN UPDATE SET
    BuildingConstructionDescription = s.BuildingConstructionDescription,
    NISSBOPStatCode = s.NISSBOPStatCode,
    ISOBOPStatCode = s.ISOBOPStatCode
 WHEN NOT MATCHED BY TARGET THEN
    INSERT(ID, BuildingConstructionDescription, NISSBOPStatCode, ISOBOPStatCode)
    VALUES(s.ID, s.BuildingConstructionDescription, s.NISSBOPStatCode, s.ISOBOPStatCode)
WHEN NOT MATCHED BY SOURCE THEN DELETE; 
 
--PRINT 'ConstructionCode: ' + CAST(@@ROWCOUNT AS VARCHAR(100));
 
--SET IDENTITY_INSERT [StatsRpt].[ConstructionCode] OFF;  