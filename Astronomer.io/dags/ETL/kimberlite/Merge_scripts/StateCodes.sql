--   Populate StateCodes Lookup Table -----------------

--SELECT '(' + CAST(ID AS VARCHAR(10)) + ',''' + STATE + ''',''' + STATE_NAME + ''',''' + ISO_STATE_CODE + ''',''' + NISS_STATE_CODE + '''),' from dbo.t_StateCodes
-------------------------------------------------------
--SET IDENTITY_INSERT [StatsRpt].[StateCodes] ON;
    
CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.StateCodes`
(
	ID  INT64, 
	STATE STRING, 
	STATE_NAME STRING, 
	ISO_STATE_CODE STRING, 
	NISS_STATE_CODE STRING
);	
	
MERGE `{project}.{dest_dataset}.StateCodes` AS t
USING (  
SELECT 59 as ID, 'AE' as STATE, 'Military' as STATE_NAME, '0' as ISO_STATE_CODE, '0' as NISS_STATE_CODE UNION ALL
SELECT 60, 'AK', 'Alaska', '54', '54' UNION ALL
SELECT 61, 'AL', 'Alabama', '1', '1' UNION ALL
SELECT 62, 'AP', 'Military', '0', '0' UNION ALL
SELECT 63, 'AZ', 'Arizona', '2', '2' UNION ALL
SELECT 64, 'AR', 'Arkansas', '3', '3' UNION ALL
SELECT 65, 'CA', 'California', '4', '4' UNION ALL
SELECT 66, 'CO', 'Colorado', '5', '5' UNION ALL
SELECT 67, 'CT', 'Connecticut', '6', '6' UNION ALL
SELECT 68, 'DE', 'Delaware', '7', '7' UNION ALL
SELECT 69, 'DC', 'District of Columbia', '8', '8' UNION ALL
SELECT 70, 'FL', 'Florida', '9', '9' UNION ALL
SELECT 71, 'GA', 'Georgia', '10', '10' UNION ALL
SELECT 72, 'HI', 'Hawaii', '52', '52' UNION ALL
SELECT 73, 'ID', 'Idaho', '11', '11' UNION ALL
SELECT 74, 'IL', 'Illinois', '12', '12' UNION ALL
SELECT 75, 'IN', 'Indiana', '13', '13' UNION ALL
SELECT 76, 'IA', 'Iowa', '14', '14' UNION ALL
SELECT 77, 'KS', 'Kansas', '15', '15' UNION ALL
SELECT 78, 'KY', 'Kentucky', '16', '16' UNION ALL
SELECT 79, 'LA', 'Louisiana', '17', '17' UNION ALL
SELECT 80, 'ME', 'Maine', '18', '18' UNION ALL
SELECT 81, 'MD', 'Maryland', '19', '19' UNION ALL
SELECT 82, 'MA', 'Massachusetts', '20', '20' UNION ALL
SELECT 83, 'MI', 'Michigan', '21', '21' UNION ALL
SELECT 84, 'MN', 'Minnesota', '22', '22' UNION ALL
SELECT 85, 'MS', 'Mississippi', '23', '23' UNION ALL
SELECT 86, 'MO', 'Missouri', '24', '24' UNION ALL
SELECT 87, 'MT', 'Montana', '25', '25' UNION ALL
SELECT 88, 'NE', 'Nebraska', '26', '26' UNION ALL
SELECT 89, 'NV', 'Nevada', '27', '27' UNION ALL
SELECT 90, 'NH', 'New Hampshire', '28', '28' UNION ALL
SELECT 91, 'NJ', 'New Jersey', '29', '29' UNION ALL
SELECT 92, 'NM', 'New Mexico', '30', '30' UNION ALL
SELECT 93, 'NY', 'New York', '31', '31' UNION ALL
SELECT 94, 'NC', 'North Carolina', '32', '32' UNION ALL
SELECT 95, 'ND', 'North Dakota', '33', '33' UNION ALL
SELECT 96, 'OH', 'Ohio', '34', '34' UNION ALL
SELECT 97, 'OK', 'Oklahoma', '35', '35' UNION ALL
SELECT 98, 'OR', 'Oregon', '36', '36' UNION ALL
SELECT 99, 'PA', 'Pennsylvania', '37', '37' UNION ALL
SELECT 100, 'RI', 'Rhode Island', '38', '38' UNION ALL
SELECT 101, 'SC', 'South Carolina', '39', '39' UNION ALL
SELECT 102, 'SD', 'South Dakota', '40', '40' UNION ALL
SELECT 103, 'TN', 'Tennessee', '41', '41' UNION ALL
SELECT 104, 'TX', 'Texas', '42', '42' UNION ALL
SELECT 105, 'UT', 'Utah', '43', '43' UNION ALL
SELECT 106, 'VT', 'Vermont', '44', '44' UNION ALL
SELECT 107, 'VA', 'Virginia', '45', '45' UNION ALL
SELECT 108, 'WA', 'Washington', '46', '46' UNION ALL
SELECT 109, 'WV', 'West Virginia', '47', '47' UNION ALL
SELECT 110, 'WI', 'Wisconsin', '48', '48' UNION ALL
SELECT 111, 'WY', 'Wyoming', '49', '49' UNION ALL
SELECT 112, 'PR', 'Puerto Rico', '58', '58' UNION ALL
SELECT 113, 'VI', 'Virgin Islands', '0', '0' UNION ALL
SELECT 114, 'AS', 'Military', '0', '0'
) as s
--([ID], [STATE], [STATE_NAME], [ISO_STATE_CODE], [NISS_STATE_CODE])
ON ( t.ID = s.ID )
WHEN MATCHED THEN UPDATE SET
    STATE = s.STATE,
    STATE_NAME = s.STATE_NAME,
    ISO_STATE_CODE = s.ISO_STATE_CODE,
    NISS_STATE_CODE = s.NISS_STATE_CODE
 WHEN NOT MATCHED BY TARGET THEN
    INSERT(ID, STATE, STATE_NAME, ISO_STATE_CODE, NISS_STATE_CODE)
    VALUES(s.ID, s.STATE, s.STATE_NAME, s.ISO_STATE_CODE, s.NISS_STATE_CODE)
WHEN NOT MATCHED BY SOURCE THEN DELETE; 
 
--PRINT 'StateCodes: ' + CAST(@@ROWCOUNT AS VARCHAR(100));
 
--SET IDENTITY_INSERT [StatsRpt].[StateCodes] OFF;
    