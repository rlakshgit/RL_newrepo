
CREATE TABLE IF NOT EXISTS  `{project}.{dest_dataset}.NameValue`
(
	Category STRING,
	Name STRING,
	Value1 STRING,
	Value2 STRING,
	Value3 STRING,
	Value4 STRING,
	Value5 STRING,
	Sequence INT64,
	IsActive INT64,
	DateCreated TIMESTAMP,
	DateModified TIMESTAMP
);


MERGE INTO `{project}.{dest_dataset}.NameValue` AS Dest
USING (
		--This table is meant for generic use. It is not geico specific. only specific categories are.
		 
		--this section, however, is specific to Geico
		--				Code		Cat1   Type		    Name				Description style 1			 Description Style 2			Unused
		SELECT 'ProducerCode' as Category,'Z100D20' as Name,'GEICO' as Value1,'CallCenter' as Value2,'Virginia Beach'	as Value3,'Z100D20 - Virginia Beach'	as Value4,'Call Center - Virginia Beach' as Value5,1 as Sequence,1 as IsActive UNION ALL --Call Center
		SELECT 'ProducerCode','Z100D30','GEICO','CallCenter','Buffalo'			,'Z100D30 - Buffalo'		,'Call Center - Buffalo',2,1 UNION ALL --Call Center
		SELECT 'ProducerCode','Z100D40','GEICO','CallCenter','Fredericksburg'	,'Z100D40 - Fredericksburg'	,'Call Center - Fredericksburg',3,1 UNION ALL --Call Center

		SELECT 'ProducerCode','Z100D60','GEICO','Field Office','Field Office'				,'Z100D60 - Field Office'			,'Field Office',4,1 UNION ALL
		SELECT 'ProducerCode','Z100D61','GEICO','Field Office','Field Office'				,'Z100D61 - Field Office'			,'Field Office',5,1 UNION ALL
		SELECT 'ProducerCode','Z100D62','GEICO','Field Office','Field Office'				,'Z100D62 - Field Office'			,'Field Office',6,1 UNION ALL
		SELECT 'ProducerCode','Z100D63','GEICO','Field Office','Field Office'				,'Z100D63 - Field Office'			,'Field Office',7,1 UNION ALL
		SELECT 'ProducerCode','Z100D64','GEICO','Field Office','Field Office'				,'Z100D64 - Field Office'			,'Field Office',8,1 UNION ALL
		SELECT 'ProducerCode','Z100D65','GEICO','Field Office','Field Office'				,'Z100D65 - Field Office'			,'Field Office',9,1 UNION ALL
		SELECT 'ProducerCode','Z100D66','GEICO','Field Office','Field Office'				,'Z100D66 - Field Office'			,'Field Office',10,1 UNION ALL
		SELECT 'ProducerCode','Z100D67','GEICO','Field Office','Field Office'				,'Z100D67 - Field Office'			,'Field Office',11,1 UNION ALL
		SELECT 'ProducerCode','Z100D68','GEICO','Field Office','Field Office'				,'Z100D68 - Field Office'			,'Field Office',12,1 UNION ALL
		SELECT 'ProducerCode','Z100D69','GEICO','Field Office','Field Office'				,'Z100D69 - Field Office'			,'Field Office',13,1 UNION ALL
		SELECT 'ProducerCode','Z100D70','GEICO','Field Office','Field Office'				,'Z100D70 - Field Office'			,'Field Office',14,1 UNION ALL
		SELECT 'ProducerCode','Z100D71','GEICO','Field Office','Field Office'				,'Z100D71 - Field Office'			,'Field Office',15,1 UNION ALL
		SELECT 'ProducerCode','Z100D72','GEICO','Field Office','Field Office'				,'Z100D72 - Field Office'			,'Field Office',16,1 UNION ALL
		SELECT 'ProducerCode','Z100D73','GEICO','Field Office','Field Office'				,'Z100D73 - Field Office'			,'Field Office',17,1 UNION ALL
		SELECT 'ProducerCode','Z100D74','GEICO','Field Office','Field Office'				,'Z100D74 - Field Office'			,'Field Office',18,1 UNION ALL
		SELECT 'ProducerCode','Z100D75','GEICO','Field Office','Field Office'				,'Z100D75 - Field Office'			,'Field Office',19,1 UNION ALL
		SELECT 'ProducerCode','Z100D76','GEICO','Field Office','Field Office'				,'Z100D76 - Field Office'			,'Field Office',20,1 UNION ALL
		SELECT 'ProducerCode','Z100D77','GEICO','Field Office','Field Office'				,'Z100D77 - Field Office'			,'Field Office',21,1 UNION ALL

		SELECT 'ProducerCode','Z100D50','GEICO','WebCrossSell','Web Cross-Sell'				,''							,'Web - Cross Sell',22,1 UNION ALL

		SELECT 'ProducerCode','Z100D45','GEICO','CallCenter','Kansas City'	,'Z100D45 - Kansas City'	,'Call Center - Kansas City',23,1 UNION ALL --Call Center

		SELECT 'ProducerCode','Z100D78','GEICO','Field Office','Field Office'				,'Z100D78 - Field Office'			,'Field Office',24,1 UNION ALL
		SELECT 'ProducerCode','Z100D79','GEICO','Field Office','Field Office'				,'Z100D79 - Field Office'			,'Field Office',25,1 UNION ALL
		SELECT 'ProducerCode','Z100D80','GEICO','Field Office','Field Office'				,'Z100D80 - Field Office'			,'Field Office',26,1 UNION ALL
		SELECT 'ProducerCode','Z100D81','GEICO','Field Office','Field Office'				,'Z100D81 - Field Office'			,'Field Office',27,1 UNION ALL
		SELECT 'ProducerCode','Z100D82','GEICO','Field Office','Field Office'				,'Z100D82 - Field Office'			,'Field Office',28,1 UNION ALL
		SELECT 'ProducerCode','Z100D83','GEICO','Field Office','Field Office'				,'Z100D83 - Field Office'			,'Field Office',29,1 UNION ALL
		SELECT 'ProducerCode','Z100D84','GEICO','Field Office','Field Office'				,'Z100D84 - Field Office'			,'Field Office',30,1 UNION ALL
		SELECT 'ProducerCode','Z100D85','GEICO','Field Office','Field Office'				,'Z100D85 - Field Office'			,'Field Office',31,1 UNION ALL
		SELECT 'ProducerCode','Z100D86','GEICO','Field Office','Field Office'				,'Z100D86 - Field Office'			,'Field Office',32,1 UNION ALL
		SELECT 'ProducerCode','Z100D87','GEICO','Field Office','Field Office'				,'Z100D87 - Field Office'			,'Field Office',33,1 UNION ALL
		SELECT 'ProducerCode','Z100D88','GEICO','Field Office','Field Office'				,'Z100D88 - Field Office'			,'Field Office',34,1 UNION ALL
		SELECT 'ProducerCode','Z100D89','GEICO','Field Office','Field Office'				,'Z100D89 - Field Office'			,'Field Office',35,1 UNION ALL
		SELECT 'ProducerCode','Z100D90','GEICO','Field Office','Field Office'				,'Z100D90 - Field Office'			,'Field Office',36,1 UNION ALL
		SELECT 'ProducerCode','Z100D91','GEICO','Field Office','Field Office'				,'Z100D91 - Field Office'			,'Field Office',37,1 UNION ALL
		SELECT 'ProducerCode','Z100D92','GEICO','Field Office','Field Office'				,'Z100D92 - Field Office'			,'Field Office',38,1 UNION ALL
		SELECT 'ProducerCode','Z100D93','GEICO','Field Office','Field Office'				,'Z100D93 - Field Office'			,'Field Office',39,1 UNION ALL
		SELECT 'ProducerCode','Z100D94','GEICO','Field Office','Field Office'				,'Z100D94 - Field Office'			,'Field Office',40,1 UNION ALL
		SELECT 'ProducerCode','Z100D95','GEICO','Field Office','Field Office'				,'Z100D95 - Field Office'			,'Field Office',41,1 UNION ALL
		SELECT 'ProducerCode','Z100D96','GEICO','Field Office','Field Office'				,'Z100D96 - Field Office'			,'Field Office',42,1 UNION ALL
		SELECT 'ProducerCode','Z100D97','GEICO','Field Office','Field Office'				,'Z100D97 - Field Office'			,'Field Office',43,1 UNION ALL
		SELECT 'ProducerCode','Z100D98','GEICO','Field Office','Field Office'				,'Z100D98 - Field Office'			,'Field Office',44,1 UNION ALL
		SELECT 'ProducerCode','Z100D99','GEICO','Field Office','Field Office'				,'Z100D99 - Field Office'			,'Field Office',45,1 UNION ALL
		SELECT 'ProducerCode','Z100D100','GEICO','Field Office','Field Office'				,'Z100D100 - Field Office'			,'Field Office',46,1 UNION ALL
		SELECT 'ProducerCode','Z100D101','GEICO','Field Office','Field Office'				 ,'Z100D101 - Field Office'			 ,'Field Office',47,1 UNION ALL
		SELECT 'ProducerCode','Z100D102','GEICO','Field Office','Field Office'				 ,'Z100D102 - Field Office'			 ,'Field Office',48,1 UNION ALL
		SELECT 'ProducerCode','Z100D103','GEICO','Field Office','Field Office'				 ,'Z100D103 - Field Office'			 ,'Field Office',49,1 UNION ALL
		SELECT 'ProducerCode','Z100D104','GEICO','Field Office','Field Office'				 ,'Z100D104 - Field Office'			 ,'Field Office',50,1 UNION ALL
		SELECT 'ProducerCode','Z100D105','GEICO','Field Office','Field Office'				 ,'Z100D105 - Field Office'			 ,'Field Office',51,1 UNION ALL
		SELECT 'ProducerCode','Z100D106','GEICO','Field Office','Field Office'				 ,'Z100D106 - Field Office'			 ,'Field Office',52,1 UNION ALL
		SELECT 'ProducerCode','Z100D107','GEICO','Field Office','Field Office'				 ,'Z100D107 - Field Office'			 ,'Field Office',53,1 UNION ALL
		SELECT 'ProducerCode','Z100D108','GEICO','Field Office','Field Office'				 ,'Z100D108 - Field Office'			 ,'Field Office',54,1 UNION ALL
		SELECT 'ProducerCode','Z100D109','GEICO','Field Office','Field Office'				 ,'Z100D109 - Field Office'			 ,'Field Office',55,1 UNION ALL
		SELECT 'ProducerCode','Z100D110','GEICO','Field Office','Field Office'				 ,'Z100D110 - Field Office'			 ,'Field Office',56,1 UNION ALL
		SELECT 'ProducerCode','Z100D111','GEICO','Field Office','Field Office'				 ,'Z100D111 - Field Office'			 ,'Field Office',57,1 UNION ALL
		SELECT 'ProducerCode','Z100D112','GEICO','Field Office','Field Office'				 ,'Z100D112 - Field Office'			 ,'Field Office',58,1 UNION ALL
		SELECT 'ProducerCode','Z100D113','GEICO','Field Office','Field Office'				 ,'Z100D113 - Field Office'			 ,'Field Office',59,1 UNION ALL
		SELECT 'ProducerCode','Z100D114','GEICO','Field Office','Field Office'				 ,'Z100D114 - Field Office'			 ,'Field Office',60,1 UNION ALL
		SELECT 'ProducerCode','Z100D115','GEICO','Field Office','Field Office'				 ,'Z100D115 - Field Office'			 ,'Field Office',61,1 UNION ALL
		SELECT 'ProducerCode','Z100D116','GEICO','Field Office','Field Office'				 ,'Z100D116 - Field Office'			 ,'Field Office',62,1 UNION ALL
		SELECT 'ProducerCode','Z100D117','GEICO','Field Office','Field Office'				 ,'Z100D117 - Field Office'			 ,'Field Office',63,1 UNION ALL
		SELECT 'ProducerCode','Z100D118','GEICO','Field Office','Field Office'				 ,'Z100D118 - Field Office'			 ,'Field Office',64,1 UNION ALL
		SELECT 'ProducerCode','Z100D119','GEICO','Field Office','Field Office'				 ,'Z100D119 - Field Office'			 ,'Field Office',65,1 UNION ALL
		SELECT 'ProducerCode','Z100D120','GEICO','Field Office','Field Office'				 ,'Z100D120 - Field Office'			 ,'Field Office',66,1 UNION ALL
		SELECT 'ProducerCode','Z100D121','GEICO','Field Office','Field Office'				 ,'Z100D121 - Field Office'			 ,'Field Office',67,1 UNION ALL
		SELECT 'ProducerCode','Z100D122','GEICO','Field Office','Field Office'				 ,'Z100D122 - Field Office'			 ,'Field Office',68,1 UNION ALL
		SELECT 'ProducerCode','Z100D123','GEICO','Field Office','Field Office'				 ,'Z100D123 - Field Office'			 ,'Field Office',69,1 UNION ALL
		SELECT 'ProducerCode','Z100D124','GEICO','Field Office','Field Office'				 ,'Z100D124 - Field Office'			 ,'Field Office',70,1 UNION ALL
		SELECT 'ProducerCode','Z100D125','GEICO','Field Office','Field Office'				 ,'Z100D125 - Field Office'			 ,'Field Office',71,1 UNION ALL
		SELECT 'ProducerCode','Z100D126','GEICO','Field Office','Field Office'				 ,'Z100D126 - Field Office'			 ,'Field Office',72,1 UNION ALL
		SELECT 'ProducerCode','Z100D127','GEICO','Field Office','Field Office'				 ,'Z100D127 - Field Office'			 ,'Field Office',73,1 UNION ALL
		SELECT 'ProducerCode','Z100D128','GEICO','Field Office','Field Office'				 ,'Z100D128 - Field Office'			 ,'Field Office',74,1 UNION ALL
		SELECT 'ProducerCode','Z100D129','GEICO','Field Office','Field Office'				 ,'Z100D129 - Field Office'			 ,'Field Office',75,1 UNION ALL
		SELECT 'ProducerCode','Z100D130','GEICO','Field Office','Field Office'				 ,'Z100D130 - Field Office'			 ,'Field Office',76,1 UNION ALL
		SELECT 'ProducerCode','Z100D131','GEICO','Field Office','Field Office'				 ,'Z100D131 - Field Office'			 ,'Field Office',77,1 UNION ALL
		SELECT 'ProducerCode','Z100D132','GEICO','Field Office','Field Office'				 ,'Z100D132 - Field Office'			 ,'Field Office',78,1 UNION ALL
		SELECT 'ProducerCode','Z100D133','GEICO','Field Office','Field Office'				 ,'Z100D133 - Field Office'			 ,'Field Office',79,1 UNION ALL
		SELECT 'ProducerCode','Z100D134','GEICO','Field Office','Field Office'				 ,'Z100D134 - Field Office'			 ,'Field Office',80,1 UNION ALL
		SELECT 'ProducerCode','Z100D135','GEICO','Field Office','Field Office'				 ,'Z100D135 - Field Office'			 ,'Field Office',81,1 UNION ALL
		SELECT 'ProducerCode','Z100D136','GEICO','Field Office','Field Office'				 ,'Z100D136 - Field Office'			 ,'Field Office',82,1 UNION ALL
		SELECT 'ProducerCode','Z100D137','GEICO','Field Office','Field Office'				 ,'Z100D137 - Field Office'			 ,'Field Office',83,1 UNION ALL
		SELECT 'ProducerCode','Z100D138','GEICO','Field Office','Field Office'				 ,'Z100D138 - Field Office'			 ,'Field Office',84,1 UNION ALL
		SELECT 'ProducerCode','Z100D139','GEICO','Field Office','Field Office'				 ,'Z100D139 - Field Office'			 ,'Field Office',85,1 UNION ALL
		SELECT 'ProducerCode','Z100D140','GEICO','Field Office','Field Office'				 ,'Z100D140 - Field Office'			 ,'Field Office',86,1 UNION ALL
		SELECT 'ProducerCode','Z100D141','GEICO','Field Office','Field Office'				 ,'Z100D141 - Field Office'			 ,'Field Office',87,1 UNION ALL
		SELECT 'ProducerCode','Z100D142','GEICO','Field Office','Field Office'				 ,'Z100D142 - Field Office'			 ,'Field Office',88,1 UNION ALL
		SELECT 'ProducerCode','Z100D143','GEICO','Field Office','Field Office'				 ,'Z100D143 - Field Office'			 ,'Field Office',89,1 UNION ALL
		SELECT 'ProducerCode','Z100D144','GEICO','Field Office','Field Office'				 ,'Z100D144 - Field Office'			 ,'Field Office',90,1 UNION ALL
		SELECT 'ProducerCode','Z100D145','GEICO','Field Office','Field Office'				 ,'Z100D145 - Field Office'			 ,'Field Office',91,1 UNION ALL
		SELECT 'ProducerCode','Z100D146','GEICO','Field Office','Field Office'				 ,'Z100D146 - Field Office'			 ,'Field Office',92,1 UNION ALL
		SELECT 'ProducerCode','Z100D147','GEICO','Field Office','Field Office'				 ,'Z100D147 - Field Office'			 ,'Field Office',93,1 UNION ALL
		SELECT 'ProducerCode','Z100D148','GEICO','Field Office','Field Office'				 ,'Z100D148 - Field Office'			 ,'Field Office',94,1 UNION ALL
		SELECT 'ProducerCode','Z100D149','GEICO','Field Office','Field Office'				 ,'Z100D149 - Field Office'			 ,'Field Office',95,1 UNION ALL
		SELECT 'ProducerCode','Z100D150','GEICO','Field Office','Field Office'				 ,'Z100D150 - Field Office'			 ,'Field Office',96,1 UNION ALL
		SELECT 'ProducerCode','Z100D151','GEICO','Field Office','Field Office'				 ,'Z100D151 - Field Office'			 ,'Field Office',97,1 UNION ALL
		SELECT 'ProducerCode','Z100D152','GEICO','Field Office','Field Office'				 ,'Z100D152 - Field Office'			 ,'Field Office',98,1 UNION ALL
		SELECT 'ProducerCode','Z100D153','GEICO','Field Office','Field Office'				 ,'Z100D153 - Field Office'			 ,'Field Office',99,1 UNION ALL
		SELECT 'ProducerCode','Z100D154','GEICO','Field Office','Field Office'				 ,'Z100D154 - Field Office'			 ,'Field Office',100,1 UNION ALL
		SELECT 'ProducerCode','Z100D155','GEICO','Field Office','Field Office'				 ,'Z100D155 - Field Office'			 ,'Field Office',101,1 UNION ALL
		SELECT 'ProducerCode','Z100D156','GEICO','Field Office','Field Office'				 ,'Z100D156 - Field Office'			 ,'Field Office',102,1 UNION ALL
		SELECT 'ProducerCode','Z100D157','GEICO','Field Office','Field Office'				 ,'Z100D157 - Field Office'			 ,'Field Office',103,1 UNION ALL
		SELECT 'ProducerCode','Z100D158','GEICO','Field Office','Field Office'				 ,'Z100D158 - Field Office'			 ,'Field Office',104,1 UNION ALL
		SELECT 'ProducerCode','Z100D159','GEICO','Field Office','Field Office'				 ,'Z100D159 - Field Office'			 ,'Field Office',105,1 UNION ALL
		SELECT 'ProducerCode','Z100D160','GEICO','Field Office','Field Office'				 ,'Z100D160 - Field Office'			 ,'Field Office',106,1 UNION ALL
		SELECT 'ProducerCode','Z100D161','GEICO','Field Office','Field Office'				 ,'Z100D161 - Field Office'			 ,'Field Office',107,1 UNION ALL
		SELECT 'ProducerCode','Z100D162','GEICO','Field Office','Field Office'				 ,'Z100D162 - Field Office'			 ,'Field Office',108,1 UNION ALL
		SELECT 'ProducerCode','Z100D163','GEICO','Field Office','Field Office'				 ,'Z100D163 - Field Office'			 ,'Field Office',109,1 UNION ALL
		SELECT 'ProducerCode','Z100D164','GEICO','Field Office','Field Office'				 ,'Z100D164 - Field Office'			 ,'Field Office',110,1 UNION ALL
		SELECT 'ProducerCode','Z100D165','GEICO','Field Office','Field Office'				 ,'Z100D165 - Field Office'			 ,'Field Office',111,1 UNION ALL
		SELECT 'ProducerCode','Z100D166','GEICO','Field Office','Field Office'				 ,'Z100D166 - Field Office'			 ,'Field Office',112,1 UNION ALL
		SELECT 'ProducerCode','Z100D167','GEICO','Field Office','Field Office'				 ,'Z100D167 - Field Office'			 ,'Field Office',113,1 UNION ALL
		SELECT 'ProducerCode','Z100D168','GEICO','Field Office','Field Office'				 ,'Z100D168 - Field Office'			 ,'Field Office',114,1 UNION ALL
		SELECT 'ProducerCode','Z100D169','GEICO','Field Office','Field Office'				 ,'Z100D169 - Field Office'			 ,'Field Office',115,1 UNION ALL
		SELECT 'ProducerCode','Z100D170','GEICO','Field Office','Field Office'				 ,'Z100D170 - Field Office'			 ,'Field Office',116,1 UNION ALL
		SELECT 'ProducerCode','Z100D171','GEICO','Field Office','Field Office'				 ,'Z100D171 - Field Office'			 ,'Field Office',117,1 UNION ALL
		SELECT 'ProducerCode','Z100D172','GEICO','Field Office','Field Office'				 ,'Z100D172 - Field Office'			 ,'Field Office',118,1 UNION ALL
		SELECT 'ProducerCode','Z100D173','GEICO','Field Office','Field Office'				 ,'Z100D173 - Field Office'			 ,'Field Office',119,1 UNION ALL
		SELECT 'ProducerCode','Z100D174','GEICO','Field Office','Field Office'				 ,'Z100D174 - Field Office'			 ,'Field Office',120,1 UNION ALL
		SELECT 'ProducerCode','Z100D175','GEICO','Field Office','Field Office'				 ,'Z100D175 - Field Office'			 ,'Field Office',121,1 UNION ALL
		SELECT 'ProducerCode','Z100D176','GEICO','Field Office','Field Office'				 ,'Z100D176 - Field Office'			 ,'Field Office',122,1 UNION ALL
		SELECT 'ProducerCode','Z100D177','GEICO','Field Office','Field Office'				 ,'Z100D177 - Field Office'			 ,'Field Office',123,1 UNION ALL
		SELECT 'ProducerCode','Z100D178','GEICO','Field Office','Field Office'				 ,'Z100D178 - Field Office'			 ,'Field Office',124,1 UNION ALL
		SELECT 'ProducerCode','Z100D179','GEICO','Field Office','Field Office'				 ,'Z100D179 - Field Office'			 ,'Field Office',125,1 UNION ALL
		SELECT 'ProducerCode','Z100D180','GEICO','Field Office','Field Office'				 ,'Z100D180 - Field Office'			 ,'Field Office',126,1 UNION ALL
		SELECT 'ProducerCode','Z100D181','GEICO','Field Office','Field Office'				 ,'Z100D181 - Field Office'			 ,'Field Office',127,1 UNION ALL
		SELECT 'ProducerCode','Z100D182','GEICO','Field Office','Field Office'				 ,'Z100D182 - Field Office'			 ,'Field Office',128,1 UNION ALL
		SELECT 'ProducerCode','Z100D183','GEICO','Field Office','Field Office'				 ,'Z100D183 - Field Office'			 ,'Field Office',129,1 UNION ALL
		SELECT 'ProducerCode','Z100D184','GEICO','Field Office','Field Office'				 ,'Z100D184 - Field Office'			 ,'Field Office',130,1 UNION ALL
		SELECT 'ProducerCode','Z100D185','GEICO','Field Office','Field Office'				 ,'Z100D185 - Field Office'			 ,'Field Office',131,1 UNION ALL
		SELECT 'ProducerCode','Z100D186','GEICO','Field Office','Field Office'				 ,'Z100D186 - Field Office'			 ,'Field Office',132,1 UNION ALL
		SELECT 'ProducerCode','Z100D187','GEICO','Field Office','Field Office'				 ,'Z100D187 - Field Office'			 ,'Field Office',133,1 UNION ALL
		SELECT 'ProducerCode','Z100D188','GEICO','Field Office','Field Office'				 ,'Z100D188 - Field Office'			 ,'Field Office',134,1 UNION ALL
		SELECT 'ProducerCode','Z100D189','GEICO','Field Office','Field Office'				 ,'Z100D189 - Field Office'			 ,'Field Office',135,1 UNION ALL
		SELECT 'ProducerCode','Z100D190','GEICO','Field Office','Field Office'				 ,'Z100D190 - Field Office'			 ,'Field Office',136,1 UNION ALL
		SELECT 'ProducerCode','Z100D191','GEICO','Field Office','Field Office'				 ,'Z100D191 - Field Office'			 ,'Field Office',137,1 UNION ALL
		SELECT 'ProducerCode','Z100D192','GEICO','Field Office','Field Office'				 ,'Z100D192 - Field Office'			 ,'Field Office',138,1 UNION ALL
		SELECT 'ProducerCode','Z100D193','GEICO','Field Office','Field Office'				 ,'Z100D193 - Field Office'			 ,'Field Office',139,1 UNION ALL
		SELECT 'ProducerCode','Z100D194','GEICO','Field Office','Field Office'				 ,'Z100D194 - Field Office'			 ,'Field Office',140,1 UNION ALL
		SELECT 'ProducerCode','Z100D195','GEICO','Field Office','Field Office'				 ,'Z100D195 - Field Office'			 ,'Field Office',141,1 UNION ALL
		SELECT 'ProducerCode','Z100D196','GEICO','Field Office','Field Office'				 ,'Z100D196 - Field Office'			 ,'Field Office',142,1 UNION ALL
		SELECT 'ProducerCode','Z100D197','GEICO','Field Office','Field Office'				 ,'Z100D197 - Field Office'			 ,'Field Office',143,1 UNION ALL
		SELECT 'ProducerCode','Z100D198','GEICO','Field Office','Field Office'				 ,'Z100D198 - Field Office'			 ,'Field Office',144,1 UNION ALL
		SELECT 'ProducerCode','Z100D199','GEICO','Field Office','Field Office'				 ,'Z100D199 - Field Office'			 ,'Field Office',145,1 UNION ALL
		SELECT 'ProducerCode','Z100D200','GEICO','Field Office','Field Office'				 ,'Z100D200 - Field Office'			 ,'Field Office',146,1 UNION ALL
		SELECT 'ProducerCode','Z100D201','GEICO','Field Office','Field Office'				 ,'Z100D201 - Field Office'			 ,'Field Office',147,1 UNION ALL
		SELECT 'ProducerCode','Z100D202','GEICO','Field Office','Field Office'				 ,'Z100D202 - Field Office'			 ,'Field Office',148,1 UNION ALL
		SELECT 'ProducerCode','Z100D203','GEICO','Field Office','Field Office'				 ,'Z100D203 - Field Office'			 ,'Field Office',149,1 UNION ALL
		SELECT 'ProducerCode','Z100D204','GEICO','Field Office','Field Office'				 ,'Z100D204 - Field Office'			 ,'Field Office',150,1 UNION ALL
		SELECT 'ProducerCode','Z100D205','GEICO','Field Office','Field Office'				 ,'Z100D205 - Field Office'			 ,'Field Office',151,1 UNION ALL
		SELECT 'ProducerCode','Z100D206','GEICO','Field Office','Field Office'				 ,'Z100D206 - Field Office'			 ,'Field Office',152,1 UNION ALL
		SELECT 'ProducerCode','Z100D207','GEICO','Field Office','Field Office'				 ,'Z100D207 - Field Office'			 ,'Field Office',153,1 UNION ALL
		SELECT 'ProducerCode','Z100D208','GEICO','Field Office','Field Office'				 ,'Z100D208 - Field Office'			 ,'Field Office',154,1 UNION ALL
		SELECT 'ProducerCode','Z100D209','GEICO','Field Office','Field Office'				 ,'Z100D209 - Field Office'			 ,'Field Office',155,1 UNION ALL
		SELECT 'ProducerCode','Z100D210','GEICO','Field Office','Field Office'				 ,'Z100D210 - Field Office'			 ,'Field Office',156,1 UNION ALL
		SELECT 'ProducerCode','Z100D211','GEICO','Field Office','Field Office'				 ,'Z100D211 - Field Office'			 ,'Field Office',157,1 UNION ALL
		SELECT 'ProducerCode','Z100D212','GEICO','Field Office','Field Office'				 ,'Z100D212 - Field Office'			 ,'Field Office',158,1 UNION ALL
		SELECT 'ProducerCode','Z100D213','GEICO','Field Office','Field Office'				 ,'Z100D213 - Field Office'			 ,'Field Office',159,1 UNION ALL
		SELECT 'ProducerCode','Z100D214','GEICO','Field Office','Field Office'				 ,'Z100D214 - Field Office'			 ,'Field Office',160,1 UNION ALL
		SELECT 'ProducerCode','Z100D215','GEICO','Field Office','Field Office'				 ,'Z100D215 - Field Office'			 ,'Field Office',161,1 UNION ALL
		SELECT 'ProducerCode','Z100D216','GEICO','Field Office','Field Office'				 ,'Z100D216 - Field Office'			 ,'Field Office',162,1 UNION ALL
		SELECT 'ProducerCode','Z100D217','GEICO','Field Office','Field Office'				 ,'Z100D217 - Field Office'			 ,'Field Office',163,1 UNION ALL
		SELECT 'ProducerCode','Z100D218','GEICO','Field Office','Field Office'				 ,'Z100D218 - Field Office'			 ,'Field Office',164,1 UNION ALL
		SELECT 'ProducerCode','Z100D219','GEICO','Field Office','Field Office'				 ,'Z100D219 - Field Office'			 ,'Field Office',165,1 UNION ALL
		SELECT 'ProducerCode','Z100D220','GEICO','Field Office','Field Office'				 ,'Z100D220 - Field Office'			 ,'Field Office',166,1 UNION ALL
		SELECT 'ProducerCode','Z100D221','GEICO','Field Office','Field Office'				 ,'Z100D221 - Field Office'			 ,'Field Office',167,1 UNION ALL
		SELECT 'ProducerCode','Z100D222','GEICO','Field Office','Field Office'				 ,'Z100D222 - Field Office'			 ,'Field Office',168,1 UNION ALL
		SELECT 'ProducerCode','Z100D223','GEICO','Field Office','Field Office'				 ,'Z100D223 - Field Office'			 ,'Field Office',169,1 UNION ALL
		SELECT 'ProducerCode','Z100D224','GEICO','Field Office','Field Office'				 ,'Z100D224 - Field Office'			 ,'Field Office',170,1 UNION ALL
		SELECT 'ProducerCode','Z100D225','GEICO','Field Office','Field Office'				 ,'Z100D225 - Field Office'			 ,'Field Office',171,1 UNION ALL
		SELECT 'ProducerCode','Z100D226','GEICO','Field Office','Field Office'				 ,'Z100D226 - Field Office'			 ,'Field Office',172,1 UNION ALL
		SELECT 'ProducerCode','Z100D227','GEICO','Field Office','Field Office'				 ,'Z100D227 - Field Office'			 ,'Field Office',173,1 UNION ALL
		SELECT 'ProducerCode','Z100D228','GEICO','Field Office','Field Office'				 ,'Z100D228 - Field Office'			 ,'Field Office',174,1 UNION ALL
		SELECT 'ProducerCode','Z100D229','GEICO','Field Office','Field Office'				 ,'Z100D229 - Field Office'			 ,'Field Office',175,1 UNION ALL
		SELECT 'ProducerCode','Z100D230','GEICO','Field Office','Field Office'				 ,'Z100D230 - Field Office'			 ,'Field Office',176,1 UNION ALL
		SELECT 'ProducerCode','Z100D231','GEICO','Field Office','Field Office'				 ,'Z100D231 - Field Office'			 ,'Field Office',177,1 UNION ALL
		SELECT 'ProducerCode','Z100D232','GEICO','Field Office','Field Office'				 ,'Z100D232 - Field Office'			 ,'Field Office',178,1 UNION ALL
		SELECT 'ProducerCode','Z100D233','GEICO','Field Office','Field Office'				 ,'Z100D233 - Field Office'			 ,'Field Office',179,1 UNION ALL
		SELECT 'ProducerCode','Z100D234','GEICO','Field Office','Field Office'				 ,'Z100D234 - Field Office'			 ,'Field Office',180,1 UNION ALL
		SELECT 'ProducerCode','Z100D235','GEICO','Field Office','Field Office'				 ,'Z100D235 - Field Office'			 ,'Field Office',181,1 UNION ALL
		SELECT 'ProducerCode','Z100D236','GEICO','Field Office','Field Office'				 ,'Z100D236 - Field Office'			 ,'Field Office',182,1 UNION ALL
		SELECT 'ProducerCode','Z100D237','GEICO','Field Office','Field Office'				 ,'Z100D237 - Field Office'			 ,'Field Office',183,1 UNION ALL
		SELECT 'ProducerCode','Z100D238','GEICO','Field Office','Field Office'				 ,'Z100D238 - Field Office'			 ,'Field Office',184,1 UNION ALL
		SELECT 'ProducerCode','Z100D239','GEICO','Field Office','Field Office'				 ,'Z100D239 - Field Office'			 ,'Field Office',185,1 UNION ALL
		SELECT 'ProducerCode','Z100D240','GEICO','Field Office','Field Office'				 ,'Z100D240 - Field Office'			 ,'Field Office',186,1 UNION ALL
		SELECT 'ProducerCode','Z100D241','GEICO','Field Office','Field Office'				 ,'Z100D241 - Field Office'			 ,'Field Office',187,1 UNION ALL
		SELECT 'ProducerCode','Z100D242','GEICO','Field Office','Field Office'				 ,'Z100D242 - Field Office'			 ,'Field Office',188,1 UNION ALL
		SELECT 'ProducerCode','Z100D243','GEICO','Field Office','Field Office'				 ,'Z100D243 - Field Office'			 ,'Field Office',189,1 UNION ALL
		SELECT 'ProducerCode','Z100D244','GEICO','Field Office','Field Office'				 ,'Z100D244 - Field Office'			 ,'Field Office',190,1 UNION ALL
		SELECT 'ProducerCode','Z100D245','GEICO','Field Office','Field Office'				 ,'Z100D245 - Field Office'			 ,'Field Office',191,1 UNION ALL
		SELECT 'ProducerCode','Z100D246','GEICO','Field Office','Field Office'				 ,'Z100D246 - Field Office'			 ,'Field Office',192,1 UNION ALL
		SELECT 'ProducerCode','Z100D247','GEICO','Field Office','Field Office'				 ,'Z100D247 - Field Office'			 ,'Field Office',193,1 UNION ALL
		SELECT 'ProducerCode','Z100D248','GEICO','Field Office','Field Office'				 ,'Z100D248 - Field Office'			 ,'Field Office',194,1 UNION ALL
		SELECT 'ProducerCode','Z100D249','GEICO','Field Office','Field Office'				 ,'Z100D249 - Field Office'			 ,'Field Office',195,1 UNION ALL
		SELECT 'ProducerCode','Z100D250','GEICO','Field Office','Field Office'				 ,'Z100D250 - Field Office'			 ,'Field Office',196,1 UNION ALL
		SELECT 'ProducerCode','Z100D251','GEICO','Field Office','Field Office'				 ,'Z100D251 - Field Office'			 ,'Field Office',197,1 UNION ALL
		SELECT 'ProducerCode','Z100D252','GEICO','Field Office','Field Office'				 ,'Z100D252 - Field Office'			 ,'Field Office',198,1 UNION ALL
		SELECT 'ProducerCode','Z100D253','GEICO','Field Office','Field Office'				 ,'Z100D253 - Field Office'			 ,'Field Office',199,1 UNION ALL
		SELECT 'ProducerCode','Z100D254','GEICO','Field Office','Field Office'				 ,'Z100D254 - Field Office'			 ,'Field Office',200,1 UNION ALL
		SELECT 'ProducerCode','Z100D255','GEICO','Field Office','Field Office'				 ,'Z100D255 - Field Office'			 ,'Field Office',201,1 UNION ALL
		SELECT 'ProducerCode','Z100D256','GEICO','Field Office','Field Office'				 ,'Z100D256 - Field Office'			 ,'Field Office',202,1 UNION ALL
		SELECT 'ProducerCode','Z100D257','GEICO','Field Office','Field Office'				 ,'Z100D257 - Field Office'			 ,'Field Office',203,1 UNION ALL
		SELECT 'ProducerCode','Z100D258','GEICO','Field Office','Field Office'				 ,'Z100D258 - Field Office'			 ,'Field Office',204,1 UNION ALL
		SELECT 'ProducerCode','Z100D259','GEICO','Field Office','Field Office'				 ,'Z100D259 - Field Office'			 ,'Field Office',205,1 UNION ALL
		SELECT 'ProducerCode','Z100D260','GEICO','Field Office','Field Office'				 ,'Z100D260 - Field Office'			 ,'Field Office',206,1 UNION ALL
		SELECT 'ProducerCode','Z100D261','GEICO','Field Office','Field Office'				 ,'Z100D261 - Field Office'			 ,'Field Office',207,1 UNION ALL
		SELECT 'ProducerCode','Z100D262','GEICO','Field Office','Field Office'				 ,'Z100D262 - Field Office'			 ,'Field Office',208,1 UNION ALL
		SELECT 'ProducerCode','Z100D263','GEICO','Field Office','Field Office'				 ,'Z100D263 - Field Office'			 ,'Field Office',209,1 UNION ALL
		SELECT 'ProducerCode','Z100D264','GEICO','Field Office','Field Office'				 ,'Z100D264 - Field Office'			 ,'Field Office',210,1 UNION ALL
		SELECT 'ProducerCode','Z100D265','GEICO','Field Office','Field Office'				 ,'Z100D265 - Field Office'			 ,'Field Office',211,1 UNION ALL
		SELECT 'ProducerCode','Z100D266','GEICO','Field Office','Field Office'				 ,'Z100D266 - Field Office'			 ,'Field Office',212,1 UNION ALL
		SELECT 'ProducerCode','Z100D267','GEICO','Field Office','Field Office'				 ,'Z100D267 - Field Office'			 ,'Field Office',213,1 UNION ALL
		SELECT 'ProducerCode','Z100D268','GEICO','Field Office','Field Office'				 ,'Z100D268 - Field Office'			 ,'Field Office',214,1 UNION ALL
		SELECT 'ProducerCode','Z100D269','GEICO','Field Office','Field Office'				 ,'Z100D269 - Field Office'			 ,'Field Office',215,1 UNION ALL
		SELECT 'ProducerCode','Z100D270','GEICO','Field Office','Field Office'				 ,'Z100D270 - Field Office'			 ,'Field Office',216,1 UNION ALL
		SELECT 'ProducerCode','Z100D271','GEICO','Field Office','Field Office'				 ,'Z100D271 - Field Office'			 ,'Field Office',217,1 UNION ALL
		SELECT 'ProducerCode','Z100D272','GEICO','Field Office','Field Office'				 ,'Z100D272 - Field Office'			 ,'Field Office',218,1 UNION ALL
		SELECT 'ProducerCode','Z100D273','GEICO','Field Office','Field Office'				 ,'Z100D273 - Field Office'			 ,'Field Office',219,1 UNION ALL
		SELECT 'ProducerCode','Z100D274','GEICO','Field Office','Field Office'				 ,'Z100D274 - Field Office'			 ,'Field Office',220,1 UNION ALL
		SELECT 'ProducerCode','Z100D275','GEICO','Field Office','Field Office'				 ,'Z100D275 - Field Office'			 ,'Field Office',221,1 UNION ALL
		SELECT 'ProducerCode','Z100D276','GEICO','Field Office','Field Office'				 ,'Z100D276 - Field Office'			 ,'Field Office',222,1 UNION ALL
		SELECT 'ProducerCode','Z100D277','GEICO','Field Office','Field Office'				 ,'Z100D277 - Field Office'			 ,'Field Office',223,1 UNION ALL
		SELECT 'ProducerCode','Z100D278','GEICO','Field Office','Field Office'				 ,'Z100D278 - Field Office'			 ,'Field Office',224,1 UNION ALL
		SELECT 'ProducerCode','Z100D279','GEICO','Field Office','Field Office'				 ,'Z100D279 - Field Office'			 ,'Field Office',225,1 UNION ALL
		SELECT 'ProducerCode','Z100D280','GEICO','Field Office','Field Office'				 ,'Z100D280 - Field Office'			 ,'Field Office',226,1 UNION ALL
		SELECT 'ProducerCode','Z100D281','GEICO','Field Office','Field Office'				 ,'Z100D281 - Field Office'			 ,'Field Office',227,1 UNION ALL
		SELECT 'ProducerCode','Z100D282','GEICO','Field Office','Field Office'				 ,'Z100D282 - Field Office'			 ,'Field Office',228,1 UNION ALL
		SELECT 'ProducerCode','Z100D283','GEICO','Field Office','Field Office'				 ,'Z100D283 - Field Office'			 ,'Field Office',229,1 UNION ALL
		SELECT 'ProducerCode','Z100D284','GEICO','Field Office','Field Office'				 ,'Z100D284 - Field Office'			 ,'Field Office',230,1 UNION ALL
		SELECT 'ProducerCode','Z100D285','GEICO','Field Office','Field Office'				 ,'Z100D285 - Field Office'			 ,'Field Office',231,1 UNION ALL
		SELECT 'ProducerCode','Z100D286','GEICO','Field Office','Field Office'				 ,'Z100D286 - Field Office'			 ,'Field Office',232,1 UNION ALL
		SELECT 'ProducerCode','Z100D287','GEICO','Field Office','Field Office'				 ,'Z100D287 - Field Office'			 ,'Field Office',233,1 UNION ALL
		SELECT 'ProducerCode','Z100D288','GEICO','Field Office','Field Office'				 ,'Z100D288 - Field Office'			 ,'Field Office',234,1 UNION ALL
		SELECT 'ProducerCode','Z100D289','GEICO','Field Office','Field Office'				 ,'Z100D289 - Field Office'			 ,'Field Office',235,1 UNION ALL
		SELECT 'ProducerCode','Z100D290','GEICO','Field Office','Field Office'				 ,'Z100D290 - Field Office'			 ,'Field Office',236,1 UNION ALL
		SELECT 'ProducerCode','Z100D291','GEICO','Field Office','Field Office'				 ,'Z100D291 - Field Office'			 ,'Field Office',237,1 UNION ALL
		SELECT 'ProducerCode','Z100D292','GEICO','Field Office','Field Office'				 ,'Z100D292 - Field Office'			 ,'Field Office',238,1 UNION ALL
		SELECT 'ProducerCode','Z100D293','GEICO','Field Office','Field Office'				 ,'Z100D293 - Field Office'			 ,'Field Office',239,1 UNION ALL
		SELECT 'ProducerCode','Z100D294','GEICO','Field Office','Field Office'				 ,'Z100D294 - Field Office'			 ,'Field Office',240,1 UNION ALL
		SELECT 'ProducerCode','Z100D295','GEICO','Field Office','Field Office'				 ,'Z100D295 - Field Office'			 ,'Field Office',241,1 UNION ALL
		SELECT 'ProducerCode','Z100D296','GEICO','Field Office','Field Office'				 ,'Z100D296 - Field Office'			 ,'Field Office',242,1 UNION ALL
		SELECT 'ProducerCode','Z100D297','GEICO','Field Office','Field Office'				 ,'Z100D297 - Field Office'			 ,'Field Office',243,1 UNION ALL
		SELECT 'ProducerCode','Z100D298','GEICO','Field Office','Field Office'				 ,'Z100D298 - Field Office'			 ,'Field Office',244,1 UNION ALL
		SELECT 'ProducerCode','Z100D299','GEICO','Field Office','Field Office'				 ,'Z100D299 - Field Office'			 ,'Field Office',245,1 UNION ALL
		SELECT 'ProducerCode','Z100D300','GEICO','Field Office','Field Office'				 ,'Z100D300 - Field Office'			 ,'Field Office',246,1 UNION ALL
		SELECT 'ProducerCode','Z100D301','GEICO','Field Office','Field Office'				 ,'Z100D301 - Field Office'			 ,'Field Office',247,1 UNION ALL
		SELECT 'ProducerCode','Z100D302','GEICO','Field Office','Field Office'				 ,'Z100D302 - Field Office'			 ,'Field Office',248,1 UNION ALL
		SELECT 'ProducerCode','Z100D303','GEICO','Field Office','Field Office'				 ,'Z100D303 - Field Office'			 ,'Field Office',249,1 UNION ALL
		SELECT 'ProducerCode','Z100D304','GEICO','Field Office','Field Office'				 ,'Z100D304 - Field Office'			 ,'Field Office',250,1 UNION ALL
		SELECT 'ProducerCode','Z100D305','GEICO','Field Office','Field Office'				 ,'Z100D305 - Field Office'			 ,'Field Office',251,1 UNION ALL
		SELECT 'ProducerCode','Z100D306','GEICO','Field Office','Field Office'				 ,'Z100D306 - Field Office'			 ,'Field Office',252,1 UNION ALL
		SELECT 'ProducerCode','Z100D307','GEICO','Field Office','Field Office'				 ,'Z100D307 - Field Office'			 ,'Field Office',253,1 UNION ALL
		SELECT 'ProducerCode','Z100D308','GEICO','Field Office','Field Office'				 ,'Z100D308 - Field Office'			 ,'Field Office',254,1 UNION ALL
		
		--these map the public ID's to values SELECT which are only accessible in XML UNION ALL
		 SELECT 'GWILMLocationHazardCodeChoiceMap','zqojefhqjud1qegnteto4ccst48','Above Standard','PhysicalProtection_JMIC'   ,'','','',1,1 UNION ALL
		 SELECT 'GWILMLocationHazardCodeChoiceMap','zhgga12pdtgfi55ipiit3c5m4o8','Standard','PhysicalProtection_JMIC'		   ,'','','',2,1 UNION ALL
		 SELECT 'GWILMLocationHazardCodeChoiceMap','zc0iqmrq57mmg5kshl3090kmda8' , 'Minimum' ,'PhysicalProtection_JMIC'	   ,'','','',3,1 UNION ALL
		 SELECT 'GWILMLocationHazardCodeChoiceMap','z11i063v87e1vftj1ltkriujc6b' , 'Above Standard','ClsdBsnssStrgPract_JMIC' ,'','','',1,1 UNION ALL
		 SELECT 'GWILMLocationHazardCodeChoiceMap','zbshoum9pgeducg32ghdknmv6d9' , 'Standard','ClsdBsnssStrgPract_JMIC'	   ,'','','',2,1 UNION ALL
		 SELECT 'GWILMLocationHazardCodeChoiceMap','zmthuchv9rboi7kbsoufuqjb0ta' , 'Minimum','ClsdBsnssStrgPract_JMIC'		   ,'','','',3,1 UNION ALL
		 SELECT 'GWILMLocationHazardCodeChoiceMap','z2ohk1o3ghroddt6tcfkl0qiitb' , '10+ years','YearsInBusiness_JMIC'		   ,'','','',1,1 UNION ALL
		 SELECT 'GWILMLocationHazardCodeChoiceMap','zhfjg9p6d979g29s0jetpa5hjq8' , '3 to 10 years' ,'YearsInBusiness_JMIC'	   ,'','','',2,1 UNION ALL
		 SELECT 'GWILMLocationHazardCodeChoiceMap','zpmgsd6dk4d249pnvdcu9e3ipkb' , '< 3 Years','YearsInBusiness_JMIC'		   ,'','','',3,1 UNION ALL
		 SELECT 'GWILMLocationHazardCodeChoiceMap','z9sio96qu91391vtu93914qs268' , 'Above Average','JBTFinancial_JMIC'		   ,'','','',1,1 UNION ALL
		 SELECT 'GWILMLocationHazardCodeChoiceMap','z9ghs74fvp2qgbbtibgfvc2b228' , 'Average','JBTFinancial_JMIC'			   ,'','','',2,1 UNION ALL
		 SELECT 'GWILMLocationHazardCodeChoiceMap','zsahudrua0t7ndgaskk3v91jrs9' , 'Below Average','JBTFinancial_JMIC'		   ,'','','',3,1 UNION ALL
		 SELECT 'GWILMLocationHazardCodeChoiceMap','zutjsppjps9ca6rof51gi4l7ss9' , 'High Value','Inventory_JMIC'			   ,'','','',1,1 UNION ALL
		 SELECT 'GWILMLocationHazardCodeChoiceMap','z8niasms7bpjr7mjdbecg58c74a' , 'Low Value','Inventory_JMIC'			   ,'','','',2,1 UNION ALL
		 SELECT 'GWILMLocationHazardCodeChoiceMap','z6sj08124dnid15hrdkaan9lbc9' , 'Above Standard','ElectronicProt_JMIC'	   ,'','','',1,1 UNION ALL
		 SELECT 'GWILMLocationHazardCodeChoiceMap','z6ejm62eulumj2tcuv2309ijgd9' , 'Standard','ElectronicProt_JMIC'		   ,'','','',2,1 UNION ALL
		 SELECT 'GWILMLocationHazardCodeChoiceMap','z85hsah9rd14efvo232771ci0jb' , 'Minimum', 'ElectronicProt_JMIC'		   ,'','','',3,1 UNION ALL
		 
		 --Payees to be excluded from the NACHA file process
		 SELECT 'NACHAExcludePayeeCode','CP SAMUELS','SAMUELS JEWELERS INC','','','','',1,1 UNION ALL
		 
		 ---
		 SELECT 'ApplicationTakenBy','Web','Web','','','','',1,1 UNION ALL
		 SELECT 'ApplicationTakenBy','Customer Care','Customer Care','','','','',2,1 UNION ALL
		 SELECT 'ApplicationTakenBy','Renters','Renters','','','','',3,1 UNION ALL
		 SELECT 'ApplicationTakenBy','Field Office','Field Office','','','','',4,1 UNION ALL
		 SELECT 'ApplicationTakenByProducerCode','Z100D20','Partner Call Center','Call Center - Virginia Beach','','','',1,1 UNION ALL
		 SELECT 'ApplicationTakenByProducerCode','Z100D30','Partner Call Center','Call Center - Buffalo','','','',2,1 UNION ALL
		 SELECT 'ApplicationTakenByProducerCode','Z100D40','Partner Call Center','Call Center - Fredericksburg','','','',3,1 UNION ALL
		 SELECT 'ApplicationTakenByProducerCode','Z100D45','Partner Call Center','Call Center - Kansas City','','','',4,1 UNION ALL

		 
		 SELECT 'ApplicationSourceIdMap','3','Field Office','Field Office','','','',1,1 UNION ALL
		 SELECT 'ApplicationSourceIdMap','4','Web-Renters Redirect','Renters','','','',2,1 UNION ALL
		 SELECT 'ApplicationSourceIdMap','5','Web','Web','','','',3,1 UNION ALL
		 SELECT 'ApplicationSourceIdMap','6','Customer Care','Customer Care','','','',4,1 UNION ALL
		 SELECT 'ApplicationSourceIdAndExpressPartnerIdMap','3','2','Call Center - Virginia Beach','','','',1,1 UNION ALL
		 SELECT 'ApplicationSourceIdAndExpressPartnerIdMap','4','2','Call Center - Buffalo','','','',2,1 UNION ALL
		 SELECT 'ApplicationSourceIdAndExpressPartnerIdMap','6','2','Call Center - Fredericksburg','','','',3,1 UNION ALL
		 SELECT 'ApplicationSourceIdAndExpressPartnerIdMap','1031','2','Call Center - Kansas City','','','',4,1 UNION ALL	
		 SELECT 'ApplicationSourceIdAndExpressPartnerIdMap','12','5','Web Cross-Sell','','','',1,1 UNION ALL
		 SELECT 'ApplicationSourceIdAndExpressPartnerIdMap','1','5','Web','','','',1,1 UNION ALL


		 --Added for Claims Dashboard
        SELECT 'ClaimExaminer' ,'CARRIE A VOLP' ,'Carrie Volp'  ,'' ,''  ,'' ,'' ,1 ,1  UNION ALL
		SELECT 'ClaimExaminer' ,'DON ELLIOTT' ,'Don Elliott'  ,'' ,''  ,'' ,'' ,2 ,1  UNION ALL
		SELECT 'ClaimExaminer' ,'JOHN GANGA' ,'John Ganga'  ,'' ,''  ,'' ,'' ,3 ,1  UNION ALL
		SELECT 'ClaimExaminer' ,'Lisa VanCuyk' ,'Lisa Van Cuyk'  ,'' ,''  ,'' ,'' ,4 ,1  UNION ALL
		SELECT 'ClaimExaminer' ,'Mike Zaharias' ,'Michael Zaharias'  ,'' ,''  ,'' ,'' ,5 ,1  UNION ALL
		SELECT 'ClaimExaminer' ,'SHEILA SALENTINE' ,'Sheila Salentine'  ,'' ,''  ,'' ,'' ,6 ,1  UNION ALL
		SELECT 'ClaimExaminer' ,'MARY B VAN GOMPEL' ,'Mary VanGompel'  ,'' ,''  ,'' ,'' ,7 ,1  UNION ALL
		SELECT 'ClaimExaminer' ,'MATT THIES' ,'Matt Thies'  ,'' ,''  ,'' ,'' ,8 ,1  UNION ALL
		SELECT 'ClaimExaminer' ,'Jessica M Verhagen' ,'Jessica Verhagen'  ,'' ,''  ,'' ,'' ,9 ,1  UNION ALL
		SELECT 'ClaimExaminer' ,'Sheila Salentinen' ,'Sheila Palmer'  ,'' ,''  ,'' ,'' ,10 ,1  UNION ALL

		--Agents to exclude on GEICO production report
		SELECT 'AgentExclusion','amandawright@geico.com','Amanda R Wright','','','','',1,1 UNION ALL
		SELECT 'AgentExclusion','BSpicer@geico.com','Brandine Babette Spicer','','','','',1,1 UNION ALL
		SELECT 'AgentExclusion','CBelden@geico.com','Cody Wayne Belden','','','','',1,1 UNION ALL
		SELECT 'AgentExclusion','DBodo@geico.com','Damien Nicholas Lee Bodo','','','','',1,1 UNION ALL
		SELECT 'AgentExclusion','DeSharpe@geico.com','De Sharpe','','','','',1,1 UNION ALL
		SELECT 'AgentExclusion','JMartus@geico.com','Jennifer Paschal Martus','','','','',1,1 UNION ALL
		SELECT 'AgentExclusion','jtorrescisneros@geico.com','Jennifer Ruth  Torres Cisneros','','','','',1,1 UNION ALL
		SELECT 'AgentExclusion','MMillner@geico.com','Melanie A Millner','','','','',1,1 UNION ALL

		 --Strategic Partners
        SELECT 'StrategicPartner' ,'Z100' ,'GEICO Insurance Agency Inc'  ,'' ,''  ,'' ,'' ,1 ,1  UNION ALL
		SELECT 'StrategicPartner' ,'KL001' ,'Kraft Lake Insurance Agency Inc'  ,'' ,''  ,'' ,'' ,2 ,1  UNION ALL
		SELECT 'StrategicPartner' ,'HE001' ,'Liberty Mutual Group Inc'  ,'' ,''  ,'' ,'' ,3 ,1  UNION ALL
		SELECT 'StrategicPartner' ,'U10' ,'McGriff Insurance Services Inc'  ,'' ,''  ,'' ,'' ,4 ,1  UNION ALL
		SELECT 'StrategicPartner' ,'Z400' ,'Big I Markets'  ,'' ,''  ,'' ,'' ,5 ,1  UNION ALL
		SELECT 'StrategicPartner' ,'CHS' ,'Coastal Homeowners Insurance Specialist Inc'  ,'' ,''  ,'' ,'' ,6 ,1  UNION ALL
		SELECT 'StrategicPartner' ,'TWFG' ,'TWFG Insurance Services Inc'  ,'' ,''  ,'' ,'' ,7 ,1 


) AS SRC --([Category],[Name],[Value1],[Value2],[Value3],[Value4],[Value5],[Sequence],[IsActive])
ON Dest.Category = SRC.Category
AND Dest.Name = SRC.Name
WHEN MATCHED AND (
	DEST.Sequence <> SRC.Sequence 
	OR DEST.IsActive<>SRC.IsActive 
	OR COALESCE(DEST.Value1,'?') <> COALESCE(SRC.Value1,'?')
	OR COALESCE(DEST.Value2,'?') <> COALESCE(SRC.Value2,'?')
	OR COALESCE(DEST.Value3,'?') <> COALESCE(SRC.Value3,'?')
	OR COALESCE(DEST.Value4,'?') <> COALESCE(SRC.Value4,'?')
	OR COALESCE(DEST.Value5,'?') <> COALESCE(SRC.Value5,'?')
	) THEN UPDATE 
	SET 
		DEST.Value1 = SRC.Value1
		,DEST.Value2 = SRC.Value2
		,DEST.Value3 = SRC.Value3
		,DEST.Value4 = SRC.Value4
		,DEST.Value5 = SRC.Value5
		,DEST.Sequence = SRC.Sequence
		,DEST.IsActive = SRC.IsActive
		,DateModified = CURRENT_TIMESTAMP()
WHEN NOT MATCHED BY Target
	THEN INSERT(Category,Name,Value1,Value2,Value3,Value4,Value5,Sequence,IsActive,DateCreated,DateModified)
	VALUES (
		SRC.Category
		,SRC.Name
		,SRC.Value1
		,SRC.Value2
		,SRC.Value3
		,SRC.Value4
		,SRC.Value5
		,SRC.Sequence
		,SRC.IsActive
		,CURRENT_TIMESTAMP()
		,CURRENT_TIMESTAMP()
	)
WHEN NOT MATCHED BY Source THEN
	DELETE
;