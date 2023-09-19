/* Kimberlite - Reference and Lookup Tables
	CauseOfLossDesc.sql
--------------------------------------------------------------------------------------------
	*****  Change History  *****

	03/01/2021	MGIDDINGS		Init create
	06/10/2022	DROBAK			Update structure to remove PAA, PCA columns -- reload data
									Fields Removed:
										--PCACauseOfLossCode
										--PCACauseOfLossDesc
										--PASCauseOfLossCode
										--PASCauseOfLossDesc
										--GWLossCauseName

--------------------------------------------------------------------------------------------

--SELECT '(' + CAST([ID] AS VARCHAR(10)) + ',''' + [ProductLine] + ''',' + ISNULL('''' + CAST([PCACauseOfLossCode] AS VARCHAR(10)) + '''','NULL') + ',' + ISNULL('''' + [PCACauseOfLossDesc] + '''','NULL') + ',' + ISNULL('''' + CAST([PASCauseOfLossCode] AS VARCHAR(10)) + '''','NULL') + ',' + ISNULL('''' + [PASCauseOfLossDesc] + '''','NULL') + ',' +  ISNULL('''' + [GWLossCauseTypecode] + '''','NULL') + ',' + ISNULL('''' + [GWLossCauseName] + '''','NULL') + ',''' + [ConformedCauseOfLossDesc] + ''',' + ISNULL('''' + [LossGroup] + '''','NULL') + ',' + ISNULL('''' + [ReportToJSAandJVC] + '''','NULL') + ',' + CAST([IsActive] AS CHAR(1)) + '),'
--  FROM [bief_src].[CauseOfLossDesc]  ORDER BY [ID]
*/
CREATE TABLE IF NOT EXISTS `{project}.{dest_dataset}.CauseOfLossDesc`
--CREATE OR REPLACE TABLE `prod-edl.ref_kimberlite.CauseOfLossDesc`
(
	ID							INT64,
	ProductLine					STRING,
	GWLossCauseTypecode			STRING,
	ConformedCauseOfLossDesc	STRING,
	LossGroup					STRING,
	ReportToJSAandJVC			STRING,
	IsActive					INT64
);

MERGE INTO `{project}.{dest_dataset}.CauseOfLossDesc` AS Target 
USING (
	SELECT 1 as ID, 'CL' as ProductLine,'air_crash' as GWLossCauseTypecode,'Crash of airplane' as ConformedCauseOfLossDesc,'Other Property' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 2 as ID, 'CL' as ProductLine,'breakage_JMIC' as GWLossCauseTypecode,'Breakage' as ConformedCauseOfLossDesc,'Other Property' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 3 as ID, 'CL' as ProductLine,'collision_JMIC' as GWLossCauseTypecode,'Collision including upset/ overturn' as ConformedCauseOfLossDesc,'Other Property' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 4 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Fraudulent Check/Credit card transactions' as ConformedCauseOfLossDesc,'Open - On Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 5 as ID, 'CL' as ProductLine,'earthquake' as GWLossCauseTypecode,'Earthquake' as ConformedCauseOfLossDesc,'Weather/Environmental' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 6 as ID, 'CL' as ProductLine,'explosion' as GWLossCauseTypecode,'Explosion' as ConformedCauseOfLossDesc,'Other Property' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 7 as ID, 'CL' as ProductLine,'fire' as GWLossCauseTypecode,'Fire' as ConformedCauseOfLossDesc,'Fire' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 8 as ID, 'CL' as ProductLine,'flood_JMIC' as GWLossCauseTypecode,'Flood' as ConformedCauseOfLossDesc,'Weather/Environmental' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 9 as ID, 'CL' as ProductLine,'freezing_JMIC' as GWLossCauseTypecode,'Freezing' as ConformedCauseOfLossDesc,'Weather/Environmental' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 10 as ID, 'CL' as ProductLine,'glassbreakage' as GWLossCauseTypecode,'Glass Breakage' as ConformedCauseOfLossDesc,'Other Property' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 11 as ID, 'CL' as ProductLine,'hail' as GWLossCauseTypecode,'Hail' as ConformedCauseOfLossDesc,'Weather/Environmental' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 12 as ID, 'CL' as ProductLine,'lightning_JMIC' as GWLossCauseTypecode,'Lightning' as ConformedCauseOfLossDesc,'Weather/Environmental' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 13 as ID, 'CL' as ProductLine,'disappearance_open_JMIC' as GWLossCauseTypecode,'Disappearance-on premises/open' as ConformedCauseOfLossDesc,'Open - On Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 14 as ID, 'CL' as ProductLine,'disappearance_off_JMIC' as GWLossCauseTypecode,'Disappearance-off premises' as ConformedCauseOfLossDesc,'Off Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 15 as ID, 'CL' as ProductLine,'property_removal_JMIC' as GWLossCauseTypecode,'Property Removal' as ConformedCauseOfLossDesc,'Other Property' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 16 as ID, 'CL' as ProductLine,'riotandcivil' as GWLossCauseTypecode,'Riot and Civil Commotion' as ConformedCauseOfLossDesc,'Other Property' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 17 as ID, 'CL' as ProductLine,'smoke_JMIC' as GWLossCauseTypecode,'Smoke' as ConformedCauseOfLossDesc,'Fire' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 18 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Theft from auto (Craftsman)' as ConformedCauseOfLossDesc,'Off Premises' as LossGroup,'N' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 19 as ID, 'CL' as ProductLine,'all_burglary_closed_JMIC' as GWLossCauseTypecode,'All other burglary-closed' as ConformedCauseOfLossDesc,'Closed - On Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 20 as ID, 'CL' as ProductLine,'vandalism_JMIC' as GWLossCauseTypecode,'Vandalism' as ConformedCauseOfLossDesc,'Other Property' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 21 as ID, 'CL' as ProductLine,'motorvehicle' as GWLossCauseTypecode,'Motor vehicle' as ConformedCauseOfLossDesc,'Other Property' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 22 as ID, 'CL' as ProductLine,'waterdamage' as GWLossCauseTypecode,'Water Damage' as ConformedCauseOfLossDesc,'Other Property' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 23 as ID, 'CL' as ProductLine,'wind' as GWLossCauseTypecode,'Wind' as ConformedCauseOfLossDesc,'Weather/Environmental' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 24 as ID, 'CL' as ProductLine,'all_burglary_not_def_JMIC' as GWLossCauseTypecode,'All Other Loss Not Defined' as ConformedCauseOfLossDesc,'Other' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 25 as ID, 'CL' as ProductLine,'liability_bodyinjury_JMIC' as GWLossCauseTypecode,'Liability-Bodily Injury' as ConformedCauseOfLossDesc,'General Liability' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 26 as ID, 'CL' as ProductLine,'liability_prop_dmg_JMIC' as GWLossCauseTypecode,'Liability-Property Damage' as ConformedCauseOfLossDesc,'General Liability' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 27 as ID, 'CL' as ProductLine,'liability_fire_JMIC' as GWLossCauseTypecode,'Liability-Fire Legal' as ConformedCauseOfLossDesc,'General Liability' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 28 as ID, 'CL' as ProductLine,'liability_other_JMIC' as GWLossCauseTypecode,'Liability-All Other' as ConformedCauseOfLossDesc,'General Liability' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 29 as ID, 'CL' as ProductLine,'medical_payment_JMIC' as GWLossCauseTypecode,'Medical Payments' as ConformedCauseOfLossDesc,'General Liability' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 30 as ID, 'CL' as ProductLine,'sprinkler_leak_JMIC' as GWLossCauseTypecode,'Sprinkler Leakage' as ConformedCauseOfLossDesc,'Other Property' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 31 as ID, 'CL' as ProductLine,'collapse_notsinkhole_JMIC' as GWLossCauseTypecode,'Collapse other than Sinkhole' as ConformedCauseOfLossDesc,'Other Property' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 32 as ID, 'CL' as ProductLine,'liab_piercing_JMIC' as GWLossCauseTypecode,'Liability-Piercing' as ConformedCauseOfLossDesc,'Jewelers Liability' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 33 as ID, 'CL' as ProductLine,'liability_appraisal_JMIC' as GWLossCauseTypecode,'Liability-Appraisal' as ConformedCauseOfLossDesc,'Jewelers Liability' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 34 as ID, 'CL' as ProductLine,'employee_dishonest_JMIC' as GWLossCauseTypecode,'Employee Dishonesty' as ConformedCauseOfLossDesc,'Employee Dishonesty' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 35 as ID, 'CL' as ProductLine,'liability_per_inj_JMIC' as GWLossCauseTypecode,'Liability-Personal Injury' as ConformedCauseOfLossDesc,'General Liability' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 36 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Attended Auto' as ConformedCauseOfLossDesc,'Off Premises' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 37 as ID, 'CL' as ProductLine,'unattended_auto_JMIC' as GWLossCauseTypecode,'Unattended Auto' as ConformedCauseOfLossDesc,'Off Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 38 as ID, 'CL' as ProductLine,'liability_products_JMIC' as GWLossCauseTypecode,'Liability-Products' as ConformedCauseOfLossDesc,'Jewelers Liability' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 39 as ID, 'CL' as ProductLine,'sinkhole_collapse_JMIC' as GWLossCauseTypecode,'Sinkhole Collapse' as ConformedCauseOfLossDesc,'Weather/Environmental' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 40 as ID, 'CL' as ProductLine,'volcano_JMIC' as GWLossCauseTypecode,'Volcanic Action' as ConformedCauseOfLossDesc,'Weather/Environmental' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 41 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Customer Property Switch' as ConformedCauseOfLossDesc,'Jewelers Liability' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 42 as ID, 'CL' as ProductLine,'sexual_harassment_JMIC' as GWLossCauseTypecode,'Sexual Harassment' as ConformedCauseOfLossDesc,'EPLI' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 43 as ID, 'CL' as ProductLine,'wrong_termination_JMIC' as GWLossCauseTypecode,'Wrongful Termination' as ConformedCauseOfLossDesc,'EPLI' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 44 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Fraudulent Check/Credit card transactions' as ConformedCauseOfLossDesc,'Open - On Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 45 as ID, 'CL' as ProductLine,'liability_adv_inj_JMIC' as GWLossCauseTypecode,'Liability-Advertising Injury' as ConformedCauseOfLossDesc,'General Liability' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 46 as ID, 'CL' as ProductLine,'liab_dogBite_JMIC' as GWLossCauseTypecode,'Liability-Dog Bite' as ConformedCauseOfLossDesc,'General Liability' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 47 as ID, 'CL' as ProductLine,'armed_rob_open_JMIC' as GWLossCauseTypecode,'Armed Robbery - open' as ConformedCauseOfLossDesc,'Open - On Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 48 as ID, 'CL' as ProductLine,'sneak_theft_JMIC' as GWLossCauseTypecode,'Sneak Theft/Shoplifting - open' as ConformedCauseOfLossDesc,'Open - On Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 49 as ID, 'CL' as ProductLine,'grab_run_JMIC' as GWLossCauseTypecode,'Grab & Run - open' as ConformedCauseOfLossDesc,'Open - On Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 50 as ID, 'CL' as ProductLine,'switch_goods_JMIC' as GWLossCauseTypecode,'Switch-Goods - open' as ConformedCauseOfLossDesc,'Open - On Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 51 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Window Smash While Open (protected)' as ConformedCauseOfLossDesc,'Open - On Premises' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 52 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Window Smash While Open (unprotected)' as ConformedCauseOfLossDesc,'Open - On Premises' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 53 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Workmanship' as ConformedCauseOfLossDesc,'Open - On Premises' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 54 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Damaged while being worked on-off prem' as ConformedCauseOfLossDesc,'Off Premises' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 55 as ID, 'CL' as ProductLine,'chemical_spill_JMIC' as GWLossCauseTypecode,'Chemical Spills' as ConformedCauseOfLossDesc,'General Liability' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 56 as ID, 'CL' as ProductLine,'transit_FCRM_JMIC' as GWLossCauseTypecode,'Transit - USPS - FCRM' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 57 as ID, 'CL' as ProductLine,'transit_UPS_JMIC' as GWLossCauseTypecode,'Transit-U.P.S.' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 58 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Transit-FedEx' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 59 as ID, 'CL' as ProductLine,'transit_msg_truck_JMIC' as GWLossCauseTypecode,'Transit-Messenger/Trucking Service' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 60 as ID, 'CL' as ProductLine,'transit_all_other_JMIC' as GWLossCauseTypecode,'Transit-All Others' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 61 as ID, 'CL' as ProductLine,'transit_express_mail_JMIC' as GWLossCauseTypecode,'Transit - USPS - Express Mail' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 62 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Transit-Airborne' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 63 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Mold-Property' as ConformedCauseOfLossDesc,'Other Property' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 64 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Liability-Mold' as ConformedCauseOfLossDesc,'General Liability' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 65 as ID, 'CL' as ProductLine,'war_on_prem_open_JMIC' as GWLossCauseTypecode,'War, Miltry Act & Terrorism-on prem/open' as ConformedCauseOfLossDesc,'Terrorism' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 66 as ID, 'CL' as ProductLine,'thrown_away_JMIC' as GWLossCauseTypecode,'Thrown Away' as ConformedCauseOfLossDesc,'Open - On Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 67 as ID, 'CL' as ProductLine,'equip_breakdown_JMIC' as GWLossCauseTypecode,'Equipment Breakdown' as ConformedCauseOfLossDesc,'Other Property' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 68 as ID, 'CL' as ProductLine,'custody_of_other_JMIC' as GWLossCauseTypecode,'Custody of others' as ConformedCauseOfLossDesc,'Off Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 69 as ID, 'CL' as ProductLine,'home_loss_JMIC' as GWLossCauseTypecode,'Loss of Merch-kept at home' as ConformedCauseOfLossDesc,'Off Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 70 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Custody of others-in transit' as ConformedCauseOfLossDesc,'Off Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 71 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Loss of Merch-while off premises' as ConformedCauseOfLossDesc,'Off Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 72 as ID, 'CL' as ProductLine,'trade_show_cdy_loss_JMIC' as GWLossCauseTypecode,'Loss at Trade Show-In Jewelers Custody' as ConformedCauseOfLossDesc,'Off Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 73 as ID, 'CL' as ProductLine,'trade_show_saf_loss_JMIC' as GWLossCauseTypecode,'Loss at Trade Show-Merch in safe keeping' as ConformedCauseOfLossDesc,'Off Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 74 as ID, 'CL' as ProductLine,'transit_mail_JMIC' as GWLossCauseTypecode,'Transit -Mail Shipments' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 75 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Smash Grab & Run-prot win/doors closed' as ConformedCauseOfLossDesc,'Closed - On Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 76 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Smash Grab & Run-unprot wndws/doors clsd' as ConformedCauseOfLossDesc,'Closed - On Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 77 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Compd Alrm-safe not entrd UL apprvd clsd' as ConformedCauseOfLossDesc,'Closed - On Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 78 as ID, 'CL' as ProductLine,'compd_alrm_JMIC' as GWLossCauseTypecode,'Compd Alrm-safe not entrd non UL clsd - on premise' as ConformedCauseOfLossDesc,'Closed - On Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 79 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Compd Alrm-safe entered UL apprvd clsd' as ConformedCauseOfLossDesc,'Closed - On Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 80 as ID, 'CL' as ProductLine,'compd_alrm_non_JMIC' as GWLossCauseTypecode,'Compd Alrm-safe entered non UL clsd - on premise' as ConformedCauseOfLossDesc,'Closed - On Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 81 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Entd prem not smsh win/dr UL apprvd clsd' as ConformedCauseOfLossDesc,'Closed - On Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 82 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Entd prem not smsh win/dr non UL clsd' as ConformedCauseOfLossDesc,'Closed - On Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 83 as ID, 'CL' as ProductLine,'power_surge_JMIC' as GWLossCauseTypecode,'Power Surge-including Brownouts' as ConformedCauseOfLossDesc,'Other Property' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 84 as ID, 'CL' as ProductLine,'weight_ice_snow_JMIC' as GWLossCauseTypecode,'Weight of Ice, Snow or Sleet' as ConformedCauseOfLossDesc,'Weather/Environmental' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 85 as ID, 'CL' as ProductLine,'discrimination_JMIC' as GWLossCauseTypecode,'Discrimination' as ConformedCauseOfLossDesc,'EPLI' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 86 as ID, 'CL' as ProductLine,'epl_personal_injury_JMIC' as GWLossCauseTypecode,'EPL - Personal Injury' as ConformedCauseOfLossDesc,'EPLI' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 87 as ID, 'CL' as ProductLine,'harassment_JMIC' as GWLossCauseTypecode,'Harassment' as ConformedCauseOfLossDesc,'EPLI' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 88 as ID, 'CL' as ProductLine,'workmanship_JMIC' as GWLossCauseTypecode,'Workmanship' as ConformedCauseOfLossDesc,'Open - On Premises' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 89 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Terrorism Certified Acts -JS JB BOP Prop' as ConformedCauseOfLossDesc,'Terrorism' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 90 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Terrorism BI Cert Acts-UMB/BOP Liab' as ConformedCauseOfLossDesc,'Terrorism' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 91 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Terrorism BI Cert Acts NBC-UMB/BOP Liab' as ConformedCauseOfLossDesc,'Terrorism' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 92 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Terrorism BI no NBC not Cert-UMB/BOP Lia' as ConformedCauseOfLossDesc,'Terrorism' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 93 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Terrorism BI NBC not Cert-UMB/BOP Liab' as ConformedCauseOfLossDesc,'Terrorism' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 94 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Terrorism PD Cert Acts-UMB/BOP Liab' as ConformedCauseOfLossDesc,'Terrorism' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 95 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Terrorism PD Cert Acts NBC-UMB/BOP Liab' as ConformedCauseOfLossDesc,'Terrorism' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 96 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Terrorism PD no NBC not Cert-UMB/BP Liab' as ConformedCauseOfLossDesc,'Terrorism' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 97 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Terrorism PD NBC not Cert-UMB/BOP Liab' as ConformedCauseOfLossDesc,'Terrorism' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 98 as ID, 'CL' as ProductLine,'transit_fed_ex_JMIC' as GWLossCauseTypecode,'Transit-FedEx' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 99 as ID, 'CL' as ProductLine,'transit_g4s_JMIC' as GWLossCauseTypecode,'Transit -G4S' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 100 as ID, 'CL' as ProductLine,'transit_parcel_JMIC' as GWLossCauseTypecode,'Transit-Parcel Pro' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 101 as ID, 'CL' as ProductLine,'transit_transguard_JMIC' as GWLossCauseTypecode,'Transit-Transguardian' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 102 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Liab related to escaped liquid fuel' as ConformedCauseOfLossDesc,'General Liability' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 103 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Liab related to lead poisoning' as ConformedCauseOfLossDesc,'General Liability' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 104 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Theft (Burglary/Robbery) - Off Premises' as ConformedCauseOfLossDesc,'Off Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 105 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Fire - From Woodburning Stove' as ConformedCauseOfLossDesc,'Fire' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 106 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Smoke - From Woodburning Stove' as ConformedCauseOfLossDesc,'Fire' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 107 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Time Element - Loss Of Earnings' as ConformedCauseOfLossDesc,'Other Property' as LossGroup,NULL as ReportToJSAandJVC,0 as IsActive  UNION ALL
	SELECT 108 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Money' as ConformedCauseOfLossDesc,'Other Property' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 109 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Time Element - Extra Expense' as ConformedCauseOfLossDesc,'Other Property' as LossGroup,NULL as ReportToJSAandJVC,0 as IsActive  UNION ALL
	SELECT 110 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Slsman Loss - Not Involving Unattended' as ConformedCauseOfLossDesc,'Off Premises' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 111 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Armed Robbery - W/O Surveillance' as ConformedCauseOfLossDesc,'Open - On Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 112 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Sneak Theft/Shoplift - W/O Surveillance' as ConformedCauseOfLossDesc,'Open - On Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 113 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Grab & Run - W/O Surveillance' as ConformedCauseOfLossDesc,'Open - On Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 114 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Ring Switch - W/O Surveillance' as ConformedCauseOfLossDesc,'Open - On Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 115 as ID, 'CL' as ProductLine,NULL as GWLossCauseTypecode,'Package Damaged' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 116 as ID, 'CL' as ProductLine,'fraudulent_check_JMIC' as GWLossCauseTypecode,'Fraudulent Check/Credit card Transaction' as ConformedCauseOfLossDesc,'Open - On Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 117 as ID, 'CL' as ProductLine,'travel_armed_robbery_JMIC' as GWLossCauseTypecode,'Travel - Armed Robbery' as ConformedCauseOfLossDesc,'Off Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 118 as ID, 'CL' as ProductLine,'smash_grab_run_open_JMIC' as GWLossCauseTypecode,'Smash Grab & Run-Open' as ConformedCauseOfLossDesc,'Open - On Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 119 as ID, 'CL' as ProductLine,'burglary_safe_JMIC' as GWLossCauseTypecode,'Burglary-Safe-on premise' as ConformedCauseOfLossDesc,'Closed - On Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 120 as ID, 'CL' as ProductLine,'burlary_on_JMIC' as GWLossCauseTypecode,'Burglary-on premise' as ConformedCauseOfLossDesc,'Closed - On Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 121 as ID, 'CL' as ProductLine,'terror_cert_JMIC' as GWLossCauseTypecode,'Certified Acts of Terrorism' as ConformedCauseOfLossDesc,'Terrorism' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 122 as ID, 'CL' as ProductLine,'terror_all_other_JMIC' as GWLossCauseTypecode,'All other acts of terrorism' as ConformedCauseOfLossDesc,'Terrorism' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 123 as ID, 'CL' as ProductLine,'transit_brinks_JMIC' as GWLossCauseTypecode,'Transit-Brinks' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 124 as ID, 'CL' as ProductLine,'transit_malca_amit_JMIC' as GWLossCauseTypecode,'Transit-Malca Amit' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 125 as ID, 'CL' as ProductLine,'burglary_auto_JMIC' as GWLossCauseTypecode,'Burglary from Auto' as ConformedCauseOfLossDesc,'Off Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 126 as ID, 'CL' as ProductLine,'armed_rob_auto_JMIC' as GWLossCauseTypecode,'Armed robbery from auto' as ConformedCauseOfLossDesc,'Off Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 127 as ID, 'PL' as ProductLine,'air_crash' as GWLossCauseTypecode,'Crash of airplane' as ConformedCauseOfLossDesc,'All Other' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 128 as ID, 'PL' as ProductLine,'creditcard_JMIC' as GWLossCauseTypecode,'Credit card' as ConformedCauseOfLossDesc,'All Other' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 129 as ID, 'PL' as ProductLine,'earthquake' as GWLossCauseTypecode,'Earthquake' as ConformedCauseOfLossDesc,'Weather/Environmental' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 130 as ID, 'PL' as ProductLine,'explosion' as GWLossCauseTypecode,'Explosion' as ConformedCauseOfLossDesc,'All Other' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 131 as ID, 'PL' as ProductLine,'flood_JMIC' as GWLossCauseTypecode,'Flood' as ConformedCauseOfLossDesc,'Weather/Environmental' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 132 as ID, 'PL' as ProductLine,'burglary' as GWLossCauseTypecode,'Burglary' as ConformedCauseOfLossDesc,'Crime' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 133 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Burglary - Safe' as ConformedCauseOfLossDesc,'Crime' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 134 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Burglary - Alarm' as ConformedCauseOfLossDesc,'Crime' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 135 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Burglary - Safe or Alarm' as ConformedCauseOfLossDesc,'Crime' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 136 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Burglary - No Safe or Alarm' as ConformedCauseOfLossDesc,'Crime' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 137 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Burglary - Safety Box' as ConformedCauseOfLossDesc,'Crime' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 138 as ID, 'PL' as ProductLine,'armed_robbery_JMIC' as GWLossCauseTypecode,'Armed robbery' as ConformedCauseOfLossDesc,'Crime' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 139 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Armed Robbery - Safe' as ConformedCauseOfLossDesc,'Crime' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 140 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Armed Robbery - Alarm' as ConformedCauseOfLossDesc,'Crime' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 141 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Armed Robbery - Safe or Alarm' as ConformedCauseOfLossDesc,'Crime' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 142 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Armed Robbery - No Safe or Alarm' as ConformedCauseOfLossDesc,'Crime' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 143 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Armed Robbery - Safety Box' as ConformedCauseOfLossDesc,'Crime' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 144 as ID, 'PL' as ProductLine,'theft_JMIC' as GWLossCauseTypecode,'Theft' as ConformedCauseOfLossDesc,'Crime' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 145 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Theft - Safe only' as ConformedCauseOfLossDesc,'Crime' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 146 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Theft - Alarm only' as ConformedCauseOfLossDesc,'Crime' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 147 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Theft - Safe and Alarm' as ConformedCauseOfLossDesc,'Crime' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 148 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Theft - No Safe or Alarm' as ConformedCauseOfLossDesc,'Crime' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 149 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Theft - Safety Box' as ConformedCauseOfLossDesc,'Crime' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 150 as ID, 'PL' as ProductLine,'chipped_stone_JMIC' as GWLossCauseTypecode,'Chipped stone' as ConformedCauseOfLossDesc,'Partial' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 151 as ID, 'PL' as ProductLine,'lost_stone_JMIC' as GWLossCauseTypecode,'Loss of a stone' as ConformedCauseOfLossDesc,'Partial' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 152 as ID, 'PL' as ProductLine,'physical_damage_JMIC' as GWLossCauseTypecode,'Physical damage' as ConformedCauseOfLossDesc,'Partial' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 153 as ID, 'PL' as ProductLine,'accidental_partial_JMIC' as GWLossCauseTypecode,'Accidental loss - partial' as ConformedCauseOfLossDesc,'Partial' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 154 as ID, 'PL' as ProductLine,'volcano_JMIC' as GWLossCauseTypecode,'Volcanic action' as ConformedCauseOfLossDesc,'All Other' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 155 as ID, 'PL' as ProductLine,'accidental_JMIC' as GWLossCauseTypecode,'Accidental loss' as ConformedCauseOfLossDesc,'Lost Item' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 156 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Accidental Loss - Safe' as ConformedCauseOfLossDesc,'Lost Item' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 157 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Accidental Loss - Alarm' as ConformedCauseOfLossDesc,'Lost Item' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 158 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Accidental Loss - Safe or Alarm' as ConformedCauseOfLossDesc,'Lost Item' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 159 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Accidental Loss  - No Safe or Alarm' as ConformedCauseOfLossDesc,'Lost Item' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 160 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Accidental Loss - Safety Box' as ConformedCauseOfLossDesc,'Lost Item' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 161 as ID, 'PL' as ProductLine,'fire' as GWLossCauseTypecode,'Fire' as ConformedCauseOfLossDesc,'Fire' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 162 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Fire - Safe' as ConformedCauseOfLossDesc,'Fire' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 163 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Fire - Alarm' as ConformedCauseOfLossDesc,'Fire' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 164 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Fire - Safe or Alarm' as ConformedCauseOfLossDesc,'Fire' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 165 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Fire - No Safe or Alarm' as ConformedCauseOfLossDesc,'Fire' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 166 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Fire - Safety Box' as ConformedCauseOfLossDesc,'Fire' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 167 as ID, 'PL' as ProductLine,'mysterious_disapear_JMIC' as GWLossCauseTypecode,'Mysterious disappearance' as ConformedCauseOfLossDesc,'Lost Item' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 168 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Mysterious Disappearance - Safe' as ConformedCauseOfLossDesc,'Lost Item' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 169 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Mysterious Disappearance - Alarm' as ConformedCauseOfLossDesc,'Lost Item' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 170 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Mysterious Disappearance - Safe or Alarm' as ConformedCauseOfLossDesc,'Lost Item' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 171 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Mysterious Disappearance - No Safe/Alarm' as ConformedCauseOfLossDesc,'Lost Item' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 172 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Mysterious Disappearance - Safety Box' as ConformedCauseOfLossDesc,'Lost Item' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 173 as ID, 'PL' as ProductLine,'miscellaneous' as GWLossCauseTypecode,'Miscellaneous causes' as ConformedCauseOfLossDesc,'All Other' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 174 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Miscellaneous - Safe' as ConformedCauseOfLossDesc,'All Other' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 175 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Miscellaneous - Alarm' as ConformedCauseOfLossDesc,'All Other' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 176 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Miscellaneous - Safe or Alarm' as ConformedCauseOfLossDesc,'All Other' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 177 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Miscellaneous - No Safe or Alarm' as ConformedCauseOfLossDesc,'All Other' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 178 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Miscellaneous - Safety Box' as ConformedCauseOfLossDesc,'All Other' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 179 as ID, 'PL' as ProductLine,'waterdamage' as GWLossCauseTypecode,'Water damage' as ConformedCauseOfLossDesc,'Weather/Environmental' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 180 as ID, 'PL' as ProductLine,'hail' as GWLossCauseTypecode,'Hail' as ConformedCauseOfLossDesc,'Weather/Environmental' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 181 as ID, 'PL' as ProductLine,'lightning_JMIC' as GWLossCauseTypecode,'Lightning' as ConformedCauseOfLossDesc,'Weather/Environmental' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 182 as ID, 'PL' as ProductLine,'riotandcivil' as GWLossCauseTypecode,'Riot and civil commotion' as ConformedCauseOfLossDesc,'All Other' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 183 as ID, 'PL' as ProductLine,'smoke_JMIC' as GWLossCauseTypecode,'Smoke' as ConformedCauseOfLossDesc,'Fire' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 184 as ID, 'PL' as ProductLine,'vandalism' as GWLossCauseTypecode,'Malicious mischief and vandalism' as ConformedCauseOfLossDesc,'All Other' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 185 as ID, 'PL' as ProductLine,'motorvehicle' as GWLossCauseTypecode,'Motor vehicle' as ConformedCauseOfLossDesc,'All Other' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 186 as ID, 'PL' as ProductLine,'wind' as GWLossCauseTypecode,'Wind' as ConformedCauseOfLossDesc,'Weather/Environmental' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 187 as ID, 'PL' as ProductLine,NULL as GWLossCauseTypecode,'Mold - Property related' as ConformedCauseOfLossDesc,'All Other' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 188 as ID, 'PL' as ProductLine,'dmg_worked_on_JMIC' as GWLossCauseTypecode,'Damage while being worked on' as ConformedCauseOfLossDesc,'Partial' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 189 as ID, 'PL' as ProductLine,'shipping_JMIC' as GWLossCauseTypecode,'Shipping' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 190 as ID, 'PL' as ProductLine,'fraudulent_check_JMIC' as GWLossCauseTypecode,'Fraudulent Check/Credit card Transaction' as ConformedCauseOfLossDesc,'All Other' as LossGroup,'N' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 191 as ID, 'PL' as ProductLine,'animal' as GWLossCauseTypecode,'Animal' as ConformedCauseOfLossDesc,'All Other' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 192 as ID, 'PL' as ProductLine,'wear_and_tear_JMIC' as GWLossCauseTypecode,'Wear and Tear' as ConformedCauseOfLossDesc,'Partial' as LossGroup,'N' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 193 as ID, 'CL' as ProductLine,'cyber_liability_JM' as GWLossCauseTypecode,'Cyber Liability' as ConformedCauseOfLossDesc,'Cyber Liability' as LossGroup,'N' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 194 as ID, 'PL' as ProductLine,'chipped_stone_center_JM' as GWLossCauseTypecode,'Chipped Stone - Center' as ConformedCauseOfLossDesc,'Partial' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 195 as ID, 'PL' as ProductLine,'chipped_stone_side_JM' as GWLossCauseTypecode,'Chipped Stone - Side' as ConformedCauseOfLossDesc,'Partial' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 196 as ID, 'PL' as ProductLine,'loss_of_Stone_Center_JM' as GWLossCauseTypecode,'Loss of Stone - Center' as ConformedCauseOfLossDesc,'Partial' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 197 as ID, 'PL' as ProductLine,'loss_of_Stone_Side_JM' as GWLossCauseTypecode,'Loss of Stone - Side' as ConformedCauseOfLossDesc,'Partial' as LossGroup,NULL as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 198 as ID, 'PL' as ProductLine,'wear_and_tear_bentbrokenworn_JM' as GWLossCauseTypecode,'Wear and Tear- Bent, broken, worn prongs' as ConformedCauseOfLossDesc,'Partial' as LossGroup,'N' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 199 as ID, 'PL' as ProductLine,'wear_and_tear_brokenclasps_JM' as GWLossCauseTypecode,'Wear and Tear- Broken clasps, bracelets, or chains' as ConformedCauseOfLossDesc,'Partial' as LossGroup,'N' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 200 as ID, 'PL' as ProductLine,'wear_and_tear_brokenearring_JM' as GWLossCauseTypecode,'Wear and Tear- Broken earring backs or posts' as ConformedCauseOfLossDesc,'Partial' as LossGroup,'N' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 201 as ID, 'PL' as ProductLine,'wear_and_tear_brokenstretched_JM' as GWLossCauseTypecode,'Wear and Tear- Broken or stretched pearl strands' as ConformedCauseOfLossDesc,'Partial' as LossGroup,'N' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 202 as ID, 'PL' as ProductLine,'wear_and_tear_thinningcracked_JM' as GWLossCauseTypecode,'Wear and Tear- Thinning or cracked ring shanks' as ConformedCauseOfLossDesc,'Partial' as LossGroup,'N' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 203 as ID, 'PL' as ProductLine,'brandguard_JM' as GWLossCauseTypecode,'BrandGuard' as ConformedCauseOfLossDesc,'Cyber Liability' as LossGroup,'N' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 204 as ID, 'PL' as ProductLine,'cyber_crime_JM' as GWLossCauseTypecode,'Cyber Crime' as ConformedCauseOfLossDesc,'Cyber Liability' as LossGroup,'N' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 205 as ID, 'PL' as ProductLine,'cyber_extortion_JM' as GWLossCauseTypecode,'Cyber Extortion' as ConformedCauseOfLossDesc,'Cyber Liability' as LossGroup,'N' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 206 as ID, 'PL' as ProductLine,'cyber_terrorism_JM' as GWLossCauseTypecode,'Cyber Terrorism' as ConformedCauseOfLossDesc,'Cyber Liability' as LossGroup,'N' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 207 as ID, 'PL' as ProductLine,'multi_media_liability_JM' as GWLossCauseTypecode,'Multi-Media Liability' as ConformedCauseOfLossDesc,'Cyber Liability' as LossGroup,'N' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 208 as ID, 'PL' as ProductLine,'network_asset_protection_JM' as GWLossCauseTypecode,'Network Asset Protection' as ConformedCauseOfLossDesc,'Cyber Liability' as LossGroup,'N' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 209 as ID, 'PL' as ProductLine,'owner_id_theft_JM' as GWLossCauseTypecode,'Business Owner ID Theft' as ConformedCauseOfLossDesc,'Cyber Liability' as LossGroup,'N' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 210 as ID, 'PL' as ProductLine,'pci_dss_assessment_JM' as GWLossCauseTypecode,'PCI DSS Assessment' as ConformedCauseOfLossDesc,'Cyber Liability' as LossGroup,'N' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 211 as ID, 'PL' as ProductLine,'privacy_breach_response_JM' as GWLossCauseTypecode,'Privacy Breach Response Costs etc.' as ConformedCauseOfLossDesc,'Cyber Liability' as LossGroup,'N' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 212 as ID, 'PL' as ProductLine,'privacy_defense_penalties_JM' as GWLossCauseTypecode,'Privacy Regulatory Defense and Penalties' as ConformedCauseOfLossDesc,'Cyber Liability' as LossGroup,'N' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 213 as ID, 'PL' as ProductLine,'security_privacy_liability_JM' as GWLossCauseTypecode,'Security and Privacy Liability' as ConformedCauseOfLossDesc,'Cyber Liability' as LossGroup,'N' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 214 as ID, 'CL' as ProductLine,'construction' as GWLossCauseTypecode,'Faulty Construction' as ConformedCauseOfLossDesc,'Other Property' as LossGroup,'N' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 215 as ID, 'CL' as ProductLine,'armed_robbery_JMIC' as GWLossCauseTypecode,'Armed robbery' as ConformedCauseOfLossDesc,'Crime' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 216 as ID, 'CL' as ProductLine,'armed_robbery_open_rolex_JM' as GWLossCauseTypecode,'Armed Robbery - Open - Rolex' as ConformedCauseOfLossDesc,'Open - On Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 217 as ID, 'CL' as ProductLine,'smash_grab_run_open_rolex_JM' as GWLossCauseTypecode,'Smash, Grab and Run - Open - Rolex' as ConformedCauseOfLossDesc,'Open - On Premises' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 218 as ID, 'CL' as ProductLine,'transit_first_class_JM' as GWLossCauseTypecode,'Transit - USPS - 1st Class' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 219 as ID, 'CL' as ProductLine,'transit_priority_mail_JM' as GWLossCauseTypecode,'Transit - USPS - Priority Mail' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 220 as ID, 'CL' as ProductLine,'transit_fedex_oneday_JM' as GWLossCauseTypecode,'Transit - Fed Ex - 1Day' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 221 as ID, 'CL' as ProductLine,'transit_two_day_JM' as GWLossCauseTypecode,'Transit - 2Day' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 222 as ID, 'CL' as ProductLine,'transit_threeday_ground_JM' as GWLossCauseTypecode,'Transit - 3Day/Ground' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 223 as ID, 'CL' as ProductLine,'transit_ups_JM' as GWLossCauseTypecode,'Transit - UPS - Other' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 224 as ID, 'CL' as ProductLine,'transit_ups_oneday_JM' as GWLossCauseTypecode,'Transit - UPS - 1Day' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 225 as ID, 'CL' as ProductLine,'transit_ups_twoday_JM' as GWLossCauseTypecode,'Transit - UPS - 2Day' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 226 as ID, 'CL' as ProductLine,'transit_ups_threeday_JM' as GWLossCauseTypecode,'Transit - UPS - 3Day' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 227 as ID, 'CL' as ProductLine,'transit_fedex_JM' as GWLossCauseTypecode,'Transit - Fed Ex - Other' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 228 as ID, 'CL' as ProductLine,'transit_usps_other_JM' as GWLossCauseTypecode,'Transit - USPS - Other' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 229 as ID, 'CL' as ProductLine,'transit_fedex_twoday_JM' as GWLossCauseTypecode,'Transit - Fed Ex - 2Day' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 230 as ID, 'CL' as ProductLine,'transit_fedex_threeday_JM' as GWLossCauseTypecode,'Transit - Fed Ex - 3Day' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 231 as ID, 'CL' as ProductLine,'transit_fedex_ground_JM' as GWLossCauseTypecode,'Transit - Fed Ex - Ground' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 232 as ID, 'CL' as ProductLine,'transit_ups_ground_JM' as GWLossCauseTypecode,'Transit - UPS - Ground' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'Y' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 233 as ID, 'CL' as ProductLine,'transit_partial_damage_JM' as GWLossCauseTypecode,'Transit - Partial - Damage' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'N' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 234 as ID, 'CL' as ProductLine,'transit_partial_missingpart_JM' as GWLossCauseTypecode,'Transit - Partial - Missing Part' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'N' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 235 as ID, 'CL' as ProductLine,'transit_partial_repair_JM' as GWLossCauseTypecode,'Transit - Partial - Repair' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'N' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 236 as ID, 'CL' as ProductLine,'transit_totalloss_notdelivered_JM' as GWLossCauseTypecode,'Transit - Total Loss - Not Delivered' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'N' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 237 as ID, 'CL' as ProductLine,'transit_total_loss_empty_JM' as GWLossCauseTypecode,'Transit - Total Loss - Empty' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'N' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 238 as ID, 'CL' as ProductLine,'transit_totalloss_damaged_JM' as GWLossCauseTypecode,'Transit - Total Loss - Damaged' as ConformedCauseOfLossDesc,'Shipping' as LossGroup,'N' as ReportToJSAandJVC,1 as IsActive  UNION ALL
	SELECT 239 as ID, 'PL' as ProductLine,'travel_JM' as GWLossCauseTypecode,'Travel' as ConformedCauseOfLossDesc,'All Other' as LossGroup,'N' as ReportToJSAandJVC,1 as IsActive
)
AS Source --([ID],[ProductLine], [GWLossCauseTypecode],[ConformedCauseOfLossDesc], [LossGroup], [ReportToJSAandJVC], [IsActive]) 
ON Target.ID = Source.ID
WHEN MATCHED THEN UPDATE
SET ProductLine = Source.ProductLine
	,GWLossCauseTypecode = Source.GWLossCauseTypecode
	,ConformedCauseOfLossDesc = Source.ConformedCauseOfLossDesc
	,LossGroup = Source.LossGroup
	,ReportToJSAandJVC = Source.ReportToJSAandJVC
	,IsActive = Source.IsActive
WHEN NOT MATCHED BY TARGET THEN 
	INSERT (ID,ProductLine, GWLossCauseTypecode, ConformedCauseOfLossDesc, LossGroup, ReportToJSAandJVC, IsActive) 
	VALUES (ID,ProductLine, GWLossCauseTypecode, ConformedCauseOfLossDesc, LossGroup, ReportToJSAandJVC, IsActive) 
WHEN NOT MATCHED BY SOURCE THEN 
	DELETE;