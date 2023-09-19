CREATE TABLE `prod-edl.dl_metrics.dl_dag_sla`
	(    
		Dag_name	STRING,
		Frequency	STRING,
		Sla_Time	TIME,
		bq_load_date	DATE
	)

INSERT INTO `prod-edl.dl_metrics.dl_dag_sla` VALUES ('BillingCenter_Prioritized_Tables','daily',time(13,00,00),current_date);
INSERT INTO `prod-edl.dl_metrics.dl_dag_sla` VALUES ('CDP_dag','daily',time(14:30,00,00),current_date);
INSERT INTO `prod-edl.dl_metrics.dl_dag_sla` VALUES ('ClaimCenter_Prioritized_Tables','daily',time(14,00,00),current_date);
INSERT INTO `prod-edl.dl_metrics.dl_dag_sla` VALUES ('Hubspot_Gold','daily',time(11,00,00),current_date);
INSERT INTO `prod-edl.dl_metrics.dl_dag_sla` VALUES ('Mixpanel_dag','daily',time(11,00,00),current_date);
INSERT INTO `prod-edl.dl_metrics.dl_dag_sla` VALUES ('Mixpanel_People_dag','daily',time(11,00,00),current_date);
INSERT INTO `prod-edl.dl_metrics.dl_dag_sla` VALUES ('nice_gold','daily',time(13,00,00),current_date);
INSERT INTO `prod-edl.dl_metrics.dl_dag_sla` VALUES ('PolicyCenter_Prioritized_Tables','daily',time(13,00,00),current_date);
INSERT INTO `prod-edl.dl_metrics.dl_dag_sla` VALUES ('PLEcom_dag','daily',time(13,00,00),current_date);
INSERT INTO `prod-edl.dl_metrics.dl_dag_sla` VALUES ('PL_Personalization_dag','daily',time(11,00,00),current_date);
INSERT INTO `prod-edl.dl_metrics.dl_dag_sla` VALUES ('QuickQuote_Gold','daily',time(14,00,00),current_date);
INSERT INTO `prod-edl.dl_metrics.dl_dag_sla` VALUES ('ratabase_dag','daily',time(11,00,00),current_date);
INSERT INTO `prod-edl.dl_metrics.dl_dag_sla` VALUES ('experian_dag_local','Monthly_load(daily dummy load)',time(15,00,00),current_date);
INSERT INTO `prod-edl.dl_metrics.dl_dag_sla` VALUES ('semimanaged_promoted_morning_dag','daily',time(14,00,00),current_date);