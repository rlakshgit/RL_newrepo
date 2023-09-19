-- tag: v_AgencyAttributes - tag ends/
/**********************************************************************************************
	Kimberlite - Building Block
		Reference and Lookup Tables
			v_AgencyAttributes.sql
	-------------------------------------------------------------------------------------------
	--Change Log--
	-------------------------------------------------------------------------------------------
	06/08/2023	DROBAK		Init

	-------------------------------------------------------------------------------------------
************************************************************************************************
*/

--CREATE VIEW `qa-edl.B_QA_ref_kimberlite.v_AgencyAttributes`
CREATE OR REPLACE VIEW `{project}.{dest_dataset}.v_AgencyAttributes`
AS(
SELECT
	SourceSystem
	,AgencyKey
	,AgencySubCode
	,ProducerCodePublicID
	,PCOrganizationPublicID
	,AgencyMasterCode
	,AgencyMasterName
	,AgencyCode
	,AgencyName
	,AgencyDescription
	,AgencyAddress1
	,AgencyAddress2
	,AgencyCity
	,AgencyStateCode
	,AgencyCounty
	,AgencyPostalCode
	,AgencyMailingAddress1
	,AgencyMailingAddress2
	,AgencyMailingCity
	,AgencyMailingState
	,AgencyMailingPostalCode
	,AgencyPrimaryPhoneNumber
	,AgencyPrimaryEmailAddress
	,AgencySubName
	,AgencySubGroupType
	,AgencySubActivationDate
	,AgencySubTerminationDate
	,AgencySubPrimaryWritingState
	,AgencySubStatus
	,IsServiceCenter
	,OriginalContractDate
	,OriginalApptDate
	,IsStrategicPartner
	,Retired
	,bq_load_date

FROM `{project}.{core_dataset}.Agency` AS Agency
);
--SELECT * FROM `qa-edl.B_QA_ref_kimberlite.v_AgencyAttributes`