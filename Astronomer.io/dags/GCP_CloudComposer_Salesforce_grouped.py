import logging
import datetime as dt
import calendar
import time
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from plugins.operators.jm_salesforce_to_gcs import SalesforceToGCSOperatorAllObjects
from plugins.operators.jm_gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from plugins.operators.jm_bq_create_dataset import BigQueryCreateEmptyDatasetOperator
from plugins.operators.jm_SalesforceAuditOperator import SalesforceAuditOperator
from plugins.operators.jm_CompletionOperator import CompletionOperator
from plugins.operators.jm_CreatePoolOperator import CreatePoolOperator, DeletePoolOperator
from airflow.models import Variable

ts = calendar.timegm(time.gmtime())
logging.info(ts)

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'email': 'nreddy@jminsure.com',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

AIRFLOW_ENV = Variable.get('ENV')

if AIRFLOW_ENV.lower() == 'dev':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'dev_edl'
    bq_target_project = 'dev-edl'
    base_gcs_folder = 'DEV_l1'
    prefix_folder = 'DEV_'
    salesforce_connection = 'salesforce_TEST'
elif AIRFLOW_ENV.lower() == 'qa':
    base_bucket = 'jm-edl-landing-wip'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_dev'
    bq_gcp_connector = 'qa_edl'
    bq_target_project = 'qa-edl'
    base_gcs_folder = 'B_QA_l1'
    prefix_folder = 'B_QA_'
    salesforce_connection = 'salesforce_TEST'
elif AIRFLOW_ENV.lower() == 'prod':
    base_bucket = 'jm-edl-landing-prod'
    project = 'jm-dl-landing'
    base_gcp_connector = 'jm_landing_prod'
    bq_target_project = 'prod-edl'
    bq_gcp_connector = 'prod_edl'
    base_gcs_folder = 'l1'
    prefix_folder = ''
    salesforce_connection = 'salesforce_PROD'

source = 'salesforce'
source_abbr = 'sf'

base_file_location = prefix_folder + 'l1/{source}/'.format(source=source)
base_audit_location = prefix_folder + 'l1_audit/{source}/'.format(source=source)
base_schema_location = prefix_folder + 'l1_schema/{source}/'.format(source=source)
base_norm_location = prefix_folder + 'l1_norm/{source}/'.format(source=source)
refine_dataset = '{folder_prefix}ref_{source}'.format(folder_prefix=prefix_folder, source=source)
# source_dag

with DAG(
        'Salesforce_grouped_dag',
        schedule_interval='0 6 * * *',  # "@daily",#dt.timedelta(days=1), #'0 23 1 * *',
        catchup=True,
        max_active_runs=1,
        default_args=default_dag_args) as dag:
    try:
        CHECK_HISTORY = Variable.get(source + '_check')
    except:
        Variable.set(source + '_check', 'True')
        CHECK_HISTORY = 'True'

    # create_salesforce_pool = CreatePoolOperator(task_id='create_salesforce_group_one_pool',
    #                                             name='salesforce-pool-1',
    #                                             slots=6,
    #                                             description='salesforce_group_one_pool',
    #                                             dag=dag)

    create_dataset_ref = BigQueryCreateEmptyDatasetOperator(task_id='check_for_refine_dataset_{source}'.format(source=source),
                                                            dataset_id='{dataset}'.format(dataset=refine_dataset),
                                                            bigquery_conn_id=bq_gcp_connector)

    bit_set = CompletionOperator(task_id='set_source_bit',
                                 source='salesforce_all',
                                 mode='SET')

    source_target_table_list = '{source}_table_list'.format(source=source.lower())
    # This was set up to load in multiple groups, but ultimately changed to 1 group.
    # The groups and for loop can be removed.
    source_object_groups = [1]
    for group_number in source_object_groups:
        refine_table = '{source_abbr}_{object}'.format(source_abbr=source_abbr, object='{object}')

        load_data = SalesforceToGCSOperatorAllObjects(
            task_id='load_data_group_{object}'.format(object=group_number),
            conn_id=salesforce_connection,
            dest_bucket=base_bucket,
            refine_table=refine_table,
            refine_dataset=refine_dataset,
            bq_project=bq_target_project,
            google_cloud_storage_conn_id=base_gcp_connector,
            google_bq_conn_id=bq_gcp_connector,
            base_location=base_file_location,
            base_schema_location=base_schema_location,
            base_norm_location=base_norm_location,
            airflow_var_set=source_target_table_list,
            obj_all=group_number,
            history_check=CHECK_HISTORY,
          #  pool='salesforce-pool-1'
          )

    SKIP_TABLES_LIST = Variable.get('salesforce_skip_tables_list',
                               default_var='contentdocumentlink,contentfolderitem,contentfolderlink,contentfoldermember')
    skip_list =  SKIP_TABLES_LIST.split(',')
    # Once the data is landed, read the list of tables and run the table-specific tasks in parallel.
    try:
        SOURCE_OBJECTS = Variable.get(source_target_table_list).split(',')
    except:
        SOURCE_OBJECTS = ['AcceptedEventRelation',
                            'Account',
                            'AccountContactRole',
                            'AccountFeed',
                            'AccountHistory',
                            'AccountPartner',
                            'AccountShare',
                            'AccountTeamMember',
                            'ActionLinkGroupTemplate',
                            'ActionLinkTemplate',
                            'AdditionalNumber',
                            'Announcement',
                            'ApexClass',
                            'ApexComponent',
                            'ApexEmailNotification',
                            'ApexLog',
                            'ApexPage',
                            'ApexPageInfo',
                            'ApexTestQueueItem',
                            'ApexTestResult',
                            'ApexTestResultLimits',
                            'ApexTestRunResult',
                            'ApexTestSuite',
                            'ApexTrigger',
                            'AppMenuItem',
                            'Appointment__History',
                            'Appointment__c',
                            'Approval',
                            'Asset',
                            'AssetFeed',
                            'AssetHistory',
                            'AssetShare',
                            'AssignmentRule',
                            'AssistantRecommendation',
                            'AssistantRecommendationShare',
                            'AsyncApexError__Share',
                            'AsyncApexError__c',
                            'AsyncApexJob',
                            'Attachment',
                            'AuraDefinition',
                            'AuraDefinitionBundle',
                            'AuraDefinitionBundleInfo',
                            'AuraDefinitionInfo',
                            'AuthConfig',
                            'AuthConfigProviders',
                            'AuthProvider',
                            'AuthSession',
                            'BackgroundOperation',
                            'Ballpark_Quote__History',
                            'Ballpark_Quote__Share',
                            'Ballpark_Quote__c',
                            'BrandTemplate',
                            'BusinessHours',
                            'BusinessProcess',
                            'Business_Plan__History',
                            'Business_Plan__c',
                            'CXP_Settings__c',
                            'CallCenter',
                            'Campaign',
                            'CampaignFeed',
                            'CampaignHistory',
                            'CampaignMember',
                            'CampaignMemberStatus',
                            'CampaignShare',
                            'CarePlanMasterProcess__c',
                            'Care_Plan_Claims__c',
                            'Care_Plan__c',
                            'Case',
                            'CaseArticle',
                            'CaseComment',
                            'CaseContactRole',
                            'CaseFeed',
                            'CaseHistory',
                            'CaseShare',
                            'CaseSolution',
                            'CaseStatus',
                            'CaseTeamMember',
                            'CaseTeamRole',
                            'CaseTeamTemplate',
                            'CaseTeamTemplateMember',
                            'CaseTeamTemplateRecord',
                            'CategoryData',
                            'CategoryNode',
                            'ChatterActivity',
                            'ClientBrowser',
                            'CollaborationGroup',
                            'CollaborationGroupFeed',
                            'CollaborationGroupMember',
                            'CollaborationGroupMemberRequest',
                            'CollaborationGroupRecord',
                            'CollaborationInvitation',
                            'Commercial_Policy__History',
                            'Commercial_Policy__c',
                            'Community',
                            'ConferenceNumber',
                            'ConnectedApplication',
                            'Contact',
                            'ContactFeed',
                            'ContactHistory',
                            'ContactShare',
                            'ContactsMasterProcess__c',
                            'ContentAsset',
                            'ContentDistribution',
                            'ContentDistributionView',
                            'ContentDocument',
                            'ContentDocumentFeed',
                            'ContentDocumentHistory',
                            'ContentDocumentLink',
                            'ContentFolder',
                            'ContentFolderItem',
                            'ContentFolderLink',
                            'ContentFolderMember',
                            'ContentNote',
                            'ContentVersion',
                            'ContentVersionHistory',
                            'ContentWorkspace',
                            'ContentWorkspaceDoc',
                            'Contract',
                            'ContractContactRole',
                            'ContractFeed',
                            'ContractHistory',
                            'ContractStatus',
                            'CorsWhitelistEntry',
                            'CronJobDetail',
                            'CronTrigger',
                            'CustomBrand',
                            'CustomBrandAsset',
                            'CustomObjectUserLicenseMetrics',
                            'CustomPermission',
                            'CustomPermissionDependency',
                            'DAPVaLY__History',
                            'DAPVaLY__c',
                            'DNBoptimizer__DNB_FamilyTreeData__c',
                            'DNBoptimizer__DNB_SIC__Share',
                            'DNBoptimizer__DNB_SIC__c',
                            'DNBoptimizer__DnBCompanyRecord__Share',
                            'DNBoptimizer__DnBCompanyRecord__c',
                            'DNBoptimizer__DnBContactRecord__Share',
                            'DNBoptimizer__DnBContactRecord__c',
                            'Dashboard',
                            'DashboardComponent',
                            'DashboardComponentFeed',
                            'DashboardFeed',
                            'DataAssessmentFieldMetric',
                            'DataAssessmentMetric',
                            'DataAssessmentValueMetric',
                            'DataLakeCalloutParameters__mdt',
                            'DataStatistics',
                            'DataType',
                            'DatacloudAddress',
                            'DeclinedEventRelation',
                            'Device__History',
                            'Device__c',
                            'Document',
                            'DocumentAttachmentMap',
                            'Domain',
                            'DomainSite',
                            'DuplicateRecordItem',
                            'DuplicateRecordSet',
                            'DuplicateRule',
                            'EmailDomainKey',
                            'EmailMessage',
                            'EmailMessageRelation',
                            'EmailServicesAddress',
                            'EmailServicesFunction',
                            'EmailTemplate',
                            'EntityDefinition',
                            'EntityParticle',
                            'EntitySubscription',
                            'Event',
                            'EventBusSubscriber',
                            'EventFeed',
                            'EventLogFile',
                            'EventRecurrenceException',
                            'EventRelation',
                            'ExternalDataSource',
                            'ExternalDataUserAuth',
                            'ExternalEvent',
                            'ExternalEventMapping',
                            'ExternalEventMappingShare',
                            'ExternalSocialAccount',
                            'FeedAttachment',
                            'FeedComment',
                            'FeedItem',
                            'FeedPollChoice',
                            'FeedPollVote',
                            'FeedRevision',
                            'FieldDefinition',
                            'FieldPermissions',
                            'Field_Trip__Field_Analysis__History',
                            'Field_Trip__Field_Analysis__c',
                            'Field_Trip__Field_Analytic_Config__Share',
                            'Field_Trip__Field_Analytic_Config__c',
                            'Field_Trip__Logistics__c',
                            'Field_Trip__Object_Analysis__History',
                            'Field_Trip__Object_Analysis__Share',
                            'Field_Trip__Object_Analysis__c',
                            'FileSearchActivity',
                            'FiscalYearSettings',
                            'FlexQueueItem',
                            'FlowInterview',
                            'FlowInterviewShare',
                            'Folder',
                            'ForecastingAdjustment',
                            'ForecastingCategoryMapping',
                            'ForecastingFact',
                            'ForecastingItem',
                            'ForecastingOwnerAdjustment',
                            'ForecastingQuota',
                            'ForecastingShare',
                            'ForecastingType',
                            'ForecastingTypeToCategory',
                            'ForecastingUserPreference',
                            'GrantedByLicense',
                            'Group',
                            'GroupMember',
                            'Holiday',
                            'Idea',
                            'IdeaComment',
                            'InstalledMobileApp',
                            'Item_Description__History',
                            'Item_Description__Share',
                            'Item_Description__c',
                            'JMCP_Business_Org__History',
                            'JMCP_Business_Org__c',
                            'JMCP_Location__History',
                            'JMCP_Location__c',
                            'JMCP_Pricing_Table__History',
                            'JMCP_Pricing_Table__c',
                            'JMI_Envrionments__c',
                            'JM_Partner_Community__Share',
                            'JM_Partner_Community__c',
                            'JM_PaymentUS_RedirectURL__c',
                            'KnowledgeArticle',
                            'KnowledgeArticleVersion',
                            'KnowledgeArticleVersionHistory',
                            'KnowledgeArticleViewStat',
                            'KnowledgeArticleVoteStat',
                            'Knowledge__DataCategorySelection',
                            'Knowledge__ViewStat',
                            'Knowledge__VoteStat',
                            'Knowledge__ka',
                            'Knowledge__kav',
                            'KnowledgeableUser',
                            'Lead',
                            'LeadFeed',
                            'LeadHistory',
                            'LeadShare',
                            'LeadStatus',
                            'Lead_Assignment_Zips__mdt',
                            'LinkedArticle',
                            'LinkedArticleFeed',
                            'LinkedArticleHistory',
                            'ListView',
                            'ListViewChart',
                            'ListViewChartInstance',
                            'LoginGeo',
                            'LoginHistory',
                            'LoginIp',
                            'Macro',
                            'MacroHistory',
                            'MacroInstruction',
                            'MacroShare',
                            'MailmergeTemplate',
                            'MasterOpportunityPB__c',
                            'MatchingInformation',
                            'MatchingRule',
                            'MatchingRuleItem',
                            'Member_Benefit_Estimate__History',
                            'Member_Benefit_Estimate__c',
                            'Member_Benefit_Statement__History',
                            'Member_Benefit_Statement__c',
                            'MobileApplicationDetail',
                            'NamedCredential',
                            'NavigationLinkSet',
                            'NavigationMenuItem',
                            'Network',
                            'NetworkActivityAudit',
                            'NetworkMember',
                            'NetworkMemberGroup',
                            'NetworkModeration',
                            'NetworkPageOverride',
                            'NetworkSelfRegistration',
                            'Newsletter__Share',
                            'Newsletter__c',
                            'Note',
                            'OauthToken',
                            'ObjectPermissions',
                            'Opportunity',
                            'OpportunityCompetitor',
                            'OpportunityContactRole',
                            'OpportunityFeed',
                            'OpportunityFieldHistory',
                            'OpportunityHistory',
                            'OpportunityLineItem',
                            'OpportunityPartner',
                            'OpportunityShare',
                            'OpportunityStage',
                            'Opportunity__hd',
                            'Order',
                            'OrderFeed',
                            'OrderHistory',
                            'OrderItem',
                            'OrderItemFeed',
                            'OrderItemHistory',
                            'OrderShare',
                            'OrgWideEmailAddress',
                            'Organization',
                            'OwnerChangeOptionInfo',
                            'PackageLicense',
                            'Partner',
                            'PartnerRole',
                            'Payment_Schedule__c',
                            'Period',
                            'PermissionSet',
                            'PermissionSetAssignment',
                            'PermissionSetLicense',
                            'PermissionSetLicenseAssign',
                            'PicklistValueInfo',
                            'PlatformAction',
                            'PlatformCachePartition',
                            'PlatformCachePartitionType',
                            'Pricebook2',
                            'Pricebook2History',
                            'PricebookEntry',
                            'ProcessDefinition',
                            'ProcessInstance',
                            'ProcessInstanceNode',
                            'ProcessInstanceStep',
                            'ProcessInstanceWorkitem',
                            'ProcessNode',
                            'Product2',
                            'Product2Feed',
                            'Product2History',
                            'Profile',
                            'ProfileSkill',
                            'ProfileSkillEndorsement',
                            'ProfileSkillEndorsementFeed',
                            'ProfileSkillEndorsementHistory',
                            'ProfileSkillFeed',
                            'ProfileSkillHistory',
                            'ProfileSkillShare',
                            'ProfileSkillUser',
                            'ProfileSkillUserFeed',
                            'ProfileSkillUserHistory',
                            'Program__Feed',
                            'Program__History',
                            'Program__c',
                            'Publisher',
                            'PushTopic',
                            'QueueSobject',
                            'QuickText',
                            'QuickTextHistory',
                            'QuickTextShare',
                            'Quote',
                            'QuoteDocument',
                            'QuoteFeed',
                            'QuoteLineItem',
                            'QuoteShare',
                            'RecentlyViewed',
                            'RecordType',
                            'RelationshipDomain',
                            'RelationshipInfo',
                            'Report',
                            'ReportFeed',
                            'ReputationLevel',
                            'ReputationPointsRule',
                            'SamlSsoConfig',
                            'Scontrol',
                            'SearchActivity',
                            'SearchLayout',
                            'SearchPromotionRule',
                            'SecureAgentsCluster',
                            'ServiceTokens__c',
                            'SessionPermSetActivation',
                            'SetupAuditTrail',
                            'SetupEntityAccess',
                            'Site',
                            'SiteDetail',
                            'SiteFeed',
                            'SiteHistory',
                            'SocialPersona',
                            'SocialPersonaHistory',
                            'SocialPost',
                            'SocialPostFeed',
                            'SocialPostHistory',
                            'SocialPostShare',
                            'Solution',
                            'SolutionFeed',
                            'SolutionHistory',
                            'SolutionStatus',
                            'StaticResource',
                            'StreamingChannel',
                            'StreamingChannelShare',
                            'Task',
                            'TaskFeed',
                            'TaskPriority',
                            'TaskRecurrenceException',
                            'TaskStatus',
                            'TenantUsageEntitlement',
                            'Territory_Assignment__History',
                            'Territory_Assignment__Share',
                            'Territory_Assignment__c',
                            'TestSuiteMembership',
                            'ThirdPartyAccountLink',
                            'TodayGoal',
                            'TodayGoalShare',
                            'Topic',
                            'TopicAssignment',
                            'TopicFeed',
                            'Trade_Associations__History',
                            'Trade_Associations__c',
                            'Tradeshow_Assignment__c',
                            'TransGuardianWebServiceSetup__c',
                            'UndecidedEventRelation',
                            'User',
                            'UserAccountTeamMember',
                            'UserAppInfo',
                            'UserAppMenuCustomization',
                            'UserAppMenuCustomizationShare',
                            'UserAppMenuItem',
                            'UserCustomBadge',
                            'UserEntityAccess',
                            'UserFeed',
                            'UserFieldAccess',
                            'UserLicense',
                            'UserListView',
                            'UserListViewCriterion',
                            'UserLogin',
                            'UserPackageLicense',
                            'UserPreference',
                            'UserProvAccount',
                            'UserProvAccountStaging',
                            'UserProvMockTarget',
                            'UserProvisioningConfig',
                            'UserProvisioningLog',
                            'UserProvisioningRequest',
                            'UserProvisioningRequestShare',
                            'UserRecordAccess',
                            'UserRole',
                            'UserShare',
                            'User_Account__History',
                            'User_Account__c',
                            'User__History',
                            'User__c',
                            'VerificationHistory',
                            'Vote',
                            'WebLink',
                            'WorkAccess',
                            'WorkAccessShare',
                            'WorkBadge',
                            'WorkBadgeDefinition',
                            'WorkBadgeDefinitionFeed',
                            'WorkBadgeDefinitionHistory',
                            'WorkBadgeDefinitionShare',
                            'WorkOrder',
                            'WorkOrderFeed',
                            'WorkOrderHistory',
                            'WorkOrderLineItem',
                            'WorkOrderLineItemFeed',
                            'WorkOrderLineItemHistory',
                            'WorkOrderShare',
                            'WorkThanks',
                            'WorkThanksShare',
                            'acdir__CUD_ZoneData__mdt',
                            'acdir__CommunityMembersDirectoryFieldSet__mdt',
                            'acdir__Members_Setting__c',
                            'acdir__Members_Settings__c',
                            'reCaptcha__c',
                            'rh2__Filter__c',
                            'rh2__PS_Describe__c',
                            'rh2__PS_Exception__c',
                            'rh2__PS_Export_Rollups__History',
                            'rh2__PS_Export_Rollups__c',
                            'rh2__PS_Object_Realtime__c',
                            'rh2__PS_Queue__c',
                            'rh2__PS_Rollup_Conditions__c',
                            'rh2__PS_Rollup_Dummy__History',
                            'rh2__PS_Rollup_Dummy__Share',
                            'rh2__PS_Rollup_Dummy__c',
                            'rh2__PS_Rollup_Group__Share',
                            'rh2__PS_Rollup_Group__c',
                            'rh2__PS_Settings__c',
                            'rh2__RH_Job__c',
                            'rh2__Rollup_Helper_Record_Scope__c',
                            'sftplib__DataLoad_Service__History',
                            'sftplib__DataLoad_Service__c',
                            'sftplib__Fields_Mapping__History',
                            'sftplib__Fields_Mapping__c',
                            'sftplib__Log__c',
                            'sftplib__Saved_Mapping__Share',
                            'sftplib__Saved_Mapping__c',
                            'sftplib__TestMaster__History',
                            'sftplib__TestMaster__c']
    #new_object = [x for x in SOURCE_OBJECTS if 'Opportunity' in x]
    for source_object in  SOURCE_OBJECTS:
        source_object=source_object.replace(' ', '')
        #table_skip_list = ['contentdocumentlink', 'contentfolderitem', 'contentfolderlink', 'contentfoldermember']
        if source_object.lower() in skip_list:


            audit_data_lnd = EmptyOperator(task_id='audit_l1_data_{obj}'.format(obj=source_object.lower())
                                           )

            refine_data =EmptyOperator(
                task_id='refine_data_{object}'.format(object=source_object.lower()))

            audit_data_ref = EmptyOperator(task_id='audit_ref_data_{obj}'.format(obj=source_object.lower()))


        else:
            audit_file_name = '{location_base}{date}/l1_audit_results_{object}.json'.format(
                location_base=base_audit_location,
                object=source_object.lower(), date="{{ ds_nodash }}")
            metadata_file_name = '{base_location}{object}/{extract_date}/l1_metadata_{object}.json'.format(
                base_location=base_file_location, object=source_object.lower(), extract_date="{{ ds_nodash }}")
            audit_data_lnd = SalesforceAuditOperator(task_id='audit_l1_data_{obj}'.format(obj=source_object.lower()),
                                                     bucket=base_bucket,
                                                     project=project,
                                                     dataset=refine_dataset,
                                                     target_gcs_bucket=base_bucket,
                                                     google_cloud_storage_conn_id=base_gcp_connector,
                                                     google_cloud_bq_conn_id=bq_gcp_connector,
                                                     source=source,
                                                     metadata_filename=metadata_file_name,
                                                     audit_filename=audit_file_name,
                                                     check_landing_only=True,
                                                     table=refine_table.format(object=source_object.lower()),
                        #                             pool='salesforce-pool-1'
                        )

            refine_data = GoogleCloudStorageToBigQueryOperator(
                task_id='refine_data_{object}'.format(object=source_object.lower()),
                bucket=base_bucket,
                source_objects=[
                    '{base_norm_location}{obj}/{extract_date}/l1_norm_*'.format(
                        base_norm_location=base_norm_location, obj=source_object.lower(), extract_date="{{ ds_nodash }}")],
                destination_project_dataset_table='{project}.{dataset}.{table}${date}'.format(
                    project=bq_target_project,
                    dataset=refine_dataset,
                    table=refine_table.format(object=source_object.lower()),
                    date="{{ ds_nodash }}"),
                schema_fields=[],
                schema_object='{base_schema_location}{obj}/{extract_date}/l1_schema_sf_{obj}.json'.format(
                    base_schema_location=base_schema_location, obj=source_object.lower(), extract_date="{{ ds_nodash }}"),
                source_format='NEWLINE_DELIMITED_JSON',
                compression='NONE',
                create_disposition='CREATE_IF_NEEDED',
                skip_leading_rows=0,
                write_disposition='WRITE_TRUNCATE',
                max_bad_records=0,
                bigquery_conn_id=bq_gcp_connector,
                google_cloud_storage_conn_id=base_gcp_connector,
                schema_update_options=['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION'],
                src_fmt_configs={},
                autodetect=False,
            #    pool='salesforce-pool-1'
            )

            audit_data_ref = SalesforceAuditOperator(task_id='audit_ref_data_{obj}'.format(obj=source_object.lower()),
                                                     bucket=base_bucket,
                                                     project=bq_target_project,
                                                     dataset=refine_dataset,
                                                     target_gcs_bucket=base_bucket,
                                                     google_cloud_storage_conn_id=base_gcp_connector,
                                                     google_cloud_bq_conn_id=bq_gcp_connector,
                                                     source=source,
                                                     metadata_filename=metadata_file_name,
                                                     audit_filename=audit_file_name,
                                                     check_landing_only=False,
                                                     table=refine_table.format(object=source_object.lower()),
                                        #             pool='salesforce-pool-1'
                                        )

        create_dataset_ref >> load_data >> audit_data_lnd >> refine_data >> audit_data_ref >> bit_set