delete  from 
            `prod-edl.ref_plecom_quoteapp.t_SavedQuote` 
            where cast(SavedQuote_ID as string) in (SELECT distinct(EntityPK01Value) 
            FROM `prod-edl.ref_90_day_purge.ref_90_day_plecom_purge_audit` WHERE tablename = 't_SavedQuote');
			
delete  from 
            `prod-edl.ref_plecom_quoteapp.t_SavedQuoteItem` 
            where cast(SavedQuoteItem_ID as string) in (SELECT distinct(EntityPK01Value) 
            FROM `prod-edl.ref_90_day_purge.ref_90_day_plecom_purge_audit` WHERE tablename = 't_SavedQuoteItem');
			
			
delete from 
            `prod-edl.ref_plecom_quoteapp.t_ApplicationPayment` 
            where cast(ApplicationPaymentId as string) in (SELECT distinct(EntityPK01Value) 
            FROM `prod-edl.ref_90_day_purge.ref_90_day_plecom_purge_audit` WHERE tablename = 't_ApplicationPayment');

delete from 
            `prod-edl.ref_plecom_quoteapp.t_QuotedJewelryItem` 
            where cast(QuotedJewelryItemId as string) in (SELECT distinct(EntityPK01Value) 
            FROM `prod-edl.ref_90_day_purge.ref_90_day_plecom_purge_audit` WHERE tablename = 't_QuotedJewelryItem');

delete from 
            `prod-edl.ref_plecom_quoteapp.t_Application` 
            where cast(ApplicationId as string) in (SELECT distinct(EntityPK01Value) 
            FROM `prod-edl.ref_90_day_purge.ref_90_day_plecom_purge_audit` WHERE tablename = 't_Application');
			
delete from 
            `prod-edl.ref_plecom_quoteapp.t_Address` 
            where cast(AddressId as string) in (SELECT distinct(EntityPK01Value) 
            FROM `prod-edl.ref_90_day_purge.ref_90_day_plecom_purge_audit` WHERE tablename = 't_Address');
			
	

			
delete from 
            `prod-edl.ref_plecom_quoteapp.t_Person` 
            where cast(PersonId as string) in (SELECT distinct(EntityPK01Value) 
            FROM `prod-edl.ref_90_day_purge.ref_90_day_plecom_purge_audit` WHERE tablename = 't_Person');
		
delete from 
            `prod-edl.ref_plecom_quoteapp.t_ApplicationQuote` 
            where cast(ApplicationQuoteId as string) in (SELECT distinct(EntityPK01Value) 
            FROM `prod-edl.ref_90_day_purge.ref_90_day_plecom_purge_audit` WHERE tablename = 't_ApplicationQuote');
			
delete from 
            `prod-edl.ref_plecom_quoteapp.t_UnderwritingInformation` 
            where cast(UnderwritingInformationId as string) in (SELECT distinct(EntityPK01Value) 
            FROM `prod-edl.ref_90_day_purge.ref_90_day_plecom_purge_audit` WHERE tablename = 't_UnderwritingInformation');