use [DataRetention_Home];
DECLARE @ScrubRunIDs TABLE (ScrubRunID BIGINT)
DECLARE @ScrubRunID INT
--Step 1, fetch the  ScrubRunIDs, where Data Lake processing needs to be performed
INSERT @ScrubRunIDs EXEC [dbo].[s_Scrub_Run_Select_Current] @IsDataLake = 1
--Step 2, grab the Primary Key values for HubSpot...
SELECT  [sr].[ScrubRunID], [sd].[DatabaseName], [st].[TableSchema], ' ' as TableName,[st].[PrimaryKey01],[ube].[EntityPK01ValueText] as EntityPK01Value,
cast(format(sr.EndDate,'yyyy-MM-dd') as date) bq_load_date
FROM [dbo].[Scrub_Run_Unbound_Entities_Purge] AS [ube]
INNER JOIN [dbo].[Scrub_Table] AS [st]
    ON [ube].[ScrubTableID] = [st].[ScrubTableID]
INNER JOIN [dbo].[Scrub_Database] AS [sd]
    ON [st].[ScrubDatabaseID] = [sd].[ScrubDatabaseID]
INNER JOIN [dbo].[Scrub_Run] AS [sr]
    ON [ube].[ScrubRunID] = [sr].[ScrubRunID]
WHERE  [sr].[ScrubRunID] IN (SELECT ScrubRunID FROM  @ScrubRunIDs)
AND [sd].[DatabaseName] = 'Mixpanel' AND [st].[TableSchema] = 'Mixpanel'
