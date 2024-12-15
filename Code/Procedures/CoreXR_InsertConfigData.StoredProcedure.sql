/*****
*****   Copyright 2016, 2024 Aaron Morelli
*****
*****   Licensed under the Apache License, Version 2.0 (the "License");
*****   you may not use this file except in compliance with the License.
*****   You may obtain a copy of the License at
*****
*****       http://www.apache.org/licenses/LICENSE-2.0
*****
*****   Unless required by applicable law or agreed to in writing, software
*****   distributed under the License is distributed on an "AS IS" BASIS,
*****   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*****   See the License for the specific language governing permissions and
*****   limitations under the License.
*****
*****	------------------------------------------------------------------------
*****
*****	PROJECT NAME: ChiRho for SQL Server https://github.com/AaronMorelli/ChiRho_SQL
*****
*****	PROJECT DESCRIPTION: A T-SQL toolkit for troubleshooting performance and stability problems on SQL Server instances
*****
*****	FILE NAME: CoreXR_InsertConfigData.StoredProcedure.sql
*****
*****	PROCEDURE NAME: CoreXR_InsertConfigData
*****
*****	AUTHOR:			Aaron Morelli
*****					aaronmorelli@zoho.com
*****					@sqlcrossjoin
*****					sqlcrossjoin.wordpress.com
*****
*****	PURPOSE: Runs at install time and inserts configuration data.
***** */
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_InsertConfigData
/* 
	OUTSTANDING ISSUES: None at this time.

To Execute
------------------------
EXEC @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_InsertConfigData

--use to reset the data:
truncate table @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_ProfilerTraceEvents;
truncate table @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_InstallationConfig;
truncate table @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_CollectionInitiators;
truncate table @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_DBIDNameMapping;
*/
AS
BEGIN
	SET NOCOUNT ON;

	--To prevent this proc from damaging the installation after it has already been run, check for existing data.
	IF EXISTS (SELECT * FROM @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_ProfilerTraceEvents)
		OR EXISTS (SELECT * FROM @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_InstallationConfig)
		OR EXISTS (SELECT * FROM @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_CollectionInitiators)
		OR EXISTS (SELECT * FROM @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_DBIDNameMapping)
	BEGIN
		RAISERROR('The configuration tables are not empty. You must clear these tables first before this procedure will insert config data', 16,1);
		RETURN -2;
	END
	
	INSERT INTO @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_CollectionInitiators 
	(CollectionInitiatorID, CollectionInitiator)
	SELECT 255, N'AutoWho_Executor' UNION ALL		--making the default trace the high key reduces page splits
	SELECT 254, N'ServerEye_Executor' UNION ALL		-- since the default/automated trace will generate collection data
	SELECT 1,   N'sp_XR_SessionViewer' UNION ALL	-- at a *much* higher rate than the sp_XR* procs
	SELECT 2,   N'sp_XR_QueryProgress';

	INSERT INTO @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_InstallationConfig(
		[EngineType],
		[EditionFeatures],
		[SQLVersion],
		[SQLTimeZone],
		[DBNameObjects],
		[SchemaNameObjects],
		[DBNameEndUser],
		[SchemaNameEndUser],
		[InstallDate],
		[InstallDateUTC]
	)
	SELECT
		'@@CHIRHO_ENGINE_TYPE@@',
		'@@CHIRHO_EDITION_FEATURES@@',
		'@@CHIRHO_SQL_VERSION@@',
		REPLACE('@@CHIRHO_SQL_TIMEZONE@@', '"', ''),
		'@@CHIRHO_DB_OBJECTS@@',
		'@@CHIRHO_SCHEMA_OBJECTS@@',
		'@@CHIRHO_DB_ENDUSER@@',
		'@@CHIRHO_SCHEMA_ENDUSER@@',
		GETDATE(),
		GETUTCDATE();

	INSERT INTO @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_DBIDNameMapping(
		[database_name],
		[database_id],
		[create_date],
		[EffectiveStartTimeUTC],
		[EffectiveEndTimeUTC],
		[EffectiveStartTime],
		[EffectiveEndTime]
	)
	SELECT 
		d.name,
		d.database_id, 

		/* For the system DBs, we hardcode a basic time that is always relevant, will not
		    change. So for tempdb, we have the same value all the time, instead of the 
			instance start-up time. (Code that needs to get the instance startup time should
			not use this table!)
			And if our DB is detached and attached to a new SQL instance, and the msdb time
			has changed, then our time will not change.

			This logic, while losing a bit of info (startup time, time that msdb was attached e.g. for a server rebuild
			or migration, or the very old times for master and model), helps to preserve the guarantee
			that our time ranges for a given system database_id will never overlap.
			We only need to consider the scenarios for user databases.

			Thus, the record for the system DBs should only ever be inserted one time, here at config time.
		*/
		CASE WHEN d.database_id in (1, 2, 3, 4) THEN '2000-01-01 00:00:00.000'
			ELSE d.create_date
		END as create_date,

		--This logic, meant to convert a datetime value to its UTC equivalent, does not actually work correctly. In older versions
		--of SQL Server, there is no good way (outside of building some sort of calendar table) to reliably convert an arbitrary
		--historical value over to its UTC equivalent. So for the "legacy" version of ChiRho, we are going to have to live with
		--inexact values. (The "current" versions of ChiRho, which support SQL 2016 onward, will use the correct logic).
		--The datetime values that we live with here will should be correct when we detect the database change within a few 
		--minutes. So going forward, once we've been installed and are running regularly, we'll pick up the correct values.
		--But for DBs that were created in the past, we cannot guarantee that the UTC time actually matches the local time in
		--sys.databases.create_date
		DATEADD(MINUTE, DATEDIFF(MINUTE,GETDATE(), GETUTCDATE()), d.create_date) as EffectiveStartTimeUTC,
		NULL as EffectiveEndTimeUTC,
		CASE WHEN d.database_id in (1, 2, 3, 4) THEN '2000-01-01 00:00:00.000'
			ELSE d.create_date
		END as EffectiveStartTime,
		NULL as EffectiveEndTime
	FROM sys.databases d;

	EXEC @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_InsertProfilerConfigData;

	RETURN 0;
END
GO