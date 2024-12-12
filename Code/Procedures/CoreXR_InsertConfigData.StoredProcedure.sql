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
*/
AS
BEGIN
	SET NOCOUNT ON;

	--To prevent this proc from damaging the installation after it has already been run, check for existing data.
	IF EXISTS (SELECT * FROM @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_ProfilerTraceEvents)
		OR EXISTS (SELECT * FROM @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_InstallationConfig)
		OR EXISTS (SELECT * FROM @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_CollectionInitiators)
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

	EXEC @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_InsertProfilerConfigData;

	EXEC @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_UpdateDBMapping;

	RETURN 0;
END
GO