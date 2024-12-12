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
*****	FILE NAME: CoreXR_UpdateDBMapping.StoredProcedure.sql
*****
*****	PROCEDURE NAME: CoreXR_UpdateDBMapping
*****
*****	AUTHOR:			Aaron Morelli
*****					aaronmorelli@zoho.com
*****					@sqlcrossjoin
*****					sqlcrossjoin.wordpress.com
*****
*****	PURPOSE: Since our app stored historical data, and DBs are sometimes detached/re-attached, etc,
*****	 we want to keep a mapping between database_id and db name. (Most of the AutoWho/ServerEye tables just store database_id rather than name).
*****	 We make the (usually-safe, but not always) assumption that 2 DBs with the same name are really the same database.
***** */
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_UpdateDBMapping
/*
	OUTSTANDING ISSUES: None at this time.

To Execute
------------------------
EXEC @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_UpdateDBMapping
*/
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @EffectiveTime DATETIME = GETDATE(),
			@EffectiveTimeUTC DATETIME = GETUTCDATE();

	CREATE TABLE #CurrentDBIDNameMapping (
		database_id	INT NOT NULL, 
		name	    NVARCHAR(256) NOT NULL,
		create_date DATETIME NOT NULL
	);

	INSERT INTO #CurrentDBIDNameMapping (
		database_id, 
		name,
		create_date
	)
	SELECT 
		d.database_id, 
		d.name,

		/* For the system DBs, we hardcode a basic time that is always relevant, will not
		    change. So for tempdb, we have the same value all the time, instead of the 
			instance start-up time. (Code that needs to get the instance startup time should
			not use this table!)
			And if our DB is detached and attached to a new SQL instance, and the msdb time
			has changed, then our time will not change.

			This logic, while losing a bit of info (startup time, time that msdb was attached e.g. for a server rebuild
			or migration, or the very old times for master and model), helps to preserve the guarantee
			that our time ranges for a given database_id will never overlap.
			We only need to consider the scenarios for user databases.

			Thus, the record for the system DBs should only ever be inserted one time.
		*/
		CASE WHEN d.database_id in (1, 2, 3, 4) THEN '2000-01-01 00:00:00.000'
			ELSE d.create_date
		END
	FROM sys.databases d;

	/* Scenarios we need to handle:
		deleted:
			- db name is completely gone (db deleted; not frequent, but normal)
		updated:
			- new database_id/name mapping  (the DB name might have been seen before, or the database_id has been used previously, but these 2 vals never together)
				- NOTE: it is possible for a database_id/name mapping to occur multiple times; an edge case, but possible. Thus, creation time is 
				  the differentiator
		new:
			- DB name never seen before (the easy case, and fairly normal)

		The upshot of these scenarios is that the logic can be this:
			If a given triad of ID/name/creation time is in the "current set" (effective end time is null)
				but not in our temp table, then it needs to be closed out. 
			And any triad in the temp table that is not in our current set needs to be inserted.
	*/
	UPDATE targ 
	SET EffectiveEndTimeUTC = @EffectiveTimeUTC,
		EffectiveEndTime = @EffectiveTime
	FROM @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_DBIDNameMapping targ 
	WHERE targ.EffectiveEndTimeUTC IS NULL
	AND targ.database_id not in (1,2,3,4)  --never close out system DBs.
	AND NOT EXISTS (
		SELECT *
		FROM #CurrentDBIDNameMapping t
		WHERE t.database_id = targ.database_id
		AND t.name = targ.name
		AND t.create_date = targ.create_date
	);

	INSERT INTO @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_DBIDNameMapping (
		database_id, 
		create_date,
		name, 
		EffectiveStartTimeUTC, 
		EffectiveEndTimeUTC, 
		EffectiveStartTime, 
		EffectiveEndTime
	)
	SELECT
		t.database_id, 
		t.create_date,
		t.name,
		--This logic, meant to convert a datetime value to its UTC equivalent, does not actually work correctly. In older versions
		--of SQL Server, there is no good way (outside of building some sort of calendar table) to reliably convert an arbitrary
		--historical value over to its UTC equivalent. So for the "legacy" version of ChiRho, we are going to have to live with
		--inexact values. (The "current" versions of ChiRho, which support SQL 2016 onward, will use the correct logic).
		--The datetime values that we live with here will be correct or pretty close when we detect the database change within a few 
		--minutes. So going forward, once we've been installed and are running regularly, we'll pick up the correct values.
		--But for DBs that were created in the past, we cannot guarantee that the UTC time actually matches the local time in
		--sys.databases.create_date
		DATEADD(MINUTE, DATEDIFF(MINUTE,GETDATE(), GETUTCDATE()), t.create_date) as EffectiveStartTimeUTC,
		NULL AS EffectiveEndTimeUTC,
		t.create_date AS EffectiveStartTime,
		NULL AS EffectiveEndTime
	FROM #CurrentDBIDNameMapping t
	WHERE NOT EXISTS (
		SELECT *
		FROM @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_DBIDNameMapping dm
		WHERE t.database_id = dm.database_id
		AND t.name = dm.name
		AND t.create_date = dm.create_date
	);

	RETURN 0;
END
GO