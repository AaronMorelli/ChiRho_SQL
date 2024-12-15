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
		database_id		INT NOT NULL, 
		database_name   NVARCHAR(256) NOT NULL,
		create_date 	DATETIME NOT NULL,
		scenario 		NVARCHAR(20) NOT NULL,
	);

	INSERT INTO #CurrentDBIDNameMapping (
		database_id, 
		database_name,
		create_date,
		scenario
	)
	SELECT 
		d.database_id, 
		d.name,
		d.create_date,

		/* This CASE block covers all of the scenarios except for when there is a database_name in our 
			"currently-active" set that is no longer in sys.databases at all. (Detecting that would require 
			a LEFT JOIN with the tables reversed, or a FULL OUTER JOIN.)
		*/
		CASE
			WHEN dbm.database_name IS NOT NULL
				AND dbm.database_id = d.database_id
				AND dbm.create_date = d.create_date
				THEN N'NO_CHANGE'  --the most common scenario, and thus placed first even though it breaks up the "db name IS NOT NULL" cases
			WHEN dbm.database_name IS NULL
				THEN N'NEW_DB'  --New, as in brand-new, or at least is not in our "currently-active-set" (it could have existed earlier and been removed)
			WHEN dbm.database_name IS NOT NULL 
				AND dbm.database_id = d.database_id
				AND dbm.create_date < d.create_date  --newer date; e.g. when on a restore or detach/re-attach DBID was reused
				THEN N'NEW_DATE'
			WHEN dbm.database_name IS NOT NULL
				AND dbm.database_id != d.database_ID  --create_date should also be newer
				THEN N'NEW_ID'
			ELSE N'?'
		END as scenario
	FROM sys.databases d
		LEFT OUTER JOIN @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_DBIDNameMapping dbm
			ON dbm.database_name = d.name
			AND dbm.EffectiveEndTime IS NULL  --restrict just to the "currently-active" set in our mapping table
	WHERE d.database_id > 4 --Sys DBs only get inserted into our mapping table once (see the comments in CoreXR_InsertConfigData)
	OPTION(MAXDOP 1);

	
	BEGIN TRANSACTION;

	--Evaluate each row for whether we should close it out.
	--Yes, this statement "updates" every row in the "active set", though in most cases will be non-updating updates...
	-- see the Paul White post for the actual impact. This method of framing the logic is easy for me to understand 
	-- and so for now this approach is "worth it". On most servers, the # of DBs will be in the dozens (at most), 
	-- not hundreds or thousands, and DB changes will not be very common, so our whole dataset will probably fit on one 
	-- page for a long time.
	UPDATE targ
	SET
		EffectiveEndTime = 
			CASE
				WHEN t.scenario = N'NO_CHANGE'
					THEN targ.EffectiveEndTime
				WHEN t.database_name IS NULL  --DB Name no longer in sys.databases, so must have been removed
					THEN @EffectiveTime
				WHEN t.scenario IN (N'NEW_DATE', N'NEW_ID')  --in both these scenarios, DBName is still present, but should have a newer create_date;
					THEN t.create_date						 --that create_date should still be earlier than @EffectiveTime, so that's our end time.
				ELSE N'?' --should never hit this; this will raise an exception
			END,
		EffectiveEndTimeUTC =
			CASE
				WHEN t.scenario = N'NO_CHANGE'
					THEN targ.EffectiveEndTimeUTC
				WHEN t.database_name IS NULL
					THEN @EffectiveTimeUTC
				WHEN t.scenario IN (N'NEW_DATE', N'NEW_ID')
					THEN DATEADD(MINUTE, DATEDIFF(MINUTE,GETDATE(), GETUTCDATE()), t.create_date)
						--See the comment below on why this calculation isn't accurate, but "good enough" for now
				ELSE N'?'
			END
	FROM @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_DBIDNameMapping targ 
		LEFT OUTER JOIN #CurrentDBIDNameMapping t
			ON targ.database_name = t.database_name
	WHERE targ.EffectiveEndTime IS NULL
	AND targ.database_id not in (1,2,3,4)  --never close out system DBs.
	OPTION(MAXDOP 1);


	--Now enter our new entries, not only for brand-new DBs, but also for DBs that had change to their time or ID
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
		d.create_date,

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
		d.create_date as EffectiveStartTime,
		NULL as EffectiveEndTime
	FROM #CurrentDBIDNameMapping t
	WHERE t.scenario IN (N'NEW_DB', N'NEW_DATE', N'NEW_ID');

	COMMIT;

	RETURN 0;
END
GO