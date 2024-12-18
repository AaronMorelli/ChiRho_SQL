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
*****	FILE NAME: CoreXR_InsertProfilerConfigData.StoredProcedure.sql
*****
*****	PROCEDURE NAME: CoreXR_InsertProfilerConfigData
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
CREATE PROCEDURE @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_InsertProfilerConfigData
/* 
	OUTSTANDING ISSUES: None at this time.

To Execute
------------------------
EXEC @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_InsertProfilerConfigData
*/
AS
BEGIN
	SET NOCOUNT ON;
	IF EXISTS (SELECT * FROM @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_ProfilerTraceEvents)
	BEGIN
		RAISERROR('The Profiler configuration table is not empty. You must clear this table first before this procedure will insert config data', 16,1);
		RETURN -2;
	END

	--For each of the main groupings (EventGroups) that we define ("xr_default", "seeouterbatch", "perfcommon", "perfdetailed"),
	--we insert a set of trace categories and their events that are relevant for that conceptual grouping. 
	--(The xr_default EventGroup contains all of the categories and events, though many of them are isEnabled='N')

	/****  EventGroup=xr_default  - trace categories are handled in alphabetical order 
	****/
	INSERT INTO @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_ProfilerTraceEvents
	(EventGroup, trace_event_id, event_name, category_name, isEnabled)
	SELECT N'xr_default',
		te.trace_event_id, 
		event_name = te.name, 
		category_name = tc.name,
		isEnabled = N'N'
	FROM sys.trace_events te
		INNER JOIN sys.trace_categories tc
			ON te.category_id = tc.category_id
	WHERE 1=1  --the "xr_default" trace includes all of the categories and events, though in practice 
	-- some of these (e.g. Broker events, Query Notifications, etc) will never be useful. They are there if needed.
	;

	--For each category in the "xr_default" EventGroup, set the most useful events enabled

	--category=Broker - we do not enable any for this category

	--category=CLR
	UPDATE @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_ProfilerTraceEvents
	SET isEnabled = N'Y'
	WHERE category_name = N'CLR'
	--This category only has 1 event: Assembly Load
	;

	--category=Cursors
	UPDATE @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_ProfilerTraceEvents
	SET isEnabled = N'Y'
	WHERE category_name = N'Cursors'
	--Category events:
		--CursorClose
		--CursorExecute
		--CursorImplicitConversion
		--CursorOpen
		--CursorPrepare
		--CursorRecompile
		--CursorUnprepare
	;

	--category=Database
	UPDATE @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_ProfilerTraceEvents
	SET isEnabled = N'Y'
	WHERE category_name = N'Database'
	AND event_name IN (
		N'Data File Auto Grow',
		N'Log File Auto Grow'
		--Other category events that we could enable:
			--Data File Auto Shrink
			--Database Mirroring Connection
			--Database Mirroring State Change
			--Log File Auto Shrink
	);

	--category=Deprecation - we do not enable any for this category

	--category=Errors and Warnings
	UPDATE @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_ProfilerTraceEvents
	SET isEnabled = N'Y'
	WHERE category_name = N'Errors and Warnings'
	AND event_name IN (
		N'Exception',
		N'Execution Warnings',
		N'User Error Message',
		N'Attention'

		/*Here is the full set of events for this category:
				Remember that this Profiler trace is "spid-filtered", and some of these events are generated
				on system spids, so would not be caught
			Attention
			Background Job Error
			Bitmap Warning
			Blocked process report
			CPU threshold exceeded
			Database Suspect Data Page
			ErrorLog
			EventLog
			Exception
			Exchange Spill Event
			Execution Warnings
			Hash Warning
			Missing Column Statistics
			Missing Join Predicate
			Sort Warnings
			User Error Message

			The perf-related ones are enabled by default in the perfdetailed EventGroup (see below)
		*/
	);

	--category=Full text - we do not enable any for this category

	--category=Locks
	UPDATE @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_ProfilerTraceEvents
	SET isEnabled = N'Y'
	WHERE category_name = N'Locks'
	AND event_name IN (
		
		N'Lock:Cancel',
		N'Lock:Escalation',
		N'Lock:Timeout',
		N'Lock:Timeout (timeout > 0)'

		/* events for this category
		Deadlock graph  --Deadlocks occur on system SPIDs, and user should use the built-in XE trace or their own tracing for those
		Lock:Acquired
		Lock:Cancel
		Lock:Deadlock
		Lock:Deadlock Chain
		Lock:Escalation
		Lock:Released
		Lock:Timeout
		Lock:Timeout (timeout > 0)
		*/
	);

	--category=Objects
	UPDATE @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_ProfilerTraceEvents
	SET isEnabled = N'Y'
	WHERE category_name = N'Objects'
	AND event_name IN (
		--These 3 are all of the events in this category
		N'Object:Altered',
		N'Object:Created',
		N'Object:Deleted'
	);

	--category=OLEDB
	UPDATE @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_ProfilerTraceEvents
	SET isEnabled = N'Y'
	WHERE category_name = N'OLEDB'
	/* the events for this category
		OLEDB Call Event
		OLEDB DataRead Event
		OLEDB Errors
		OLEDB Provider Information
		OLEDB QueryInterface Event
	 */
	;

	--category=Performance
	UPDATE @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_ProfilerTraceEvents
	SET isEnabled = N'Y'
	WHERE category_name = N'Performance'
	AND event_name IN (
		N'Auto Stats',
		N'Showplan XML Statistics Profile'

		/* all of the events in this category 
		Auto Stats
		Degree of Parallelism
		Performance statistics
		Plan Guide Successful
		Plan Guide Unsuccessful
		Showplan All
		Showplan All For Query Compile
		Showplan Statistics Profile
		Showplan Text
		Showplan Text (Unencoded)
		Showplan XML
		Showplan XML For Query Compile
		Showplan XML Statistics Profile
		SQL:FullTextQuery
		*/
	);

	--category=Progress Report
	UPDATE @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_ProfilerTraceEvents
	SET isEnabled = N'Y'
	WHERE category_name = N'Progress Report'
		--the only event here is "Progress Report: Online Index Operation"
	;

	--category=Query Notifications - we do not enable any for this category

	--category=Scans
	UPDATE @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_ProfilerTraceEvents
	SET isEnabled = N'Y'
	WHERE category_name = N'Scans'
		--only 2 events in this category:
		--Scan:Started
		--Scan:Stopped
	;

	--category=Security Audit
	UPDATE @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_ProfilerTraceEvents
	SET isEnabled = N'Y'
	WHERE category_name = N'Security Audit'
	AND event_name = N'Audit Change Database Owner'
	/*We just turn on 1 security audit event. If someone really wants to use this for security stuff, they need
	  to think through which events they want
	  all category events:
			Audit Add DB User Event
			Audit Add Login to Server Role Event
			Audit Add Member to DB Role Event
			Audit Add Role Event
			Audit Addlogin Event
			Audit App Role Change Password Event
			Audit Backup/Restore Event
			Audit Broker Conversation
			Audit Broker Login
			Audit Change Audit Event
			Audit Change Database Owner
			Audit Database Management Event
			Audit Database Mirroring Login
			Audit Database Object Access Event
			Audit Database Object GDR Event
			Audit Database Object Management Event
			Audit Database Object Take Ownership Event
			Audit Database Operation Event
			Audit Database Principal Impersonation Event
			Audit Database Principal Management Event
			Audit Database Scope GDR Event
			Audit DBCC Event
			Audit Fulltext
			Audit Login
			Audit Login Change Password Event
			Audit Login Change Property Event
			Audit Login Failed
			Audit Login GDR Event
			Audit Logout
			Audit Object Derived Permission Event
			Audit Schema Object Access Event
			Audit Schema Object GDR Event
			Audit Schema Object Management Event
			Audit Schema Object Take Ownership Event
			Audit Server Alter Trace Event
			Audit Server Object GDR Event
			Audit Server Object Management Event
			Audit Server Object Take Ownership Event
			Audit Server Operation Event
			Audit Server Principal Impersonation Event
			Audit Server Principal Management Event
			Audit Server Scope GDR Event
			Audit Server Starts And Stops
			Audit Statement Permission Event
	*/
	;

	--category=Server - we do not enable any for this category

	--category=Sessions
	UPDATE @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_ProfilerTraceEvents
	SET isEnabled = N'Y'
	WHERE category_name = N'Sessions'
	/* all events for this category:
		ExistingConnection
		PreConnect:Completed
		PreConnect:Starting
	 */
	;

	--category=Stored Procedures
	UPDATE @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_ProfilerTraceEvents
	SET isEnabled = N'Y'
	WHERE category_name = N'Stored Procedures'
	AND event_name IN (
		N'RPC:Completed',
		N'SP:Completed',
		N'SP:StmtCompleted'

		/* all events for this category
			RPC Output Parameter
			RPC:Completed
			RPC:Starting
			SP:CacheHit
			SP:CacheInsert
			SP:CacheMiss
			SP:CacheRemove
			SP:Completed
			SP:Recompile
			SP:Starting
			SP:StmtCompleted
			SP:StmtStarting
		 */
	);

	--category=Transactions
	UPDATE @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_ProfilerTraceEvents
	SET isEnabled = N'Y'
	WHERE category_name = N'Transactions'
	AND event_name = N'TransactionLog'
		/* All events for this category
			DTCTransaction
			SQLTransaction
			TM: Begin Tran completed
			TM: Begin Tran starting
			TM: Commit Tran completed
			TM: Commit Tran starting
			TM: Promote Tran completed
			TM: Promote Tran starting
			TM: Rollback Tran completed
			TM: Rollback Tran starting
			TM: Save Tran completed
			TM: Save Tran starting
			TransactionLog
		 */
	;

	--category=TSQL
	UPDATE @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_ProfilerTraceEvents
	SET isEnabled = N'Y'
	WHERE category_name = N'TSQL'
	AND event_name IN (
		N'SQL:BatchCompleted',
		N'SQL:StmtCompleted'
		/* all events for this category
			Exec Prepared SQL
			Prepare SQL
			SQL:BatchCompleted
			SQL:BatchStarting
			SQL:StmtCompleted
			SQL:StmtRecompile
			SQL:StmtStarting
			Unprepare SQL
			XQuery Static Type
		 */
	);

	--category=User configurable
	UPDATE @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_ProfilerTraceEvents
	SET isEnabled = N'Y'
	WHERE category_name = N'User configurable'
		--10 events in this category, all with the name format
		-- of "UserConfigurable:0"  (numbered 0 thru 9)
	;


	/**** EventGroup=SeeOuterBatch 
		(usually to see param values via the RPC events)
		You can't filter by object ID for RPC, so often you filter by LIKE on the text field
	****/
	INSERT INTO @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_ProfilerTraceEvents
		(EventGroup, trace_event_id, event_name, category_name, isEnabled)
	SELECT N'SeeOuterBatch',
		te.trace_event_id, 
		event_name = te.name, 
		category_name = tc.name,
		isEnabled = CASE WHEN te.name IN (N'RPC:Completed', N'SQL:BatchCompleted',N'Exec Prepared SQL')
						THEN N'Y'
						ELSE N'N'
					END
	FROM sys.trace_events te
		INNER JOIN sys.trace_categories tc
			ON te.category_id = tc.category_id
	WHERE 1=1
	AND (
		(tc.name = N'Stored Procedures'
		AND te.name IN (
			N'RPC Output Parameter',
			N'RPC:Completed',
			N'RPC:Starting'
			)
		)

		OR 
		
		(tc.name = N'TSQL'
		AND te.name IN (
			N'Exec Prepared SQL',
			N'Prepare SQL',
			N'SQL:BatchCompleted',
			N'SQL:BatchStarting',
			N'Unprepare SQL'
			)
		)
	)
	;

	/**** EventGroup=PerfCommon
		(events the author has used most often to tune slow statements & objects)
	 ****/
	INSERT INTO @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_ProfilerTraceEvents
		(EventGroup, trace_event_id, event_name, category_name, isEnabled)
	SELECT N'PerfCommon',
		te.trace_event_id, 
		event_name = te.name, 
		category_name = tc.name,
		isEnabled = CASE 
						WHEN tc.name = N'TSQL'
							AND te.name = N'SQL:StmtCompleted' THEN N'Y'
						WHEN tc.name = N'Performance' THEN N'Y'
						WHEN tc.name = N'Transactions' THEN N'Y'
						WHEN tc.name = N'Stored Procedures'
							AND te.name IN (N'SP:Completed', N'SP:StmtCompleted', N'SP:Recompile')
							THEN N'Y'
						WHEN tc.name = N'Errors and Warnings' THEN N'Y'
						ELSE N'N'
					END
	FROM sys.trace_events te
		INNER JOIN sys.trace_categories tc
			ON te.category_id = tc.category_id
	WHERE 1=1
	AND (
		(tc.name = N'Stored Procedures'
		AND te.name IN (
			N'RPC:Completed',
			N'SP:CacheHit',
			N'SP:CacheInsert',
			N'SP:CacheMiss',
			N'SP:CacheRemove',
			N'SP:Completed',
			N'SP:Recompile',
			N'SP:StmtCompleted'
			)
		)

		OR
		(tc.name = N'TSQL'
		AND te.name IN (
			N'Exec Prepared SQL',
			N'Prepare SQL',
			N'SQL:BatchCompleted',
			N'SQL:StmtCompleted',
			N'SQL:StmtRecompile',
			N'Unprepare SQL'
			)
		)

		OR
		(tc.name = N'Performance'
		AND te.name IN (
			N'Auto Stats',
			N'Degree of Parallelism',
			N'Showplan XML Statistics Profile'
			)
		)

		OR (tc.name = N'Transactions' AND te.name = N'TransactionLog')

		OR 
		(tc.name = N'Errors and Warnings'
		AND te.name IN (
			N'Bitmap Warning',
			N'CPU threshold exceeded',
			N'Exchange Spill Event',
			N'Hash Warning',
			N'Missing Column Statistics',
			N'Missing Join Predicate',
			N'Sort Warnings'
			)
		)
	)
	ORDER BY category_name, event_name
	;


	/**** EventGroup=PerfDetailed
		(more info for those tough tuning efforts)
	****/
	INSERT INTO @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_ProfilerTraceEvents
		(EventGroup, trace_event_id, event_name, category_name, isEnabled)
	SELECT N'PerfDetailed',
		te.trace_event_id, 
		event_name = te.name, 
		category_name = tc.name,
		isEnabled = CASE 
						WHEN tc.name = N'Cursors' THEN N'Y'
						WHEN tc.name = N'Database' THEN N'Y'
						WHEN tc.name = N'Errors and Warnings' THEN N'Y'
						WHEN tc.name = N'Locks' THEN N'Y'
						WHEN tc.name = N'Performance' AND te.name <> N'Performance statistics' THEN N'Y'
							--that event can be kinda annoying, so leave off by default
						WHEN tc.name = N'Stored Procedures' THEN N'Y'
						WHEN tc.name = N'Transactions' THEN N'Y'
						WHEN tc.name = N'TSQL' THEN N'Y'
						ELSE N'N'
					END
	FROM sys.trace_events te
		INNER JOIN sys.trace_categories tc
			ON te.category_id = tc.category_id
	WHERE 1=1
	AND (
		(tc.name = N'Cursors'
		)		

		OR
		(tc.name = N'Database'
		AND te.name IN (
			N'Data File Auto Grow',
			N'Log File Auto Grow'
			)
		)

		OR
		(tc.name = N'Errors and Warnings'
		AND te.name IN (
			N'Background Job Error',
			N'Bitmap Warning',
			N'Blocked process report',
			N'CPU threshold exceeded',
			N'Exchange Spill Event',
			N'Hash Warning',
			N'Missing Column Statistics',
			N'Missing Join Predicate',
			N'Sort Warnings'
			)
		)

		OR 
		(tc.name = N'Locks' AND te.name IN (
			N'Lock:Cancel',
			N'Lock:Escalation',
			N'Lock:Timeout',
			N'Lock:Timeout (timeout > 0)'
			)
		)

		OR
		(tc.name = N'Performance'
		AND te.name IN (
			N'Auto Stats',
			N'Degree of Parallelism',
			N'Performance statistics',
			N'Plan Guide Successful',
			N'Plan Guide Unsuccessful',
			N'Showplan XML Statistics Profile'
			)
		)

		OR
		(tc.name = N'Stored Procedures'
		AND te.name IN (
			N'RPC Output Parameter',
			N'RPC:Completed',
			N'RPC:Starting',
			N'SP:CacheHit',
			N'SP:CacheInsert',
			N'SP:CacheMiss',
			N'SP:CacheRemove',
			N'SP:Completed',
			N'SP:Recompile',
			N'SP:Starting',
			N'SP:StmtCompleted',
			N'SP:StmtStarting'
			)
		)

		OR (tc.name = N'Transactions' AND te.name = N'TransactionLog')

		OR
		(tc.name = N'TSQL'
		AND te.name IN (
			N'Exec Prepared SQL',
			N'Prepare SQL',
			N'SQL:BatchCompleted',
			N'SQL:BatchStarting',
			N'SQL:StmtCompleted',
			N'SQL:StmtRecompile',
			N'SQL:StmtStarting',
			N'Unprepare SQL'
			)
		)
	)
	ORDER BY category_name, event_name;

	RETURN 0;
END
GO