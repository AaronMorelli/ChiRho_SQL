/*****
*****	Copyright 2016, 2024 Aaron Morelli
*****
*****	Licensed under the Apache License, Version 2.0 (the "License");
*****	you may not use this file except in compliance with the License.
*****	You may obtain a copy of the License at
*****
*****		http://www.apache.org/licenses/LICENSE-2.0
*****
*****	Unless required by applicable law or agreed to in writing, software
*****	distributed under the License is distributed on an "AS IS" BASIS,
*****	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*****	See the License for the specific language governing permissions and
*****	limitations under the License.
*****
*****	------------------------------------------------------------------------
*****
*****	PROJECT NAME: ChiRho for SQL Server https://github.com/AaronMorelli/ChiRho_SQL
*****
*****	PROJECT DESCRIPTION: A T-SQL toolkit for troubleshooting performance and stability problems on SQL Server instances
*****
*****	FILE NAME: AutoWho_InsertConfigData.StoredProcedure.sql
*****
*****	PROCEDURE NAME: AutoWho_InsertConfigData
*****
*****	AUTHOR:			Aaron Morelli
*****					aaronmorelli@zoho.com
*****					@sqlcrossjoin
*****					sqlcrossjoin.wordpress.com
*****
*****	PURPOSE: Runs at install time and inserts configuration data.
******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_InsertConfigData
/*
	OUTSTANDING ISSUES: None at this time.

To Execute
------------------------
EXEC @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_InsertConfigData @HoursToKeep=336	--14 days

--use to reset the data:
truncate table @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_ProcessingTimes;
truncate table @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options;
truncate table @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options_History;
truncate table @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_UserCollectionOptions;
truncate table @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_UserCollectionOptions_History;
truncate table @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_CollectorOptFakeout;
truncate table @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimCommand;
truncate table @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimConnectionAttribute;
truncate table @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimLoginName;
truncate table @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimNetAddress;
truncate table @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimSessionAttribute;
truncate table @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimWaitType;
delete from @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_ProcessingTimes WHERE Label IN (N'AutoWhoStoreLastTouched');
*/
(
	@HoursToKeep INT
)
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE
		@lv__nullstring						NVARCHAR(8),
		@lv__nullint						INT,
		@lv__nullsmallint					SMALLINT;

	SET @lv__nullstring = N'<nul5>';		--used the # 5 just to make it that much more unlikely that our "special value" 
											-- would collide with a DMV value
	SET @lv__nullint = -929;				--ditto, used a strange/random number rather than -999, so there is even less of a chance of 
	SET @lv__nullsmallint = -929;			-- overlapping with some special system value

	IF ISNULL(@HoursToKeep,-1) <= 0 OR ISNULL(@HoursToKeep,9999) > 4320
	BEGIN
		RAISERROR('The @HoursToKeep parameter cannot be <= 0 or > 4320 (180 days).',16,1);
		RETURN -1;
	END

	--To prevent this proc from damaging the installation after it has already been run, check for existing data.
	IF EXISTS (SELECT * FROM @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options)
		OR EXISTS (SELECT * FROM @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_UserCollectionOptions)
		OR EXISTS (SELECT * FROM @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_CollectorOptFakeout)
		OR EXISTS (SELECT * FROM @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimCommand)
		OR EXISTS (SELECT * FROM @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimConnectionAttribute)
		OR EXISTS (SELECT * FROM @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimLoginName)
		OR EXISTS (SELECT * FROM @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimNetAddress)
		OR EXISTS (SELECT * FROM @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimSessionAttribute)
		OR EXISTS (SELECT * FROM @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimWaitType)
		OR EXISTS (SELECT * FROM @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_ProcessingTimes WHERE Label IN (
										N'AutoWhoStoreLastTouched')
					)
	BEGIN
		RAISERROR('The configuration tables are not empty. You must clear these tables first before this procedure will insert config data', 16,1);
		RETURN -2;
	END


	--Holds 2 rows. Used in the AutoWho.Collector procedure to achieve as close to a "snapshot time" as possible.
	INSERT INTO @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_CollectorOptFakeout (ZeroOrOne)
	SELECT 0 UNION SELECT 1

	--*** DimCommand
	SET IDENTITY_INSERT @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimCommand ON;

	--We want the special null value to be ID = 1 so we can fill it in for null values via code even though the join will fail to produce a match
	INSERT INTO @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimCommand (DimCommandID, command, TimeAdded) SELECT 1, @lv__nullstring, GETDATE();
	--similarly, we want a special code for the GHOST CLEANUP spid, because we want to avoid page latch resolution if GHOST CLEANUP is running (since we've
	-- seen very long DBCC PAGE runtimes when GHOST CLEANUP was running). 
	INSERT INTO @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimCommand (DimCommandID, command, TimeAdded) SELECT 2, 'GHOST CLEANUP',GETDATE();
	--Pre-defining the TM REQUEST command lets us handle certain patterns in the dm_exec_requests view more easily
	INSERT INTO @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimCommand (DimCommandID, command, TimeAdded) SELECT 3, 'TM REQUEST',GETDATE();

	SET IDENTITY_INSERT @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimCommand OFF;

	--*** DimConnectionAttribute
	SET IDENTITY_INSERT @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimConnectionAttribute ON;

	--System spids don't have a connection attribute, so assign them to ID=1 in the code
	INSERT INTO @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimConnectionAttribute (
		DimConnectionAttributeID, 
		net_transport, 
		protocol_type, 
		protocol_version, 
		endpoint_id, 
		node_affinity, 
		net_packet_size, 
		encrypt_option, 
		auth_scheme
	)
	SELECT 
		1, 
		@lv__nullstring as net_transport,
		@lv__nullstring as protocol_type,
		@lv__nullint as protocol_version,
		@lv__nullint as endpoint_id,
		@lv__nullsmallint as node_affinity,
		@lv__nullint as net_packet_size,
		@lv__nullstring as encrypt_option,
		@lv__nullstring as auth_scheme;

	SET IDENTITY_INSERT @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimConnectionAttribute OFF;

	--***
	SET IDENTITY_INSERT @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimLoginName ON;

	--spids with NULL values in both fields get code 1. I'm not sure if this is even possible?
	INSERT INTO @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimLoginName (DimLoginNameID, login_name, original_login_name)
	SELECT 1, @lv__nullstring, @lv__nullstring;
	--system spids (which I believe always have 'sa' for both) will get code 2. But what happens if 'sa' login has been disabled?
	INSERT INTO @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimLoginName (DimLoginNameID, login_name, original_login_name)
	SELECT 2, 'sa', 'sa';
	SET IDENTITY_INSERT @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimLoginName OFF;

	SET IDENTITY_INSERT @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimNetAddress ON;
	--Local connections that come through Shared Memory will have several null fields, so prepopulate this dim with
	-- a pre-defined ID value, so that we can assign via hard-coded logic rather than the join
	INSERT INTO @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimNetAddress (DimNetAddressID, client_net_address, local_net_address, local_tcp_port)
	SELECT 1, @lv__nullstring, @lv__nullstring, @lv__nullint;

	INSERT INTO @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimNetAddress (DimNetAddressID, client_net_address, local_net_address, local_tcp_port)
	SELECT 2, N'<local machine>', @lv__nullstring, @lv__nullint;

	SET IDENTITY_INSERT @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimNetAddress OFF;


	SET IDENTITY_INSERT @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimSessionAttribute ON;

	--This is a "null row"; however, system spids have values for several of the attributes, based on what I have seen. 
	-- Thus, most system spids will map to that row when it is inserted.
	INSERT INTO @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimSessionAttribute (
		DimSessionAttributeID, 
		host_name, 
		program_name, 
		client_version, 
		client_interface_name, 
		endpoint_id, 
		transaction_isolation_level, 
		deadlock_priority, 
		group_id
	)
	SELECT 
		1, 
		@lv__nullstring as host_name,
		@lv__nullstring as program_name,
		@lv__nullint as client_version,
		@lv__nullstring as client_interface_name,
		@lv__nullint as endpoint_id,
		@lv__nullsmallint as transaction_isolation_level,
		@lv__nullsmallint as deadlock_priority,
		@lv__nullint as group_id;

	SET IDENTITY_INSERT @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimSessionAttribute OFF;

	SET IDENTITY_INSERT @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimWaitType ON;

	-- No value... we interpret this to mean "not waiting"
	INSERT INTO @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimWaitType
	(DimWaitTypeID, wait_type, wait_type_short, latch_subtype)
	SELECT 1, @lv__nullstring, @lv__nullstring, N'';

	INSERT INTO @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimWaitType
	(DimWaitTypeID, wait_type, wait_type_short, latch_subtype)
	SELECT 2, 'WAITFOR', 'WAITFOR', N'';

	SET IDENTITY_INSERT @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimWaitType OFF;

	--Options
	EXEC @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_ResetOptions; 

	--Retention variables are based on the DaysToKeep input parameter
	UPDATE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options 
	SET 
		Retention_IdleSPIDs_NoTran = @HoursToKeep,
		Retention_IdleSPIDs_WithShortTran = @HoursToKeep,
		Retention_IdleSPIDs_WithLongTran = @HoursToKeep,
		Retention_IdleSPIDs_HighTempDB = @HoursToKeep,
		Retention_ActiveLow = @HoursToKeep,
		Retention_ActiveMedium = @HoursToKeep,
		Retention_ActiveHigh = @HoursToKeep,
		Retention_ActiveBatch = @HoursToKeep,
		Retention_CaptureTimes = (@HoursToKeep/24) + 2
	;

	--TODO: uncomment once we introduce the user collector logic
	--EXEC @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_ResetUserCollectionOptions;

	INSERT INTO @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_ProcessingTimes (Label, LastProcessedTime, LastProcessedTimeUTC)
	SELECT N'AutoWhoStoreLastTouched', NULL, NULL;

	RETURN 0;
END
GO