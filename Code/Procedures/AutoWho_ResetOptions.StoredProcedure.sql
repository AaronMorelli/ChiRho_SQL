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
*****	FILE NAME: AutoWho_ResetOptions.StoredProcedure.sql
*****
*****	PROCEDURE NAME: AutoWho_ResetOptions
*****
*****	AUTHOR:			Aaron Morelli
*****					aaronmorelli@zoho.com
*****					@sqlcrossjoin
*****					sqlcrossjoin.wordpress.com
*****
*****	PURPOSE: Deletes the row in @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options and re-inserts a row based on default values
******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_ResetOptions
/*
	OUTSTANDING ISSUES: None at this time.

To Execute
------------------------
EXEC @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_ResetOptions

SELECT * FROM @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options
*/
AS
BEGIN
	SET NOCOUNT ON;

	INSERT INTO @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options_History (
		RowID, AutoWhoEnabled, BeginTime, EndTime, BeginEndIsUTC, IntervalLength, IncludeIdleWithTran, IncludeIdleWithoutTran, DurationFilter, IncludeDBs, ExcludeDBs, HighTempDBThreshold, CollectSystemSpids, HideSelf, ObtainBatchText, ObtainQueryPlanForStatement, ObtainQueryPlanForBatch, ObtainLocksForBlockRelevantThreshold, InputBufferThreshold, ParallelWaitsThreshold, QueryPlanThreshold, QueryPlanThresholdBlockRel, BlockingChainThreshold, BlockingChainDepth, TranDetailsThreshold, MediumDurationThreshold, HighDurationThreshold, BatchDurationThreshold, LongTransactionThreshold, Retention_IdleSPIDs_NoTran, Retention_IdleSPIDs_WithShortTran, Retention_IdleSPIDs_WithLongTran, Retention_IdleSPIDs_HighTempDB, Retention_ActiveLow, Retention_ActiveMedium, Retention_ActiveHigh, Retention_ActiveBatch, Retention_CaptureTimes, DebugSpeed, ThresholdFilterRefresh, SaveBadDims, Enable8666, ResolvePageLatches, ResolveLockWaits, PurgeUnextractedData,
		HistoryInsertDate,
		HistoryInsertDateUTC,
		TriggerAction,
		LastModifiedUser
	)
	SELECT 
		RowID, AutoWhoEnabled, BeginTime, EndTime, BeginEndIsUTC, IntervalLength, IncludeIdleWithTran, IncludeIdleWithoutTran, DurationFilter, IncludeDBs, ExcludeDBs, HighTempDBThreshold, CollectSystemSpids, HideSelf, ObtainBatchText, ObtainQueryPlanForStatement, ObtainQueryPlanForBatch, ObtainLocksForBlockRelevantThreshold, InputBufferThreshold, ParallelWaitsThreshold, QueryPlanThreshold, QueryPlanThresholdBlockRel, BlockingChainThreshold, BlockingChainDepth, TranDetailsThreshold, MediumDurationThreshold, HighDurationThreshold, BatchDurationThreshold, LongTransactionThreshold, Retention_IdleSPIDs_NoTran, Retention_IdleSPIDs_WithShortTran, Retention_IdleSPIDs_WithLongTran, Retention_IdleSPIDs_HighTempDB, Retention_ActiveLow, Retention_ActiveMedium, Retention_ActiveHigh, Retention_ActiveBatch, Retention_CaptureTimes, DebugSpeed, ThresholdFilterRefresh, SaveBadDims, Enable8666, ResolvePageLatches, ResolveLockWaits, PurgeUnextractedData,
		GETDATE(),
		GETUTCDATE(),
		'Reset',
		SUSER_NAME()
	FROM @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options;

	DISABLE TRIGGER @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_trgDEL_AutoWho_Options ON @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options;

	DELETE FROM @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options;

	ENABLE TRIGGER @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_trgDEL_AutoWho_Options ON @@CHIRCHO_SCHEMA@@.AutoWho_Options;

	INSERT INTO @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options 
		DEFAULT VALUES;

	RETURN 0;
END
GO