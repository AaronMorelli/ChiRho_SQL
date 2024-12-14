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
*****	FILE NAME: AutoWho_Options.Table.sql
*****
*****	TABLE NAME: AutoWho_Options
*****
*****	AUTHOR:			Aaron Morelli
*****					aaronmorelli@zoho.com
*****					@sqlcrossjoin
*****					sqlcrossjoin.wordpress.com
*****
*****	PURPOSE: Stores just 1 row, 1 column per AutoWho option.
******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options(
	[RowID] [int] NOT NULL CONSTRAINT [DF_Options_RowID]  DEFAULT ((1)),
		--Enforces just 1 row in the table
	[AutoWhoEnabled] [nchar](1) NOT NULL CONSTRAINT [DF_Options_AutoWhoEnabled]  DEFAULT (N'Y'),
		--Master on/off switch for the AutoWho tracing portion of ChiRho. Takes "Y" or "N"
	[BeginTime] [time](0) NOT NULL CONSTRAINT [DF_Options_BeginTime]  DEFAULT (('00:00:00')),
		--The time at which to start running the AutoWho trace.
	[EndTime] [time](0) NOT NULL CONSTRAINT [DF_Options_EndTime]  DEFAULT (('23:59:30')),
		--The time at which to stop running the AutoWho trace.
	[BeginEndIsUTC] [nchar](1) NOT NULL CONSTRAINT [DF_Options_BeginEndIsUTC]  DEFAULT (N'N'),
		--Whether BeginTime and EndTime are specified in UTC or not.
	[IntervalLength] [smallint] NOT NULL CONSTRAINT [DF_Options_IntervalLength]  DEFAULT ((15)),
		--The length, in seconds, of each interval. If AutoWho collects its data almost instantaneously, this is the time between executions of the AutoWho Collector.
		-- However, if AutoWho runs several seconds, the idle duration is adjusted so that the next AutoWho execution falls roughly on a boundary point (multiple of IntervalLength)
	[IncludeIdleWithTran] [nchar](1) NOT NULL CONSTRAINT [DF_Options_IncludeIdleWithTran]  DEFAULT (N'Y'),
		--Whether to collect sessions that are not actively running a batch but DO have an open transaction.
	[IncludeIdleWithoutTran] [nchar](1) NOT NULL CONSTRAINT [DF_Options_IncludeIdleWithoutTran]  DEFAULT (N'N'),
		--Whether to collect sessions that are completely idle (no running batch, no open transactions)
	[DurationFilter] [int] NOT NULL CONSTRAINT [DF_Options_DurationFilter]  DEFAULT ((0)),
		--When > 0, filters out spids whose "effective duration" (in milliseconds) is < this duration. 
		--For running SPIDs, the "effective duration" is the duration of the current batch (based on request start_time). 
		--For idle SPIDs, it is the time since the last_request_end_time value, aka the time the spid has been idle. Takes a number between 0 and max(int)
	[IncludeDBs] [nvarchar](4000) NOT NULL CONSTRAINT [DF_Options_IncludeDBs]  DEFAULT (N''),
		--A comma-delimited list of database names to INCLUDE (this is the context DB of the SPID, not necessarily the object DB of the proc/function/trigger/etc). 
		--SPIDs with a context DB other than in this list will be excluded unless they are blockers of an included SPID.
	[ExcludeDBs] [nvarchar](4000) NOT NULL CONSTRAINT [DF_Options_ExcludeDBs]  DEFAULT (N''),
		--A comma-delimited list of database names to EXCLUDE (this is the context DB of the SPID, not necessarily the object DB of the proc/function/trigger/etc). 
		--SPIDs with a context DB in this list will be excluded unless they are blockers of an included SPID.
	[HighTempDBThreshold] [int] NOT NULL CONSTRAINT [DF_Options_HighTempDBThreshold]  DEFAULT ((64000)),
		--The threshold (in # of 8KB pages) at which point a SPID becomes a High TempDB user. 
		--SPIDs with TempDB usage above this threshold are always captured, regardless of whether they are idle or have open trans or not.
	[CollectSystemSpids] [nchar](1) NOT NULL CONSTRAINT [DF_Options_CollectSystemSpids]  DEFAULT (N'Y'),
		--Whether to collect system spids (typically session_id <=50, but not always). Takes "Y" or "N". 
		--If "Y", only "interesting" system spids (those not in their normal wait/idle state) will be captured.
	[HideSelf] [nchar](1) NOT NULL CONSTRAINT [DF_Options_HideSelf]  DEFAULT (N'Y'),
		--Whether to hide the session that is running AutoWho. Takes "Y" or "N". "N" is typically only useful when debugging AutoWho performance or resource utilization.
	[ObtainBatchText] [nchar](1) NOT NULL CONSTRAINT [DF_Options_ObtainBatchText]  DEFAULT (N'N'),
		--Whether the complete T-SQL batch is obtained. Takes "Y" or "N". Regardless of this value, the text of the current statement for active spids is always obtained.
	[ObtainQueryPlanForStatement] [nchar](1) NOT NULL CONSTRAINT [DF_Options_ObtainQueryPlanForStatement]  DEFAULT (N'Y'),
		--Whether statement-level query plans are obtained for the statements currently executed by running spids. Takes "Y" or "N"
	[ObtainQueryPlanForBatch] [nchar](1) NOT NULL CONSTRAINT [DF_Options_ObtainQueryPlanForBatch]  DEFAULT (N'N'),
		--Whether query plans are obtained for the complete batch a spid is running. Takes "Y" or "N"
	[ObtainLocksForBlockRelevantThreshold] [int] NOT NULL CONSTRAINT [DF_Options_ObtainLocksForBlockRelevantThreshold]  DEFAULT ((20000)),
		--The # of a milliseconds that an active spid must be blocked before a special query is run to grab info about 
		--what locks are held by all blocking-relevant (blocked and blockers) spids. Takes a number between 0 and max(smallint)
	[InputBufferThreshold] [int] NOT NULL CONSTRAINT [DF_Options_InputBufferThreshold]  DEFAULT ((15000)),
		--The # of milliseconds a spid must be running its current batch or be idle w/open tran before the Input Buffer is obtained for it. 
		--Takes a number between 0 and max(int)
	[ParallelWaitsThreshold] [int] NOT NULL CONSTRAINT [DF_Options_ParallelWaitsThreshold]  DEFAULT ((15000)),
		--The # of milliseconds a batch running in parallel must be running before all of its tasks/waiting tasks DMV info is saved off to the TasksAndWaits table. 
		--The "top wait/top task" is always saved, regardless of the duration of the batch. Takes a number between 0 and max(int)
	[QueryPlanThreshold] [int] NOT NULL CONSTRAINT [DF_Options_QueryPlanThreshold]  DEFAULT ((3000)),
		--The # of milliseconds that an active SPID must be running before its query plan will be captured.
	[QueryPlanThresholdBlockRel] [int] NOT NULL CONSTRAINT [DF_Options_QueryPlanThresholdBlockRel]  DEFAULT ((2000)),
		--The # of seconds that an active SPID that is block-relevant must be running before its query plan will be captured.
	[BlockingChainThreshold] [int] NOT NULL CONSTRAINT [DF_Options_BlockingChainThreshold]  DEFAULT ((15000)),
		--The # of a milliseconds that an active spid must be blocked before the blocking chain code is executed. 
		--Note that the spids are not excluded from the Bchain, regardless of their duration, once the Bchain logic is triggered. 
		--Rather, this parameter just defines what kind of blocking duration must be seen for the Bchain logic to trigger.
		--Takes a number between 0 and max(smallint)
	[BlockingChainDepth] [tinyint] NOT NULL CONSTRAINT [DF_Options_BlockingChainDepth]  DEFAULT ((4)),
		--If the blocking chain code is executed, how many blocking-levels deep are collected and stored. 
		--Takes a number between 0 and 10 inclusive. 0 means "off", and the Bchain logic will never be executed
	[TranDetailsThreshold] [int] NOT NULL CONSTRAINT [DF_Options_TranDetailsThreshold]  DEFAULT ((60000)),
		--If an active spid has been running this long (unit=milliseconds), or an idle w/tran spid has been idle this long, 
		--its transaction data will be captured from the tran DMVs. 
		--Note that tran data is also captured for spids with sys.dm_exec_sessions.open_transaction_count > 0, regardless of duration.
	[MediumDurationThreshold] [int] NOT NULL CONSTRAINT [DF_Options_MediumDurationThreshold]  DEFAULT ((10)),
		--Active SPIDs with a duration < this # of seconds will be considered to have a "Low Duration" class, 
		--while active SPIDs >= this (but < HighDurationThreshold) will be in the "Medium Duration" class when purge logic is run.
	[HighDurationThreshold] [int] NOT NULL CONSTRAINT [DF_Options_HighDurationThreshold]  DEFAULT ((30)),
		--Active SPIDs with a duration < this # of seconds will be considered to have a "Medium Duration" class, 
		--while active SPIDs >= this (but < BatchDurationThreshold) will be in the "High Duration" class when purge is run.
	[BatchDurationThreshold] [int] NOT NULL CONSTRAINT [DF_Options_BatchDurationThreshold]  DEFAULT ((120)),
		--Active SPIDs with a duration < this # of seconds will be considered to have a "High Duration" class, 
		--while active SPIDs >= this will be in the "Batch Duration" class, when purge logic is run.
	[LongTransactionThreshold] [int] NOT NULL CONSTRAINT [DF_Options_LongTransactionThreshold]  DEFAULT ((300)),
		--SPIDs that have an open transaction >= this value (unit is seconds) are declared to have a "long" transaction. 
		--This affects which purge retention policy is applied.
	[Retention_IdleSPIDs_NoTran] [int] NOT NULL CONSTRAINT [DF_Options_Retention_IdleSPIDs_NoTran]  DEFAULT ((168)),
		--The # of hours to retain entries for idle SPIDs that do not have an open transaction.
	[Retention_IdleSPIDs_WithShortTran] [int] NOT NULL CONSTRAINT [DF_Options_Retention_IdleSPIDs_WithShortTran]  DEFAULT ((168)),
		--The # of hours to retain entries for idle SPIDs that DO have an open transaction, and that transaction is < than the LongTransactionThreshold value.
	[Retention_IdleSPIDs_WithLongTran] [int] NOT NULL CONSTRAINT [DF_Options_Retention_IdleSPIDs_WithLongTran]  DEFAULT ((168)),
		--The # of hours to retain entries for idle SPIDs that DO have an open transaction, and that transaction is >= the LongTransactionThreshold value.
	[Retention_IdleSPIDs_HighTempDB] [int] NOT NULL CONSTRAINT [DF_Options_Retention_IdleSPIDs_HighTempDB]  DEFAULT ((168)),
		--The # of hours to retain entries for idle SPIDs that use >= than HighTempDBThreshold # of pages.
	[Retention_ActiveLow] [int] NOT NULL CONSTRAINT [DF_Options_Retention_ActiveLow]  DEFAULT ((168)),
		--The # of hours to retain entries for active SPIDs that fall into the Low Duration category.
	[Retention_ActiveMedium] [int] NOT NULL CONSTRAINT [DF_Options_Retention_ActiveMedium]  DEFAULT ((168)),
		--The # of hours to retain entries for active SPIDs that fall into the Medium Duration category.
	[Retention_ActiveHigh] [int] NOT NULL CONSTRAINT [DF_Options_Retention_ActiveHigh]  DEFAULT ((168)),
		--The # of hours to retain entries for active SPIDs that fall into the High Duration category.
	[Retention_ActiveBatch] [int] NOT NULL CONSTRAINT [DF_Options_Retention_ActiveBatch]  DEFAULT ((168)),
		--The # of hours to retain entries for active SPIDs that fall into the Batch Duration category.
	[Retention_CaptureTimes] [int] NOT NULL CONSTRAINT [DF_Options_Retention_CaptureTimes]  DEFAULT ((10)),
		--The # of days to retain rows in the AutoWho_CaptureTimes table. This should be a longer time frame than all of the Retention_* variables
	[DebugSpeed] [nchar](1) NOT NULL CONSTRAINT [DF_Options_DebugSpeed]  DEFAULT (N'Y'),
		--Whether to capture duration info for each significant statement in the AutoWho Collection procedure and write that duration info to a table.
	[ThresholdFilterRefresh] [smallint] NOT NULL CONSTRAINT [DF_Options_ThresholdFilterRefresh]  DEFAULT ((10)),
		--The # of minutes in which to rerun the code that determines which SPIDs should NOT count toward various threshold-based triggers
	[SaveBadDims] [nchar](1) NOT NULL CONSTRAINT [DF_Options_SaveBadDims]  DEFAULT (N'Y'),
		--Saves spid records that could not be mapped to dimension keys to a separate table.
	[Enable8666] [nchar](1) NOT NULL CONSTRAINT [DF_Options_Enable8666]  DEFAULT (N'N'),
		--Whether the AutoWho process will enable (undocumented) TF 8666; enabling this flag causes "InternalInfo" 
		--nodes to be added to the XML showplans that are captured by AutoWho. Takes "Y" or "N"
	[ResolvePageLatches] [nchar](1) NOT NULL CONSTRAINT [DF_Options_ResolvePageLatches]  DEFAULT (N'Y'),
		--Whether the AutoWho Post-processor will attempt to resolve page and pageio latch strings into which object/index they map to via DBCC PAGE. Takes "Y" or "N"
	[ResolveLockWaits] [nchar](1) NOT NULL CONSTRAINT [DF_Options_ResolveLockWaits]  DEFAULT (N'Y'),
		--Whether the AutoWho procedure will attempt to resolve locks into the objects/indexes that they map to.
	[PurgeUnextractedData] [nchar](1) NOT NULL CONSTRAINT [DF_Options_PurgeUnextractedData]  DEFAULT (N'Y'),
		--Whether purge is allowed to delete data for capture times rows (in @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_CaptureTimes) that has not been extracted for the DW yet. Takes "Y" or "N"
 CONSTRAINT [PKAutoWhoOptions] PRIMARY KEY CLUSTERED 
(
	[RowID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsAutoWhoEnabled] CHECK  (([AutoWhoEnabled]=N'Y' OR [AutoWhoEnabled]=N'N'))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsAutoWhoEnabled]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsBatchDurationThreshold] CHECK  (([BatchDurationThreshold]>=(3)))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsBatchDurationThreshold]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsBeginEndTime] CHECK  ([BeginTime]<>[EndTime])
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsBeginEndTime]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsBeginEndIsUTC] CHECK  (([BeginEndIsUTC]=N'Y' OR [BeginEndIsUTC]=N'N'))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsBlockingChainDepth] CHECK  (([BlockingChainDepth]>=(0) AND [BlockingChainDepth]<=(10)))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsBlockingChainDepth]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsBlockingChainThreshold] CHECK  (([BlockingChainThreshold]>=(0)))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsBlockingChainThreshold]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsCollectSystemSpids] CHECK  (([CollectSystemSpids]=N'N' OR [CollectSystemSpids]=N'Y'))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsCollectSystemSpids]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsDebugSpeed] CHECK  (([DebugSpeed]=N'Y' OR [DebugSpeed]=N'N'))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsDebugSpeed]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsDurationFilter] CHECK  (([DurationFilter]>=(0)))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsDurationFilter]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsDurationThresholdOrder] CHECK  (([MediumDurationThreshold]<[HighDurationThreshold] AND [HighDurationThreshold]<[BatchDurationThreshold]))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsDurationThresholdOrder]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsEnable8666] CHECK  (([Enable8666]=N'Y' OR [Enable8666]=N'N'))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsEnable8666]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsForce1Row] CHECK  (([RowID]=(1)))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsForce1Row]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsHideSelf] CHECK  (([HideSelf]=N'Y' OR [HideSelf]=N'N'))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsHideSelf]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsHighDurationThreshold] CHECK  (([HighDurationThreshold]>=(2)))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsHighDurationThreshold]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsHighTempDBThreshold] CHECK  (([HighTempDBThreshold]>=(0)))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsHighTempDBThreshold]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsIncludeIdleWithoutTran] CHECK  (([IncludeIdleWithoutTran]=N'N' OR [IncludeIdleWithoutTran]=N'Y'))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsIncludeIdleWithoutTran]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsIncludeIdleWithTran] CHECK  (([IncludeIdleWithTran]=N'N' OR [IncludeIdleWithTran]=N'Y'))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsIncludeIdleWithTran]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsInputBufferThreshold] CHECK  (([InputBufferThreshold]>=(0)))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsInputBufferThreshold]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsIntervalLength] CHECK  (([IntervalLength]>=(5) AND [IntervalLength]<=(300)))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsIntervalLength]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsLongTransactionThreshold] CHECK  (([LongTransactionThreshold]>=(0)))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsLongTransactionThreshold]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsMediumDurationThreshold] CHECK  (([MediumDurationThreshold]>=(1)))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsMediumDurationThreshold]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsObtainBatchText] CHECK  (([ObtainBatchText]=N'Y' OR [ObtainBatchText]=N'N'))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsObtainBatchText]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsObtainLocksForBlockRelevantThreshold] CHECK  (([ObtainLocksForBlockRelevantThreshold]>=(0)))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsObtainLocksForBlockRelevantThreshold]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsObtainQueryPlanForBatch] CHECK  (([ObtainQueryPlanForBatch]=N'Y' OR [ObtainQueryPlanForBatch]=N'N'))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsObtainQueryPlanForBatch]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsObtainQueryPlanForStatement] CHECK  (([ObtainQueryPlanForStatement]=N'N' OR [ObtainQueryPlanForStatement]=N'Y'))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsObtainQueryPlanForStatement]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsParallelWaitsThreshold] CHECK  (([ParallelWaitsThreshold]>=(0)))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsParallelWaitsThreshold]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsQPBvsQP] CHECK  (([QueryPlanThresholdBlockRel]<=[QueryPlanThreshold]))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsQPBvsQP]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsQueryPlanThreshold] CHECK  (([QueryPlanThreshold]>=(0)))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsQueryPlanThreshold]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsQueryPlanThresholdBlockRel] CHECK  (([QueryPlanThresholdBlockRel]>=(0)))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsQueryPlanThresholdBlockRel]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsResolveLockWaits] CHECK  (([ResolveLockWaits]=N'Y' OR [ResolveLockWaits]=N'N'))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsResolveLockWaits]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsResolvePageLatches] CHECK  (([ResolvePageLatches]=N'N' OR [ResolvePageLatches]=N'Y'))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsResolvePageLatches]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsRetention_ActiveBatch] CHECK  (([Retention_ActiveBatch]>=(0)))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsRetention_ActiveBatch]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsRetention_ActiveHigh] CHECK  (([Retention_ActiveHigh]>=(0)))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsRetention_ActiveHigh]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsRetention_ActiveLow] CHECK  (([Retention_ActiveLow]>=(0)))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsRetention_ActiveLow]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsRetention_ActiveMedium] CHECK  (([Retention_ActiveMedium]>=(0)))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsRetention_ActiveMedium]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsRetention_CaptureTimes] CHECK  (([Retention_CaptureTimes]>=(0)))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsRetention_CaptureTimes]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsRetention_IdleSPIDs_HighTempDB] CHECK  (([Retention_IdleSPIDs_HighTempDB]>=(0)))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsRetention_IdleSPIDs_HighTempDB]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsRetention_IdleSPIDs_NoTran] CHECK  (([Retention_IdleSPIDs_NoTran]>=(0)))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsRetention_IdleSPIDs_NoTran]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsRetention_IdleSPIDs_WithLongTran] CHECK  (([Retention_IdleSPIDs_WithLongTran]>=(0)))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsRetention_IdleSPIDs_WithLongTran]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsRetention_IdleSPIDs_WithShortTran] CHECK  (([Retention_IdleSPIDs_WithShortTran]>=(0)))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsRetention_IdleSPIDs_WithShortTran]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsSaveBadDims] CHECK  (([SaveBadDims]=N'N' OR [SaveBadDims]=N'Y'))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsSaveBadDims]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsThresholdFilterRefresh] CHECK  (([ThresholdFilterRefresh]>=(0)))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsThresholdFilterRefresh]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsTranDetailsThreshold] CHECK  (([TranDetailsThreshold]>=(0)))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsTranDetailsThreshold]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options  WITH CHECK ADD  CONSTRAINT [CK_OptionsPurgeUnextractedData] CHECK  (([PurgeUnextractedData]=N'N' OR [PurgeUnextractedData]=N'Y'))
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Options CHECK CONSTRAINT [CK_OptionsPurgeUnextractedData]
GO