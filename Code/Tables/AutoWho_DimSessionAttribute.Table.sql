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
*****	FILE NAME: AutoWho_DimSessionAttribute.Table.sql
*****
*****	TABLE NAME: AutoWho_DimSessionAttribute
*****
*****	AUTHOR:			Aaron Morelli
*****					aaronmorelli@zoho.com
*****					@sqlcrossjoin
*****					sqlcrossjoin.wordpress.com
*****
*****	PURPOSE: Holds the distinct list of session attributes (a subset
*****	of fields from sys.dm_exec_sessions) observed by @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_Collector
******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimSessionAttribute(
	[DimSessionAttributeID] [int] IDENTITY(30,1) NOT NULL,
	[host_name] [nvarchar](128) NOT NULL,
	[program_name] [nvarchar](128) NOT NULL,
	[client_version] [int] NOT NULL,
	[client_interface_name] [nvarchar](32) NOT NULL,
	[endpoint_id] [int] NOT NULL,
	[transaction_isolation_level] [smallint] NOT NULL,
	[deadlock_priority] [smallint] NOT NULL,
	[group_id] [int] NOT NULL,
	[TimeAdded] [datetime] NOT NULL,
	[TimeAddedUTC] [datetime] NOT NULL,
 CONSTRAINT [PK_AutoWho_DimSessionAttributes] PRIMARY KEY CLUSTERED 
(
	[DimSessionAttributeID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
CREATE UNIQUE NONCLUSTERED INDEX [AK_AutoWho_DimSessionAttribute] ON @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimSessionAttribute
(
	[host_name] ASC,
	[program_name] ASC,
	[client_version] ASC,
	[client_interface_name] ASC,
	[endpoint_id] ASC,
	[transaction_isolation_level] ASC,
	[deadlock_priority] ASC,
	[group_id] ASC
)
INCLUDE ( 	[DimSessionAttributeID],
	[TimeAdded],
	[TimeAddedUTC]
) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimSessionAttribute ADD  CONSTRAINT [DF_AutoWho_DimSessionAttributes_TimeAdded]  DEFAULT (GETDATE()) FOR [TimeAdded]
GO
ALTER TABLE @@CHIRHO_SCHEMA_OBJECTS@@.AutoWho_DimSessionAttribute ADD  CONSTRAINT [DF_AutoWho_DimSessionAttributes_TimeAddedUTC]  DEFAULT (GETUTCDATE()) FOR [TimeAddedUTC]
GO