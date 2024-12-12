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
*****	FILE NAME: CoreXR_InstallationConfig_History.Table.sql
*****
*****	TABLE NAME: CoreXR_InstallationConfig_History
*****
*****	AUTHOR:			Aaron Morelli
*****					aaronmorelli@zoho.com
*****					@sqlcrossjoin
*****					sqlcrossjoin.wordpress.com
*****
*****	PURPOSE: Tracks any changes made to the CoreXR_InstallationConfig table via triggers
***** */
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_InstallationConfig_History(
	[EngineType]        [nvarchar](60) NOT NULL,
	[EditionFeatures]   [nvarchar](60) NOT NULL,
	[SQLVersion]        [nvarchar](30) NOT NULL,
	[SQLTimeZone]       [nvarchar](60) NOT NULL,
	[DBNameObjects]     sysname NOT NULL,
	[SchemaNameObjects] sysname NOT NULL,
	[DBNameEndUser]     sysname NOT NULL,
	[SchemaNameEndUser] sysname NOT NULL,
	[InstallDate]       [datetime] NOT NULL,
	[InstallDateUTC]    [datetime] NOT NULL,
	[TriggerAction] 	[nvarchar](20) NOT NULL,
	[TriggerActionTime] [datetime] NOT NULL,
	[TriggerActionTimeUTC] [datetime] NOT NULL
) ON [PRIMARY]
GO