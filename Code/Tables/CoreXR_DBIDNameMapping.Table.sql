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
*****	FILE NAME: CoreXR_DBIDNameMapping.Table.sql
*****
*****	TABLE NAME: CoreXR_DBIDNameMapping
*****
*****	AUTHOR:			Aaron Morelli
*****					aaronmorelli@zoho.com
*****					@sqlcrossjoin
*****					sqlcrossjoin.wordpress.com
*****
*****	PURPOSE: The various components of ChiRho typically store just the DBID.
*****	This table provides a mapping to DB name, and serves as a type-2 dimension, handling
*****	changes over time.
***** */
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_DBIDNameMapping(
	[database_id]			[int] NOT NULL,
	[name]				    [nvarchar](256) NOT NULL,
	[create_date]           [datetime] NOT NULL,
	[EffectiveStartTimeUTC] [datetime] NOT NULL,
	[EffectiveEndTimeUTC]	[datetime] NULL,
	[EffectiveStartTime]	[datetime] NOT NULL,  --This should always match create_date
	[EffectiveEndTime]		[datetime] NULL,
 CONSTRAINT [PKDBIDNameMapping] PRIMARY KEY CLUSTERED 
(
	--Note that in theory, a given DBID and DB Name mapping could appear multiple times (edge case, but possible).
	--Thus, we make DB creation time a part of the key. Since there is millisecond granularity, we should never have
	--a problem with duplicates on either the (DB ID, creation time) or (DB Name, creation time) key combinations
	[database_id] ASC,
	[create_date] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
CREATE UNIQUE NONCLUSTERED INDEX [AKDBIDNameMapping] ON @@CHIRHO_SCHEMA_OBJECTS@@.CoreXR_DBIDNameMapping
(
	[name] ASC,
	[create_date] ASC
)
INCLUDE ([database_id],
	[EffectiveStartTimeUTC],
	[EffectiveEndTimeUTC],
	[EffectiveStartTime],
	[EffectiveEndTime]) 
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, 
	DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
