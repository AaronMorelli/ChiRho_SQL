-- TODO: need to tweak this quite a bit.
-- Script to drop CoreXR procedures, views, and tables across all schemas

-- Declare a temporary table to store objects to be dropped
DECLARE @ObjectsToDrop TABLE (
    ObjectType NVARCHAR(10),
    SchemaName NVARCHAR(128),
    ObjectName NVARCHAR(128)
);

-- Collect CoreXR Stored Procedures
INSERT INTO @ObjectsToDrop (ObjectType, SchemaName, ObjectName)
SELECT 'PROCEDURE', SCHEMA_NAME(schema_id), name
FROM sys.procedures
WHERE name LIKE 'CoreXR%';

-- Collect CoreXR Views
INSERT INTO @ObjectsToDrop (ObjectType, SchemaName, ObjectName)
SELECT 'VIEW', SCHEMA_NAME(schema_id), name
FROM sys.views
WHERE name LIKE 'CoreXR%';

-- Collect CoreXR Tables
INSERT INTO @ObjectsToDrop (ObjectType, SchemaName, ObjectName)
SELECT 'TABLE', SCHEMA_NAME(schema_id), name
FROM sys.tables
WHERE name LIKE 'CoreXR%';

-- Drop Procedures
DECLARE @DropProcedure NVARCHAR(MAX) = '';
SELECT @DropProcedure = @DropProcedure + 'DROP PROCEDURE ' + QUOTENAME(SchemaName) + '.' + QUOTENAME(ObjectName) + ';' + CHAR(13)
FROM @ObjectsToDrop
WHERE ObjectType = 'PROCEDURE';

EXEC sp_executesql @DropProcedure;

-- Drop Views
DECLARE @DropView NVARCHAR(MAX) = '';
SELECT @DropView = @DropView + 'DROP VIEW ' + QUOTENAME(SchemaName) + '.' + QUOTENAME(ObjectName) + ';' + CHAR(13)
FROM @ObjectsToDrop
WHERE ObjectType = 'VIEW';

EXEC sp_executesql @DropView;

-- Drop Tables
-- Note: This will fail if the tables have dependencies. Use WITH (DROPTABLE) if needed.
DECLARE @DropTable NVARCHAR(MAX) = '';
SELECT @DropTable = @DropTable + 'DROP TABLE ' + QUOTENAME(SchemaName) + '.' + QUOTENAME(ObjectName) + ';' + CHAR(13)
FROM @ObjectsToDrop
WHERE ObjectType = 'TABLE';

EXEC sp_executesql @DropTable;

-- Optional: Print out the objects that were dropped
SELECT ObjectType, SchemaName, ObjectName
FROM @ObjectsToDrop
ORDER BY 
    CASE ObjectType 
        WHEN 'PROCEDURE' THEN 1 
        WHEN 'VIEW' THEN 2 
        WHEN 'TABLE' THEN 3 
    END;