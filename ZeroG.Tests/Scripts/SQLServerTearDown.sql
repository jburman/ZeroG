USE [ZeroGTestDB]

ALTER DATABASE [ZeroGTestDB]
    SET SINGLE_USER 
    WITH ROLLBACK IMMEDIATE
GO

EXEC sp_dropuser ZeroGSchema
EXEC sp_dropuser ZeroGData
GO

DROP DATABASE [ZeroGTestDB]
GO

USE [master]
GO

IF  EXISTS (SELECT * FROM sys.server_principals WHERE name = N'ZeroGSchema')
DROP LOGIN [ZeroGSchema]
GO

IF  EXISTS (SELECT * FROM sys.server_principals WHERE name = N'ZeroGData')
DROP LOGIN [ZeroGData]
GO