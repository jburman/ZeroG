USE [master]
GO
CREATE DATABASE [ZeroGTestDB]
GO
CREATE LOGIN [ZeroGData] WITH PASSWORD=N'ZeroG,4621Dat', DEFAULT_DATABASE=[ZeroGTestDB], DEFAULT_LANGUAGE=[us_english], CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF
GO
CREATE LOGIN [ZeroGSchema] WITH PASSWORD=N'ZeroG,988Sch', DEFAULT_DATABASE=[ZeroGTestDB], DEFAULT_LANGUAGE=[us_english], CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF
GO

USE [ZeroGTestDB]
GO

EXEC sp_grantdbaccess 'ZeroGSchema', 'ZeroGSchema'
EXEC sp_addrolemember 'db_ddladmin', 'ZeroGSchema'
EXEC sp_addrolemember 'db_datareader', 'ZeroGSchema'
EXEC sp_addrolemember 'db_datawriter', 'ZeroGSchema'

EXEC sp_grantdbaccess 'ZeroGData', 'ZeroGData'
EXEC sp_addrolemember 'db_datareader', 'ZeroGData'
EXEC sp_addrolemember 'db_datawriter', 'ZeroGData'
GO

