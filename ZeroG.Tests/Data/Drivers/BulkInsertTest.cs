using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text;
using ZeroG.Data.Database.Drivers;
using System.Collections.Generic;

namespace ZeroG.Tests.Data.Drivers
{
    [TestClass]
    public class BulkInsertTest
    {
        [TestInitialize]
        public void InitTests()
        {
            CleanupTests();

            using (var db = DataTestHelper.GetDefaultSchemaService())
            {
                db.Open();

                if (db is MySQLDatabaseService)
                {
                    db.ExecuteNonQuery(@"CREATE TABLE IF NOT EXISTS `ZG_BulkInsertTest` (
    `ID` INT NOT NULL PRIMARY KEY,
    `Name` VARCHAR(36) NOT NULL,
    `DecValue` DECIMAL(9,4) NOT NULL,
    `DTValue` DATETIME NOT NULL,
    `BinValue` VARBINARY(16) NOT NULL
) ENGINE = InnoDB DEFAULT CHARSET=utf8;");
                }
                else if (db is SQLServerDatabaseService)
                {
                    db.ExecuteNonQuery(@"IF NOT EXISTS (select * from sysobjects where name='ZG_BulkInsertTest' and xtype='U')
    CREATE TABLE [ZeroG].[ZG_BulkInsertTest](
	[ID] [int] NOT NULL,
	[Name] [nvarchar](36) NOT NULL,
	[DecValue] [decimal](9, 4) NOT NULL,
	[DTValue] [datetime] NOT NULL,
	[BinValue] [varbinary](16) NOT NULL,
 CONSTRAINT [PK_ZG_BulkInsertTest] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]");
                }
                else if (db is SQLiteDatabaseService)
                {
                    db.ExecuteNonQuery(@"CREATE TABLE IF NOT EXISTS `ZG_BulkInsertTest` (
    [ID] INT NOT NULL PRIMARY KEY,
    [Name] VARCHAR(36) NOT NULL,
    [DecValue] DECIMAL(9,4) NOT NULL,
    [DTValue] DATETIME NOT NULL,
    [BinValue] VARBINARY(16) NOT NULL
)");
                }
            }
        }

        [TestCleanup]
        public void CleanupTests()
        {
            using (var db = DataTestHelper.GetDefaultSchemaService())
            {
                db.Open();

                bool tableExists = false;

                if (db is MySQLDatabaseService)
                {
                    using (var reader = db.ExecuteReader("SHOW TABLES LIKE 'ZG_BulkInsertTest'"))
                    {
                        if (reader.Read())
                        {
                            tableExists = true;
                        }
                    }
                    if (tableExists)
                    {
                        db.ExecuteNonQuery("DROP TABLE ZG_BulkInsertTest");
                    }
                }
                else if (db is SQLServerDatabaseService)
                {
                    if (0 < db.ExecuteScalar<int>("select COUNT(*) from sysobjects where name='ZG_BulkInsertTest' and xtype='U'", 0))
                    {
                        db.ExecuteNonQuery("DROP TABLE [ZeroG].[ZG_BulkInsertTest]");
                    }
                }
                else if (db is SQLiteDatabaseService)
                {
                    db.ExecuteNonQuery("DROP TABLE IF EXISTS [ZG_BulkInsertTest]");
                }
            }
        }

        [TestMethod]
        public void BulkInsert()
        {
            var dt1 = new DateTime(2012, 1, 1);

            var guid1 = new Guid("{14A30DE4-6794-4C2A-BB30-CF8F794184DB}");

            using (var db = DataTestHelper.GetDefaultSchemaService())
            {
                db.Open();

                string tableName = "ZG_BulkInsertTest";
                if (db is SQLServerDatabaseService)
                {
                    tableName = "[ZeroG].[ZG_BulkInsertTest]";
                }

                db.ExecuteBulkInsert(
                    new object[][] 
                    {
                        new object[] { 2, "Test1", 7.5, dt1, guid1.ToByteArray() },
                        new object[] { 3, "Test2", 101.2, dt1, guid1.ToByteArray() },
                        new object[] { 4, "Test3", 7.134, dt1, guid1.ToByteArray() },
                    },
                    tableName,
                    new string[] { "ID", "Name", "DecValue", "DTValue", "BinValue" });

                Assert.AreEqual(3, db.ExecuteScalar<int>("SELECT COUNT(*) FROM " + tableName,
                    0));

                Assert.AreEqual(2, db.ExecuteScalar<int>("SELECT COUNT(*) FROM " + tableName + " WHERE ID > 2",
                    0));

                Assert.AreEqual(1, db.ExecuteScalar<int>("SELECT COUNT(*) FROM " + tableName + " WHERE Name = 'Test2'",
                    0));

                Assert.AreEqual(1, db.ExecuteScalar<int>("SELECT COUNT(*) FROM " + tableName + " WHERE DecValue = @p",
                    0, db.MakeParam("p", 7.134)));

                Assert.AreEqual(3, db.ExecuteScalar<int>("SELECT COUNT(*) FROM " + tableName + " WHERE BinValue = @p",
                    0, db.MakeParam("p", guid1.ToByteArray())));

                // test adding more values
                db.ExecuteBulkInsert(
                    new object[][] 
                    {
                        new object[] { 5, "TestA", 7.5, dt1, guid1.ToByteArray() },
                        new object[] { 6, "TestB", 101.2, dt1, guid1.ToByteArray() },
                        new object[] { 7, "TestC", 11.1, dt1, guid1.ToByteArray() },
                    },
                    tableName,
                    new string[] { "ID", "Name", "DecValue", "DTValue", "BinValue" });

                Assert.AreEqual(6, db.ExecuteScalar<int>("SELECT COUNT(*) FROM " + tableName,
                    0));

                Assert.AreEqual(3, db.ExecuteScalar<int>("SELECT COUNT(*) FROM " + tableName + " WHERE ID > 4",
                    0));

                Assert.AreEqual(1, db.ExecuteScalar<int>("SELECT COUNT(*) FROM " + tableName + " WHERE Name = 'TestB'",
                    0));

                Assert.AreEqual(1, db.ExecuteScalar<int>("SELECT COUNT(*) FROM " + tableName + " WHERE DecValue = @p",
                    0, db.MakeParam("p", 11.1)));

                Assert.AreEqual(6, db.ExecuteScalar<int>("SELECT COUNT(*) FROM " + tableName + " WHERE BinValue = @p",
                    0, db.MakeParam("p", guid1.ToByteArray())));

                // test values with escaped chars in them
                db.ExecuteBulkInsert(
                    new object[][] 
                    {
                        new object[] { 8, "test\tval\n123", 7.5, dt1, 
                            Encoding.UTF8.GetBytes("aa\tbb\ncc") }
                    },
                    tableName,
                    new string[] { "ID", "Name", "DecValue", "DTValue", "BinValue" });

                Assert.AreEqual(7, db.ExecuteScalar<int>("SELECT COUNT(*) FROM " + tableName,
                    0));

                Assert.AreEqual(1, db.ExecuteScalar<int>("SELECT COUNT(*) FROM " + tableName + " WHERE Name = @p",
                    0, db.MakeParam("p", "test\tval\n123")));

                Assert.AreEqual(1, db.ExecuteScalar<int>("SELECT COUNT(*) FROM " + tableName + " WHERE BinValue = @p",
                    0, db.MakeParam("p", Encoding.UTF8.GetBytes("aa\tbb\ncc"))));
            }
        }

        [TestMethod]
        public void BulkInsertMany()
        {
            using (var db = DataTestHelper.GetDefaultSchemaService())
            {
                db.Open();

                string tableName = "ZG_BulkInsertTest";
                if (db is SQLServerDatabaseService)
                {
                    tableName = "[ZeroG].[ZG_BulkInsertTest]";
                }

                var list = new List<object[]>();
                var count = 100000;
                for (int i = 0; count > i; i++)
                {
                    list.Add(new object[] { i, "test" + i, (decimal)(i + 0.1), DateTime.Now, new Guid().ToByteArray() });
                }

                db.ExecuteNonQuery("DELETE FROM " + tableName);

                db.ExecuteBulkInsert(
                    list,
                    tableName,
                    new string[] { "ID", "Name", "DecValue", "DTValue", "BinValue" });
                Assert.AreEqual(count, db.ExecuteScalar<int>("SELECT COUNT(*) FROM " + tableName, 0));
            }
        }
    }
}
