using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text;

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

                db.ExecuteNonQuery(@"CREATE TABLE IF NOT EXISTS `ZG_BulkInsertTest` (
    `ID` INT NOT NULL PRIMARY KEY,
    `Name` VARCHAR(36) NOT NULL,
    `DecValue` DECIMAL(7,4) NOT NULL,
    `DTValue` DATETIME NOT NULL,
    `BinValue` BINARY(16) NOT NULL
) ENGINE = InnoDB DEFAULT CHARSET=utf8;");
            }
        }

        [TestCleanup]
        public void CleanupTests()
        {
            using (var db = DataTestHelper.GetDefaultSchemaService())
            {
                db.Open();

                bool tableExists = false;

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
        }

        [TestMethod]
        public void BulkInsert()
        {
            var dt1 = new DateTime(2012, 1, 1);

            var guid1 = new Guid("{14A30DE4-6794-4C2A-BB30-CF8F794184DB}");

            using (var db = DataTestHelper.GetDefaultDataService())
            {
                db.Open();

                db.ExecuteBulkInsert(
                    new object[][] 
                    {
                        new object[] { 2, "Test1", 7.5, dt1, guid1.ToByteArray() },
                        new object[] { 3, "Test2", 101.2, dt1, guid1.ToByteArray() },
                        new object[] { 4, "Test3", 7.134, dt1, guid1.ToByteArray() },
                    },
                    "ZG_BulkInsertTest",
                    new string[] { "ID", "Name", "DecValue", "DTValue", "BinValue" });

                Assert.AreEqual(3, db.ExecuteScalar<int>("SELECT COUNT(*) FROM ZG_BulkInsertTest",
                    0));

                Assert.AreEqual(2, db.ExecuteScalar<int>("SELECT COUNT(*) FROM ZG_BulkInsertTest WHERE ID > 2",
                    0));

                Assert.AreEqual(1, db.ExecuteScalar<int>("SELECT COUNT(*) FROM ZG_BulkInsertTest WHERE Name = 'Test2'",
                    0));

                Assert.AreEqual(1, db.ExecuteScalar<int>("SELECT COUNT(*) FROM ZG_BulkInsertTest WHERE DecValue = @p",
                    0, db.MakeParam("p", 7.134)));

                Assert.AreEqual(3, db.ExecuteScalar<int>("SELECT COUNT(*) FROM ZG_BulkInsertTest WHERE BinValue = @p",
                    0, db.MakeParam("p", guid1.ToByteArray())));

                // test values are overwritten
                db.ExecuteBulkInsert(
                    new object[][] 
                    {
                        new object[] { 2, "TestA", 7.5, dt1, guid1.ToByteArray() },
                        new object[] { 3, "TestB", 101.2, dt1, guid1.ToByteArray() },
                        new object[] { 4, "TestC", 11.1, dt1, guid1.ToByteArray() },
                    },
                    "ZG_BulkInsertTest",
                    new string[] { "ID", "Name", "DecValue", "DTValue", "BinValue" });

                Assert.AreEqual(3, db.ExecuteScalar<int>("SELECT COUNT(*) FROM ZG_BulkInsertTest",
                    0));

                Assert.AreEqual(2, db.ExecuteScalar<int>("SELECT COUNT(*) FROM ZG_BulkInsertTest WHERE ID > 2",
                    0));

                Assert.AreEqual(1, db.ExecuteScalar<int>("SELECT COUNT(*) FROM ZG_BulkInsertTest WHERE Name = 'TestB'",
                    0));

                Assert.AreEqual(1, db.ExecuteScalar<int>("SELECT COUNT(*) FROM ZG_BulkInsertTest WHERE DecValue = @p",
                    0, db.MakeParam("p", 11.1)));

                Assert.AreEqual(3, db.ExecuteScalar<int>("SELECT COUNT(*) FROM ZG_BulkInsertTest WHERE BinValue = @p",
                    0, db.MakeParam("p", guid1.ToByteArray())));

                // test values with escaped chars in them
                db.ExecuteBulkInsert(
                    new object[][] 
                    {
                        new object[] { 5, "test\tval\n123", 7.5, dt1, 
                            Encoding.UTF8.GetBytes("aa\tbb\ncc") }
                    },
                    "ZG_BulkInsertTest",
                    new string[] { "ID", "Name", "DecValue", "DTValue", "BinValue" });

                Assert.AreEqual(4, db.ExecuteScalar<int>("SELECT COUNT(*) FROM ZG_BulkInsertTest",
                    0));

                Assert.AreEqual(1, db.ExecuteScalar<int>("SELECT COUNT(*) FROM ZG_BulkInsertTest WHERE Name = @p",
                    0, db.MakeParam("p", "test\tval\n123")));

                Assert.AreEqual(1, db.ExecuteScalar<int>("SELECT COUNT(*) FROM ZG_BulkInsertTest WHERE LEFT(BinValue, 8) = @p",
                    0, db.MakeParam("p", Encoding.UTF8.GetBytes("aa\tbb\ncc"))));
            }
        }
    }
}
