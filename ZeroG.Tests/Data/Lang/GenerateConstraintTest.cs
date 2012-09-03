using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ZeroG.Data.Database.Lang;
using System.Data;
using System.Collections.Generic;

namespace ZeroG.Tests.Data.Lang
{
    [TestClass]

    public class GenerateConstraintTest
    {
        public static Guid TestGuidVal = new Guid("{EF9D853C-6054-410D-8293-F2920EB63A90}");
        public static Guid TestGuidVal2 = new Guid("{4227208A-D1DA-4428-B005-EDC825799C4B}");
        public static Guid TestGuidVal3 = new Guid("{1A077C0C-506C-4B7A-B0F3-CA2DADAFE3A5}");

        public static DateTime TestDateTimeVal1 = new DateTime(2012, 1, 30, 12, 30, 0);
        public static DateTime TestDateTimeVal2 = new DateTime(2000, 7, 1, 23, 0, 59);
        public static DateTime TestDateTimeVal3 = new DateTime(1990, 11, 20, 0, 1, 1);

        [ClassInitialize()]
        public static void SetupConstraintTestTables(TestContext testContext)
        {
            TearDownConstraintTestTables();

            using (var db = DataTestHelper.GetMySQLSchemaService())
            {
                db.Open();

                db.ExecuteNonQuery(@"CREATE TABLE IF NOT EXISTS `ZeroGConstraintTest` (
    `TextCol` VARCHAR(36) NOT NULL,
    `DecimalCol` DECIMAL(7,4) NOT NULL,
    `IntCol` INT NOT NULL,
    `DateCol` DATETIME NOT NULL,
    `BinaryCol` BINARY(36) NOT NULL
    )
ENGINE = InnoDB DEFAULT CHARSET=utf8;");

                var insertSql = @"INSERT INTO `ZeroGConstraintTest` (`TextCol`, `DecimalCol`,`IntCol`,`DateCol`,`BinaryCol`)
VALUES (@textCol,@decCol,@intCol,@dateCol,@binCol)";

                db.ExecuteNonQuery(insertSql, 
                    db.MakeParam("textCol","ZeroG"),
                    db.MakeParam("decCol",1.0),
                    db.MakeParam("intCol",1),
                    db.MakeParam("dateCol",TestDateTimeVal1),
                    db.MakeParam("binCol",TestGuidVal.ToByteArray()));

                db.ExecuteNonQuery(insertSql,
                    db.MakeParam("textCol", "A"),
                    db.MakeParam("decCol", 3.2),
                    db.MakeParam("intCol", 3),
                    db.MakeParam("dateCol", TestDateTimeVal2),
                    db.MakeParam("binCol", TestGuidVal2.ToByteArray()));

                db.ExecuteNonQuery(insertSql,
                    db.MakeParam("textCol", "A"),
                    db.MakeParam("decCol", 7.33),
                    db.MakeParam("intCol", 10),
                    db.MakeParam("dateCol", TestDateTimeVal3),
                    db.MakeParam("binCol", TestGuidVal3.ToByteArray()));
            }
        }

        [ClassCleanup()]
        public static void TearDownConstraintTestTables()
        {
            using (var db = DataTestHelper.GetMySQLSchemaService())
            {
                db.Open();

                db.ExecuteNonQuery("DROP TABLE IF EXISTS `ZeroGConstraintTest`");
            }
        }

        [TestMethod]
        [TestCategory("MySQL")]
        public void SingleConstraintTest()
        {
            var json = @"{ ""TextCol"" : ""ZeroG"", ""Op"": ""="" }";

            using (var db = DataTestHelper.GetMySQLDataService())
            {
                db.Open();

                var typeMappings = new Dictionary<string, Type>(StringComparer.InvariantCultureIgnoreCase);
                typeMappings.Add("TextCol", typeof(string));

                var constraint = JSONToSQLConstraint.GenerateSQLConstraint(db, typeMappings, json);

                Assert.IsNotNull(constraint);

                Assert.IsTrue(!string.IsNullOrEmpty(constraint.SQL));

                var parameters = constraint.Parameters.ToArray();

                Assert.AreEqual(1, parameters.Length);

                Assert.AreEqual(DbType.String, parameters[0].DbType);
                Assert.AreEqual("ZeroG", parameters[0].Value);

                var sql = "SELECT TextCol, IntCol FROM `ZeroGConstraintTest` WHERE" +
                    constraint.SQL;

                var dt = db.GetDataTable(sql, constraint.Parameters.ToArray());

                Assert.AreEqual(1, dt.Rows.Count);
                Assert.AreEqual("ZeroG", dt.Rows[0]["TextCol"]);
                Assert.AreEqual(1, dt.Rows[0]["IntCol"]);
            }
        }

        [TestMethod]
        [TestCategory("MySQL")]
        public void MultiConstraintTest()
        {
            var json = @"{ ""TextCol"" : ""ZeroG"", ""Op"": ""="",
""AND"": { ""IntCol"" : 2, ""Op"": ""<""} }";

            using (var db = DataTestHelper.GetMySQLDataService())
            {
                db.Open();

                var typeMappings = new Dictionary<string, Type>(StringComparer.InvariantCultureIgnoreCase);
                typeMappings.Add("TextCol", typeof(string));
                typeMappings.Add("IntCol", typeof(int));

                var constraint = JSONToSQLConstraint.GenerateSQLConstraint(db, typeMappings, json);

                Assert.IsNotNull(constraint);

                Assert.IsTrue(!string.IsNullOrEmpty(constraint.SQL));

                var parameters = constraint.Parameters.ToArray();

                Assert.AreEqual(2, parameters.Length);

                Assert.AreEqual(DbType.String, parameters[0].DbType);
                Assert.AreEqual("ZeroG", parameters[0].Value);

                Assert.AreEqual(DbType.Int32, parameters[1].DbType);
                Assert.AreEqual(2, parameters[1].Value);

                var sql = "SELECT TextCol, IntCol FROM `ZeroGConstraintTest` WHERE" +
                    constraint.SQL;

                var dt = db.GetDataTable(sql, constraint.Parameters.ToArray());

                Assert.AreEqual(1, dt.Rows.Count);
                Assert.AreEqual("ZeroG", dt.Rows[0]["TextCol"]);
                Assert.AreEqual(1, dt.Rows[0]["IntCol"]);
            }
        }

        [TestMethod]
        [TestCategory("MySQL")]
        public void GroupedConstraintTest()
        {
            var json = @"{ ""IntCol"" : 1, ""Op"": ""="",
""OR"": [{ ""TextCol"" : ""ZeroG"", ""Op"": ""=""}, ""AND"", {""DateCol"" : ""2012-1-30 12:30:00"", ""Op"": ""="" }] }";

            using (var db = DataTestHelper.GetMySQLDataService())
            {
                db.Open();

                var typeMappings = new Dictionary<string, Type>(StringComparer.InvariantCultureIgnoreCase);
                typeMappings.Add("TextCol", typeof(string));
                typeMappings.Add("IntCol", typeof(int));
                typeMappings.Add("DateCol", typeof(DateTime));

                var constraint = JSONToSQLConstraint.GenerateSQLConstraint(db, typeMappings, json);

                Assert.IsNotNull(constraint);

                Assert.IsTrue(!string.IsNullOrEmpty(constraint.SQL));

                var parameters = constraint.Parameters.ToArray();

                Assert.AreEqual(3, parameters.Length);

                Assert.AreEqual(DbType.Int32, parameters[0].DbType);
                Assert.AreEqual(1, parameters[0].Value);

                Assert.AreEqual(DbType.String, parameters[1].DbType);
                Assert.AreEqual("ZeroG", parameters[1].Value);

                Assert.AreEqual(DbType.DateTime, parameters[2].DbType);
                Assert.AreEqual(TestDateTimeVal1, parameters[2].Value);

                var sql = "SELECT TextCol, IntCol FROM `ZeroGConstraintTest` WHERE" +
                    constraint.SQL;

                var dt = db.GetDataTable(sql, constraint.Parameters.ToArray());

                Assert.AreEqual(1, dt.Rows.Count);
                Assert.AreEqual("ZeroG", dt.Rows[0]["TextCol"]);
                Assert.AreEqual(1, dt.Rows[0]["IntCol"]);
            }
        }

        [TestMethod]
        [TestCategory("MySQL")]
        public void OperatorsTest()
        {
            using (var db = DataTestHelper.GetMySQLDataService())
            {
                db.Open();



                /* Tests the following list of operators
                    Equals,
                    NotEquals,
                    Like,
                    NotLike,
                    In,
                    NotIn,
                    LessThan,
                    GreaterThan
                    */
                
            }
        }
    }
}
