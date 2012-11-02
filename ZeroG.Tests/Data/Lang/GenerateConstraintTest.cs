using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ZeroG.Data.Database.Lang;
using System.Data;
using System.Collections.Generic;
using ZeroG.Data.Database.Drivers;
using ZeroG.Data.Database;

namespace ZeroG.Tests.Data.Lang
{
    [TestClass]
    public class GenerateConstraintTest
    {
        public static Guid TestGuidVal = new Guid("{EF9D853C-6054-410D-8293-F2920EB63A90}");
        public static Guid TestGuidVal2 = new Guid("{4227208A-D1DA-4428-B005-EDC825799C4B}");
        public static Guid TestGuidVal3 = new Guid("{1A077C0C-506C-4B7A-B0F3-CA2DADAFE3A5}");
        public static Guid TestGuidVal4 = new Guid("{3643A5D3-597D-4CA0-A618-273456F9571E}");

        public static DateTime TestDateTimeVal1 = new DateTime(2012, 1, 30, 12, 30, 0);
        public static DateTime TestDateTimeVal2 = new DateTime(2000, 7, 1, 23, 0, 59);
        public static DateTime TestDateTimeVal3 = new DateTime(1990, 11, 20, 0, 1, 1);
        public static DateTime TestDateTimeVal4 = new DateTime(1789, 5, 5, 0, 5, 5);

        [ClassInitialize()]
        public static void SetupConstraintTestTables(TestContext testContext)
        {
            TearDownConstraintTestTables();

            using (var db = DataTestHelper.GetDefaultSchemaService())
            {
                db.Open();

                var createTableSQL = @"CREATE TABLE IF NOT EXISTS `ZeroGConstraintTest` (
    `TextCol` VARCHAR(36) NULL,
    `DecimalCol` DECIMAL(7,4) NOT NULL,
    `IntCol` INT NOT NULL,
    `DateCol` DATETIME NOT NULL,
    `BinaryCol` VARBINARY(36) NOT NULL
    )
ENGINE = InnoDB DEFAULT CHARSET=utf8;";

                var insertSql = @"INSERT INTO `ZeroGConstraintTest` (`TextCol`, `DecimalCol`,`IntCol`,`DateCol`,`BinaryCol`)
VALUES (@textCol,@decCol,@intCol,@dateCol,@binCol)";

                if (db is SQLServerDatabaseService)
                {
                    createTableSQL = @"IF NOT EXISTS (select * from sysobjects where name='ZeroGConstraintTest' and xtype='U')
    CREATE TABLE [ZeroG].[ZeroGConstraintTest](
	[TextCol] [nvarchar](36) NULL,
	[DecimalCol] [decimal](7, 4) NOT NULL,
    [IntCol] [int] NOT NULL,
	[DateCol] [datetime] NOT NULL,
	[BinaryCol] [varbinary](16) NOT NULL
) ON [PRIMARY]";

                    insertSql = @"INSERT INTO [ZeroG].[ZeroGConstraintTest] ([TextCol], [DecimalCol],[IntCol],[DateCol],[BinaryCol])
VALUES (@textCol,@decCol,@intCol,@dateCol,@binCol)";

                }
                else if (db is SQLiteDatabaseService)
                {
                    createTableSQL = @"CREATE TABLE IF NOT EXISTS `ZeroGConstraintTest` (
    [TextCol] VARCHAR(36) NULL,
    [DecimalCol] DECIMAL(7,4) NOT NULL,
    [IntCol] INT NOT NULL,
    [DateCol] DATETIME NOT NULL,
    [BinaryCol] VARBINARY(36) NOT NULL
    )";

                    insertSql = @"INSERT INTO [ZeroGConstraintTest] ([TextCol], [DecimalCol],[IntCol],[DateCol],[BinaryCol])
VALUES (@textCol,@decCol,@intCol,@dateCol,@binCol)";
                }

                db.ExecuteNonQuery(createTableSQL);

                db.ExecuteNonQuery(insertSql, 
                    db.MakeParam("textCol","ZeroG"),
                    db.MakeParam("decCol", 1.0M),
                    db.MakeParam("intCol", 1),
                    db.MakeParam("dateCol", TestDateTimeVal1),
                    db.MakeParam("binCol", TestGuidVal.ToByteArray()));

                db.ExecuteNonQuery(insertSql,
                    db.MakeParam("textCol", "A"),
                    db.MakeParam("decCol", 3.2M),
                    db.MakeParam("intCol", 3),
                    db.MakeParam("dateCol", TestDateTimeVal2),
                    db.MakeParam("binCol", TestGuidVal2.ToByteArray()));

                db.ExecuteNonQuery(insertSql,
                    db.MakeParam("textCol", "A"),
                    db.MakeParam("decCol", 7.33M),
                    db.MakeParam("intCol", 10),
                    db.MakeParam("dateCol", TestDateTimeVal3),
                    db.MakeParam("binCol", TestGuidVal3.ToByteArray()));

                db.ExecuteNonQuery(insertSql,
                    db.MakeParam("textCol", null),
                    db.MakeParam("decCol", 12M),
                    db.MakeParam("intCol", 13),
                    db.MakeParam("dateCol", TestDateTimeVal4),
                    db.MakeParam("binCol", TestGuidVal4.ToByteArray()));
            }
        }

        [ClassCleanup()]
        public static void TearDownConstraintTestTables()
        {
            using (var db = DataTestHelper.GetDefaultSchemaService())
            {
                db.Open();

                if (db is SQLServerDatabaseService)
                {
                    db.ExecuteNonQuery(@"IF EXISTS (select * from sysobjects where name='ZeroGConstraintTest' and xtype='U')
DROP TABLE " + _GetTableName(db));
                }
                else
                {
                    db.ExecuteNonQuery("DROP TABLE IF EXISTS " + _GetTableName(db));
                }
            }
        }

        private static string _GetTableName(IDatabaseService db)
        {
            if (db is SQLServerDatabaseService)
            {
                return "[ZeroG].[ZeroGConstraintTest]";
            }
            else 
            {
                return "`ZeroGConstraintTest`";
            }
        }

        [TestMethod]
        public void SingleConstraintTest()
        {
            var json = @"{ ""TextCol"" : ""ZeroG"", ""Op"": ""="" }";

            using (var db = DataTestHelper.GetDefaultDataService())
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

                var sql = "SELECT TextCol, IntCol FROM " + _GetTableName(db) + " WHERE" +
                    constraint.SQL;

                var dt = db.GetDataTable(sql, constraint.Parameters.ToArray());

                Assert.AreEqual(1, dt.Rows.Count);
                Assert.AreEqual("ZeroG", dt.Rows[0]["TextCol"]);
                Assert.AreEqual(1, dt.Rows[0]["IntCol"]);
            }
        }

        [TestMethod]
        public void NullConstraintTest()
        {
            var json = @"{ ""TextCol"" : null }";

            using (var db = DataTestHelper.GetDefaultDataService())
            {
                db.Open();

                var typeMappings = new Dictionary<string, Type>(StringComparer.InvariantCultureIgnoreCase);
                typeMappings.Add("TextCol", typeof(string));

                var constraint = JSONToSQLConstraint.GenerateSQLConstraint(db, typeMappings, json);

                Assert.IsNotNull(constraint);

                Assert.IsTrue(!string.IsNullOrEmpty(constraint.SQL));

                var parameters = constraint.Parameters.ToArray();

                Assert.AreEqual(0, parameters.Length);

                var sql = "SELECT TextCol, IntCol FROM " + _GetTableName(db) + " WHERE" +
                    constraint.SQL;

                var dt = db.GetDataTable(sql, constraint.Parameters.ToArray());

                Assert.AreEqual(1, dt.Rows.Count);
                Assert.AreEqual(DBNull.Value, dt.Rows[0]["TextCol"]);
                Assert.AreEqual(13, dt.Rows[0]["IntCol"]);
            }
        }

        [TestMethod]
        public void MultiConstraintTest()
        {
            var json = @"{ ""TextCol"" : ""ZeroG"", ""Op"": ""="",
""AND"": { ""IntCol"" : 2, ""Op"": ""<""} }";

            using (var db = DataTestHelper.GetDefaultDataService())
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

                var sql = "SELECT TextCol, IntCol FROM " + _GetTableName(db) + " WHERE" +
                    constraint.SQL;

                var dt = db.GetDataTable(sql, constraint.Parameters.ToArray());

                Assert.AreEqual(1, dt.Rows.Count);
                Assert.AreEqual("ZeroG", dt.Rows[0]["TextCol"]);
                Assert.AreEqual(1, dt.Rows[0]["IntCol"]);
            }
        }

        [TestMethod]
        public void MultiConstraintWithNullTest()
        {
            var json = @"{ ""IntCol"" : 12, ""Op"": "">"",
""AND"": { ""TextCol"" : null } }";

            using (var db = DataTestHelper.GetDefaultDataService())
            {
                db.Open();

                var typeMappings = new Dictionary<string, Type>(StringComparer.InvariantCultureIgnoreCase);
                typeMappings.Add("TextCol", typeof(string));
                typeMappings.Add("IntCol", typeof(int));

                var constraint = JSONToSQLConstraint.GenerateSQLConstraint(db, typeMappings, json);

                Assert.IsNotNull(constraint);

                Assert.IsTrue(!string.IsNullOrEmpty(constraint.SQL));

                var parameters = constraint.Parameters.ToArray();

                Assert.AreEqual(1, parameters.Length);

                Assert.AreEqual(DbType.Int32, parameters[0].DbType);
                Assert.AreEqual(12, parameters[0].Value);

                var sql = "SELECT TextCol, IntCol FROM " + _GetTableName(db) + " WHERE" +
                    constraint.SQL;

                var dt = db.GetDataTable(sql, constraint.Parameters.ToArray());

                Assert.AreEqual(1, dt.Rows.Count);
                Assert.AreEqual(DBNull.Value, dt.Rows[0]["TextCol"]);
                Assert.AreEqual(13, dt.Rows[0]["IntCol"]);
            }
        }

        [TestMethod]
        public void GroupedConstraintTest()
        {
            var json = @"{ ""IntCol"" : 1, ""Op"": ""="",
""OR"": [{ ""TextCol"" : ""ZeroG"", ""Op"": ""=""}, ""AND"", {""DateCol"" : ""2012-1-30 12:30:00"", ""Op"": ""="" }] }";

            using (var db = DataTestHelper.GetDefaultDataService())
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

                var sql = "SELECT TextCol, IntCol FROM " + _GetTableName(db) + " WHERE" +
                    constraint.SQL;

                var dt = db.GetDataTable(sql, constraint.Parameters.ToArray());

                Assert.AreEqual(1, dt.Rows.Count);
                Assert.AreEqual("ZeroG", dt.Rows[0]["TextCol"]);
                Assert.AreEqual(1, dt.Rows[0]["IntCol"]);
            }
        }

        [TestMethod]
        public void OperatorsTest()
        {
            using (var db = DataTestHelper.GetDefaultDataService())
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
