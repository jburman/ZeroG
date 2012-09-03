using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ZeroG.Data.Database.Lang;
using System.Data;

namespace ZeroG.Tests.Data.Lang
{
    [TestClass]

    public class GenerateConstraintTest
    {
        [TestMethod]
        [TestCategory("MySQL")]
        public void SingleConstraintTest()
        {
            var json = @"{ ""TestCol1"" : ""ZeroG"", ""Op"": ""="" }";

            using (var db = DataTestHelper.GetMySQLDataService())
            {
                var constraint = JSONToSQLConstraint.GenerateSQLConstraint(db, json);

                Assert.IsNotNull(constraint);

                Assert.IsTrue(!string.IsNullOrEmpty(constraint.SQL));

                var parameters = constraint.Parameters.ToArray();

                Assert.AreEqual(1, parameters.Length);

                Assert.AreEqual(DbType.String, parameters[0].DbType);
                Assert.AreEqual("ZeroG", parameters[0].Value);
            }
        }

        [TestMethod]
        [TestCategory("MySQL")]
        public void NestedConstraintTest()
        {
            var json = @"{ ""TestCol1"" : ""ZeroG"", ""Op"": ""="",
""AND"": { ""TestCol2"" : 5, ""Op"": "">""} }";

            using (var db = DataTestHelper.GetMySQLDataService())
            {
                var constraint = JSONToSQLConstraint.GenerateSQLConstraint(db, json);

                Assert.IsNotNull(constraint);

                Assert.IsTrue(!string.IsNullOrEmpty(constraint.SQL));

                var parameters = constraint.Parameters.ToArray();

                Assert.AreEqual(2, parameters.Length);

                Assert.AreEqual(DbType.String, parameters[0].DbType);
                Assert.AreEqual("ZeroG", parameters[0].Value);

                Assert.AreEqual(DbType.Double, parameters[1].DbType);
                Assert.AreEqual(5d, parameters[1].Value);
            }
        }
    }
}
