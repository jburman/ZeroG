using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Text;
using ZeroG.Data.Database;
using ZeroG.Data.Database.Drivers.Object.Provider;
using ZeroG.Data.Object.Index;
using ZeroG.Data.Object.Metadata;
using ZeroG.Data.Object;

namespace ZeroG.Tests.Data.Drivers.MySQL
{
    /// <summary>
    /// Summary description for IndexProviderTest
    /// </summary>
    [TestClass]
    public class IndexProviderTest
    {
        public IndexProviderTest()
        {
        }

        private TestContext testContextInstance;

        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext
        {
            get
            {
                return testContextInstance;
            }
            set
            {
                testContextInstance = value;
            }
        }

        internal static string NameSpace1 = "ZGTestNS";
        internal static string ObjectName1 = "ZGTestObj1";
        internal static string ObjectFullName1 = "ZGTestNS.ZGTestObj1";

        #region Additional test attributes
        internal static MySQLObjectIndexProvider IndexProvider
        {
            get
            {
                return new MySQLObjectIndexProvider(DataTestHelper.MySQLSchemaUpdater, DataTestHelper.MySQLDataAccess);
            }
        }

        [TestInitialize()]
        public void TestInitialize() 
        {
            try
            {
                IndexProvider.UnprovisionIndex(ObjectFullName1);
            }
            catch
            {
            }
        }
        
        [TestCleanup()]
        public void TestCleanup() 
        {
            try
            {
                IndexProvider.UnprovisionIndex(ObjectFullName1);
            }
            catch
            {
            }
        }
        
        #endregion

        [TestMethod]
        [TestCategory("MySQL")]
        public void ProvisionIndex()
        {
            var provider = IndexProvider;

            provider.ProvisionIndex(
                new ObjectMetadata(NameSpace1, ObjectName1,
                    new ObjectIndexMetadata[]
                    {
                        new ObjectIndexMetadata("TestCol1", ObjectIndexType.Integer)
                    }));

            // verify that we can lookup using the index
            int[] ids = provider.Find(ObjectFullName1,
                new ObjectIndex("TestCol1", 100));

            Assert.IsNotNull(ids);

            Assert.AreEqual(0, ids.Length);
        }

        [TestMethod]
        [TestCategory("MySQL")]
        [ExpectedException(typeof(MySql.Data.MySqlClient.MySqlException))]
        public void UnprovisionTest()
        {
            var provider = IndexProvider;

            try
            {
                provider.ProvisionIndex(
                    new ObjectMetadata(NameSpace1, ObjectName1,
                        new ObjectIndexMetadata[]
                    {
                        new ObjectIndexMetadata("TestCol1", ObjectIndexType.Integer)
                    }));

                // verify that we can lookup using the index
                int[] ids = provider.Find(ObjectFullName1,
                    new ObjectIndex("TestCol1", 100));

                Assert.IsNotNull(ids);

                Assert.AreEqual(0, ids.Length);

                provider.UnprovisionIndex(ObjectFullName1);
            }
            catch(Exception ex)
            {
                Assert.Fail("Unexpected exception: " + ex.ToString());
            }

            // should throw
            provider.Find(ObjectFullName1,
                new ObjectIndex("TestCol1", 100));
        }

        [TestMethod]
        [TestCategory("MySQL")]
        public void SimpleFind()
        {
            var provider = IndexProvider;

            provider.ProvisionIndex(
                new ObjectMetadata(NameSpace1, ObjectName1,
                    new ObjectIndexMetadata[]
                    {
                        new ObjectIndexMetadata("TestCol1", ObjectIndexType.Integer),
                        new ObjectIndexMetadata("TestCol2", ObjectIndexType.String, 15)
                    }));

            provider.UpsertIndexValues(ObjectFullName1, 1,
                new ObjectIndex("TestCol1", 100),
                new ObjectIndex("TestCol2", "A"));

            provider.UpsertIndexValues(ObjectFullName1, 2,
                new ObjectIndex("TestCol1", 105),
                new ObjectIndex("TestCol2", "A"));

            provider.UpsertIndexValues(ObjectFullName1, 3,
                new ObjectIndex("TestCol1", 500),
                new ObjectIndex("TestCol2", "B"));

            provider.UpsertIndexValues(ObjectFullName1, 4,
                new ObjectIndex("TestCol1", 500),
                new ObjectIndex("TestCol2", "C"));


            // test single constraint value that should return a single result
            int[] ids = provider.Find(ObjectFullName1,
                new ObjectIndex("TestCol1", 100));

            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1, ids[0]);

            // test two constraint values that should return a single result
            ids = provider.Find(ObjectFullName1,
                new ObjectIndex("TestCol1", 100),
                new ObjectIndex("TestCol2", "A"));

            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1, ids[0]);

            // test single constraint value that should return two results
            ids = provider.Find(ObjectFullName1,
                new ObjectIndex("TestCol1", 500));

            Assert.AreEqual(2, ids.Length);
            Assert.AreEqual(3, ids[0]);
            Assert.AreEqual(4, ids[1]);

            // test single constraint value that should return zero results
            ids = provider.Find(ObjectFullName1,
                new ObjectIndex("TestCol1", 105),
                new ObjectIndex("TestCol2", "B"));

            Assert.AreEqual(0, ids.Length);
        }

        [TestMethod]
        [TestCategory("MySQL")]
        public void SimpleFindDataTypes()
        {
            var provider = IndexProvider;

            provider.ProvisionIndex(
                new ObjectMetadata(NameSpace1, ObjectName1,
                    new ObjectIndexMetadata[]
                    {
                        new ObjectIndexMetadata("IntCol", ObjectIndexType.Integer),
                        new ObjectIndexMetadata("TextCol", ObjectIndexType.String, 15),
                        new ObjectIndexMetadata("DecCol", ObjectIndexType.Decimal, 7, 2),
                        new ObjectIndexMetadata("DateTimeCol", ObjectIndexType.DateTime),
                        new ObjectIndexMetadata("BinCol", ObjectIndexType.Binary, 32)
                    }));

            Int32 testInt = 3447;
            String testStr = "Test Value";
            Decimal testDec = 156.12M;
            DateTime testDate = new DateTime(2011, 2, 14, 3, 10, 0);
            Guid testGuid = new Guid("76F5FB10BAEF4DE09578B3EB91FF6653");
            string testBinStr = DatabaseHelper.ByteToHexString(testGuid.ToByteArray());

            provider.UpsertIndexValues(ObjectFullName1,
                1000,
                new ObjectIndex("IntCol", testInt),
                new ObjectIndex("TextCol", testStr),
                new ObjectIndex("DecCol", testDec),
                new ObjectIndex("DateTimeCol", testDate),
                new ObjectIndex("BinCol", testGuid.ToByteArray()));

            provider.UpsertIndexValues(ObjectFullName1,
                1001,
                new ObjectIndex("IntCol", 500),
                new ObjectIndex("TextCol", "asdf"),
                new ObjectIndex("DecCol", 5.4),
                new ObjectIndex("DateTimeCol", DateTime.UtcNow),
                new ObjectIndex("BinCol", Guid.NewGuid().ToByteArray()));

            int[] ids = provider.Find(ObjectFullName1, new ObjectIndex("ID", 1000));
            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1000, ids[0]);

            ids = provider.Find(ObjectFullName1, new ObjectIndex("IntCol", testInt));
            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1000, ids[0]);

            ids = provider.Find(ObjectFullName1, new ObjectIndex("TextCol", testStr));
            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1000, ids[0]);

            ids = provider.Find(ObjectFullName1, new ObjectIndex("DecCol", testDec));
            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1000, ids[0]);

            ids = provider.Find(ObjectFullName1, new ObjectIndex("DateTimeCol", testDate));
            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1000, ids[0]);

            ids = provider.Find(ObjectFullName1, new ObjectIndex("BinCol", testGuid.ToByteArray()));
            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1000, ids[0]);
        }

        [TestMethod]
        [TestCategory("MySQL")]
        public void SimpleFindWithOr()
        {
            var provider = IndexProvider;

            provider.ProvisionIndex(
                new ObjectMetadata(NameSpace1, ObjectName1,
                    new ObjectIndexMetadata[]
                    {
                        new ObjectIndexMetadata("TestCol1", ObjectIndexType.Integer),
                        new ObjectIndexMetadata("TestCol2", ObjectIndexType.String, 15)
                    }));

            provider.UpsertIndexValues(ObjectFullName1, 1,
                new ObjectIndex("TestCol1", 100),
                new ObjectIndex("TestCol2", "A"));

            provider.UpsertIndexValues(ObjectFullName1, 2,
                new ObjectIndex("TestCol1", 105),
                new ObjectIndex("TestCol2", "A"));

            provider.UpsertIndexValues(ObjectFullName1, 3,
                new ObjectIndex("TestCol1", 500),
                new ObjectIndex("TestCol2", "B"));

            provider.UpsertIndexValues(ObjectFullName1, 4,
                new ObjectIndex("TestCol1", 500),
                new ObjectIndex("TestCol2", "C"));


            // test two constraints on the same index that should return a two results
            int[] ids = provider.Find(ObjectFullName1, ObjectFindLogic.Or, ObjectFindOperator.Equals,
                new ObjectIndex("TestCol1", 100),
                new ObjectIndex("TestCol1", 105));

            Assert.IsNotNull(ids);
            Assert.AreEqual(2, ids.Length);
            Assert.AreEqual(1, ids[0]);
            Assert.AreEqual(2, ids[1]);

            // test two constraint on separate indexes that should return two results
            ids = provider.Find(ObjectFullName1, ObjectFindLogic.Or, ObjectFindOperator.Equals,
                new ObjectIndex("TestCol1", 105),
                new ObjectIndex("TestCol2", "C"));

            Assert.IsNotNull(ids);
            Assert.AreEqual(2, ids.Length);
            Assert.AreEqual(2, ids[0]);
            Assert.AreEqual(4, ids[1]);
        }

        [TestMethod]
        [TestCategory("MySQL")]
        public void SimpleFindWithLike()
        {
            var provider = IndexProvider;

            provider.ProvisionIndex(
                new ObjectMetadata(NameSpace1, ObjectName1,
                    new ObjectIndexMetadata[]
                    {
                        new ObjectIndexMetadata("TestCol1", ObjectIndexType.Integer),
                        new ObjectIndexMetadata("TestCol2", ObjectIndexType.String, 15)
                    }));

            provider.UpsertIndexValues(ObjectFullName1, 1,
                new ObjectIndex("TestCol1", 100),
                new ObjectIndex("TestCol2", "AsDf"));

            provider.UpsertIndexValues(ObjectFullName1, 2,
                new ObjectIndex("TestCol1", 105),
                new ObjectIndex("TestCol2", "ASdZZz"));

            provider.UpsertIndexValues(ObjectFullName1, 3,
                new ObjectIndex("TestCol1", 500),
                new ObjectIndex("TestCol2", "B"));

            // should return one result
            int[] ids = provider.Find(ObjectFullName1, ObjectFindLogic.And, ObjectFindOperator.Like,
                new ObjectIndex("TestCol1", 100));

            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1, ids[0]);

            // should return one result
            ids = provider.Find(ObjectFullName1, ObjectFindLogic.Or, ObjectFindOperator.Like,
                new ObjectIndex("TestCol2", "asdf"));

            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1, ids[0]);

            // should return two results
            ids = provider.Find(ObjectFullName1, ObjectFindLogic.Or, ObjectFindOperator.Like,
                new ObjectIndex("TestCol2", "as%"));

            Assert.IsNotNull(ids);
            Assert.AreEqual(2, ids.Length);
            Assert.AreEqual(1, ids[0]);
            Assert.AreEqual(2, ids[1]);
        }

        [TestMethod]
        [TestCategory("MySQL")]
        public void JSONConstraintFind()
        {
            var provider = IndexProvider;

            var indexMetadata = new ObjectIndexMetadata[]
            {
                new ObjectIndexMetadata("TestCol1", ObjectIndexType.Integer),
                new ObjectIndexMetadata("TestCol2", ObjectIndexType.String, 15)
            };

            provider.ProvisionIndex(
                new ObjectMetadata(NameSpace1, ObjectName1,
                    indexMetadata));

            provider.UpsertIndexValues(ObjectFullName1, 1,
                new ObjectIndex("TestCol1", 100),
                new ObjectIndex("TestCol2", "A"));

            provider.UpsertIndexValues(ObjectFullName1, 2,
                new ObjectIndex("TestCol1", 105),
                new ObjectIndex("TestCol2", "A"));

            provider.UpsertIndexValues(ObjectFullName1, 3,
                new ObjectIndex("TestCol1", 500),
                new ObjectIndex("TestCol2", "B"));

            provider.UpsertIndexValues(ObjectFullName1, 4,
                new ObjectIndex("TestCol1", 500),
                new ObjectIndex("TestCol2", "C"));

            // test single constraint value that should return a single result
            int[] ids = provider.Find(ObjectFullName1,
                @"{ ""TestCol1"" : 100, ""Op"" : ""="" }",
                indexMetadata);

            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1, ids[0]);

            // test two constraint values that should return a single result
            ids = provider.Find(ObjectFullName1,
                @"{ ""TestCol1"" : 100, ""Op"" : ""="",
""AND"" : { ""TestCol2"" : ""A"", ""Op"" : ""=""}}",
                indexMetadata);

            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1, ids[0]);

            // test single constraint value that should return two results
            ids = provider.Find(ObjectFullName1,
                @"{ ""TestCol1"" : 500, ""Op"" : ""="" }",
                indexMetadata);

            Assert.AreEqual(2, ids.Length);
            Assert.AreEqual(3, ids[0]);
            Assert.AreEqual(4, ids[1]);

            // test single constraint value that should return zero results
            ids = provider.Find(ObjectFullName1,
                @"{ ""TestCol1"" : 105, ""Op"" : ""="",
""AND"" : { ""TestCol2"" : ""B"", ""Op"" : ""=""}}",
                indexMetadata);

            Assert.AreEqual(0, ids.Length);
        }

        [TestMethod]
        [TestCategory("MySQL")]
        public void JSONConstraintLike()
        {
            var provider = IndexProvider;

            var indexMetadata = new ObjectIndexMetadata[]
            {
                new ObjectIndexMetadata("TestCol1", ObjectIndexType.Integer),
                new ObjectIndexMetadata("TestCol2", ObjectIndexType.String, 15)
            };

            provider.ProvisionIndex(
                new ObjectMetadata(NameSpace1, ObjectName1,
                    indexMetadata));

            provider.UpsertIndexValues(ObjectFullName1, 1,
                new ObjectIndex("TestCol1", 100),
                new ObjectIndex("TestCol2", "asdf"));

            provider.UpsertIndexValues(ObjectFullName1, 2,
                new ObjectIndex("TestCol1", 200),
                new ObjectIndex("TestCol2", "zxzy"));

            // test single constraint value that should return a single result
            int[] ids = provider.Find(ObjectFullName1,
                @"{ ""TestCol2"" : ""%sd%"", ""Op"" : ""LIKE"" }",
                indexMetadata);

            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1, ids[0]);

            ids = provider.Find(ObjectFullName1,
                @"{ ""TestCol2"" : ""as%"", ""Op"" : ""NOT LIKE"" }",
                indexMetadata);

            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(2, ids[0]);
        }
    }
}
