using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using ZeroG.Data.Database;
using ZeroG.Data.Database.Drivers.Object.Provider;
using ZeroG.Data.Object;
using ZeroG.Data.Object.Index;
using ZeroG.Data.Object.Metadata;
using ZeroG.Tests.Object;

namespace ZeroG.Tests.Data.Drivers
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
        private static IObjectIndexProvider _provider;
        internal static IObjectIndexProvider IndexProvider
        {
            get
            {
                if (null == _provider)
                {
                    _provider = ObjectTestHelper.CreateObjectIndexProvider();
                }
                return _provider;
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
                ObjectIndex.Create("TestCol1", 100));

            Assert.IsNotNull(ids);

            Assert.AreEqual(0, ids.Length);
        }

        [TestMethod]
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
                    ObjectIndex.Create("TestCol1", 100));

                Assert.IsNotNull(ids);

                Assert.AreEqual(0, ids.Length);

                provider.UnprovisionIndex(ObjectFullName1);
            }
            catch(Exception ex)
            {
                Assert.Fail("Unexpected exception: " + ex.ToString());
            }

            Assert.IsFalse(provider.ObjectExists(ObjectFullName1));

            try
            {
                // should throw
                provider.Find(ObjectFullName1,
                    ObjectIndex.Create("TestCol1", 100));

                Assert.IsTrue(false, "Expected exception to be thrown before reaching this line.");
            }
            catch (DbException)
            {
            }
        }

        [TestMethod]
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
                ObjectIndex.Create("TestCol1", 100),
                ObjectIndex.Create("TestCol2", "A"));

            provider.UpsertIndexValues(ObjectFullName1, 2,
                ObjectIndex.Create("TestCol1", 105),
                ObjectIndex.Create("TestCol2", "A"));

            provider.UpsertIndexValues(ObjectFullName1, 3,
                ObjectIndex.Create("TestCol1", 500),
                ObjectIndex.Create("TestCol2", "B"));

            provider.UpsertIndexValues(ObjectFullName1, 4,
                ObjectIndex.Create("TestCol1", 500),
                ObjectIndex.Create("TestCol2", "C"));


            // test single constraint value that should return a single result
            int[] ids = provider.Find(ObjectFullName1,
                ObjectIndex.Create("TestCol1", 100));

            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1, ids[0]);

            // test two constraint values that should return a single result
            ids = provider.Find(ObjectFullName1,
                ObjectIndex.Create("TestCol1", 100),
                ObjectIndex.Create("TestCol2", "A"));

            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1, ids[0]);

            // test single constraint value that should return two results
            ids = provider.Find(ObjectFullName1,
                ObjectIndex.Create("TestCol1", 500));

            Assert.AreEqual(2, ids.Length);
            Assert.AreEqual(3, ids[0]);
            Assert.AreEqual(4, ids[1]);

            // test single constraint value that should return zero results
            ids = provider.Find(ObjectFullName1,
                ObjectIndex.Create("TestCol1", 105),
                ObjectIndex.Create("TestCol2", "B"));

            Assert.AreEqual(0, ids.Length);
        }

        [TestMethod]
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
                ObjectIndex.Create("IntCol", testInt),
                ObjectIndex.Create("TextCol", testStr),
                ObjectIndex.Create("DecCol", testDec),
                ObjectIndex.Create("DateTimeCol", testDate),
                ObjectIndex.Create("BinCol", testGuid.ToByteArray()));

            provider.UpsertIndexValues(ObjectFullName1,
                1001,
                ObjectIndex.Create("IntCol", 500),
                ObjectIndex.Create("TextCol", "asdf"),
                ObjectIndex.Create("DecCol", new Decimal(5.4)),
                ObjectIndex.Create("DateTimeCol", DateTime.UtcNow),
                ObjectIndex.Create("BinCol", Guid.NewGuid().ToByteArray()));

            int[] ids = provider.Find(ObjectFullName1, ObjectIndex.Create("ID", 1000));
            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1000, ids[0]);

            ids = provider.Find(ObjectFullName1, ObjectIndex.Create("IntCol", testInt));
            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1000, ids[0]);

            ids = provider.Find(ObjectFullName1, ObjectIndex.Create("TextCol", testStr));
            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1000, ids[0]);

            ids = provider.Find(ObjectFullName1, ObjectIndex.Create("DecCol", testDec));
            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1000, ids[0]);

            ids = provider.Find(ObjectFullName1, ObjectIndex.Create("DateTimeCol", testDate));
            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1000, ids[0]);

            ids = provider.Find(ObjectFullName1, ObjectIndex.Create("BinCol", testGuid.ToByteArray()));
            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1000, ids[0]);
        }

        [TestMethod]
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
                ObjectIndex.Create("TestCol1", 100),
                ObjectIndex.Create("TestCol2", "A"));

            provider.UpsertIndexValues(ObjectFullName1, 2,
                ObjectIndex.Create("TestCol1", 105),
                ObjectIndex.Create("TestCol2", "A"));

            provider.UpsertIndexValues(ObjectFullName1, 3,
                ObjectIndex.Create("TestCol1", 500),
                ObjectIndex.Create("TestCol2", "B"));

            provider.UpsertIndexValues(ObjectFullName1, 4,
                ObjectIndex.Create("TestCol1", 500),
                ObjectIndex.Create("TestCol2", "C"));


            // test two constraints on the same index that should return a two results
            int[] ids = provider.Find(ObjectFullName1, new ObjectFindOptions()
                {
                    Logic = ObjectFindLogic.Or,
                    Operator = ObjectFindOperator.Equals
                },
                ObjectIndex.Create("TestCol1", 100),
                ObjectIndex.Create("TestCol1", 105));

            Assert.IsNotNull(ids);
            Assert.AreEqual(2, ids.Length);
            Assert.AreEqual(1, ids[0]);
            Assert.AreEqual(2, ids[1]);

            // test two constraint on separate indexes that should return two results
            ids = provider.Find(ObjectFullName1, new ObjectFindOptions()
                {
                    Logic = ObjectFindLogic.Or,
                    Operator = ObjectFindOperator.Equals
                },
                ObjectIndex.Create("TestCol1", 105),
                ObjectIndex.Create("TestCol2", "C"));

            Assert.IsNotNull(ids);
            Assert.AreEqual(2, ids.Length);
            Assert.AreEqual(2, ids[0]);
            Assert.AreEqual(4, ids[1]);
        }

        [TestMethod]
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
                ObjectIndex.Create("TestCol1", 100),
                ObjectIndex.Create("TestCol2", "AsDf"));

            provider.UpsertIndexValues(ObjectFullName1, 2,
                ObjectIndex.Create("TestCol1", 105),
                ObjectIndex.Create("TestCol2", "ASdZZz"));

            provider.UpsertIndexValues(ObjectFullName1, 3,
                ObjectIndex.Create("TestCol1", 500),
                ObjectIndex.Create("TestCol2", "B"));

            // should return one result
            int[] ids = provider.Find(ObjectFullName1, new ObjectFindOptions()
                {
                    Logic = ObjectFindLogic.And,
                    Operator = ObjectFindOperator.Like
                },
                ObjectIndex.Create("TestCol1", 100));

            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1, ids[0]);

            // should return one result
            ids = provider.Find(ObjectFullName1, new ObjectFindOptions()
                {
                    Logic = ObjectFindLogic.Or,
                    Operator = ObjectFindOperator.Like
                },
                ObjectIndex.Create("TestCol2", "asdf"));

            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1, ids[0]);

            // should return two results
            ids = provider.Find(ObjectFullName1, new ObjectFindOptions()
                {
                    Logic = ObjectFindLogic.Or,
                    Operator = ObjectFindOperator.Like
                },
                ObjectIndex.Create("TestCol2", "as%"));

            Assert.IsNotNull(ids);
            Assert.AreEqual(2, ids.Length);
            Assert.AreEqual(1, ids[0]);
            Assert.AreEqual(2, ids[1]);
        }

        [TestMethod]
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
                ObjectIndex.Create("TestCol1", 100),
                ObjectIndex.Create("TestCol2", "A"));

            provider.UpsertIndexValues(ObjectFullName1, 2,
                ObjectIndex.Create("TestCol1", 105),
                ObjectIndex.Create("TestCol2", "A"));

            provider.UpsertIndexValues(ObjectFullName1, 3,
                ObjectIndex.Create("TestCol1", 500),
                ObjectIndex.Create("TestCol2", "B"));

            provider.UpsertIndexValues(ObjectFullName1, 4,
                ObjectIndex.Create("TestCol1", 500),
                ObjectIndex.Create("TestCol2", "C"));

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
        public void JSONConstraintOperators()
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
                ObjectIndex.Create("TestCol1", 100),
                ObjectIndex.Create("TestCol2", "asdf"));

            provider.UpsertIndexValues(ObjectFullName1, 2,
                ObjectIndex.Create("TestCol1", 200),
                ObjectIndex.Create("TestCol2", "zxzy"));

            // test LIKE operator
            int[] ids = provider.Find(ObjectFullName1,
                @"{ ""TestCol2"" : ""%sd%"", ""Op"" : ""LIKE"" }",
                indexMetadata);

            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1, ids[0]);

            // test NOT LIKE operator
            ids = provider.Find(ObjectFullName1,
                @"{ ""TestCol2"" : ""as%"", ""Op"" : ""NOT LIKE"" }",
                indexMetadata);

            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(2, ids[0]);

            // test EQUALS operator
            ids = provider.Find(ObjectFullName1,
                @"{ ""TestCol1"" : 100, ""Op"" : ""="" }",
                indexMetadata);

            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1, ids[0]);

            // test NOT EQUALS operator
            ids = provider.Find(ObjectFullName1,
                @"{ ""TestCol1"" : 100, ""Op"" : ""<>"" }",
                indexMetadata);

            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(2, ids[0]);

            // test IN operator
            ids = provider.Find(ObjectFullName1,
                @"{ ""TestCol1"" : [100], ""Op"" : ""IN"" }",
                indexMetadata);

            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1, ids[0]);

            // test NOT IN operator
            ids = provider.Find(ObjectFullName1,
                @"{ ""TestCol1"" : [100], ""Op"" : ""NOT IN"" }",
                indexMetadata);

            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(2, ids[0]);

            // test LESS THAN operator
            ids = provider.Find(ObjectFullName1,
                @"{ ""TestCol1"" : 200, ""Op"" : ""<"" }",
                indexMetadata);

            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1, ids[0]);

            // test LESS THAN OR EQUALS operator
            ids = provider.Find(ObjectFullName1,
                @"{ ""TestCol1"" : 100, ""Op"" : ""<="" }",
                indexMetadata);

            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1, ids[0]);

            // test GREATER THAN operator
            ids = provider.Find(ObjectFullName1,
                @"{ ""TestCol1"" : 100, ""Op"" : "">"" }",
                indexMetadata);

            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(2, ids[0]);

            // test GREATER THAN OR EQUALS operator
            ids = provider.Find(ObjectFullName1,
                @"{ ""TestCol1"" : 200, ""Op"" : "">="" }",
                indexMetadata);

            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(2, ids[0]);
        }

        [TestMethod]
        public void Exists()
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
                ObjectIndex.Create("TestCol1", 100),
                ObjectIndex.Create("TestCol2", "A"));

            provider.UpsertIndexValues(ObjectFullName1, 2,
                ObjectIndex.Create("TestCol1", 105),
                ObjectIndex.Create("TestCol2", "A"));

            provider.UpsertIndexValues(ObjectFullName1, 3,
                ObjectIndex.Create("TestCol1", 500),
                ObjectIndex.Create("TestCol2", "B"));

            provider.UpsertIndexValues(ObjectFullName1, 4,
                ObjectIndex.Create("TestCol1", 500),
                ObjectIndex.Create("TestCol2", "C"));


            Assert.IsTrue(provider.Exists(ObjectFullName1,
                @"{ ""TestCol1"" : 100, ""Op"" : ""="" }", indexMetadata));

            Assert.IsFalse(provider.Exists(ObjectFullName1,
                @"{ ""TestCol1"" : 102, ""Op"" : ""="" }", indexMetadata));
        }

        [TestMethod]
        public void Count()
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
                ObjectIndex.Create("TestCol1", 100),
                ObjectIndex.Create("TestCol2", "A"));

            provider.UpsertIndexValues(ObjectFullName1, 2,
                ObjectIndex.Create("TestCol1", 105),
                ObjectIndex.Create("TestCol2", "A"));

            provider.UpsertIndexValues(ObjectFullName1, 3,
                ObjectIndex.Create("TestCol1", 500),
                ObjectIndex.Create("TestCol2", "B"));

            provider.UpsertIndexValues(ObjectFullName1, 4,
                ObjectIndex.Create("TestCol1", 500),
                ObjectIndex.Create("TestCol2", "C"));


            Assert.AreEqual(1, provider.Count(ObjectFullName1,
                @"{ ""TestCol1"" : 100, ""Op"" : ""="" }", indexMetadata));

            Assert.AreEqual(0, provider.Count(ObjectFullName1,
                @"{ ""TestCol1"" : 102, ""Op"" : ""="" }", indexMetadata));

            Assert.AreEqual(4, provider.Count(ObjectFullName1,
                @"{ ""TestCol1"" : 0, ""Op"" : "">"" }", indexMetadata));

            Assert.AreEqual(2, provider.Count(ObjectFullName1,
                @"{ ""TestCol2"" : ""a"", ""Op"" : ""LIKE"" }", indexMetadata));
        }

        [TestMethod]
        public void Limit()
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
                ObjectIndex.Create("TestCol1", 100),
                ObjectIndex.Create("TestCol2", "A"));

            provider.UpsertIndexValues(ObjectFullName1, 2,
                ObjectIndex.Create("TestCol1", 105),
                ObjectIndex.Create("TestCol2", "A"));

            provider.UpsertIndexValues(ObjectFullName1, 3,
                ObjectIndex.Create("TestCol1", 500),
                ObjectIndex.Create("TestCol2", "A"));

            provider.UpsertIndexValues(ObjectFullName1, 4,
                ObjectIndex.Create("TestCol1", 500),
                ObjectIndex.Create("TestCol2", "B"));

            var ids = provider.Find(ObjectFullName1,
                new ObjectFindOptions()
                {
                    Logic = ObjectFindLogic.And,
                    Operator = ObjectFindOperator.Equals,
                    Limit = 0
                }, ObjectIndex.Create("TestCol2", "A"));

            Assert.AreEqual(3, ids.Length);

            ids = provider.Find(ObjectFullName1,
                new ObjectFindOptions()
                {
                    Logic = ObjectFindLogic.And,
                    Operator = ObjectFindOperator.Equals,
                    Limit = 1
                }, ObjectIndex.Create("TestCol2", "A"));

            Assert.AreEqual(1, ids.Length);

            ids = provider.Find(ObjectFullName1,
                new ObjectFindOptions()
                {
                    Logic = ObjectFindLogic.And,
                    Operator = ObjectFindOperator.Equals,
                    Limit = 2
                }, ObjectIndex.Create("TestCol2", "A"));

            Assert.AreEqual(2, ids.Length);

            ids = provider.Find(ObjectFullName1,
                new ObjectFindOptions()
                {
                    Logic = ObjectFindLogic.And,
                    Operator = ObjectFindOperator.Equals,
                    Limit = 10
                }, ObjectIndex.Create("TestCol2", "A"));

            Assert.AreEqual(3, ids.Length);

            ids = provider.Find(ObjectFullName1,
                @"{ ""TestCol2"" : ""a"", ""Op"" : ""LIKE"" }", 0, null, indexMetadata);
            Assert.AreEqual(3, ids.Length);

            ids = provider.Find(ObjectFullName1,
                @"{ ""TestCol2"" : ""a"", ""Op"" : ""LIKE"" }", 1, null, indexMetadata);
            Assert.AreEqual(1, ids.Length);

            ids = provider.Find(ObjectFullName1,
                @"{ ""TestCol2"" : ""a"", ""Op"" : ""LIKE"" }", 2, null, indexMetadata);
            Assert.AreEqual(2, ids.Length);

            ids = provider.Find(ObjectFullName1,
                @"{ ""TestCol2"" : ""a"", ""Op"" : ""LIKE"" }", 10, null, indexMetadata);
            Assert.AreEqual(3, ids.Length);
        }

        [TestMethod]
        public void Order()
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
                ObjectIndex.Create("TestCol1", 100),
                ObjectIndex.Create("TestCol2", "A"));

            provider.UpsertIndexValues(ObjectFullName1, 2,
                ObjectIndex.Create("TestCol1", 105),
                ObjectIndex.Create("TestCol2", "A"));

            provider.UpsertIndexValues(ObjectFullName1, 3,
                ObjectIndex.Create("TestCol1", 500),
                ObjectIndex.Create("TestCol2", "B"));

            provider.UpsertIndexValues(ObjectFullName1, 4,
                ObjectIndex.Create("TestCol1", 500),
                ObjectIndex.Create("TestCol2", "C"));

            var vals = provider.Find(ObjectFullName1,
                @"{ ""TestCol2"" : ""A"", ""Op"" : ""="" }",
                0,
                new OrderOptions()
                {
                    Descending = true,
                    Indexes = new string[] { "TestCol1" }
                },
                indexMetadata);

            Assert.AreEqual(2, vals.Length);
            Assert.AreEqual(2, vals[0]);
            Assert.AreEqual(1, vals[1]);

            vals = provider.Find(ObjectFullName1,
                @"{ ""TestCol2"" : ""A"", ""Op"" : ""="" }",
                0,
                new OrderOptions()
                {
                    Descending = false,
                    Indexes = new string[] { "TestCol1" }
                },
                indexMetadata);

            Assert.AreEqual(2, vals.Length);
            Assert.AreEqual(1, vals[0]);
            Assert.AreEqual(2, vals[1]);

            vals = provider.Find(ObjectFullName1,
                new ObjectFindOptions() 
                {
                    Logic = ObjectFindLogic.And,
                    Operator = ObjectFindOperator.Equals,
                    Limit = 0,
                    Order = new OrderOptions()
                    {
                        Descending = true,
                        Indexes = new string[] { "TestCol1" }
                    }
                },
                ObjectIndex.Create("TestCol2", "A"));

            Assert.AreEqual(2, vals.Length);
            Assert.AreEqual(2, vals[0]);
            Assert.AreEqual(1, vals[1]);

            vals = provider.Find(ObjectFullName1,
                new ObjectFindOptions()
                {
                    Logic = ObjectFindLogic.And,
                    Operator = ObjectFindOperator.Equals,
                    Limit = 0,
                    Order = new OrderOptions()
                    {
                        Descending = false,
                        Indexes = new string[] { "TestCol1" }
                    }
                },
                ObjectIndex.Create("TestCol2", "A"));

            Assert.AreEqual(2, vals.Length);
            Assert.AreEqual(1, vals[0]);
            Assert.AreEqual(2, vals[1]);

            // Test and Order with Limit
            vals = provider.Find(ObjectFullName1,
                new ObjectFindOptions()
                {
                    Logic = ObjectFindLogic.And,
                    Operator = ObjectFindOperator.Equals,
                    Limit = 1,
                    Order = new OrderOptions()
                    {
                        Descending = true,
                        Indexes = new string[] { "TestCol1" }
                    }
                },
                ObjectIndex.Create("TestCol2", "A"));

            Assert.AreEqual(1, vals.Length);
            Assert.AreEqual(2, vals[0]);

            vals = provider.Find(ObjectFullName1,
                new ObjectFindOptions()
                {
                    Logic = ObjectFindLogic.And,
                    Operator = ObjectFindOperator.Equals,
                    Limit = 1,
                    Order = new OrderOptions()
                    {
                        Descending = false,
                        Indexes = new string[] { "TestCol1" }
                    }
                },
                ObjectIndex.Create("TestCol2", "A"));

            Assert.AreEqual(1, vals.Length);
            Assert.AreEqual(1, vals[0]);
        }

    }
}
