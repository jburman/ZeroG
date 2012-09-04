using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Text;
using ZeroG.Data.Database.Drivers.Object.Provider;
using ZeroG.Data.Object.Index;
using ZeroG.Data.Object.Metadata;

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
                IndexProvider.UnprovisionIndex(NameSpace1, ObjectName1);
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
                IndexProvider.UnprovisionIndex(NameSpace1, ObjectName1);
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
            int[] ids = provider.Find(NameSpace1, ObjectName1,
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
                int[] ids = provider.Find(NameSpace1, ObjectName1,
                    new ObjectIndex("TestCol1", 100));

                Assert.IsNotNull(ids);

                Assert.AreEqual(0, ids.Length);

                provider.UnprovisionIndex(NameSpace1, ObjectName1);
            }
            catch(Exception ex)
            {
                Assert.Fail("Unexpected exception: " + ex.ToString());
            }

            // should throw
            provider.Find(NameSpace1, ObjectName1,
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

            provider.UpsertIndexValues(NameSpace1, ObjectName1, 1,
                new ObjectIndex("TestCol1", 100),
                new ObjectIndex("TestCol2", "A"));

            provider.UpsertIndexValues(NameSpace1, ObjectName1, 2,
                new ObjectIndex("TestCol1", 105),
                new ObjectIndex("TestCol2", "A"));

            provider.UpsertIndexValues(NameSpace1, ObjectName1, 2,
                new ObjectIndex("TestCol1", 500),
                new ObjectIndex("TestCol2", "B"));

            provider.UpsertIndexValues(NameSpace1, ObjectName1, 3,
                new ObjectIndex("TestCol1", 500),
                new ObjectIndex("TestCol2", "C"));


            // test single constraint value that should return a single result
            int[] ids = provider.Find(NameSpace1, ObjectName1,
                new ObjectIndex("TestCol1", 100));

            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1, ids[0]);

            // test two constraint values that should return a single result
            ids = provider.Find(NameSpace1, ObjectName1,
                new ObjectIndex("TestCol1", 100),
                new ObjectIndex("TestCol1", "A"));

            Assert.IsNotNull(ids);
            Assert.AreEqual(1, ids.Length);
            Assert.AreEqual(1, ids[0]);

            // test single constraint value that should return two results
            ids = provider.Find(NameSpace1, ObjectName1,
                new ObjectIndex("TestCol1", 500));

            Assert.AreEqual(2, ids.Length);
            Assert.AreEqual(2, ids[0]);
            Assert.AreEqual(3, ids[1]);

            // test single constraint value that should return zero results
            ids = provider.Find(NameSpace1, ObjectName1,
                new ObjectIndex("TestCol1", 105),
                new ObjectIndex("TestCol2", "B"));

            Assert.AreEqual(0, ids.Length);
        }
    }
}
