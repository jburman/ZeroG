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

        //
        // You can use the following additional attributes as you write your tests:
        //
        // Use ClassInitialize to run code before running the first test in the class
        // [ClassInitialize()]
        // public static void MyClassInitialize(TestContext testContext) { }
        //
        // Use ClassCleanup to run code after all tests in a class have run
        // [ClassCleanup()]
        // public static void MyClassCleanup() { }
        //
        // Use TestInitialize to run code before running each test 
        [TestInitialize()]
        public void MyTestInitialize() 
        {
            try
            {
                IndexProvider.UnprovisionIndex(NameSpace1, ObjectName1);
            }
            catch
            {
            }
        }
        
        // Use TestCleanup to run code after each test has run
        [TestCleanup()]
        public void MyTestCleanup() 
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
    }
}
