using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ZeroG.Data.Object;
using ZeroG.Data.Object.Metadata;
using ZeroG.Data.Object.Index;

namespace ZeroG.Tests.Object
{
    [TestClass]
    public class ObjectMetadataTest
    {
        public static readonly string NameSpace1 = ObjectTestHelper.NameSpace1;
        public static readonly string ObjectName1 = ObjectTestHelper.ObjectName1;
        public static readonly string ObjectName2 = ObjectTestHelper.ObjectName2;
        public static readonly string ObjectName3 = ObjectTestHelper.ObjectName3;

        [TestInitialize]
        public void PreTest()
        {
            ObjectTestHelper.CleanTestObjects();
        }

        [TestCleanup]
        public void PostTest()
        {
            ObjectTestHelper.CleanTestObjects();
        }

        #region Name Space tests
        [TestMethod]
        public void CreateNameSpace()
        {
            using (var svc = new ObjectService())
            {
                Assert.IsFalse(svc.NameSpaceExists(NameSpace1));

                var now = DateTime.Now;

                svc.CreateNameSpace(new ObjectNameSpaceConfig(NameSpace1,
                    "ZeroGTest", "Unit Test", now));

                Assert.IsTrue(svc.NameSpaceExists(NameSpace1));

                var ns = svc.GetNameSpace(NameSpace1);

                Assert.IsNotNull(ns);

                Assert.AreEqual(NameSpace1, ns.Name);
                Assert.AreEqual("ZeroGTest", ns.Owner);
                Assert.AreEqual("Unit Test", ns.StoreLocation);
                Assert.AreEqual(now, ns.Created);
            }
        }

        [TestMethod]
        [ExpectedException(typeof(System.ArgumentException))]
        public void CreateDuplicateNameSpace()
        {
            using (var svc = new ObjectService())
            {
                Assert.IsFalse(svc.NameSpaceExists(NameSpace1));

                var now = DateTime.Now;

                svc.CreateNameSpace(new ObjectNameSpaceConfig(NameSpace1,
                    "ZeroGTest", "Unit Test", now));

                svc.CreateNameSpace(new ObjectNameSpaceConfig(NameSpace1,
                    null, null, now));
            }
        }

        [TestMethod]
        [ExpectedException(typeof(System.ArgumentException))]
        public void CreateMalformedNameSpace()
        {
            using (var svc = new ObjectService())
            {
                try
                {
                    svc.RemoveNameSpace("NS \\' Test");

                    Assert.IsFalse(svc.NameSpaceExists("NS \\' Test"));

                    svc.CreateNameSpace(new ObjectNameSpaceConfig("NS \\' Test",
                        "ZeroGTest", "Unit Test", DateTime.Now));
                }
                finally
                {
                    // just in case the test fails, we want to clean up the namespace
                    svc.RemoveNameSpace("NS \\' Test");
                }
            }
        }
        #endregion

        #region Object Metadata tests

        [TestMethod]
        [TestCategory("Core")]
        public void ProvisionObjectStoreNoIndexes()
        {
            using (var svc = new ObjectService())
            {
                Assert.IsFalse(svc.ObjectNameExists(NameSpace1, ObjectName1));

                svc.CreateNameSpace(new ObjectNameSpaceConfig(NameSpace1,
                    "ZeroG Test", "Unit Test", DateTime.Now));

                svc.ProvisionObjectStore(
                    new ObjectMetadata(NameSpace1, ObjectName1));

                Assert.IsTrue(svc.ObjectNameExists(NameSpace1, ObjectName1));

                var metadata = svc.GetObjectMetadata(NameSpace1, ObjectName1);
                Assert.IsNotNull(metadata);

                Assert.AreEqual(NameSpace1, metadata.NameSpace);
                Assert.AreEqual(ObjectName1, metadata.ObjectName);
                Assert.IsNull(metadata.Indexes);
            }
        }

        [TestMethod]
        [TestCategory("Core")]
        public void ProvisionObjectStoreWithIndexes()
        {
            using (var svc = new ObjectService())
            {
                Assert.IsFalse(svc.ObjectNameExists(NameSpace1, ObjectName1));

                svc.CreateNameSpace(new ObjectNameSpaceConfig(NameSpace1,
                    "ZeroG Test", "Unit Test", DateTime.Now));

                svc.ProvisionObjectStore(
                    new ObjectMetadata(NameSpace1, ObjectName1,
                        new ObjectIndexMetadata[] 
                        {
                            new ObjectIndexMetadata("Test1", ObjectIndexType.String, 15),
                            new ObjectIndexMetadata("Test2", ObjectIndexType.Integer),
                            new ObjectIndexMetadata("Test3", ObjectIndexType.Decimal, 7,3)
                        }));

                Assert.IsTrue(svc.ObjectNameExists(NameSpace1, ObjectName1));

                var metadata = svc.GetObjectMetadata(NameSpace1, ObjectName1);
                Assert.IsNotNull(metadata);

                Assert.AreEqual(NameSpace1, metadata.NameSpace);
                Assert.AreEqual(ObjectName1, metadata.ObjectName);
                Assert.IsNotNull(metadata.Indexes);

                Assert.AreEqual("Test1", metadata.Indexes[0].Name);
                Assert.AreEqual(ObjectIndexType.String, metadata.Indexes[0].DataType);
                Assert.AreEqual(15u, metadata.Indexes[0].Precision);

                Assert.AreEqual("Test2", metadata.Indexes[1].Name);
                Assert.AreEqual(ObjectIndexType.Integer, metadata.Indexes[1].DataType);

                Assert.AreEqual("Test3", metadata.Indexes[2].Name);
                Assert.AreEqual(ObjectIndexType.Decimal, metadata.Indexes[2].DataType);
                Assert.AreEqual(7u, metadata.Indexes[2].Precision);
                Assert.AreEqual(3u, metadata.Indexes[2].Scale);

            }
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        [TestCategory("Core")]
        public void ProvisionDuplicateObjectStore()
        {
            using (var svc = new ObjectService())
            {
                Assert.IsFalse(svc.ObjectNameExists(NameSpace1, ObjectName1));

                svc.CreateNameSpace(new ObjectNameSpaceConfig(NameSpace1,
                    "ZeroG Test", "Unit Test", DateTime.Now));

                svc.ProvisionObjectStore(
                    new ObjectMetadata(NameSpace1, ObjectName1));

                svc.ProvisionObjectStore(
                    new ObjectMetadata(NameSpace1, ObjectName1));
            }
        }

        [TestMethod]
        [TestCategory("Core")]
        public void ProvisionObjectStoreWithDependencies()
        {
            using (var svc = new ObjectService())
            {
                Assert.IsFalse(svc.ObjectNameExists(NameSpace1, ObjectName1));

                svc.CreateNameSpace(new ObjectNameSpaceConfig(NameSpace1,
                    "ZeroG Test", "Unit Test", DateTime.Now));

                svc.ProvisionObjectStore(
                    new ObjectMetadata(NameSpace1, ObjectName2));

                svc.ProvisionObjectStore(
                    new ObjectMetadata(NameSpace1, ObjectName3));

                svc.ProvisionObjectStore(
                    new ObjectMetadata(NameSpace1, ObjectName1, null, new string[]
                        {
                            ObjectTestHelper.ObjectName2,
                            ObjectTestHelper.ObjectName3
                        }));

                Assert.IsTrue(svc.ObjectNameExists(NameSpace1, ObjectName1));

                var metadata = svc.GetObjectMetadata(NameSpace1, ObjectName1);
                Assert.IsNotNull(metadata);

                Assert.AreEqual(NameSpace1, metadata.NameSpace);
                Assert.AreEqual(ObjectName1, metadata.ObjectName);
                Assert.IsNull(metadata.Indexes);

                Assert.IsNotNull(metadata.Dependencies);
                Assert.AreEqual(2, metadata.Dependencies.Length);
                Assert.AreEqual(ObjectTestHelper.ObjectName2, metadata.Dependencies[0]);
                Assert.AreEqual(ObjectTestHelper.ObjectName3, metadata.Dependencies[1]);
            }
        }

        [TestMethod]
        [TestCategory("Core")]
        [ExpectedException(typeof(ArgumentException))]
        public void ProvisionObjectStoreWithNonExistingDependencies()
        {
            using (var svc = new ObjectService())
            {
                Assert.IsFalse(svc.ObjectNameExists(NameSpace1, ObjectName1));

                svc.CreateNameSpace(new ObjectNameSpaceConfig(NameSpace1,
                    "ZeroG Test", "Unit Test", DateTime.Now));

                svc.ProvisionObjectStore(
                    new ObjectMetadata(NameSpace1, ObjectName1, null, new string[]
                        {
                            ObjectTestHelper.ObjectName2,
                            ObjectTestHelper.ObjectName3
                        }));
            }
        }

        #endregion
    }
}
