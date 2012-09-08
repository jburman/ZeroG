using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ZeroG.Data.Object;

namespace ZeroG.Tests.Object
{
    [TestClass]
    public class ObjectMetadataTest
    {
        public static readonly string NameSpace1 = "ZG_testNS1";
        public static readonly string ObjectName1 = "ZG_testObj1";

        [TestInitialize]
        public void PreTest()
        {
            using (var svc = new ObjectService())
            {
                svc.RemoveNameSpace(NameSpace1);
            }
        }

        [TestCleanup]
        public void PostTest()
        {
            using (var svc = new ObjectService())
            {
                svc.RemoveNameSpace(NameSpace1);
            }
        }

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
                svc.RemoveNameSpace("NS \\' Test");

                Assert.IsFalse(svc.NameSpaceExists("NS \\' Test"));

                svc.CreateNameSpace(new ObjectNameSpaceConfig("NS \\' Test",
                    "ZeroGTest", "Unit Test", DateTime.Now));
            }
        }
    }
}
