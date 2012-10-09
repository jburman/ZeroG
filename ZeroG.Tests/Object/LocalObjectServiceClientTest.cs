using System;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ZeroG.Data;
using ZeroG.Data.Object;
using ZeroG.Data.Object.Metadata;

namespace ZeroG.Tests.Object
{
    [TestClass]
    public class LocalObjectServiceClientTest
    {
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

        [TestMethod]
        public void SetAndGetTest()
        {
            using (var svc = new ObjectService(ObjectTestHelper.GetConfig()))
            {
                var ns = ObjectTestHelper.NameSpace1;
                var obj = ObjectTestHelper.ObjectName1;

                svc.CreateNameSpace(new ObjectNameSpaceConfig(ns,
                    "ZeroG Test", "Unit Test", DateTime.Now));

                svc.ProvisionObjectStore(
                    new ObjectMetadata(ns, obj));

                var client = new LocalObjectServiceClient(svc, ns, obj);

                var val1 = new Guid("{C8159CCF-9401-404E-A11E-0B5DF8BA6DB1}");
                var val2 = new Guid("{F1B3E0E4-0C96-4671-8BBF-A086ED1C96BC}");

                var objId1 = client.Store(val1.ToByteArray());
                var objId2 = client.Store(val2.ToByteArray());

                var getVal = client.Get(objId1.ID);
                Assert.IsNotNull(getVal);
                Assert.AreEqual(val1, new Guid(getVal));

                getVal = client.Get(objId2.ID);
                Assert.IsNotNull(getVal);
                Assert.AreEqual(val2, new Guid(getVal));
            }
        }

        [TestMethod]
        public void SecondaryKeyTest()
        {
            using (var svc = new ObjectService(ObjectTestHelper.GetConfig()))
            {
                var ns = ObjectTestHelper.NameSpace1;
                var obj = ObjectTestHelper.ObjectName1;

                svc.CreateNameSpace(new ObjectNameSpaceConfig(ns,
                    "ZeroG Test", "Unit Test", DateTime.Now));

                svc.ProvisionObjectStore(
                    new ObjectMetadata(ns, obj));

                var client = new LocalObjectServiceClient(svc, ns, obj);

                var val1 = new Guid("{C8159CCF-9401-404E-A11E-0B5DF8BA6DB1}");
                var val2 = new Guid("{F1B3E0E4-0C96-4671-8BBF-A086ED1C96BC}");
                var key1 = Encoding.UTF8.GetBytes("val1");
                var key2 = Encoding.UTF8.GetBytes("val2");

                var objId1 = client.Store(key1, val1.ToByteArray());
                var objId2 = client.Store(key2, val2.ToByteArray());

                var getVal = client.GetBySecondaryKey(key1);
                Assert.IsNotNull(getVal);
                Assert.AreEqual(val1, new Guid(getVal));

                getVal = client.GetBySecondaryKey(key2);
                Assert.IsNotNull(getVal);
                Assert.AreEqual(val2, new Guid(getVal));
            }
        }

        [TestMethod]
        public void RemoveTest()
        {
            using (var svc = new ObjectService(ObjectTestHelper.GetConfig()))
            {
                var ns = ObjectTestHelper.NameSpace1;
                var obj = ObjectTestHelper.ObjectName1;

                svc.CreateNameSpace(new ObjectNameSpaceConfig(ns,
                    "ZeroG Test", "Unit Test", DateTime.Now));

                svc.ProvisionObjectStore(
                    new ObjectMetadata(ns, obj));

                var client = new LocalObjectServiceClient(svc, ns, obj);

                var val1 = new Guid("{C8159CCF-9401-404E-A11E-0B5DF8BA6DB1}");
                var val2 = new Guid("{F1B3E0E4-0C96-4671-8BBF-A086ED1C96BC}");
                var key1 = Encoding.UTF8.GetBytes("val1");
                var key2 = Encoding.UTF8.GetBytes("val2");

                var objId1 = client.Store(val1.ToByteArray());
                var objId2 = client.Store(key2, val2.ToByteArray());

                var getVal = client.Get(objId1.ID);
                Assert.IsNotNull(getVal);
                Assert.AreEqual(val1, new Guid(getVal));

                getVal = client.GetBySecondaryKey(key2);
                Assert.IsNotNull(getVal);
                Assert.AreEqual(val2, new Guid(getVal));

                client.Remove(objId1.ID);
                getVal = client.Get(objId1.ID);
                Assert.IsNull(getVal);
                getVal = client.GetBySecondaryKey(key1);
                Assert.IsNull(getVal);

                getVal = client.Get(objId2.ID);
                Assert.IsNotNull(getVal);

                client.Remove(objId2.ID);
                getVal = client.Get(objId2.ID);
                Assert.IsNull(getVal);
                getVal = client.GetBySecondaryKey(key2);
                Assert.IsNull(getVal);
            }
        }
    }
}
