using System;
using System.Linq;
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
                var val3 = new Guid("{46C4CD37-EECA-416C-AD7A-52F4EA1CC999}");
                var key1 = Encoding.UTF8.GetBytes("val1");
                var key2 = Encoding.UTF8.GetBytes("val2");
                var key3 = Encoding.UTF8.GetBytes("val3");

                var objId1 = client.Store(val1.ToByteArray());
                var objId2 = client.Store(key2, val2.ToByteArray());
                var objId3 = client.Store(key3, val3.ToByteArray());

                // test that all objects are returned
                var getVal = client.Get(objId1.ID);
                Assert.IsNotNull(getVal);
                Assert.AreEqual(val1, new Guid(getVal));
                getVal = client.GetBySecondaryKey(key2);
                Assert.IsNotNull(getVal);
                Assert.AreEqual(val2, new Guid(getVal));
                getVal = client.GetBySecondaryKey(key3);
                Assert.IsNotNull(getVal);
                Assert.AreEqual(val3, new Guid(getVal));

                // remove an object and test that it is no longer returned
                client.Remove(objId1.ID);
                getVal = client.Get(objId1.ID);
                Assert.IsNull(getVal);
                getVal = client.GetBySecondaryKey(key1);
                Assert.IsNull(getVal);
                getVal = client.Get(objId2.ID);
                Assert.IsNotNull(getVal);
                getVal = client.GetBySecondaryKey(key3);
                Assert.IsNotNull(getVal);
                Assert.AreEqual(val3, new Guid(getVal));

                // remove a second object
                client.Remove(objId2.ID);
                getVal = client.Get(objId2.ID);
                Assert.IsNull(getVal);
                getVal = client.GetBySecondaryKey(key2);
                Assert.IsNull(getVal);
                getVal = client.GetBySecondaryKey(key3);
                Assert.IsNotNull(getVal);
                Assert.AreEqual(val3, new Guid(getVal));

                // remove a third object by its secondary key
                bool removed = client.RemoveBySecondaryKey(key3);
                Assert.IsTrue(removed);
                getVal = client.Get(objId2.ID);
                Assert.IsNull(getVal);
                getVal = client.GetBySecondaryKey(key2);
                Assert.IsNull(getVal);
                getVal = client.GetBySecondaryKey(key3);
                Assert.IsNull(getVal);
            }
        }


        [TestMethod]
        public void SetAndFindTest()
        {
            using (var svc = new ObjectService(ObjectTestHelper.GetConfig()))
            {
                var ns = ObjectTestHelper.NameSpace1;
                var obj = ObjectTestHelper.ObjectName1;

                svc.CreateNameSpace(new ObjectNameSpaceConfig(ns,
                    "ZeroG Test", "Unit Test", DateTime.Now));

                var indexName1 = "StrIndex1";
                var indexName2 = "StrIndex2";

                svc.ProvisionObjectStore(
                    new ObjectMetadata(ns, obj,
                        new ObjectIndexMetadata[] 
                        {
                            new ObjectIndexMetadata(indexName1, ObjectIndexType.String, 8),
                            new ObjectIndexMetadata(indexName2, ObjectIndexType.String, 1)
                        }));

                var client = new LocalObjectServiceClient(svc, ns, obj);

                var val1 = new Guid("{C8159CCF-9401-404E-A11E-0B5DF8BA6DB1}");
                var val2 = new Guid("{F1B3E0E4-0C96-4671-8BBF-A086ED1C96BC}");
                var indexes1 = new string[] { "AA BB CC", "00 11 22"};
                var indexes2 = new string[] { "F", "M" };

                var objId1 = client.Store(val1.ToByteArray(),
                    new ObjectIndex[] 
                    { 
                        ObjectIndex.Create(indexName1, indexes1[0]),
                        ObjectIndex.Create(indexName2, indexes2[0])
                    });

                var objId2 = client.Store(val2.ToByteArray(),
                    new ObjectIndex[] 
                    { 
                        ObjectIndex.Create(indexName1, indexes1[1]),
                        ObjectIndex.Create(indexName2, indexes2[1])
                    });

                var getVal = client.Get(objId1.ID);
                Assert.IsNotNull(getVal);
                Assert.AreEqual(val1, new Guid(getVal));

                getVal = client.Get(objId2.ID);
                Assert.IsNotNull(getVal);
                Assert.AreEqual(val2, new Guid(getVal));

                getVal = client.Find(@"{ ""StrIndex1"" : ""AA BB CC"" }").FirstOrDefault();
                Assert.IsNotNull(getVal);
                Assert.AreEqual(val1, new Guid(getVal));
            }
        }
    }
}
