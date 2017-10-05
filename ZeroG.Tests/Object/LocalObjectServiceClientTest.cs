using System;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ZeroG.Data;
using ZeroG.Data.Object;
using ZeroG.Data.Object.Metadata;
using System.Collections.Generic;

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
        [TestCategory("Core")]
        public void BulkStoreWithClient()
        {
            using (var scope = TestContext.ScopedInstance)
            {
                var svc = scope.GetObjectServiceWithoutIndexCache();
                
                var ns = ObjectTestHelper.NameSpace1;
                var obj = ObjectTestHelper.ObjectName1;

                svc.CreateNameSpace(new ObjectNameSpaceConfig(ns,
                    "ZeroG Test", "Unit Test", DateTime.Now));

                svc.ProvisionObjectStore(new ObjectMetadata(ns, obj));

                var client = new LocalObjectServiceClient(svc, ns, obj);

                var val1 = new Guid("{D22640F0-7D87-4F1C-8817-119FC036FAC1}");
                var val2 = new Guid("{72FC1391-EC51-4826-890B-D02071A9A2DE}");
                var val3 = new Guid("{72FC1391-EC51-4826-890B-D02071A9A2DE}");

                var val2SecondaryKey = Encoding.UTF8.GetBytes("val2key");

                BulkStore bulk = client.BeginBulkStore();

                bulk.Add(12, val1.ToByteArray());
                bulk.Add(val2SecondaryKey, val2.ToByteArray(), null);
                bulk.Add(500, val3.ToByteArray());

                var ids = bulk.Complete().ToArray();
                Assert.AreEqual(3, ids.Length);
                Assert.AreEqual(12, ids[0].ID);
                Assert.IsFalse(ids[0].HasSecondaryKey());
                Assert.AreEqual(1, ids[1].ID);
                Assert.IsTrue(ids[1].HasSecondaryKey());
                Assert.AreEqual(val2SecondaryKey, ids[1].SecondaryKey);
                Assert.AreEqual(500, ids[2].ID);
                Assert.IsFalse(ids[2].HasSecondaryKey());

                Assert.AreEqual(val1, new Guid(client.Get(12)));
                Assert.AreEqual(val2, new Guid(client.Get(1)));
                Assert.AreEqual(val3, new Guid(client.Get(500)));

                Assert.AreEqual(val2, new Guid(client.GetBySecondaryKey(val2SecondaryKey)));

                Assert.AreEqual(3, client.Count());
            }
        }

        [TestMethod]
        [TestCategory("Core")]
        public void BulkStoreManyWithClient()
        {
            using (var scope = TestContext.ScopedInstance)
            {
                var svc = scope.GetObjectServiceWithoutIndexCache();
                var ns = ObjectTestHelper.NameSpace1;
                var obj = ObjectTestHelper.ObjectName1;

                svc.CreateNameSpace(new ObjectNameSpaceConfig(ns,
                    "ZeroG Test", "Unit Test", DateTime.Now));

                // stores the object's metadata and builds the database tables
                svc.ProvisionObjectStore(
                    new ObjectMetadata(ns, obj,
                        new ObjectIndexMetadata[] 
                        {
                            new ObjectIndexMetadata("IntIndex1", ObjectIndexType.Integer),
                            new ObjectIndexMetadata("StrIndex1", ObjectIndexType.String, 15)
                        }));

                var client = new LocalObjectServiceClient(svc, ns, obj);

                var objCount = 50000;

                var random = new Random();
                var buf = new byte[100];

                BulkStore bulk = client.BeginBulkStore();

                // generate a list of objects to store
                for (int i = 0; objCount > i; i++)
                {
                    random.NextBytes(buf);

                    bulk.Add(buf,
                        new ObjectIndex[] 
                        { 
                            ObjectIndex.Create("IntIndex1", i + 100),
                            ObjectIndex.Create("StrIndex1", "idx_" + i)
                        });
                }

                // Complete the operation and store and index the objects
                var ids = bulk.Complete();

                Assert.AreEqual(objCount, ids.Count());

                // query 100 objects from the index
                var vals = client.Find(@"{""IntIndex1"":10000, ""Op"": "">"", ""And"" : {""IntIndex1"":10101, ""Op"": ""<""}}");
                Assert.AreEqual(100, vals.Count());
            }
        }


        [TestMethod]
        [TestCategory("Core")]
        public void SetAndGetTest()
        {
            using (var scope = TestContext.ScopedInstance)
            {
                var svc = scope.GetObjectServiceWithoutIndexCache();

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
        [TestCategory("Core")]
        public void SecondaryKeyTest()
        {
            using (var scope = TestContext.ScopedInstance)
            {
                var svc = scope.GetObjectServiceWithoutIndexCache();

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
        [TestCategory("Core")]
        public void RemoveTest()
        {
            using (var scope = TestContext.ScopedInstance)
            {
                var svc = scope.GetObjectServiceWithoutIndexCache();

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
        [TestCategory("Core")]
        public void SetAndFindTest()
        {
            using (var scope = TestContext.ScopedInstance)
            {
                var svc = scope.GetObjectServiceWithoutIndexCache();

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
