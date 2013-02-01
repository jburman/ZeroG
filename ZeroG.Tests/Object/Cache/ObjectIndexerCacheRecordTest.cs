using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ZeroG.Data.Object.Cache;
using ZeroG.Data.Object;

namespace ZeroG.Tests.Object.Cache
{
    [TestClass]
    public class ObjectIndexerCacheRecordTest
    {
        [TestMethod]
        [TestCategory("Core")]
        public void CreateCacheRecord()
        {
            string objectFullName = ObjectNaming.CreateFullObjectName(
                ObjectTestHelper.NameSpace1,
                ObjectTestHelper.ObjectName1);

            var record = new ObjectIndexerCacheRecord();
            Assert.IsFalse(record.IsDirty);
            Assert.IsNull(record.ObjectFullName);
            Assert.AreEqual(0u, record.Version);
            Assert.AreEqual(0u, record.TotalObjectIDs);
            Assert.IsNotNull(record.Cache);
            Assert.AreEqual(0, record.Cache.Count);
            Assert.IsNull(record.GetFromCache(2));

            record.IsDirty = true;
            record.ObjectFullName = objectFullName;
            record.Version = 3u;
            record.TotalObjectIDs = 5;

            Assert.IsTrue(record.IsDirty);
            Assert.AreEqual(objectFullName, record.ObjectFullName);
            Assert.AreEqual(3u, record.Version);
            Assert.AreEqual(5u, record.TotalObjectIDs);
        }

        [TestMethod]
        [TestCategory("Core")]
        public void AddToCacheRecord()
        {
            var record = new ObjectIndexerCacheRecord();
            Assert.IsNull(record.GetFromCache(3));
            record.AddToCache(3, new int[] { 1, 2, 3 });
            Assert.AreEqual(3u, record.TotalObjectIDs);
            
            int[] vals = record.GetFromCache(3);
            Assert.IsNotNull(vals);
            Assert.AreEqual(3, vals.Length);
            Assert.AreEqual(1, vals[0]);
            Assert.AreEqual(2, vals[1]);
            Assert.AreEqual(3, vals[2]);

            record.AddToCache(4, new int[] { 4 });
            Assert.AreEqual(4u, record.TotalObjectIDs);
            vals = record.GetFromCache(3);
            Assert.IsNotNull(vals);
            Assert.AreEqual(3, vals.Length);

            vals = record.GetFromCache(4);
            Assert.IsNotNull(vals);
            Assert.AreEqual(1, vals.Length);
            Assert.AreEqual(4, vals[0]);
        }

        [TestMethod]
        [TestCategory("Core")]
        public void GetFromCacheRecord()
        {
            var record = new ObjectIndexerCacheRecord();
            Assert.IsNull(record.GetFromCache(3));
            record.AddToCache(3, new int[] { 1, 2, 3 });
            Assert.AreEqual(3u, record.TotalObjectIDs);
            Assert.AreEqual(0, record.Cache[3].Counter);
            int[] vals = record.GetFromCache(3);
            Assert.IsNotNull(vals);
            Assert.AreEqual(1, record.Cache[3].Counter);
            vals = record.GetFromCache(3);
            vals = record.GetFromCache(3);
            Assert.AreEqual(3, record.Cache[3u].Counter);
            record.AddToCache(4, new int[] { 4 });
            Assert.AreEqual(0, record.Cache[4u].Counter);
            vals = record.GetFromCache(3);
            vals = record.GetFromCache(4);
            Assert.AreEqual(4, record.Cache[3].Counter);
            Assert.AreEqual(1, record.Cache[4].Counter);
        }

        [TestMethod]
        [TestCategory("Core")]
        public void RemoveFromCacheRecord()
        {
            var record = new ObjectIndexerCacheRecord();
            Assert.IsNull(record.GetFromCache(3));
            record.AddToCache(3, new int[] { 1, 2, 3 });
            record.AddToCache(4, new int[] { 4 });
            record.AddToCache(5, new int[] { 5 });
            Assert.AreEqual(5u, record.TotalObjectIDs);
            Assert.IsNotNull(record.GetFromCache(3));
            Assert.IsNotNull(record.GetFromCache(4));
            Assert.IsNotNull(record.GetFromCache(5));
            record.RemoveFromCache(4);
            Assert.AreEqual(4u, record.TotalObjectIDs);
            Assert.IsNotNull(record.GetFromCache(3));
            Assert.IsNull(record.GetFromCache(4));
            Assert.IsNotNull(record.GetFromCache(5));
            record.RemoveFromCache(3);
            Assert.AreEqual(1u, record.TotalObjectIDs);
            Assert.IsNull(record.GetFromCache(3));
            Assert.IsNull(record.GetFromCache(4));
            Assert.IsNotNull(record.GetFromCache(5));
        }
    }
}
