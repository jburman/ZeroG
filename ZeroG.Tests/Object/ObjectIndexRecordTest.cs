using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ZeroG.Data.Object.Index;
using ZeroG.Data.Object;
using ZeroG.Data.Object.Metadata;

namespace ZeroG.Tests.Object
{
    [TestClass]
    public class ObjectIndexRecordTest
    {
        internal static string NameSpace1 = "ZGTestNS";
        internal static string ObjectName1 = "ZGTestObj1";
        internal static string ObjectFullName1 = "ZGTestNS.ZGTestObj1";

        private static IObjectIndexProvider _provider;
        internal static IObjectIndexProvider IndexProvider
        {
            get
            {
                if (null == _provider)
                {
                    _provider = ObjectTestHelper.GetObjectIndexProvider(ObjectTestHelper.GetConfig());
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

        [TestMethod]
        public void CreateFromRecord()
        {
            var provider = IndexProvider;

            var indexMetadata = new ObjectIndexMetadata[]
            {
                new ObjectIndexMetadata("TestCol1", ObjectIndexType.Integer),
                new ObjectIndexMetadata("TestCol2", ObjectIndexType.String, 15),
                new ObjectIndexMetadata("TestCol3", ObjectIndexType.Binary, 16),
                new ObjectIndexMetadata("TestCol4", ObjectIndexType.DateTime)
            };

            provider.ProvisionIndex(
                new ObjectMetadata(NameSpace1, ObjectName1,
                    indexMetadata));

            var guid1 = new Guid("{36EB4248-6835-4CDC-9FDC-B7E7678AAE5A}");
            var guid2 = new Guid("{0BF33DA5-0D39-4189-8CC2-3DC8DDC122A7}");
            var dt1 = new DateTime(2008, 8, 1, 22, 0, 5);
            var dt2 = new DateTime(2014, 1, 20, 1, 30, 0);

            provider.UpsertIndexValues(ObjectFullName1, 1,
                ObjectIndex.Create("TestCol1", 100),
                ObjectIndex.Create("TestCol2", "A"),
                ObjectIndex.Create("TestCol3", SerializerHelper.Serialize(guid1)),
                ObjectIndex.Create("TestCol4", dt1));

            provider.UpsertIndexValues(ObjectFullName1, 2,
                ObjectIndex.Create("TestCol1", 105),
                ObjectIndex.Create("TestCol2", "A"),
                ObjectIndex.Create("TestCol3", SerializerHelper.Serialize(guid2)),
                ObjectIndex.Create("TestCol4", dt2));

            var records = provider.Iterate(ObjectFullName1, indexMetadata);

            var recordEnum = records.GetEnumerator();
            recordEnum.MoveNext();

            var indexRecord = ObjectIndexRecord.CreateFromDataRecord(recordEnum.Current);
            var vals = indexRecord.Values;

            // 5 instead of 4 because ID is included when all values are iterated
            Assert.AreEqual(5, vals.Length);
            Assert.AreEqual("ID", vals[0].Name);
            Assert.AreEqual(1, vals[0].GetObjectValue());

            Assert.AreEqual("TestCol1", vals[1].Name);
            Assert.AreEqual(100, vals[1].GetObjectValue());

            Assert.AreEqual("TestCol2", vals[2].Name);
            Assert.AreEqual("A", vals[2].GetObjectValue());

            Assert.AreEqual("TestCol3", vals[3].Name);
            Assert.AreEqual(guid1, new Guid(vals[3].Value));

            Assert.AreEqual("TestCol4", vals[4].Name);
            Assert.AreEqual(dt1, vals[4].GetObjectValue());

            
            recordEnum.MoveNext();

            indexRecord = ObjectIndexRecord.CreateFromDataRecord(recordEnum.Current);
            vals = indexRecord.Values;

            // 5 instead of 4 because ID is included when all values are iterated
            Assert.AreEqual(5, vals.Length);
            Assert.AreEqual("ID", vals[0].Name);
            Assert.AreEqual(2, vals[0].GetObjectValue());

            Assert.AreEqual("TestCol1", vals[1].Name);
            Assert.AreEqual(105, vals[1].GetObjectValue());

            Assert.AreEqual("TestCol2", vals[2].Name);
            Assert.AreEqual("A", vals[2].GetObjectValue());

            Assert.AreEqual("TestCol3", vals[3].Name);
            Assert.AreEqual(guid2, new Guid(vals[3].Value));

            Assert.AreEqual("TestCol4", vals[4].Name);
            Assert.AreEqual(dt2, vals[4].GetObjectValue());

            Assert.IsFalse(recordEnum.MoveNext());
        }
    }
}
