using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ZeroG.Data.Object;
using ZeroG.Data.Object.Metadata;
using System.Text;
using System.IO;

namespace ZeroG.Tests.Object
{
    [TestClass]
    public class ObjectBackupTest
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
        public void BackupNameSpaceWithoutCompressionNoIndexes()
        {
            using (var svc = new ObjectService(ObjectTestHelper.GetConfig()))
            {
                var ns = ObjectTestHelper.NameSpace1;
                var obj = ObjectTestHelper.ObjectName1;

                svc.CreateNameSpace(new ObjectNameSpaceConfig(ns,
                    "ZeroG Test", "Unit Test", DateTime.Now));

                svc.ProvisionObjectStore(
                    new ObjectMetadata(ns, obj));

                var val1 = new Guid("{D22640F0-7D87-4F1C-8817-119FC036FAC1}");
                var val2 = new Guid("{72FC1391-EC51-4826-890B-D02071A9A2DE}");

                var secKey1 = Encoding.UTF8.GetBytes("001");
                var secKey2 = Encoding.UTF8.GetBytes("002");

                var objID1 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val1.ToByteArray(),
                    SecondaryKey = secKey1
                });

                var objID2 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val2.ToByteArray(),
                    SecondaryKey = secKey2
                });

                var tempFile = Path.GetTempFileName();
                try
                {
                    svc.BackupNameSpace(ns, tempFile, false);
                }
                finally
                {
                    File.Delete(tempFile);
                }
            }
        }

        [TestMethod]
        public void BackupNameSpaceWithoutCompressionWithIndexes()
        {
            using (var svc = new ObjectService(ObjectTestHelper.GetConfig()))
            {
                var ns = ObjectTestHelper.NameSpace1;
                var obj = ObjectTestHelper.ObjectName1;
                var objFullName1 = ObjectNaming.CreateFullObjectName(ns, obj);

                svc.CreateNameSpace(new ObjectNameSpaceConfig(ns,
                    "ZeroG Test", "Unit Test", DateTime.Now));

                svc.ProvisionObjectStore(
                    new ObjectMetadata(ns, obj,
                        new ObjectIndexMetadata[]
                        {
                            new ObjectIndexMetadata("IntCol", ObjectIndexType.Integer),
                            new ObjectIndexMetadata("TextCol", ObjectIndexType.String, 15),
                            new ObjectIndexMetadata("DecCol", ObjectIndexType.Decimal, 7, 2),
                            new ObjectIndexMetadata("DateTimeCol", ObjectIndexType.DateTime),
                            new ObjectIndexMetadata("BinCol", ObjectIndexType.Binary, 16)
                        }));

                var val1 = new Guid("{D22640F0-7D87-4F1C-8817-119FC036FAC1}");
                var val2 = new Guid("{72FC1391-EC51-4826-890B-D02071A9A2DE}");

                var secKey1 = Encoding.UTF8.GetBytes("001");
                var secKey2 = Encoding.UTF8.GetBytes("002");

                Int32 testInt = 3447, testInt2 = 100;
                String testStr = "Test Value", testStr2 = "asdf";
                Decimal testDec = 156.12M, testDec2 = 1M;
                DateTime testDate = new DateTime(2011, 2, 14, 3, 10, 0), testDate2 = DateTime.Now;
                Guid testGuid = new Guid("76F5FB10BAEF4DE09578B3EB91FF6653"),
                    testGuid2 = new Guid("00000000000000000000000000000000");

                var objID1 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val1.ToByteArray(),
                    SecondaryKey = secKey1,
                    Indexes = new ObjectIndex[] {
                        ObjectIndex.Create("IntCol", testInt),
                        ObjectIndex.Create("TextCol", testStr),
                        ObjectIndex.Create("DecCol", testDec),
                        ObjectIndex.Create("DateTimeCol", testDate),
                        ObjectIndex.Create("BinCol", testGuid.ToByteArray())
                    }
                });

                var objID2 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val2.ToByteArray(),
                    SecondaryKey = secKey2,
                    Indexes = new ObjectIndex[] {
                        ObjectIndex.Create("IntCol", testInt2),
                        ObjectIndex.Create("TextCol", testStr2),
                        ObjectIndex.Create("DecCol", testDec2),
                        ObjectIndex.Create("DateTimeCol", testDate2),
                        ObjectIndex.Create("BinCol", testGuid2.ToByteArray())
                    }
                });

                var tempFile = Path.GetTempFileName();
                try
                {
                    svc.BackupNameSpace(ns, tempFile, false);
                }
                finally
                {
                    File.Delete(tempFile);
                }
            }
        }

        [TestMethod]
        public void BackupNameSpaceWithCompressionWithIndexes()
        {
            using (var svc = new ObjectService(ObjectTestHelper.GetConfig()))
            {
                var ns = ObjectTestHelper.NameSpace1;
                var obj = ObjectTestHelper.ObjectName1;
                var objFullName1 = ObjectNaming.CreateFullObjectName(ns, obj);

                svc.CreateNameSpace(new ObjectNameSpaceConfig(ns,
                    "ZeroG Test", "Unit Test", DateTime.Now));

                svc.ProvisionObjectStore(
                    new ObjectMetadata(ns, obj,
                        new ObjectIndexMetadata[]
                        {
                            new ObjectIndexMetadata("IntCol", ObjectIndexType.Integer),
                            new ObjectIndexMetadata("TextCol", ObjectIndexType.String, 15),
                            new ObjectIndexMetadata("DecCol", ObjectIndexType.Decimal, 7, 2),
                            new ObjectIndexMetadata("DateTimeCol", ObjectIndexType.DateTime),
                            new ObjectIndexMetadata("BinCol", ObjectIndexType.Binary, 16)
                        }));

                var val1 = new Guid("{D22640F0-7D87-4F1C-8817-119FC036FAC1}");
                var val2 = new Guid("{72FC1391-EC51-4826-890B-D02071A9A2DE}");

                var secKey1 = Encoding.UTF8.GetBytes("001");
                var secKey2 = Encoding.UTF8.GetBytes("002");

                Int32 testInt = 3447, testInt2 = 100;
                String testStr = "Test Value", testStr2 = "asdf";
                Decimal testDec = 156.12M, testDec2 = 1M;
                DateTime testDate = new DateTime(2011, 2, 14, 3, 10, 0), testDate2 = DateTime.Now;
                Guid testGuid = new Guid("76F5FB10BAEF4DE09578B3EB91FF6653"),
                    testGuid2 = new Guid("00000000000000000000000000000000");

                var objID1 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val1.ToByteArray(),
                    SecondaryKey = secKey1,
                    Indexes = new ObjectIndex[] {
                        ObjectIndex.Create("IntCol", testInt),
                        ObjectIndex.Create("TextCol", testStr),
                        ObjectIndex.Create("DecCol", testDec),
                        ObjectIndex.Create("DateTimeCol", testDate),
                        ObjectIndex.Create("BinCol", testGuid.ToByteArray())
                    }
                });

                var objID2 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val2.ToByteArray(),
                    SecondaryKey = secKey2,
                    Indexes = new ObjectIndex[] {
                        ObjectIndex.Create("IntCol", testInt2),
                        ObjectIndex.Create("TextCol", testStr2),
                        ObjectIndex.Create("DecCol", testDec2),
                        ObjectIndex.Create("DateTimeCol", testDate2),
                        ObjectIndex.Create("BinCol", testGuid2.ToByteArray())
                    }
                });

                var tempFile = Path.GetTempFileName();
                try
                {
                    svc.BackupNameSpace(ns, tempFile, true);
                }
                finally
                {
                    File.Delete(tempFile);
                }
            }
        }
    }
}
