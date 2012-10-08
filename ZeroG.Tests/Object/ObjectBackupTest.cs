using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ZeroG.Data.Object;
using ZeroG.Data.Object.Metadata;
using System.Text;
using System.IO;
using ZeroG.Data.Object.Backup;

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
                var nsDt = DateTime.Now;

                svc.CreateNameSpace(new ObjectNameSpaceConfig(ns,
                    "ZeroG Test", "Unit Test", nsDt));

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

                    // Read the file back in

                    ObjectStoreRecord objStoreRecTest = null;
                    int objectCount = 0, indexCount = 0;

                    using (var reader = new ObjectBackupReader(tempFile, false))
                    {
                        // Store Version handler
                        reader.ReadBackup((string version) =>
                        {
                            Assert.AreEqual(Config.StoreVersion, version);
                        },
                            // Name Space handler
                        (ObjectNameSpaceConfig nameSpace) =>
                        {
                            Assert.IsNotNull(nameSpace);
                            Assert.AreEqual(ns, nameSpace.Name);
                            Assert.AreEqual("ZeroG Test", nameSpace.Owner);
                            Assert.AreEqual("Unit Test", nameSpace.StoreLocation);
                            Assert.AreEqual(nsDt, nameSpace.Created);
                        },
                            // Object Metadata handler
                        (ObjectMetadata metadata) =>
                        {
                            Assert.AreEqual(ns, metadata.NameSpace);
                            Assert.AreEqual(obj, metadata.ObjectName);
                            Assert.IsNull(metadata.Indexes);
                            Assert.IsNull(metadata.Dependencies);
                        },
                            // Object ID handler
                        (int objectId) =>
                        {
                            Assert.AreEqual(2, objectId);
                        },
                            // Object Value handler
                        (ObjectStoreRecord objRec) =>
                        {
                            ++objectCount;
                            Assert.IsNotNull(objRec);
                            
                            if (null == objStoreRecTest)
                            {
                                objStoreRecTest = objRec;
                                Assert.AreEqual(1, objRec.ID);
                                Assert.AreEqual(val1, new Guid(objRec.Value));
                                Assert.AreEqual("001", Encoding.UTF8.GetString(objRec.SecondaryKey));
                            }
                            else
                            {
                                objStoreRecTest = objRec;
                                Assert.AreEqual(2, objRec.ID);
                                Assert.AreEqual(val2, new Guid(objRec.Value));
                                Assert.AreEqual("002", Encoding.UTF8.GetString(objRec.SecondaryKey));
                            }
                        },
                            // Object Index handler
                        (ObjectIndexRecord idxRec) =>
                        {
                            ++indexCount;
                            Assert.IsNotNull(idxRec);
                        });
                    }

                    Assert.AreEqual(2, objectCount);
                    Assert.AreEqual(0, indexCount);
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
                var nsDt = DateTime.Now;

                svc.CreateNameSpace(new ObjectNameSpaceConfig(ns,
                    "ZeroG Test", "Unit Test", nsDt));

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
                DateTime testDate = new DateTime(2011, 2, 14, 3, 10, 0), testDate2 = new DateTime(2015, 1, 1, 0, 0, 1);
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

                    // Read the file back in

                    ObjectStoreRecord objStoreRecTest = null;
                    ObjectIndexRecord objIndexRecTest = null;
                    int objectCount = 0, indexCount = 0;

                    using (var reader = new ObjectBackupReader(tempFile, false))
                    {
                        // Store Version handler
                        reader.ReadBackup((string version) =>
                        {
                            Assert.AreEqual(Config.StoreVersion, version);
                        },
                            // Name Space handler
                        (ObjectNameSpaceConfig nameSpace) =>
                        {
                            Assert.IsNotNull(nameSpace);
                            Assert.AreEqual(ns, nameSpace.Name);
                            Assert.AreEqual("ZeroG Test", nameSpace.Owner);
                            Assert.AreEqual("Unit Test", nameSpace.StoreLocation);
                            Assert.AreEqual(nsDt, nameSpace.Created);
                        },
                            // Object Metadata handler
                        (ObjectMetadata metadata) =>
                        {
                            Assert.AreEqual(ns, metadata.NameSpace);
                            Assert.AreEqual(obj, metadata.ObjectName);
                            Assert.IsNotNull(metadata.Indexes);
                            Assert.AreEqual(5, metadata.Indexes.Length);
                            Assert.AreEqual("IntCol", metadata.Indexes[0].Name);
                            Assert.AreEqual(ObjectIndexType.Integer, metadata.Indexes[0].DataType);
                            Assert.IsNull(metadata.Dependencies);
                        },
                            // Object ID handler
                        (int objectId) =>
                        {
                            Assert.AreEqual(2, objectId);
                        },
                            // Object Value handler
                        (ObjectStoreRecord objRec) =>
                        {
                            ++objectCount;
                            Assert.IsNotNull(objRec);

                            if (null == objStoreRecTest)
                            {
                                objStoreRecTest = objRec;
                                Assert.AreEqual(1, objRec.ID);
                                Assert.AreEqual(val1, new Guid(objRec.Value));
                                Assert.AreEqual("001", Encoding.UTF8.GetString(objRec.SecondaryKey));
                            }
                            else
                            {
                                objStoreRecTest = objRec;
                                Assert.AreEqual(2, objRec.ID);
                                Assert.AreEqual(val2, new Guid(objRec.Value));
                                Assert.AreEqual("002", Encoding.UTF8.GetString(objRec.SecondaryKey));
                            }
                        },
                            // Object Index handler
                        (ObjectIndexRecord idxRec) =>
                        {
                            ++indexCount;
                            Assert.IsNotNull(idxRec);

                            if (null == objIndexRecTest)
                            {
                                objIndexRecTest = idxRec;
                                Assert.AreEqual(6, idxRec.Values.Length);
                                
                                Assert.AreEqual(1, idxRec.Values[0].GetObjectValue());
                                Assert.AreEqual("ID", idxRec.Values[0].Name);
                                Assert.AreEqual("IntCol", idxRec.Values[1].Name);
                                Assert.AreEqual(testInt, idxRec.Values[1].GetObjectValue());
                                Assert.AreEqual("TextCol", idxRec.Values[2].Name);
                                Assert.AreEqual(testStr, idxRec.Values[2].GetObjectValue());
                                Assert.AreEqual("DecCol", idxRec.Values[3].Name);
                                Assert.AreEqual(testDec, idxRec.Values[3].GetObjectValue());
                                Assert.AreEqual("DateTimeCol", idxRec.Values[4].Name);
                                Assert.AreEqual(testDate, idxRec.Values[4].GetObjectValue());
                                Assert.AreEqual("BinCol", idxRec.Values[5].Name);
                                Assert.AreEqual(testGuid, new Guid(idxRec.Values[5].Value));
                            }
                            else
                            {
                                objIndexRecTest = idxRec;
                                Assert.AreEqual(6, idxRec.Values.Length);

                                Assert.AreEqual(2, idxRec.Values[0].GetObjectValue());
                                Assert.AreEqual("ID", idxRec.Values[0].Name);
                                Assert.AreEqual("IntCol", idxRec.Values[1].Name);
                                Assert.AreEqual(testInt2, idxRec.Values[1].GetObjectValue());
                                Assert.AreEqual("TextCol", idxRec.Values[2].Name);
                                Assert.AreEqual(testStr2, idxRec.Values[2].GetObjectValue());
                                Assert.AreEqual("DecCol", idxRec.Values[3].Name);
                                Assert.AreEqual(testDec2, idxRec.Values[3].GetObjectValue());
                                Assert.AreEqual("DateTimeCol", idxRec.Values[4].Name);
                                Assert.AreEqual(testDate2, idxRec.Values[4].GetObjectValue());
                                Assert.AreEqual("BinCol", idxRec.Values[5].Name);
                                Assert.AreEqual(testGuid2, new Guid(idxRec.Values[5].Value));
                            }
                        });
                    }

                    Assert.AreEqual(2, objectCount);
                    Assert.AreEqual(2, indexCount);
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
                var nsDt = DateTime.Now;

                svc.CreateNameSpace(new ObjectNameSpaceConfig(ns,
                    "ZeroG Test", "Unit Test", nsDt));

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
                DateTime testDate = new DateTime(2011, 2, 14, 3, 10, 0), testDate2 = new DateTime(2015, 1, 1, 0, 0, 1);
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

                    // Read the file back in

                    ObjectStoreRecord objStoreRecTest = null;
                    ObjectIndexRecord objIndexRecTest = null;
                    int objectCount = 0, indexCount = 0;

                    using (var reader = new ObjectBackupReader(tempFile, true))
                    {
                        // Store Version handler
                        reader.ReadBackup((string version) =>
                        {
                            Assert.AreEqual(Config.StoreVersion, version);
                        },
                            // Name Space handler
                        (ObjectNameSpaceConfig nameSpace) =>
                        {
                            Assert.IsNotNull(nameSpace);
                            Assert.AreEqual(ns, nameSpace.Name);
                            Assert.AreEqual("ZeroG Test", nameSpace.Owner);
                            Assert.AreEqual("Unit Test", nameSpace.StoreLocation);
                            Assert.AreEqual(nsDt, nameSpace.Created);
                        },
                            // Object Metadata handler
                        (ObjectMetadata metadata) =>
                        {
                            Assert.AreEqual(ns, metadata.NameSpace);
                            Assert.AreEqual(obj, metadata.ObjectName);
                            Assert.IsNotNull(metadata.Indexes);
                            Assert.AreEqual(5, metadata.Indexes.Length);
                            Assert.AreEqual("IntCol", metadata.Indexes[0].Name);
                            Assert.AreEqual(ObjectIndexType.Integer, metadata.Indexes[0].DataType);
                            Assert.IsNull(metadata.Dependencies);
                        },
                            // Object ID handler
                        (int objectId) =>
                        {
                            Assert.AreEqual(2, objectId);
                        },
                            // Object Value handler
                        (ObjectStoreRecord objRec) =>
                        {
                            ++objectCount;
                            Assert.IsNotNull(objRec);

                            if (null == objStoreRecTest)
                            {
                                objStoreRecTest = objRec;
                                Assert.AreEqual(1, objRec.ID);
                                Assert.AreEqual(val1, new Guid(objRec.Value));
                                Assert.AreEqual("001", Encoding.UTF8.GetString(objRec.SecondaryKey));
                            }
                            else
                            {
                                objStoreRecTest = objRec;
                                Assert.AreEqual(2, objRec.ID);
                                Assert.AreEqual(val2, new Guid(objRec.Value));
                                Assert.AreEqual("002", Encoding.UTF8.GetString(objRec.SecondaryKey));
                            }
                        },
                            // Object Index handler
                        (ObjectIndexRecord idxRec) =>
                        {
                            ++indexCount;
                            Assert.IsNotNull(idxRec);

                            if (null == objIndexRecTest)
                            {
                                objIndexRecTest = idxRec;
                                Assert.AreEqual(6, idxRec.Values.Length);

                                Assert.AreEqual(1, idxRec.Values[0].GetObjectValue());
                                Assert.AreEqual("ID", idxRec.Values[0].Name);
                                Assert.AreEqual("IntCol", idxRec.Values[1].Name);
                                Assert.AreEqual(testInt, idxRec.Values[1].GetObjectValue());
                                Assert.AreEqual("TextCol", idxRec.Values[2].Name);
                                Assert.AreEqual(testStr, idxRec.Values[2].GetObjectValue());
                                Assert.AreEqual("DecCol", idxRec.Values[3].Name);
                                Assert.AreEqual(testDec, idxRec.Values[3].GetObjectValue());
                                Assert.AreEqual("DateTimeCol", idxRec.Values[4].Name);
                                Assert.AreEqual(testDate, idxRec.Values[4].GetObjectValue());
                                Assert.AreEqual("BinCol", idxRec.Values[5].Name);
                                Assert.AreEqual(testGuid, new Guid(idxRec.Values[5].Value));
                            }
                            else
                            {
                                objIndexRecTest = idxRec;
                                Assert.AreEqual(6, idxRec.Values.Length);

                                Assert.AreEqual(2, idxRec.Values[0].GetObjectValue());
                                Assert.AreEqual("ID", idxRec.Values[0].Name);
                                Assert.AreEqual("IntCol", idxRec.Values[1].Name);
                                Assert.AreEqual(testInt2, idxRec.Values[1].GetObjectValue());
                                Assert.AreEqual("TextCol", idxRec.Values[2].Name);
                                Assert.AreEqual(testStr2, idxRec.Values[2].GetObjectValue());
                                Assert.AreEqual("DecCol", idxRec.Values[3].Name);
                                Assert.AreEqual(testDec2, idxRec.Values[3].GetObjectValue());
                                Assert.AreEqual("DateTimeCol", idxRec.Values[4].Name);
                                Assert.AreEqual(testDate2, idxRec.Values[4].GetObjectValue());
                                Assert.AreEqual("BinCol", idxRec.Values[5].Name);
                                Assert.AreEqual(testGuid2, new Guid(idxRec.Values[5].Value));
                            }
                        });
                    }

                    Assert.AreEqual(2, objectCount);
                    Assert.AreEqual(2, indexCount);
                }
                finally
                {
                    File.Delete(tempFile);
                }
            }
        }
    }
}
