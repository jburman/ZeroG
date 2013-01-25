using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ZeroG.Data.Object;
using ZeroG.Data.Object.Metadata;
using ZeroG.Data.Object.Index;
using System.IO;
using ZeroG.Lang;
using System.Diagnostics;
using System.Threading;

namespace ZeroG.Tests.Object
{
    [TestClass]
    public class ObjectServiceTest
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
        public void StoreAndRetrieve()
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
                var val3 = new Guid("{82B2056A-7F32-4CDE-AC57-DB375086B40F}");

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

                // retrieve by object ID
                var retval1 = svc.Get(ns, obj, objID1.ID);
                var retval2 = svc.Get(ns, obj, objID2.ID);

                Assert.IsNotNull(retval1);
                Assert.IsNotNull(retval2);

                Assert.AreEqual(val1, new Guid(retval1));
                Assert.AreEqual(val2, new Guid(retval2));

                // retrieve by unique ID
                var secretval1 = svc.GetBySecondaryKey(ns, obj, objID1.SecondaryKey);
                var secretval2 = svc.GetBySecondaryKey(ns, obj, objID2.SecondaryKey);

                Assert.IsNotNull(secretval1);
                Assert.IsNotNull(secretval2);

                Assert.AreEqual(val1, new Guid(secretval1));
                Assert.AreEqual(val2, new Guid(secretval2));

                // this tests setting pre-defined IDs
                int id = 5;
                byte[] uniqueId = Encoding.UTF8.GetBytes("test1");

                svc.Store(ns, new PersistentObject()
                {
                    ID = id,
                    SecondaryKey = uniqueId,
                    Name = obj,
                    Value = val3.ToByteArray()
                });
                var retval3 = svc.Get(ns, obj, id);

                Assert.IsNotNull(retval1);

                Assert.AreEqual(val3, new Guid(retval3));

                // retrieve by unique ID
                var secretval3 = svc.GetBySecondaryKey(ns, obj, uniqueId);

                Assert.IsNotNull(secretval3);
                Assert.AreEqual(val3, new Guid(secretval3));

                // store another value against unique ID and test that it was overwritten
                id = 6;

                svc.Store(ns, new PersistentObject()
                {
                    ID = id,
                    SecondaryKey = uniqueId,
                    Name = obj,
                    Value = val1.ToByteArray()
                });

                var secretval4 = svc.GetBySecondaryKey(ns, obj, uniqueId);
                Assert.IsNotNull(secretval4);
                Assert.AreEqual(val1, new Guid(secretval4));
            }
        }

        [TestMethod]
        [TestCategory("Core")]
        public void StoreAndRemoveNoIndexes()
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

                Assert.AreEqual(2, svc.Count(ns, obj));

                // retrieve by object ID
                var retval1 = svc.Get(ns, obj, objID1.ID);
                var retval2 = svc.Get(ns, obj, objID2.ID);

                Assert.IsNotNull(retval1);
                Assert.IsNotNull(retval2);

                Assert.AreEqual(val1, new Guid(retval1));
                Assert.AreEqual(val2, new Guid(retval2));

                // retrieve by unique ID
                var secretval1 = svc.GetBySecondaryKey(ns, obj, objID1.SecondaryKey);
                var secretval2 = svc.GetBySecondaryKey(ns, obj, objID2.SecondaryKey);

                Assert.IsNotNull(secretval1);
                Assert.IsNotNull(secretval2);

                Assert.AreEqual(val1, new Guid(secretval1));
                Assert.AreEqual(val2, new Guid(secretval2));

                // remove a value and make sure it can't be retrieved
                svc.Remove(ns, obj, objID1.ID);

                Assert.AreEqual(1, svc.Count(ns, obj));

                // retrieve by object ID
                retval1 = svc.Get(ns, obj, objID1.ID);
                retval2 = svc.Get(ns, obj, objID2.ID);

                Assert.IsNull(retval1);
                Assert.IsNotNull(retval2);

                Assert.AreEqual(val2, new Guid(retval2));

                // retrieve by unique ID
                secretval1 = svc.GetBySecondaryKey(ns, obj, objID1.SecondaryKey);
                secretval2 = svc.GetBySecondaryKey(ns, obj, objID2.SecondaryKey);

                Assert.IsNull(secretval1);
                Assert.IsNotNull(secretval2);

                Assert.AreEqual(val2, new Guid(secretval2));
            }
        }

        [TestMethod]
        [TestCategory("Core")]
        public void StoreAndRemoveWithIndexes()
        {
            using (var svc = new ObjectService(ObjectTestHelper.GetConfig()))
            {
                var ns = ObjectTestHelper.NameSpace1;
                var obj = ObjectTestHelper.ObjectName1;

                svc.CreateNameSpace(new ObjectNameSpaceConfig(ns,
                    "ZeroG Test", "Unit Test", DateTime.Now));

                svc.ProvisionObjectStore(
                    new ObjectMetadata(ns, obj,
                        new ObjectIndexMetadata[]
                        {
                            new ObjectIndexMetadata("Test", ObjectIndexType.String, 5)
                        }));

                var val1 = new Guid("{D22640F0-7D87-4F1C-8817-119FC036FAC1}");
                var val2 = new Guid("{72FC1391-EC51-4826-890B-D02071A9A2DE}");

                var secKey1 = Encoding.UTF8.GetBytes("001");
                var secKey2 = Encoding.UTF8.GetBytes("002");

                var objID1 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val1.ToByteArray(),
                    SecondaryKey = secKey1,
                    Indexes = new ObjectIndex[]
                    {
                        ObjectIndex.Create("Test", "asdf")
                    }
                });

                var objID2 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val2.ToByteArray(),
                    SecondaryKey = secKey2,
                    Indexes = new ObjectIndex[]
                    {
                        ObjectIndex.Create("Test", "2")
                    }
                });

                Assert.AreEqual(2, svc.Count(ns, obj));

                // retrieve by object ID
                var retval1 = svc.Get(ns, obj, objID1.ID);
                var retval2 = svc.Get(ns, obj, objID2.ID);

                Assert.IsNotNull(retval1);
                Assert.IsNotNull(retval2);

                Assert.AreEqual(val1, new Guid(retval1));
                Assert.AreEqual(val2, new Guid(retval2));

                // retrieve by unique ID
                var secretval1 = svc.GetBySecondaryKey(ns, obj, objID1.SecondaryKey);
                var secretval2 = svc.GetBySecondaryKey(ns, obj, objID2.SecondaryKey);

                Assert.IsNotNull(secretval1);
                Assert.IsNotNull(secretval2);

                Assert.AreEqual(val1, new Guid(secretval1));
                Assert.AreEqual(val2, new Guid(secretval2));

                // remove a value and make sure it can't be retrieved
                svc.Remove(ns, obj, objID1.ID);

                Assert.AreEqual(1, svc.Count(ns, obj));

                // retrieve by object ID
                retval1 = svc.Get(ns, obj, objID1.ID);
                retval2 = svc.Get(ns, obj, objID2.ID);

                Assert.IsNull(retval1);
                Assert.IsNotNull(retval2);

                Assert.AreEqual(val2, new Guid(retval2));

                // retrieve by unique ID
                secretval1 = svc.GetBySecondaryKey(ns, obj, objID1.SecondaryKey);
                secretval2 = svc.GetBySecondaryKey(ns, obj, objID2.SecondaryKey);

                Assert.IsNull(secretval1);
                Assert.IsNotNull(secretval2);

                Assert.AreEqual(val2, new Guid(secretval2));
            }
        }

        [TestMethod]
        [TestCategory("Core")]
        public void GetNonExistingObject()
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

                var objID1 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val1.ToByteArray()
                });

                var objID2 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val2.ToByteArray()
                });

                var uniqueId = new Guid("{8AD7F9E4-B2B8-4511-B520-08914B999044}").ToByteArray();

                Assert.IsNull(svc.Get(ns, obj, 5));
                Assert.IsNull(svc.GetBySecondaryKey(ns, obj, uniqueId));
            }
        }

        [TestMethod]
        [TestCategory("Core")]
        public void StoreAndRetrieveByIndex()
        {
            using (var svc = new ObjectService(ObjectTestHelper.GetConfig()))
            {
                var ns = ObjectTestHelper.NameSpace1;
                var obj = ObjectTestHelper.ObjectName1;

                svc.CreateNameSpace(new ObjectNameSpaceConfig(ns,
                    "ZeroG Test", "Unit Test", DateTime.Now));

                svc.ProvisionObjectStore(
                    new ObjectMetadata(ns, obj,
                        new ObjectIndexMetadata[] 
                        {
                            new ObjectIndexMetadata("IntIndex1", ObjectIndexType.Integer),
                            new ObjectIndexMetadata("StrIndex1", ObjectIndexType.String, 15),
                            new ObjectIndexMetadata("StrNullIndex1", ObjectIndexType.String, 5, true)
                        }));

                var val1 = new Guid("{D22640F0-7D87-4F1C-8817-119FC036FAC1}");
                var val2 = new Guid("{72FC1391-EC51-4826-890B-D02071A9A2DE}");
                var val3 = new Guid("{72FC1391-EC51-4826-890B-D02071A9A2DE}");
                var intIndex1 = 5;
                var intIndex2 = 12500;
                var intIndex3 = -100;
                var strIndex1 = "asdf";
                var strIndex2 = "index test val";
                var strIndex3 = "zzyyxx";

                var strNullIndexVal = "0011";

                var objID1 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val1.ToByteArray(),
                    Indexes = new ObjectIndex[] 
                    { 
                        ObjectIndex.Create("IntIndex1", intIndex1),
                        ObjectIndex.Create("StrIndex1", strIndex1),
                        ObjectIndex.Create("StrNullIndex1", null)
                    }
                });

                var objID2 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val2.ToByteArray(),
                    Indexes = new ObjectIndex[] 
                    { 
                        ObjectIndex.Create("IntIndex1", intIndex2),
                        ObjectIndex.Create("StrIndex1", strIndex2),
                        ObjectIndex.Create("StrNullIndex1", strNullIndexVal)
                    }
                });

                var objID3 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val3.ToByteArray(),
                    Indexes = new ObjectIndex[] 
                    { 
                        ObjectIndex.Create("IntIndex1", intIndex3),
                        ObjectIndex.Create("StrIndex1", strIndex3),
                        ObjectIndex.Create("StrNullIndex1", null)
                    }
                });

                // count all index values
                Assert.AreEqual(3, svc.Count(ns, obj));

                // test a single index lookup using And
                var options = new ObjectFindOptions()
                {
                    Operator = ObjectFindOperator.Equals,
                    Logic = ObjectFindLogic.And
                };
                var findVals = svc.Find(ns, obj, options, new ObjectIndex[]
                {
                    ObjectIndex.Create("IntIndex1", 12500)
                }).ToArray();

                Assert.AreEqual(1, findVals.Length);
                Assert.AreEqual(val2, new Guid(findVals[0]));

                // test two index lookups using And
                options = new ObjectFindOptions()
                {
                    Operator = ObjectFindOperator.Equals,
                    Logic = ObjectFindLogic.And
                };
                findVals = svc.Find(ns, obj, options, new ObjectIndex[]
                {
                    ObjectIndex.Create("StrIndex1", "index test val"),
                    ObjectIndex.Create("IntIndex1", 12500)
                }).ToArray();

                Assert.AreEqual(1, findVals.Length);
                Assert.AreEqual(val2, new Guid(findVals[0]));

                // test a single lookup using Or
                options = new ObjectFindOptions()
                {
                    Operator = ObjectFindOperator.Equals,
                    Logic = ObjectFindLogic.Or
                };
                findVals = svc.Find(ns, obj, options, new ObjectIndex[]
                {
                    ObjectIndex.Create("IntIndex1", 12500)
                }).ToArray();

                Assert.AreEqual(1, findVals.Length);
                Assert.AreEqual(val2, new Guid(findVals[0]));

                // test two index lookups using Or
                options = new ObjectFindOptions()
                {
                    Operator = ObjectFindOperator.Equals,
                    Logic = ObjectFindLogic.Or
                };
                findVals = svc.Find(ns, obj, options, new ObjectIndex[]
                {
                    ObjectIndex.Create("IntIndex1", 12500),
                    ObjectIndex.Create("StrIndex1", "asdf")
                }).ToArray();

                Assert.AreEqual(2, findVals.Length);
                var findVal1 = new Guid(findVals[0]);
                var findVal2 = new Guid(findVals[1]);
                Assert.IsFalse(findVal1 == findVal2);
                Assert.IsTrue(findVal1 == val1 || findVal1 == val2);
                Assert.IsTrue(findVal2 == val1 || findVal2 == val2);

                // test with Like
                // test a single lookup using And
                options = new ObjectFindOptions()
                {
                    Operator = ObjectFindOperator.Like,
                    Logic = ObjectFindLogic.And
                };
                findVals = svc.Find(ns, obj, options, new ObjectIndex[]
                {
                    ObjectIndex.Create("StrIndex1", "%test%")
                }).ToArray();

                Assert.AreEqual(1, findVals.Length);
                Assert.AreEqual(val2, new Guid(findVals[0]));

                // test with Like
                // test two lookup values using Or
                options = new ObjectFindOptions()
                {
                    Operator = ObjectFindOperator.Like,
                    Logic = ObjectFindLogic.Or
                };
                findVals = svc.Find(ns, obj, options, new ObjectIndex[]
                {
                    ObjectIndex.Create("StrIndex1", "%test%"),
                    ObjectIndex.Create("StrIndex1", "%xx")
                }).ToArray();

                Assert.AreEqual(2, findVals.Length);
                Assert.AreEqual(val2, new Guid(findVals[0]));
                Assert.AreEqual(val3, new Guid(findVals[1]));

                // test with Like
                // test two lookup values using And - should return 0
                options = new ObjectFindOptions()
                {
                    Operator = ObjectFindOperator.Like,
                    Logic = ObjectFindLogic.And
                };
                findVals = svc.Find(ns, obj, options, new ObjectIndex[]
                {
                    ObjectIndex.Create("StrIndex1", "%test%"),
                    ObjectIndex.Create("StrIndex1", "%zz")
                }).ToArray();

                // should not return any values
                Assert.AreEqual(0, findVals.Length);

                // check nulls
                options = new ObjectFindOptions()
                {
                    Operator = ObjectFindOperator.Equals,
                    Logic = ObjectFindLogic.And
                };

                findVals = svc.Find(ns, obj, options, 
                    new ObjectIndex[] 
                    {
                        ObjectIndex.Create("StrNullIndex1", strNullIndexVal)
                    }
                ).ToArray();

                Assert.AreEqual(1, findVals.Length);
                Assert.AreEqual(val2, new Guid(findVals[0]));

                options = new ObjectFindOptions()
                {
                    Operator = ObjectFindOperator.IsNull,
                    Logic = ObjectFindLogic.And
                };

                findVals = svc.Find(ns, obj, options,
                    new ObjectIndex[] 
                    {
                        ObjectIndex.Create("StrNullIndex1", null)
                    }
                ).ToArray();

                Assert.AreEqual(2, findVals.Length);
                Assert.AreEqual(val1, new Guid(findVals[0]));
                Assert.AreEqual(val3, new Guid(findVals[1]));

                findVals = svc.Find(ns, obj, @"{""StrNullIndex1"" : [""" + strNullIndexVal + @"""], ""Op"" : ""IN"" }").ToArray();
                Assert.AreEqual(1, findVals.Length);
                Assert.AreEqual(val2, new Guid(findVals[0]));

                findVals = svc.Find(ns, obj, @"{""StrNullIndex1"" : null, ""Op"" : ""="" }").ToArray();
                Assert.AreEqual(2, findVals.Length);
                Assert.AreEqual(val1, new Guid(findVals[0]));
                Assert.AreEqual(val3, new Guid(findVals[1]));
            }
        }

        [TestMethod]
        [TestCategory("Core")]
        public void StoreAndRetrieveManyObjects()
        {
            using (var svc = new ObjectService(ObjectTestHelper.GetConfig()))
            {
                var ns = ObjectTestHelper.NameSpace1;
                var obj = ObjectTestHelper.ObjectName1;

                svc.CreateNameSpace(new ObjectNameSpaceConfig(ns,
                    "ZeroG Test", "Unit Test", DateTime.Now));

                svc.ProvisionObjectStore(
                    new ObjectMetadata(ns, obj));

                int ObjCount = 100000;
                int[] ids = new int[ObjCount];

                var val1 = new Guid("{8AD7F9E4-B2B8-4511-B520-08914B999044}").ToByteArray();

                // store objects
                for (int i = 0; ObjCount > i; i++)
                {
                    var storeObj = new PersistentObject()
                    {
                        Name = obj,
                        ID = i,
                        Value = val1
                    };
                    svc.Store(ns, storeObj);
                    ids[i] = i;
                }

                // retrieve objects
                int count = 0;
                
                
                for (int i = 0; ObjCount > i; i++)
                {
                    var getObj = svc.Get(ns, obj, i);
                    if (null != getObj)
                    {
                        ++count;
                    }
                }
                Assert.AreEqual(ObjCount, count);
            }
        }

        [TestMethod]
        [TestCategory("Core")]
        public void StoreAndRetrieveManyObjectsBySecondaryKey()
        {
            using (var svc = new ObjectService(ObjectTestHelper.GetConfig()))
            {
                var ns = ObjectTestHelper.NameSpace1;
                var obj = ObjectTestHelper.ObjectName1;

                svc.CreateNameSpace(new ObjectNameSpaceConfig(ns,
                    "ZeroG Test", "Unit Test", DateTime.Now));

                svc.ProvisionObjectStore(
                    new ObjectMetadata(ns, obj));

                int ObjCount = 100000;

                var val1 = new Guid("{8AD7F9E4-B2B8-4511-B520-08914B999044}").ToByteArray();
                var utf8 = Encoding.UTF8;

                // store objects
                for (int i = 0; ObjCount > i; i++)
                {
                    var storeObj = new PersistentObject()
                    {
                        Name = obj,
                        ID = i,
                        SecondaryKey = utf8.GetBytes("secKey" + i),
                        Value = val1
                    };
                    svc.Store(ns, storeObj);
                }

                // retrieve objects
                int count = 0;
                for (int i = 0; ObjCount > i; i++)
                {
                    var key = utf8.GetBytes("secKey" + i);
                    var getObj = svc.GetBySecondaryKey(ns, obj, key);
                    if (null != getObj)
                    {
                        ++count;
                    }
                }

                Assert.AreEqual(ObjCount, count);
            }
        }

        [TestMethod]
        [TestCategory("Core")]
        public void StoreAndRetrieveManyLargeObjects()
        {
            using (var svc = new ObjectService(ObjectTestHelper.GetConfig()))
            {
                var ns = ObjectTestHelper.NameSpace1;
                var obj = ObjectTestHelper.ObjectName1;

                svc.CreateNameSpace(new ObjectNameSpaceConfig(ns,
                    "ZeroG Test", "Unit Test", DateTime.Now));

                svc.ProvisionObjectStore(
                    new ObjectMetadata(ns, obj));

                int ObjCount = 1000;

                // roughly 47k
                var textBlob = Encoding.UTF8.GetBytes(File.ReadAllText("TestData\\blob.txt"));

                // store objects
                for (int i = 0; ObjCount > i; i++)
                {
                    var storeObj = new PersistentObject()
                    {
                        Name = obj,
                        ID = i,
                        Value = textBlob
                    };
                    svc.Store(ns, storeObj);
                }

                // retrieve objects
                int count = 0;
                for (int i = 0; ObjCount > i; i++)
                {
                    var getObj = svc.Get(ns, obj, i);
                    if (null != getObj)
                    {
                        ++count;
                    }
                }

                Assert.AreEqual(ObjCount, count);
            }
        }

        [TestMethod]
        [TestCategory("Core")]
        public void StoreAndRetrieveByLargeKey()
        {
            var utf8 = Encoding.UTF8;
            var count = 10000;

            using (var svc = new ObjectService(ObjectTestHelper.GetConfig()))
            {
                var ns = ObjectTestHelper.NameSpace1;
                var obj = ObjectTestHelper.ObjectName1;

                svc.CreateNameSpace(new ObjectNameSpaceConfig(ns,
                    "ZeroG Test", "Unit Test", DateTime.Now));

                svc.ProvisionObjectStore(
                    new ObjectMetadata(ns, obj));

                var keyList = new byte[count][];

                for (int i = 0; count > i; i++)
                {
                    string keyStr = "";

                    for (int j = 0; 10 > j; j++)
                    {
                        keyStr += Guid.NewGuid().ToString("N");
                    }

                    // creates a 320 byte key, which is fairly large
                    keyList[i] = utf8.GetBytes(keyStr);

                    svc.Store(
                        ns,
                        new PersistentObject()
                        {
                            Name = obj,
                            Value = utf8.GetBytes("<div>test value " + i + "</div>"),
                            SecondaryKey = keyList[i]
                        });
                }

                for (int i = 0; count > i; i++)
                {
                    var val = svc.GetBySecondaryKey(ns, obj, keyList[i]);

                    Assert.AreEqual("<div>test value " + i + "</div>",
                        utf8.GetString(val));
                }
            }
        }

        [TestMethod]
        [TestCategory("Core")]
        public void Count()
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

                Assert.AreEqual(0, svc.Count(ns, obj));

                var objID1 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val1.ToByteArray(),
                    SecondaryKey = secKey1
                });

                Assert.AreEqual(1, svc.Count(ns, obj));

                var objID2 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val2.ToByteArray(),
                    SecondaryKey = secKey2
                });

                Assert.AreEqual(2, svc.Count(ns, obj));

                svc.Remove(ns, obj, objID1.ID);

                Assert.AreEqual(1, svc.Count(ns, obj));

                svc.Remove(ns, obj, objID2.ID);

                Assert.AreEqual(0, svc.Count(ns, obj));
            }
        }

        [TestMethod]
        [TestCategory("Core")]
        public void Iterate()
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

                Assert.AreEqual(0, svc.Iterate(ns, obj).Count());

                var objID1 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val1.ToByteArray(),
                    SecondaryKey = secKey1
                });

                Assert.AreEqual(1, svc.Iterate(ns, obj).Count());

                var objID2 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val2.ToByteArray(),
                    SecondaryKey = secKey2
                });

                Assert.AreEqual(2, svc.Iterate(ns, obj).Count());

                svc.Remove(ns, obj, objID1.ID);

                Assert.AreEqual(1, svc.Iterate(ns, obj).Count());

                svc.Remove(ns, obj, objID2.ID);

                Assert.AreEqual(0, svc.Iterate(ns, obj).Count());
            }
        }

        [TestMethod]
        [TestCategory("Core")]
        public void BulkStore()
        {
            using (var svc = new ObjectService(ObjectTestHelper.GetConfig()))
            {
                var ns = ObjectTestHelper.NameSpace1;
                var obj = ObjectTestHelper.ObjectName1;

                svc.CreateNameSpace(new ObjectNameSpaceConfig(ns,
                    "ZeroG Test", "Unit Test", DateTime.Now));

                svc.ProvisionObjectStore(
                    new ObjectMetadata(ns, obj,
                        new ObjectIndexMetadata[] 
                        {
                            new ObjectIndexMetadata("IntIndex1", ObjectIndexType.Integer),
                            new ObjectIndexMetadata("StrIndex1", ObjectIndexType.String, 15),
                            new ObjectIndexMetadata("StrNullIndex1", ObjectIndexType.String, 5, true)
                        }));

                var val1 = new Guid("{D22640F0-7D87-4F1C-8817-119FC036FAC1}");
                var val2 = new Guid("{72FC1391-EC51-4826-890B-D02071A9A2DE}");
                var val3 = new Guid("{72FC1391-EC51-4826-890B-D02071A9A2DE}");
                var intIndex1 = 5;
                var intIndex2 = 12500;
                var intIndex3 = -100;
                var strIndex1 = "asdf";
                var strIndex2 = "index test val";
                var strIndex3 = "zzyyxx";

                var strNullIndexVal = "0011";

                var objList = new List<PersistentObject>();
                objList.Add(new PersistentObject()
                {
                    Name = obj,
                    Value = val1.ToByteArray(),
                    Indexes = new ObjectIndex[] 
                    { 
                        ObjectIndex.Create("IntIndex1", intIndex1),
                        ObjectIndex.Create("StrIndex1", strIndex1),
                        ObjectIndex.Create("StrNullIndex1", null)
                    }
                });
                objList.Add(new PersistentObject()
                {
                    Name = obj,
                    Value = val2.ToByteArray(),
                    Indexes = new ObjectIndex[] 
                    { 
                        ObjectIndex.Create("IntIndex1", intIndex2),
                        ObjectIndex.Create("StrIndex1", strIndex2),
                        ObjectIndex.Create("StrNullIndex1", strNullIndexVal)
                    }
                });
                objList.Add(new PersistentObject()
                {
                    Name = obj,
                    Value = val3.ToByteArray(),
                    Indexes = new ObjectIndex[] 
                    { 
                        ObjectIndex.Create("IntIndex1", intIndex3),
                        ObjectIndex.Create("StrIndex1", strIndex3),
                        ObjectIndex.Create("StrNullIndex1", null)
                    }
                });

                var objectIDs = svc.BulkStore(ns, objList);
                Assert.AreEqual(3, objectIDs.Count());

                var objLookup = svc.Get(ns, obj, objectIDs[0].ID);
                Assert.IsNotNull(objLookup);
                Assert.AreEqual(val1, new Guid(objLookup));

                // count all index values
                Assert.AreEqual(3, svc.Count(ns, obj));

                // test a single index lookup using And
                var options = new ObjectFindOptions()
                {
                    Operator = ObjectFindOperator.Equals,
                    Logic = ObjectFindLogic.And
                };
                var findVals = svc.Find(ns, obj, options, new ObjectIndex[]
                {
                    ObjectIndex.Create("IntIndex1", 12500)
                }).ToArray();

                Assert.AreEqual(1, findVals.Length);
                Assert.AreEqual(val2, new Guid(findVals[0]));

                // test two index lookups using And
                options = new ObjectFindOptions()
                {
                    Operator = ObjectFindOperator.Equals,
                    Logic = ObjectFindLogic.And
                };
                findVals = svc.Find(ns, obj, options, new ObjectIndex[]
                {
                    ObjectIndex.Create("StrIndex1", "index test val"),
                    ObjectIndex.Create("IntIndex1", 12500)
                }).ToArray();

                Assert.AreEqual(1, findVals.Length);
                Assert.AreEqual(val2, new Guid(findVals[0]));

                // test a single lookup using Or
                options = new ObjectFindOptions()
                {
                    Operator = ObjectFindOperator.Equals,
                    Logic = ObjectFindLogic.Or
                };
                findVals = svc.Find(ns, obj, options, new ObjectIndex[]
                {
                    ObjectIndex.Create("IntIndex1", 12500)
                }).ToArray();

                Assert.AreEqual(1, findVals.Length);
                Assert.AreEqual(val2, new Guid(findVals[0]));

                // test two index lookups using Or
                options = new ObjectFindOptions()
                {
                    Operator = ObjectFindOperator.Equals,
                    Logic = ObjectFindLogic.Or
                };
                findVals = svc.Find(ns, obj, options, new ObjectIndex[]
                {
                    ObjectIndex.Create("IntIndex1", 12500),
                    ObjectIndex.Create("StrIndex1", "asdf")
                }).ToArray();

                Assert.AreEqual(2, findVals.Length);
                var findVal1 = new Guid(findVals[0]);
                var findVal2 = new Guid(findVals[1]);
                Assert.IsFalse(findVal1 == findVal2);
                Assert.IsTrue(findVal1 == val1 || findVal1 == val2);
                Assert.IsTrue(findVal2 == val1 || findVal2 == val2);

                // test with Like
                // test a single lookup using And
                options = new ObjectFindOptions()
                {
                    Operator = ObjectFindOperator.Like,
                    Logic = ObjectFindLogic.And
                };
                findVals = svc.Find(ns, obj, options, new ObjectIndex[]
                {
                    ObjectIndex.Create("StrIndex1", "%test%")
                }).ToArray();

                Assert.AreEqual(1, findVals.Length);
                Assert.AreEqual(val2, new Guid(findVals[0]));

                // test with Like
                // test two lookup values using Or
                options = new ObjectFindOptions()
                {
                    Operator = ObjectFindOperator.Like,
                    Logic = ObjectFindLogic.Or
                };
                findVals = svc.Find(ns, obj, options, new ObjectIndex[]
                {
                    ObjectIndex.Create("StrIndex1", "%test%"),
                    ObjectIndex.Create("StrIndex1", "%xx")
                }).ToArray();

                Assert.AreEqual(2, findVals.Length);
                Assert.AreEqual(val2, new Guid(findVals[0]));
                Assert.AreEqual(val3, new Guid(findVals[1]));

                // test with Like
                // test two lookup values using And - should return 0
                options = new ObjectFindOptions()
                {
                    Operator = ObjectFindOperator.Like,
                    Logic = ObjectFindLogic.And
                };
                findVals = svc.Find(ns, obj, options, new ObjectIndex[]
                {
                    ObjectIndex.Create("StrIndex1", "%test%"),
                    ObjectIndex.Create("StrIndex1", "%zz")
                }).ToArray();

                // should not return any values
                Assert.AreEqual(0, findVals.Length);

                // check nulls
                options = new ObjectFindOptions()
                {
                    Operator = ObjectFindOperator.Equals,
                    Logic = ObjectFindLogic.And
                };

                findVals = svc.Find(ns, obj, options,
                    new ObjectIndex[] 
                    {
                        ObjectIndex.Create("StrNullIndex1", strNullIndexVal)
                    }
                ).ToArray();

                Assert.AreEqual(1, findVals.Length);
                Assert.AreEqual(val2, new Guid(findVals[0]));

                options = new ObjectFindOptions()
                {
                    Operator = ObjectFindOperator.IsNull,
                    Logic = ObjectFindLogic.And
                };

                findVals = svc.Find(ns, obj, options,
                    new ObjectIndex[] 
                    {
                        ObjectIndex.Create("StrNullIndex1", null)
                    }
                ).ToArray();

                Assert.AreEqual(2, findVals.Length);
                Assert.AreEqual(val1, new Guid(findVals[0]));
                Assert.AreEqual(val3, new Guid(findVals[1]));

                findVals = svc.Find(ns, obj, @"{""StrNullIndex1"" : [""" + strNullIndexVal + @"""], ""Op"" : ""IN"" }").ToArray();
                Assert.AreEqual(1, findVals.Length);
                Assert.AreEqual(val2, new Guid(findVals[0]));

                findVals = svc.Find(ns, obj, @"{""StrNullIndex1"" : null, ""Op"" : ""="" }").ToArray();
                Assert.AreEqual(2, findVals.Length);
                Assert.AreEqual(val1, new Guid(findVals[0]));
                Assert.AreEqual(val3, new Guid(findVals[1]));
            }
        }

        [TestMethod]
        [TestCategory("Core")]
        public void BulkStoreMany()
        {
            using (var svc = new ObjectService(ObjectTestHelper.GetConfig()))
            {
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

                var objCount = 50000;

                var objList = new List<PersistentObject>();
                var random = new Random();
                var buf = new byte[100];

                // generate a list of objects to store
                for (int i = 0; objCount > i; i++)
                {
                    random.NextBytes(buf);

                    objList.Add(new PersistentObject()
                    {
                        Name = obj,
                        Value = buf,
                        Indexes = new ObjectIndex[] 
                        { 
                            ObjectIndex.Create("IntIndex1", i + 100),
                            ObjectIndex.Create("StrIndex1", "idx_" + i)
                        }
                    });
                }

                // store and index the objects
                svc.BulkStore(ns, objList);

                // query 100 objects from the index
                var vals = svc.Find(ns, obj, @"{""IntIndex1"":10000, ""Op"": "">"", ""And"" : {""IntIndex1"":10101, ""Op"": ""<""}}");
                Assert.AreEqual(100, vals.Count());
            }
        }

        [TestMethod]
        [TestCategory("Core")]
        public void UpdateIndexes()
        {
            using (var svc = new ObjectService(ObjectTestHelper.GetConfig()))
            {
                var ns = ObjectTestHelper.NameSpace1;
                var obj = ObjectTestHelper.ObjectName1;

                svc.CreateNameSpace(new ObjectNameSpaceConfig(ns,
                    "ZeroG Test", "Unit Test", DateTime.Now));

                svc.ProvisionObjectStore(
                    new ObjectMetadata(ns, obj,
                        new ObjectIndexMetadata[]
                        {
                            new ObjectIndexMetadata("Test", ObjectIndexType.String, 5)
                        }));

                var val1 = new Guid("{D22640F0-7D87-4F1C-8817-119FC036FAC1}");
                var val2 = new Guid("{72FC1391-EC51-4826-890B-D02071A9A2DE}");

                var objID1 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val1.ToByteArray(),
                    Indexes = new ObjectIndex[]
                    {
                        ObjectIndex.Create("Test", "asdf")
                    }
                });

                var objID2 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val2.ToByteArray(),
                    Indexes = new ObjectIndex[]
                    {
                        ObjectIndex.Create("Test", "2")
                    }
                });

                Assert.AreEqual(2, svc.Count(ns, obj));

                // Find by index
                var retval1 = svc.Find(ns, obj, "{\"Test\":\"asdf\"}").FirstOrDefault();
                var retval2 = svc.Find(ns, obj, "{\"Test\":\"2\"}").FirstOrDefault();
                // This is a test value - should return null
                var retval3 = svc.Find(ns, obj, "{\"Test\":\"3\"}").FirstOrDefault();

                Assert.IsNotNull(retval1);
                Assert.IsNotNull(retval2);
                Assert.IsNull(retval3);

                Assert.AreEqual(val1, new Guid(retval1));
                Assert.AreEqual(val2, new Guid(retval2));

                // Update index and re-fetch
                svc.UpdateIndexes(ns, obj, objID2.ID,
                    new ObjectIndex[] { ObjectIndex.Create("Test", "3") });
                
                Assert.AreEqual(2, svc.Count(ns, obj));

                retval1 = svc.Find(ns, obj, "{\"Test\":\"asdf\"}").FirstOrDefault();
                // verify that old value is gone
                retval2 = svc.Find(ns, obj, "{\"Test\":\"2\"}").FirstOrDefault();
                retval3 = svc.Find(ns, obj, "{\"Test\":\"3\"}").FirstOrDefault();

                Assert.IsNotNull(retval1);
                Assert.IsNull(retval2); // should now be null
                Assert.IsNotNull(retval3);

                Assert.AreEqual(val1, new Guid(retval1));
                Assert.AreEqual(val2, new Guid(retval3));

                // remove object who's index was changed to make sure it goes away
                svc.Remove(ns, obj, objID2.ID);

                Assert.AreEqual(1, svc.Count(ns, obj));

                retval3 = svc.Find(ns, obj, "{\"Test\":\"3\"}").FirstOrDefault();
                Assert.IsNull(retval3);
            }
        }
    }
}
