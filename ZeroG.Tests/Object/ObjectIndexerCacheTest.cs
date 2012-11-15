using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ZeroG.Data.Object;
using ZeroG.Data.Object.Metadata;
using ZeroG.Data.Object.Index;
using System.Diagnostics;

namespace ZeroG.Tests.Object
{
    [TestClass]
    public class ObjectIndexerCacheTest
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
        public void FindWithCachingTest()
        {
            using (var svc = new ObjectService(ObjectTestHelper.GetConfigWithCaching()))
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
                            new ObjectIndexMetadata("StrIndex1", ObjectIndexType.String, 15)
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

                var objID1 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val1.ToByteArray(),
                    Indexes = new ObjectIndex[] 
                    { 
                        ObjectIndex.Create("IntIndex1", intIndex1),
                        ObjectIndex.Create("StrIndex1", strIndex1)
                    }
                });

                var objID2 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val2.ToByteArray(),
                    Indexes = new ObjectIndex[] 
                    { 
                        ObjectIndex.Create("IntIndex1", intIndex2),
                        ObjectIndex.Create("StrIndex1", strIndex2)
                    }
                });

                var objID3 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val3.ToByteArray(),
                    Indexes = new ObjectIndex[] 
                    { 
                        ObjectIndex.Create("IntIndex1", intIndex3),
                        ObjectIndex.Create("StrIndex1", strIndex3)
                    }
                });

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
            }
        }

        [TestMethod]
        [TestCategory("Core")]
        public void FindByConstraintWithCachingTest()
        {
            using (var svc = new ObjectService(ObjectTestHelper.GetConfigWithCaching()))
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
                            new ObjectIndexMetadata("StrIndex1", ObjectIndexType.String, 15)
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

                var objID1 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val1.ToByteArray(),
                    Indexes = new ObjectIndex[] 
                    { 
                        ObjectIndex.Create("IntIndex1", intIndex1),
                        ObjectIndex.Create("StrIndex1", strIndex1)
                    }
                });

                var objID2 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val2.ToByteArray(),
                    Indexes = new ObjectIndex[] 
                    { 
                        ObjectIndex.Create("IntIndex1", intIndex2),
                        ObjectIndex.Create("StrIndex1", strIndex2)
                    }
                });

                var objID3 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val3.ToByteArray(),
                    Indexes = new ObjectIndex[] 
                    { 
                        ObjectIndex.Create("IntIndex1", intIndex3),
                        ObjectIndex.Create("StrIndex1", strIndex3)
                    }
                });

                var findVals = svc.Find(ns, obj, @"{ ""IntIndex1"" : 12500, ""Op"" : ""="" }").ToArray();

                Assert.AreEqual(1, findVals.Length);
                Assert.AreEqual(val2, new Guid(findVals[0]));
            }
        }

        [TestMethod]
        [TestCategory("Core")]
        public void CachePerfTest()
        {
            var stopWatchUncached = new Stopwatch();
            var stopWatchCached = new Stopwatch();

            var ns = ObjectTestHelper.NameSpace1;
            var obj = ObjectTestHelper.ObjectName1;

            var val1 = new Guid("{D22640F0-7D87-4F1C-8817-119FC036FAC1}");
            var val2 = new Guid("{72FC1391-EC51-4826-890B-D02071A9A2DE}");
            var val3 = new Guid("{72FC1391-EC51-4826-890B-D02071A9A2DE}");
            var intIndex1 = 5;
            var intIndex2 = 12500;
            var intIndex3 = -100;
            var strIndex1 = "asdf";
            var strIndex2 = "index test val";
            var strIndex3 = "zzyyxx";

            using (var svc = new ObjectService(ObjectTestHelper.GetConfig()))
            {
                svc.CreateNameSpace(new ObjectNameSpaceConfig(ns,
                    "ZeroG Test", "Unit Test", DateTime.Now));

                svc.ProvisionObjectStore(
                    new ObjectMetadata(ns, obj,
                        new ObjectIndexMetadata[] 
                        {
                            new ObjectIndexMetadata("IntIndex1", ObjectIndexType.Integer),
                            new ObjectIndexMetadata("StrIndex1", ObjectIndexType.String, 15)
                        }));

                var objID1 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val1.ToByteArray(),
                    Indexes = new ObjectIndex[] 
                    { 
                        ObjectIndex.Create("IntIndex1", intIndex1),
                        ObjectIndex.Create("StrIndex1", strIndex1)
                    }
                });

                var objID2 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val2.ToByteArray(),
                    Indexes = new ObjectIndex[] 
                    { 
                        ObjectIndex.Create("IntIndex1", intIndex2),
                        ObjectIndex.Create("StrIndex1", strIndex2)
                    }
                });

                var objID3 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val3.ToByteArray(),
                    Indexes = new ObjectIndex[] 
                    { 
                        ObjectIndex.Create("IntIndex1", intIndex3),
                        ObjectIndex.Create("StrIndex1", strIndex3)
                    }
                });

                stopWatchUncached.Start();
                for (int i = 0; 10 > i; i++)
                {
                    var findVals = svc.Find(ns, obj, @"{ ""IntIndex1"" : 12500 }").ToArray();
                    Assert.AreEqual(1, findVals.Length);
                    Assert.AreEqual(val2, new Guid(findVals[0]));
                }
                stopWatchUncached.Stop();
            }

            using (var svc = new ObjectService(ObjectTestHelper.GetConfigWithCaching()))
            {
                stopWatchCached.Start();
                for (int i = 0; 10 > i; i++)
                {
                    var findVals = svc.Find(ns, obj, @"{ ""IntIndex1"" : 12500 }").ToArray();
                    Assert.AreEqual(1, findVals.Length);
                    Assert.AreEqual(val2, new Guid(findVals[0]));
                }
                stopWatchCached.Stop();
            }

            Assert.IsTrue(stopWatchUncached.Elapsed > stopWatchCached.Elapsed);
        }

        [TestMethod]
        [TestCategory("Core")]
        public void RemoveAndFindWithCachingTest()
        {
            using (var svc = new ObjectService(ObjectTestHelper.GetConfigWithCaching()))
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
                            new ObjectIndexMetadata("StrIndex1", ObjectIndexType.String, 15)
                        }));

                var val1 = new Guid("{D22640F0-7D87-4F1C-8817-119FC036FAC1}");
                var val2 = new Guid("{72FC1391-EC51-4826-890B-D02071A9A2DE}");
                var val3 = new Guid("{72FC1391-EC51-4826-890B-D02071A9A2DE}");
                var intIndex1 = 5;
                var intIndex2 = 5;
                var intIndex3 = 5;
                var strIndex1 = "asdf";
                var strIndex2 = "index test val";
                var strIndex3 = "zzyyxx";

                var objID1 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val1.ToByteArray(),
                    Indexes = new ObjectIndex[] 
                    { 
                        ObjectIndex.Create("IntIndex1", intIndex1),
                        ObjectIndex.Create("StrIndex1", strIndex1)
                    }
                });

                var objID2 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val2.ToByteArray(),
                    Indexes = new ObjectIndex[] 
                    { 
                        ObjectIndex.Create("IntIndex1", intIndex2),
                        ObjectIndex.Create("StrIndex1", strIndex2)
                    }
                });

                var objID3 = svc.Store(ns, new PersistentObject()
                {
                    Name = obj,
                    Value = val3.ToByteArray(),
                    Indexes = new ObjectIndex[] 
                    { 
                        ObjectIndex.Create("IntIndex1", intIndex3),
                        ObjectIndex.Create("StrIndex1", strIndex3)
                    }
                });

                // test a single index lookup using And
                var options = new ObjectFindOptions()
                {
                    Operator = ObjectFindOperator.Equals,
                    Logic = ObjectFindLogic.And
                };
                var findVals = svc.Find(ns, obj, options, new ObjectIndex[]
                {
                    ObjectIndex.Create("IntIndex1", 5)
                }).ToArray();

                Assert.AreEqual(3, findVals.Length);

                svc.Remove(ns, obj, objID1.ID);

                findVals = svc.Find(ns, obj, options, new ObjectIndex[]
                {
                    ObjectIndex.Create("IntIndex1", 5)
                }).ToArray();

                Assert.AreEqual(2, findVals.Length);

                Assert.IsNotNull(findVals[0]);
                Assert.IsNotNull(findVals[1]);
            }
        }
    }
}
