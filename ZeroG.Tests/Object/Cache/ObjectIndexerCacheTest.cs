using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ZeroG.Data.Object;
using ZeroG.Data.Object.Metadata;
using ZeroG.Data.Object.Index;
using System.Diagnostics;
using ZeroG.Data.Object.Cache;
using System.Collections.Generic;
using System.Threading.Tasks;

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
        public void CreateAndModify()
        {
            Config config = ObjectTestHelper.GetConfigWithCaching();
            
            ObjectMetadataStore metadata = new ObjectMetadataStore(config);
            ObjectVersionStore versions = new ObjectVersionStore(config, metadata);
            ObjectIndexerCache cache = new ObjectIndexerCache(metadata, versions);

            string objectName = "TestObj";
            string intVal1 = ObjectIndex.Create("IntIdx", 5).ToString();
            string strVal1 = ObjectIndex.Create("StrIdx", "Val1").ToString();
            string strVal2 = ObjectIndex.Create("StrIdx", "Val3").ToString();

            object[] cacheParams = new object[] { objectName, intVal1, strVal1 };
            object[] cacheParams2 = new object[] { objectName, intVal1, strVal2 };
            int[] objectIds = new int[] { 1000, 2000 };
            int[] objectIds2 = new int[] { 3000, 4000 };

            try
            {
                Assert.AreEqual(0, cache.EnumerateCache().Count());
                CacheTotals totals = cache.Totals;
                Assert.AreEqual(0, totals.TotalQueries);
                Assert.AreEqual(0, totals.TotalObjectIDs);
                Assert.IsNull(cache.Get(cacheParams));

                cache.Reset();

                Assert.AreEqual(0, cache.EnumerateCache().Count());
                totals = cache.Totals;
                Assert.AreEqual(0, totals.TotalQueries);
                Assert.AreEqual(0, totals.TotalObjectIDs);
                Assert.IsNull(cache.Get(cacheParams));

                // add one query to cache
                cache.Set(objectIds, cacheParams);

                Assert.AreEqual(1, cache.EnumerateCache().Count());
                totals = cache.Totals;
                Assert.AreEqual(1, totals.TotalQueries);
                Assert.AreEqual(2, totals.TotalObjectIDs);

                int[] lookupIds = cache.Get(cacheParams);
                Assert.AreEqual(objectIds.Length, lookupIds.Length);
                Assert.AreEqual(objectIds[0], lookupIds[0]);
                Assert.AreEqual(objectIds[1], lookupIds[1]);

                cache.Reset();

                Assert.AreEqual(0, cache.EnumerateCache().Count());
                totals = cache.Totals;
                Assert.AreEqual(0, totals.TotalQueries);
                Assert.AreEqual(0, totals.TotalObjectIDs);
                Assert.IsNull(cache.Get(cacheParams));

                // add more than one query to cache
                cache.Set(objectIds, cacheParams);
                cache.Set(objectIds2, cacheParams2);

                Assert.AreEqual(2, cache.EnumerateCache().Count());
                totals = cache.Totals;
                Assert.AreEqual(2, totals.TotalQueries);
                Assert.AreEqual(4, totals.TotalObjectIDs);

                lookupIds = cache.Get(cacheParams);
                Assert.AreEqual(objectIds.Length, lookupIds.Length);
                Assert.AreEqual(objectIds[0], lookupIds[0]);
                Assert.AreEqual(objectIds[1], lookupIds[1]);

                lookupIds = cache.Get(cacheParams2);
                Assert.AreEqual(objectIds2.Length, lookupIds.Length);
                Assert.AreEqual(objectIds2[0], lookupIds[0]);
                Assert.AreEqual(objectIds2[1], lookupIds[1]);

                // add the same query to cache again - make sure it replaces the existing one
                cache.Set(objectIds2, cacheParams2);

                Assert.AreEqual(2, cache.EnumerateCache().Count());
                totals = cache.Totals;
                Assert.AreEqual(2, totals.TotalQueries);
                Assert.AreEqual(4, totals.TotalObjectIDs);

                lookupIds = cache.Get(cacheParams);
                Assert.AreEqual(objectIds.Length, lookupIds.Length);
                Assert.AreEqual(objectIds[0], lookupIds[0]);
                Assert.AreEqual(objectIds[1], lookupIds[1]);

                lookupIds = cache.Get(cacheParams2);
                Assert.AreEqual(objectIds2.Length, lookupIds.Length);
                Assert.AreEqual(objectIds2[0], lookupIds[0]);
                Assert.AreEqual(objectIds2[1], lookupIds[1]);

                // remove query from cache
                var entryToRemove = cache.EnumerateCache().Last();
                // we don't know what order they will be returned so find out 
                // which value is being removed
                object[] remainingCacheParams = cacheParams;
                int[] remainingObjectIds = objectIds;
                if (entryToRemove.Hash == ObjectIndexerCache.ConstructHash(cacheParams))
                {
                    // then the first entry as removed so the second will be the 
                    // remaining entry
                    remainingCacheParams = cacheParams2;
                    remainingObjectIds = objectIds2;
                }
                cache.Remove(new ICacheEntry[] { entryToRemove });

                Assert.AreEqual(1, cache.EnumerateCache().Count());
                totals = cache.Totals;
                Assert.AreEqual(1, totals.TotalQueries);
                Assert.AreEqual(2, totals.TotalObjectIDs);

                lookupIds = cache.Get(remainingCacheParams);
                Assert.AreEqual(remainingObjectIds.Length, lookupIds.Length);
                Assert.AreEqual(remainingObjectIds[0], lookupIds[0]);
                Assert.AreEqual(remainingObjectIds[1], lookupIds[1]);

                Assert.IsNull(cache.Get(cacheParams2));
            }
            finally
            {
                cache.Dispose();
                versions.Dispose();
                metadata.Dispose();
            }
        }

        [TestMethod]
        [TestCategory("Core")]
        public void CacheWithVersionChangeEvent()
        {
            Config config = ObjectTestHelper.GetConfigWithCaching();

            ObjectMetadataStore metadata = new ObjectMetadataStore(config);
            ObjectVersionStore versions = new ObjectVersionStore(config, metadata);
            ObjectIndexerCache cache = new ObjectIndexerCache(metadata, versions);

            string objectName = "TestObj";
            string intVal1 = ObjectIndex.Create("IntIdx", 5).ToString();
            string strVal1 = ObjectIndex.Create("StrIdx", "Val1").ToString();
            string strVal2 = ObjectIndex.Create("StrIdx", "Val3").ToString();

            object[] cacheParams = new object[] { objectName, intVal1, strVal1 };
            object[] cacheParams2 = new object[] { objectName, intVal1, strVal2 };
            int[] objectIds = new int[] { 1000, 2000 };
            int[] objectIds2 = new int[] { 3000, 4000 };

            try
            {
                // add a couple items to cache - they should not be returned
                // once the object's version changes (they become "dirty")
                cache.Set(objectIds, cacheParams);
                cache.Set(objectIds2, cacheParams2);
                
                Assert.AreEqual(2, cache.EnumerateCache().Count());
                CacheTotals totals = cache.Totals;
                Assert.AreEqual(2, totals.TotalQueries);
                Assert.AreEqual(4, totals.TotalObjectIDs);
                Assert.IsNotNull(cache.Get(cacheParams));
                Assert.IsNotNull(cache.Get(cacheParams2));

                versions.Update(objectName);

                // NOTE: Dirty cache entries are not removed from cache as soon as 
                // they become dirty. They are removed lazily when Get/Set 
                // are called.
                // CacheTotals and Enumerate will return Dirty objects and are 
                // only meant to be used by the Cache Cleaner and not directly 
                // by the Object Indexer.
                Assert.AreEqual(2, cache.EnumerateCache().Count());
                totals = cache.Totals;
                Assert.AreEqual(2, totals.TotalQueries);
                Assert.AreEqual(4, totals.TotalObjectIDs);

                // Since the object is dirty, these should return null.
                Assert.IsNull(cache.Get(cacheParams));
                Assert.IsNull(cache.Get(cacheParams2));

                // Reset the cache values and they should be returned again.
                cache.Set(objectIds, cacheParams);
                cache.Set(objectIds2, cacheParams2);

                Assert.AreEqual(2, cache.EnumerateCache().Count());
                totals = cache.Totals;
                Assert.AreEqual(2, totals.TotalQueries);
                Assert.AreEqual(4, totals.TotalObjectIDs);
                Assert.IsNotNull(cache.Get(cacheParams));
                Assert.IsNotNull(cache.Get(cacheParams2));
            }
            finally
            {
                cache.Dispose();
                versions.Dispose();
                metadata.Dispose();
            }
        }

        [TestMethod]
        [TestCategory("Core")]
        public void CacheWithObjectRemovedEvent()
        {
            Config config = ObjectTestHelper.GetConfigWithCaching();
            string ns = ObjectTestHelper.NameSpace1;
            string obj = ObjectTestHelper.ObjectName1;
            string objectFullName = ObjectNaming.CreateFullObjectName(ns, obj);
            ObjectMetadata objectMetadata = new ObjectMetadata(ns, obj,
                new ObjectIndexMetadata[] 
                {
                    new ObjectIndexMetadata("IntIndex1", ObjectIndexType.Integer),
                    new ObjectIndexMetadata("StrIndex1", ObjectIndexType.String, 15)
                });

            // Temporarily use the ObjectService to provision the Object's Metadata.
            using (var svc = new ObjectService(config))
            {
                svc.CreateNameSpace(new ObjectNameSpaceConfig(ns,
                    "ZeroG Test", "Unit Test", DateTime.Now));

                svc.ProvisionObjectStore(objectMetadata);
            }

            ObjectMetadataStore metadata = new ObjectMetadataStore(config);
            ObjectVersionStore versions = new ObjectVersionStore(config, metadata);
            ObjectIndexerCache cache = new ObjectIndexerCache(metadata, versions);

            string intVal1 = ObjectIndex.Create("IntIdx", 5).ToString();
            string strVal1 = ObjectIndex.Create("StrIdx", "Val1").ToString();
            string strVal2 = ObjectIndex.Create("StrIdx", "Val3").ToString();

            object[] cacheParams = new object[] { objectFullName, intVal1, strVal1 };
            object[] cacheParams2 = new object[] { objectFullName, intVal1, strVal2 };
            int[] objectIds = new int[] { 1000, 2000 };
            int[] objectIds2 = new int[] { 3000, 4000 };

            try
            {
                // add a couple items to cache - they should not be cleared
                // when the object's metadata is removed
                cache.Set(objectIds, cacheParams);
                cache.Set(objectIds2, cacheParams2);

                Assert.AreEqual(2, cache.EnumerateCache().Count());
                CacheTotals totals = cache.Totals;
                Assert.AreEqual(2, totals.TotalQueries);
                Assert.AreEqual(4, totals.TotalObjectIDs);
                Assert.IsNotNull(cache.Get(cacheParams));
                Assert.IsNotNull(cache.Get(cacheParams2));

                metadata.Remove(objectFullName);

                // NOTE: Dirty cache entries are not removed from cache as soon as 
                // they become dirty. They are removed lazily when Get/Set 
                // are called.
                // CacheTotals and Enumerate will return Dirty objects and are 
                // only meant to be used by the Cache Cleaner and not directly 
                // by the Object Indexer.
                Assert.AreEqual(0, cache.EnumerateCache().Count());
                totals = cache.Totals;
                Assert.AreEqual(0, totals.TotalQueries);
                Assert.AreEqual(0, totals.TotalObjectIDs);
                Assert.IsNull(cache.Get(cacheParams));
                Assert.IsNull(cache.Get(cacheParams2));

                // The metadata needs to be added back so TestCleanup completes
                metadata.StoreMetadata(objectMetadata);
            }
            finally
            {
                cache.Dispose();
                versions.Dispose();
                metadata.Dispose();
            }
        }

        [TestMethod]
        [TestCategory("Core")]
        public void CacheWithDependencyVersionChangeEvent()
        {
            Config config = ObjectTestHelper.GetConfigWithCaching();
            string ns = ObjectTestHelper.NameSpace1;
            string obj = ObjectTestHelper.ObjectName1;
            string obj2 = ObjectTestHelper.ObjectName2;
            string objectFullName = ObjectNaming.CreateFullObjectName(ns, obj);
            string objectFullName2 = ObjectNaming.CreateFullObjectName(ns, obj2);

            ObjectMetadata objectMetadata = new ObjectMetadata(ns, obj,
                new ObjectIndexMetadata[] 
                {
                    new ObjectIndexMetadata("IntIndex1", ObjectIndexType.Integer),
                    new ObjectIndexMetadata("StrIndex1", ObjectIndexType.String, 15)
                }); 

            ObjectMetadata objectMetadata2 = new ObjectMetadata(ns, obj2, null,
                new string[] { obj }); // Make Object1 a dependency of Object2.
            // Now whenever Object2's version changes, Object1's change event will fire as well.
            // NOTE: Do not supply Full Object Name as objects can only depend on other objects
            // within their namespace.

            // Temporarily use the ObjectService to provision the Object's Metadata.
            using (var svc = new ObjectService(config))
            {
                svc.CreateNameSpace(new ObjectNameSpaceConfig(ns,
                    "ZeroG Test", "Unit Test", DateTime.Now));

                svc.ProvisionObjectStore(objectMetadata);
                svc.ProvisionObjectStore(objectMetadata2);
            }

            ObjectMetadataStore metadata = new ObjectMetadataStore(config);
            ObjectVersionStore versions = new ObjectVersionStore(config, metadata);
            ObjectIndexerCache cache = new ObjectIndexerCache(metadata, versions);

            string intVal1 = ObjectIndex.Create("IntIdx", 5).ToString();
            string strVal1 = ObjectIndex.Create("StrIdx", "Val1").ToString();
            string strVal2 = ObjectIndex.Create("StrIdx", "Val3").ToString();

            object[] cacheParams = new object[] { objectFullName, intVal1, strVal1 };
            object[] cacheParams2 = new object[] { objectFullName, intVal1, strVal2 };
            int[] objectIds = new int[] { 1000, 2000 };
            int[] objectIds2 = new int[] { 3000, 4000 };

            try
            {
                // add a couple items to cache - they should not be returned
                // once the objects dependency object version changes
                cache.Set(objectIds, cacheParams);
                cache.Set(objectIds2, cacheParams2);

                Assert.AreEqual(2, cache.EnumerateCache().Count());
                CacheTotals totals = cache.Totals;
                Assert.AreEqual(2, totals.TotalQueries);
                Assert.AreEqual(4, totals.TotalObjectIDs);
                Assert.IsNotNull(cache.Get(cacheParams));
                Assert.IsNotNull(cache.Get(cacheParams2));

                versions.Update(objectFullName2);

                Assert.AreEqual(2, cache.EnumerateCache().Count());
                totals = cache.Totals;
                Assert.AreEqual(2, totals.TotalQueries);
                Assert.AreEqual(4, totals.TotalObjectIDs);

                Assert.IsNull(cache.Get(cacheParams));
                Assert.IsNull(cache.Get(cacheParams2));

                cache.Set(objectIds, cacheParams);
                cache.Set(objectIds2, cacheParams2);

                Assert.AreEqual(2, cache.EnumerateCache().Count());
                totals = cache.Totals;
                Assert.AreEqual(2, totals.TotalQueries);
                Assert.AreEqual(4, totals.TotalObjectIDs);
                Assert.IsNotNull(cache.Get(cacheParams));
                Assert.IsNotNull(cache.Get(cacheParams2));
            }
            finally
            {
                cache.Dispose();
                versions.Dispose();
                metadata.Dispose();
            }
        }

        [TestMethod]
        [TestCategory("Core")]
        public void CacheWithDependencyRemoveEvent()
        {
            Config config = ObjectTestHelper.GetConfigWithCaching();
            string ns = ObjectTestHelper.NameSpace1;
            string obj = ObjectTestHelper.ObjectName1;
            string obj2 = ObjectTestHelper.ObjectName2;
            string objectFullName = ObjectNaming.CreateFullObjectName(ns, obj);
            string objectFullName2 = ObjectNaming.CreateFullObjectName(ns, obj2);

            ObjectMetadata objectMetadata = new ObjectMetadata(ns, obj,
                new ObjectIndexMetadata[] 
                {
                    new ObjectIndexMetadata("IntIndex1", ObjectIndexType.Integer),
                    new ObjectIndexMetadata("StrIndex1", ObjectIndexType.String, 15)
                });

            ObjectMetadata objectMetadata2 = new ObjectMetadata(ns, obj2, null,
                new string[] { obj }); // Make Object1 a dependency of Object2.
            // Now whenever Object2's version changes, Object1's change event will fire as well.
            // NOTE: Do not supply Full Object Name as objects can only depend on other objects
            // within their namespace.

            // Temporarily use the ObjectService to provision the Object's Metadata.
            using (var svc = new ObjectService(config))
            {
                svc.CreateNameSpace(new ObjectNameSpaceConfig(ns,
                    "ZeroG Test", "Unit Test", DateTime.Now));

                svc.ProvisionObjectStore(objectMetadata);
                svc.ProvisionObjectStore(objectMetadata2);
            }

            ObjectMetadataStore metadata = new ObjectMetadataStore(config);
            ObjectVersionStore versions = new ObjectVersionStore(config, metadata);
            ObjectIndexerCache cache = new ObjectIndexerCache(metadata, versions);

            string intVal1 = ObjectIndex.Create("IntIdx", 5).ToString();
            string strVal1 = ObjectIndex.Create("StrIdx", "Val1").ToString();
            string strVal2 = ObjectIndex.Create("StrIdx", "Val3").ToString();

            object[] cacheParams = new object[] { objectFullName, intVal1, strVal1 };
            object[] cacheParams2 = new object[] { objectFullName, intVal1, strVal2 };
            int[] objectIds = new int[] { 1000, 2000 };
            int[] objectIds2 = new int[] { 3000, 4000 };

            try
            {
                // Add a couple items to cache - they should not be returned
                // once the objects dependency object is removed
                cache.Set(objectIds, cacheParams);
                cache.Set(objectIds2, cacheParams2);

                Assert.AreEqual(2, cache.EnumerateCache().Count());
                CacheTotals totals = cache.Totals;
                Assert.AreEqual(2, totals.TotalQueries);
                Assert.AreEqual(4, totals.TotalObjectIDs);
                Assert.IsNotNull(cache.Get(cacheParams));
                Assert.IsNotNull(cache.Get(cacheParams2));

                // Remove should fire a change event on its dependencies
                versions.Remove(objectFullName2);

                Assert.AreEqual(2, cache.EnumerateCache().Count());
                totals = cache.Totals;
                Assert.AreEqual(2, totals.TotalQueries);
                Assert.AreEqual(4, totals.TotalObjectIDs);

                Assert.IsNull(cache.Get(cacheParams));
                Assert.IsNull(cache.Get(cacheParams2));

                cache.Set(objectIds, cacheParams);
                cache.Set(objectIds2, cacheParams2);

                Assert.AreEqual(2, cache.EnumerateCache().Count());
                totals = cache.Totals;
                Assert.AreEqual(2, totals.TotalQueries);
                Assert.AreEqual(4, totals.TotalObjectIDs);
                Assert.IsNotNull(cache.Get(cacheParams));
                Assert.IsNotNull(cache.Get(cacheParams2));

                // The metadata needs to be added back so TestCleanup completes
                metadata.StoreMetadata(objectMetadata2);
            }
            finally
            {
                cache.Dispose();
                versions.Dispose();
                metadata.Dispose();
            }
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
                for (int i = 0; i < 10; i++)
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
                for (int i = 0; i < 10; i++)
                {
                    var findVals = svc.Find(ns, obj, @"{ ""IntIndex1"" : 12500 }").ToArray();
                    Assert.AreEqual(1, findVals.Length);
                    Assert.AreEqual(val2, new Guid(findVals[0]));
                }
                stopWatchCached.Stop();
            }

            Assert.IsTrue(stopWatchUncached.Elapsed > stopWatchCached.Elapsed);

            Console.WriteLine("Uncached time: {0} -- Cached time: {1}",
                stopWatchUncached.Elapsed,
                stopWatchCached.Elapsed);
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

        [TestMethod]
        [TestCategory("Core")]
        public void CreateAndModifyMultithreaded()
        {
            Config config = ObjectTestHelper.GetConfigWithCaching();

            ObjectMetadataStore metadata = new ObjectMetadataStore(config);
            ObjectVersionStore versions = new ObjectVersionStore(config, metadata);
            ObjectIndexerCache cache = new ObjectIndexerCache(metadata, versions);

            try
            {
                // Create 4 actions to simulate 4 threads
                // This test simulates 4 concurrent processes adding and reading items to the cache
                // while they are also removed.
                Action[] actions = new Action[4];
                
                for (int i = 0; i < 4; i++)
                {
                    string nextObjName = "Obj" + i;
                    actions[i] = new Action(() => {

                        for (int loops = 0; loops < 100; loops++)
                        {
                            object[][] cacheParams = new object[100][];

                            // create 100 cache entries
                            for (int add = 0; add < 100; add++)
                            {
                                cacheParams[add] = new object[] { nextObjName, "Idx" + ((loops * 100) + add) };

                                cache.Set(
                                    new int[] { loops, loops + 1, loops + 2, loops + 3 },
                                    cacheParams[add]);
                            }

                            // read 1000 entries
                            for (int read = 0; read < 1000; read++)
                            {
                                int[] getIds = cache.Get(cacheParams[read % 100]);
                                // may get removed so check for null
                                if (null != getIds)
                                {
                                    Assert.AreEqual(loops, getIds[0]);
                                    Assert.AreEqual(loops + 1, getIds[1]);
                                }
                            }

                            // Remove 20 entries - this can remove objects added by other threads
                            // Linq query is executed once write lock is aquired which is why this works
                            cache.Remove(cache.EnumerateCache().Take(20));
                            
                        }
                        // that 100 times - total at end will equal 8000 entries and 32000 object ids
                    });
                }

                Parallel.Invoke(actions);

                Assert.AreEqual(8000 * actions.Length, cache.EnumerateCache().Count());
                CacheTotals totals = cache.Totals;
                Assert.AreEqual(8000 * actions.Length, totals.TotalQueries);
                Assert.AreEqual(32000 * actions.Length, totals.TotalObjectIDs);
                
                cache.Reset();

                Assert.AreEqual(0, cache.EnumerateCache().Count());
                totals = cache.Totals;
                Assert.AreEqual(0, totals.TotalQueries);
                Assert.AreEqual(0, totals.TotalObjectIDs);
            }
            finally
            {
                cache.Dispose();
                versions.Dispose();
                metadata.Dispose();
            }
        }
    }
}
