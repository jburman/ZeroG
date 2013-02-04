using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ZeroG.Data.Object;
using ZeroG.Data.Object.Metadata;
using ZeroG.Data.Object.Cache;
using System.Threading.Tasks;
using System.Threading;

namespace ZeroG.Tests.Object.Cache
{
    [TestClass]
    public class HardPruneCacheCleanerTest
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
        public void CreateDefault()
        {
            Config config = ObjectTestHelper.GetConfigWithCaching();
            ObjectMetadataStore metadata = new ObjectMetadataStore(config);
            ObjectVersionStore versions = new ObjectVersionStore(config, metadata);
            ObjectIndexerCache cache = new ObjectIndexerCache(metadata, versions);
            using (var cleaner = new HardPruneCacheCleaner(cache))
            {
                try
                {
                    Assert.AreEqual(HardPruneCacheCleaner.DefaultMaxQueries, cleaner.MaxQueries);
                    Assert.AreEqual(HardPruneCacheCleaner.DefaultMaxObjects, cleaner.MaxObjects);
                    Assert.AreEqual(HardPruneCacheCleaner.DefaultReductionFactor, cleaner.ReductionFactor);
                    Assert.AreEqual(HardPruneCacheCleaner.DefaultCleanFrequency, cleaner.CleanFrequency);
                }
                finally
                {
                    cache.Dispose();
                    versions.Dispose();
                    metadata.Dispose();
                }
            }
        }

        [TestMethod]
        [TestCategory("Core")]
        public void BasicCreateAndClean()
        {
            Config config = ObjectTestHelper.GetConfigWithCaching();
            
            ObjectMetadataStore metadata = new ObjectMetadataStore(config);
            ObjectVersionStore versions = new ObjectVersionStore(config, metadata);
            ObjectIndexerCache cache = new ObjectIndexerCache(metadata, versions);

            string objectName = "TestObj";

            try
            {
                // Load up the cache - first we will test that it is not cleaned when it shouldn't be
                for (int i = 0; i < 20; i++) // generate 20 queries and 80 object Ids
                {
                    cache.Set(
                        new int[] { 1, 2, 3, 4 },
                        new object[] { objectName, "Idx" + i + "=2" });
                }

                // Verify the totals
                Assert.AreEqual(20, cache.EnumerateCache().Count());
                CacheTotals totals = cache.Totals;
                Assert.AreEqual(20, totals.TotalQueries);
                Assert.AreEqual(80, totals.TotalObjectIDs);
                Assert.IsNotNull(cache.Get(new object[] { objectName, "Idx1=2" }));

                using (var cleaner = new HardPruneCacheCleaner(cache,
                    50,
                    200,
                    2,
                    500)) // run every half second
                {
                    // verify properties
                    Assert.AreEqual(50, cleaner.MaxQueries);
                    Assert.AreEqual(200, cleaner.MaxObjects);
                    Assert.AreEqual(2, cleaner.ReductionFactor);
                    Assert.AreEqual(500, cleaner.CleanFrequency);

                    Thread.Sleep(700);
                }

                // Check that no changes occurred
                Assert.AreEqual(20, cache.EnumerateCache().Count());
                totals = cache.Totals;
                Assert.AreEqual(20, totals.TotalQueries);
                Assert.AreEqual(80, totals.TotalObjectIDs);
                Assert.IsNotNull(cache.Get(new object[] { objectName, "Idx1=2" }));

                // Fill to max
                for (int i = 20; i < 50; i++)
                {
                    cache.Set(
                        new int[] { 1, 2, 3, 4 },
                        new object[] { objectName, "Idx" + i + "=2" });
                }

                Assert.AreEqual(50, cache.EnumerateCache().Count());
                totals = cache.Totals;
                Assert.AreEqual(50, totals.TotalQueries);
                Assert.AreEqual(200, totals.TotalObjectIDs);
                Assert.IsNotNull(cache.Get(new object[] { objectName, "Idx1=2" }));

                using (var cleaner = new HardPruneCacheCleaner(cache,
                    50,
                    200,
                    2,
                    500)) // run every half second
                {
                    Thread.Sleep(700);
                }

                Assert.AreEqual(50, cache.EnumerateCache().Count());
                totals = cache.Totals;
                Assert.AreEqual(50, totals.TotalQueries);
                Assert.AreEqual(200, totals.TotalObjectIDs);
                Assert.IsNotNull(cache.Get(new object[] { objectName, "Idx1=2" }));

                // fill to 60 queries - with a cleanFactor of 2 the amount should be cut in half
                for (int i = 50; i < 60; i++)
                {
                    cache.Set(
                        new int[] { 1, 2, 3, 4 },
                        new object[] { objectName, "Idx" + i + "=2" });
                }

                Assert.AreEqual(60, cache.EnumerateCache().Count());
                totals = cache.Totals;
                Assert.AreEqual(60, totals.TotalQueries);
                Assert.AreEqual(240, totals.TotalObjectIDs);
                Assert.IsNotNull(cache.Get(new object[] { objectName, "Idx1=2" }));

                using (var cleaner = new HardPruneCacheCleaner(cache,
                    50,
                    200,
                    2,
                    500)) // run every half second
                {
                    Thread.Sleep(700);
                }

                // half of the queries should be gone
                Assert.AreEqual(30, cache.EnumerateCache().Count());
                totals = cache.Totals;
                Assert.AreEqual(30, totals.TotalQueries);
                Assert.AreEqual(120, totals.TotalObjectIDs);
                Assert.IsNotNull(cache.Get(new object[] { objectName, "Idx1=2" }));

                // Now overfill 
                for (int i = 100; i < 600; i++)
                {
                    cache.Set(
                        new int[] { 1, 2, 3, 4 },
                        new object[] { objectName, "Idx" + i + "=2" });
                }
                Assert.AreEqual(530, cache.EnumerateCache().Count());
                totals = cache.Totals;
                Assert.AreEqual(530, totals.TotalQueries);
                Assert.AreEqual(2120, totals.TotalObjectIDs);
                Assert.IsNotNull(cache.Get(new object[] { objectName, "Idx1=2" }));

                // this should cut out half
                using (var cleaner = new HardPruneCacheCleaner(cache,
                    50,
                    200,
                    2,
                    500)) // run every half second
                {
                    Thread.Sleep(700);
                }

                Assert.AreEqual(265, cache.EnumerateCache().Count());
                totals = cache.Totals;
                Assert.AreEqual(265, totals.TotalQueries);
                Assert.AreEqual(1060, totals.TotalObjectIDs);
                Assert.IsNotNull(cache.Get(new object[] { objectName, "Idx1=2" }));

                // this should cut out another half
                using (var cleaner = new HardPruneCacheCleaner(cache,
                    50,
                    200,
                    2,
                    500)) // run every half second
                {
                    Thread.Sleep(700);
                }

                Assert.AreEqual(133, cache.EnumerateCache().Count());
                totals = cache.Totals;
                Assert.AreEqual(133, totals.TotalQueries);
                Assert.AreEqual(532, totals.TotalObjectIDs);
                Assert.IsNotNull(cache.Get(new object[] { objectName, "Idx1=2" }));
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
