using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ZeroG.Data.Object;

namespace ZeroG.Tests.Object
{
    [TestClass]
    public class MultiThreadingTest
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
        public void MultiThreadedObjectStoreGet()
        {
            var taskCount = 50;
            var taskList = new List<Task>();
            using (var scope = TestContext.ScopedInstance)
            {
                var svc = scope.GetObjectServiceWithoutIndexCache();

                var ns = ObjectTestHelper.NameSpace1;
                var obj = ObjectTestHelper.ObjectName1;

                for (int i = 0; taskCount > i; i++)
                {
                    taskList.Add(Task.Factory.StartNew((object state) =>
                    {
                        lock (svc)
                        {
                            if (!svc.NameSpaceExists(ns))
                            {
                                svc.CreateNameSpace(new ObjectNameSpaceConfig(ns,
                                    "ZeroG Test", "Unit Test", DateTime.Now));
                            }

                            if (!svc.ObjectNameExists(ns, obj))
                            {
                                svc.ProvisionObjectStore(
                                    new ObjectMetadata(ns, obj,
                                        new ObjectIndexMetadata[] 
                                {
                                    new ObjectIndexMetadata("IntIndex1", ObjectIndexType.Integer),
                                    new ObjectIndexMetadata("StrIndex1", ObjectIndexType.String, 15),
                                    new ObjectIndexMetadata("StrNullIndex1", ObjectIndexType.String, 5, true)
                                }));
                            }
                        }

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

                        var options = new ObjectFindOptions()
                        {
                            Operator = ObjectFindOperator.Equals,
                            Logic = ObjectFindLogic.And
                        };
                        var findVals = svc.Find(ns, obj, options, new ObjectIndex[]
                            {
                                ObjectIndex.Create("ID", objID3.ID),
                                ObjectIndex.Create("StrIndex1", strIndex3)
                            }).ToArray();

                        Assert.AreEqual(1, findVals.Length);
                        Assert.AreEqual(val3, new Guid(findVals[0]));
                    }, i, TaskCreationOptions.LongRunning));
                }
                foreach (Task t in taskList)
                {
                    t.Wait();
                }

                var id = svc.GetNextObjectID(ns, obj);
                Assert.AreEqual(3 * taskCount + 1, id);

                Assert.AreEqual(3 * taskCount, svc.Iterate(ns, obj).Count());

                // count all index values
                Assert.AreEqual(3 * taskCount, svc.Count(ns, obj));
            }
        }
    }
}
