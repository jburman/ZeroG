using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ZeroG.Data.Object;
using ZeroG.Data.Object.Metadata;

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
        public void StoreAndRetrieve()
        {
            using (var svc = new ObjectService())
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

                // retrieve by object ID
                var retval1 = svc.Get(ns, obj, objID1.ID);
                var retval2 = svc.Get(ns, obj, objID2.ID);

                Assert.IsNotNull(retval1);
                Assert.IsNotNull(retval2);

                Assert.AreEqual(val1, new Guid(retval1));
                Assert.AreEqual(val2, new Guid(retval2));

                // retrieve by unique ID
                retval1 = svc.GetByUniqueID(ns, obj, objID1.UniqueID);
                retval2 = svc.GetByUniqueID(ns, obj, objID2.UniqueID);

                Assert.IsNotNull(retval1);
                Assert.IsNotNull(retval2);

                Assert.AreEqual(val1, new Guid(retval1));
                Assert.AreEqual(val2, new Guid(retval2));

                // this tests setting pre-defined IDs
                int id = 5;
                byte[] uniqueId = Encoding.UTF8.GetBytes("test1");

                svc.Store(ns, new PersistentObject()
                {
                    ID = id,
                    UniqueID = uniqueId,
                    Name = obj,
                    Value = val3.ToByteArray()
                });
                var retval3 = svc.Get(ns, obj, id);

                Assert.IsNotNull(retval1);

                Assert.AreEqual(val3, new Guid(retval3));

                // retrieve by unique ID
                retval1 = svc.GetByUniqueID(ns, obj, uniqueId);

                Assert.IsNotNull(retval3);

                Assert.AreEqual(val3, new Guid(retval3));
            }
        }

        [TestMethod]
        public void GetNonExistingObject()
        {
            using (var svc = new ObjectService())
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
                Assert.IsNull(svc.GetByUniqueID(ns, obj, uniqueId));
            }
        }

        [TestMethod]
        public void StoreAndRetrieveByIndex()
        {
        }
    }
}
