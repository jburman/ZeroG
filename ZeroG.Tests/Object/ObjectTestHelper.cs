using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ZeroG.Data.Object;

namespace ZeroG.Tests.Object
{
    public class ObjectTestHelper
    {
        public static readonly string NameSpace1 = "ZG_testNS1";
        public static readonly string ObjectName1 = "ZG_testObj1";
        public static readonly string ObjectName2 = "ZG_testObj2";
        public static readonly string ObjectName3 = "ZG_testObj3";

        public static void CleanTestObjects()
        {
            using (var svc = new ObjectService())
            {
                if (svc.NameSpaceExists(NameSpace1))
                {
                    if (svc.ObjectNameExists(NameSpace1, ObjectName1))
                    {
                        svc.UnprovisionObjectStore(NameSpace1, ObjectName1);
                    }

                    if (svc.ObjectNameExists(NameSpace1, ObjectName2))
                    {
                        svc.UnprovisionObjectStore(NameSpace1, ObjectName2);
                    }

                    if (svc.ObjectNameExists(NameSpace1, ObjectName3))
                    {
                        svc.UnprovisionObjectStore(NameSpace1, ObjectName3);
                    }

                    svc.RemoveNameSpace(NameSpace1);
                }
            }
        }
    }
}
