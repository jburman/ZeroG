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
            using (var scope = TestContext.ScopedInstance)
            {
                var svc = scope.GetObjectServiceWithoutIndexCache();
                
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
