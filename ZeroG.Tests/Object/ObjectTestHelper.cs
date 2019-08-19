using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ZeroG.Data.Database;
using ZeroG.Data.Object;
using ZeroG.Data.Object.Index;
using ZeroG.Tests.Data;

namespace ZeroG.Tests.Object
{
    public class ObjectTestHelper
    {
        public static readonly string NameSpace1 = "ZG_testNS1";
        public static readonly string ObjectName1 = "ZG_testObj1";
        public static readonly string ObjectName2 = "ZG_testObj2";
        public static readonly string ObjectName3 = "ZG_testObj3";

        static ObjectTestHelper()
        {
            DataTestHelper.Configure();
        }

        public static Config GetConfig()
        {
            return Config.Default;
        }

        public static Config GetConfigWithCaching()
        {
            var defaultConfig = GetConfig();
            return new Config(defaultConfig.BaseDataPath,
                true,
                defaultConfig.IndexCacheMaxQueries,
                defaultConfig.IndexCacheMaxValues,
                defaultConfig.ObjectIndexSchemaConnection,
                defaultConfig.ObjectIndexDataConnection,
                defaultConfig.MaxObjectDependencies,
                defaultConfig.ObjectStoreAutoClose,
                defaultConfig.ObjectStoreAutoCloseTimeout,
                defaultConfig.ObjectStoreCacheSize);
        }

        public static ObjectService GetService(Config config) =>
            new ObjectService(config, GetObjectIndexProvider(config));

        public static IObjectIndexProvider GetObjectIndexProvider(Config config)
        {
            var appConfig = TestConfig.Config;
            var setting = appConfig["ObjectIndexProvider"];
            Type type = Type.GetType(setting, true);
            return (IObjectIndexProvider)Activator.CreateInstance(type, config);
        }

        public static void CleanTestObjects()
        {
            var config = GetConfig();
            using (var svc = new ObjectService(config, GetObjectIndexProvider(config)))
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
