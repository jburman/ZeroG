using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ZeroG.Data.Database;
using ZeroG.Data.Object;
using ZeroG.Data.Object.Index;

namespace ZeroG.Tests.Object
{
    public class ObjectTestHelper
    {
        public static readonly string NameSpace1 = "ZG_testNS1";
        public static readonly string ObjectName1 = "ZG_testObj1";
        public static readonly string ObjectName2 = "ZG_testObj2";
        public static readonly string ObjectName3 = "ZG_testObj3";

        public static Config GetConfig()
        {
            return Config.Default;
        }

        public static Config GetConfigWithCaching()
        {
            var defaultConfig = GetConfig();
            return new Config(defaultConfig.BaseDataPath,
                true,
                defaultConfig.ObjectIndexSchemaConnection,
                defaultConfig.ObjectIndexDataConnection,
                defaultConfig.MaxObjectDependencies,
                defaultConfig.ObjectStoreAutoClose,
                defaultConfig.ObjectStoreAutoCloseTimeout);
        }

        public static IObjectIndexProvider CreateObjectIndexProvider()
        {
            IObjectIndexProvider indexer = null;
            Type indexerType = null;

            var setting = ConfigurationManager.AppSettings[Config.ObjectIndexProviderConfigKey];
            if (!string.IsNullOrEmpty(setting))
            {
                var objectIndexProviderType = Type.GetType(setting, true);

                if (typeof(IObjectIndexProvider).IsAssignableFrom(objectIndexProviderType))
                {
                    indexer = (IObjectIndexProvider)Activator.CreateInstance(objectIndexProviderType);
                    indexerType = objectIndexProviderType;
                }
                else
                {
                    throw new InvalidOperationException("Unsupported IObjectIndexProvider type: " + objectIndexProviderType.FullName);
                }
            }
            return (IObjectIndexProvider)Activator.CreateInstance(indexerType);
        }

        public static void CleanTestObjects()
        {
            using (var svc = new ObjectService(GetConfig()))
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
