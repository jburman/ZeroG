using System;
using System.IO;
using ZeroG.Data.Object;
using ZeroG.Data.Object.Configure;

namespace ZeroG.Data.TestConsole
{
    class Program
    {
        public static void Main(string[] args)
        {
            string testBaseDir = "C:\\temp\\ZeroG\\TestConsole";

            try
            {

                if (!Directory.Exists(testBaseDir))
                {
                    Directory.CreateDirectory(testBaseDir);
                }

                //TODO - handle as a normal builder dependency
                SerializerHelper.SetSerializer(new ProtobufSerializer());

                var builderOptions = new ObjectServiceOptions();
                builderOptions.WithKeyValueStoreProvider<RazorDBKeyValueStoreProvider>(new RazorDBKeyValueStoreProviderOptions(testBaseDir,
                    KeyValueCacheConfiguration.None,
                    10 * 1024 * 1024));

                var objectServiceBuilder = new ObjectServiceBuilder(builderOptions);
                using (var svc = objectServiceBuilder.GetObjectService())
                {
                    var ns = svc.GetNameSpace("Test1");
                    if(ns == null)
                    {
                        Console.WriteLine("creating namespace");
                        svc.CreateNameSpace(new ObjectNameSpaceConfig("Test1", "testconsole", "testconsole", DateTime.Now));
                    }

                    var objMetadata = svc.GetObjectMetadata("Test1", "ObjStore1");
                    if(objMetadata == null)
                    {
                        Console.WriteLine("provisioning object store");
                        svc.ProvisionObjectStore(new ObjectMetadata("Test1", "ObjStore1"));
                    }

                    var objId = svc.Store("Test1", new PersistentObject()
                    {
                        Name = "ObjStore1",
                        Value = new byte[] { 0,0,1,1 }
                    });

                    var obj = svc.Get("Test1", "ObjStore1", objId.ID);
                    
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }
    }
}
