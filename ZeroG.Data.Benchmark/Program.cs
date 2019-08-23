using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Microsoft.Extensions.Configuration;
using System;
using System.IO;
using System.Text;
using ZeroG.Data.Database;
using ZeroG.Data.Database.Drivers;
using ZeroG.Data.Object;

namespace ZeroG.Data.Benchmark
{
    class Program
    {
        

        static void Main(string[] args)
        {
            

            var summary = BenchmarkRunner.Run<MemoryEfficiencyTests>();
        }
    }

    [MemoryDiagnoser]
    public class MemoryEfficiencyTests : IDisposable
    {
        internal static IConfiguration AppConfig;
        private ObjectService _svc;
        private string _dataDir;
        private byte[] _value;
        const string ObjectNameSpace = "benchmark";

        public MemoryEfficiencyTests()
        {
            var builder = new ConfigurationBuilder()
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.json");

            AppConfig = builder.Build();

            DatabaseService.Configure(new DatabaseServiceConfig(
                ObjectIndexProvider.DefaultSchemaConnection, typeof(SQLiteDatabaseService),
                AppConfig.GetConnectionString(ObjectIndexProvider.DefaultSchemaConnection)));

            DatabaseService.Configure(new DatabaseServiceConfig(
                ObjectIndexProvider.DefaultDataAccessConnection, typeof(SQLiteDatabaseService),
                AppConfig.GetConnectionString(ObjectIndexProvider.DefaultDataAccessConnection)));

            _dataDir = Path.Combine(Directory.GetCurrentDirectory(), "Data");
            var objectConfig = new Config(_dataDir, true);
            _svc = new ObjectService(objectConfig, GetObjectIndexProvider(objectConfig));
            _value = Encoding.UTF8.GetBytes("Test RazorDB Value");

            if(!_svc.NameSpaceExists(ObjectNameSpace))
                _svc.CreateNameSpace(new ObjectNameSpaceConfig(ObjectNameSpace, "ZeroG", string.Empty, DateTime.Now));
            if (!_svc.ObjectNameExists(ObjectNameSpace, "onek"))
                _svc.ProvisionObjectStore(new ObjectMetadata(ObjectNameSpace, "onek"));
        }

        public static IObjectIndexProvider GetObjectIndexProvider(Config config)
        {
            var appConfig = AppConfig;
            var setting = appConfig["ObjectIndexProvider"];
            Type type = Type.GetType(setting, true);
            return (IObjectIndexProvider)Activator.CreateInstance(type, config);
        }

        public void Clean()
        {
            if (Directory.Exists(_dataDir))
                Directory.Delete(_dataDir, true);
        }

        public void Dispose()
        {
            _svc.Dispose();
            Clean();
        }

        [Benchmark]
        public void OneHundredKSmallKeyWriteRead()
        {
            for (int i = 0; i < 100_000; i++)
            {
                _svc.Store(ObjectNameSpace, new PersistentObject()
                {
                    ID = i + 1,
                    Value = _value,
                    Name = "onek"
                });
            }

            for (int i = 0; i < 100_000; i++)
            {
                byte[] val = _svc.Get(ObjectNameSpace, "onek", i + 1);
                if (val.Length != _value.Length)
                    throw new Exception("Invalid test result. Expected value lengths to match");
            }
        }
    }
}
