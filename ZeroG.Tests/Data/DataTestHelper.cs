using Microsoft.Extensions.Configuration;
using System.IO;
using ZeroG.Data.Database;
using ZeroG.Data.Database.Drivers;
using ZeroG.Data.Object;

namespace ZeroG.Tests.Data
{
    public class DataTestHelper
    {
        private static bool _configured = false;

        static DataTestHelper()
        {
            Configure();
        }

        public static void Configure()
        {
            if (!_configured)
            {
                _configured = true;

                var config = TestConfig.Config;

                DatabaseService.Configure(new DatabaseServiceConfig(
                    ObjectIndexProvider.DefaultSchemaConnection, typeof(SQLiteDatabaseService),
                    config.GetConnectionString(ObjectIndexProvider.DefaultSchemaConnection)));

                DatabaseService.Configure(new DatabaseServiceConfig(
                    ObjectIndexProvider.DefaultDataAccessConnection, typeof(SQLiteDatabaseService),
                    config.GetConnectionString(ObjectIndexProvider.DefaultDataAccessConnection)));
            }
        }

        public static IDatabaseService GetDefaultSchemaService() =>
            DatabaseService.GetService(ObjectIndexProvider.DefaultSchemaConnection);

        public static IDatabaseService GetDefaultDataService() =>
            DatabaseService.GetService(ObjectIndexProvider.DefaultDataAccessConnection);
    }
}
