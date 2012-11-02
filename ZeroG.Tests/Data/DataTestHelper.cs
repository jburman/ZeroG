using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ZeroG.Data.Database;

namespace ZeroG.Tests.Data
{
    public class DataTestHelper
    {
        public static IDatabaseService GetDefaultSchemaService()
        {
            return DatabaseService.GetService(ObjectIndexProvider.DefaultSchemaConnection);
        }

        public static IDatabaseService GetDefaultDataService()
        {
            return DatabaseService.GetService(ObjectIndexProvider.DefaultDataAccessConnection);
        }
    }
}
