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
        public static readonly string MySQLSchemaUpdater = "MySQLSchemaUpdater";
        public static readonly string MySQLDataAccess = "MySQLDataAccess";
        public static readonly string SQLServerSchemaUpdater = "SQLServerSchemaUpdater";
        public static readonly string SQLServerDataAccess = "SQLServerDataAccess";

        public static IDatabaseService GetMySQLSchemaService()
        {
            return DatabaseService.GetService(MySQLSchemaUpdater);
        }

        public static IDatabaseService GetMySQLDataService()
        {
            return DatabaseService.GetService(MySQLDataAccess);
        }

        public static IDatabaseService GetSQLServerSchemaService()
        {
            return DatabaseService.GetService(SQLServerSchemaUpdater);
        }

        public static IDatabaseService GetSQLServerDataService()
        {
            return DatabaseService.GetService(SQLServerDataAccess);
        }
    }
}
