using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ZeroG.Data.Database;
using ZeroG.Data.Database.Drivers;

namespace ZeroG.Tests.Data.Drivers
{
    [TestClass]
    public class SQLiteTests
    {
        [TestMethod]
        public void ConstructWithConnStrSubstitutions()
        {
            var appDir = AppDomain.CurrentDomain.BaseDirectory;

            var db = new SQLiteDatabaseService();
            db.Configure(new DatabaseServiceConfiguration("SQLite",
                typeof(SQLiteDatabaseService).FullName,
                "Data Source={AppDir}\\DataDir\\DB.db3;Version=3;Pooling=True;Max Pool Size=10;Synchronous=off;FailIfMissing=False;Journal Mode=On;", 
                null));

            var connStr = db.CurrentConnectionString;
            Assert.AreEqual(-1, connStr.IndexOf("{AppDir}"));
            Assert.AreEqual("Data Source=" + (Path.Combine(appDir, "DataDir\\DB.db3")) + ";Version=3;Pooling=True;Max Pool Size=10;Synchronous=off;FailIfMissing=False;Journal Mode=On;", connStr);

            db.Configure(new DatabaseServiceConfiguration("SQLite",
                typeof(SQLiteDatabaseService).FullName,
                "Data Source={AppDir}\\..\\App_Data\\DB.db3;Version=3;Pooling=True;Max Pool Size=10;Synchronous=off;FailIfMissing=False;Journal Mode=On;",
                null));
            connStr = db.CurrentConnectionString;
            Assert.AreEqual(-1, connStr.IndexOf("{AppDir}"));
            Assert.AreEqual("Data Source=" + (Path.Combine(appDir, "..\\App_Data\\DB.db3")) + ";Version=3;Pooling=True;Max Pool Size=10;Synchronous=off;FailIfMissing=False;Journal Mode=On;", connStr);
        }
    }
}
