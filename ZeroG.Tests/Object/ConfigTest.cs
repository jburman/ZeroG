using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Configuration;
using ZeroG.Data.Object;

namespace ZeroG.Tests.Object
{
    [TestClass]
    public class ConfigTest
    {
        [TestMethod]
        public void DefaultConfig()
        {
            var config = Config.Default;
            Assert.AreEqual(ConfigurationManager.AppSettings["ObjectServiceDataDir"], config.BaseDataPath);
        }

        [TestMethod]
        public void BasePathConfig()
        {
            var customBasePath = "C:\\Test\\BasePathConfigTest";
            var config = new Config(customBasePath);
            Assert.AreEqual(customBasePath, config.BaseDataPath);
        }

        [TestMethod]
        public void NonAppConfigFileConfig()
        {
            var config = new Config("TestPath", true, 20, 1100, "SchemaConn", "DataConn", 100, true, 10000, 20);
            Assert.AreEqual("TestPath", config.BaseDataPath);
            Assert.IsTrue(config.IndexCacheEnabled);
            Assert.AreEqual(20, config.IndexCacheMaxQueries);
            Assert.AreEqual(1100, config.IndexCacheMaxValues);
            Assert.AreEqual("SchemaConn", config.ObjectIndexSchemaConnection);
            Assert.AreEqual("DataConn", config.ObjectIndexDataConnection);
            Assert.AreEqual(100, config.MaxObjectDependencies);
            Assert.AreEqual(true, config.ObjectStoreAutoClose);
            Assert.AreEqual(10000, config.ObjectStoreAutoCloseTimeout);
            Assert.AreEqual(20, config.ObjectStoreCacheSize);
        }

        [TestMethod]
        public void BasePathWithPlaceHolder()
        {
            var customBasePath = "{appdir}\\BasePathConfigTest";
            var expectedBasePath = AppDomain.CurrentDomain.BaseDirectory + "\\BasePathConfigTest";

            var config = new Config(customBasePath);
            Assert.AreEqual(expectedBasePath, config.BaseDataPath);

            config = new Config(customBasePath, true, 1000, 10000, string.Empty, string.Empty, 10, false, 0, 10);
            Assert.AreEqual(expectedBasePath, config.BaseDataPath);
        }
    }
}
