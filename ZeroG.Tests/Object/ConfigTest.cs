using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ZeroG.Data.Object;

namespace ZeroG.Tests.Object
{
    [TestClass]
    public class ConfigTest
    {
        [TestMethod]
        public void DefaultConfigTest()
        {
            var config = Config.Default;

            Assert.AreEqual(ConfigurationManager.AppSettings["ObjectServiceDataDir"], config.BaseDataPath);
        }

        [TestMethod]
        public void BasePathConfigTest()
        {
            var customBasePath = "C:\\Test\\BasePathConfigTest";

            var config = new Config(customBasePath);

            Assert.AreEqual(customBasePath, config.BaseDataPath);

        }
    }
}
