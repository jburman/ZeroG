using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ZeroG.Data;

namespace ZeroG.Tests.Configuration
{
    [TestClass]
    public class ObjectServiceConfigurationTests
    {
        [TestMethod]
        public void CreateKeyValueStoreProviderOptions()
        {
            var options = new KeyValueStoreProviderOptions();
            var val1 = DateTime.Now;
            options.Set("started", val1);
            options.Set("baseDataPath", ".\\path\\foo");

            Assert.AreEqual(val1, options.Get<DateTime>("started"));
            Assert.AreEqual(".\\path\\foo", options.Get<string>("baseDataPath"));
            Assert.IsNull(options.Get<string>("someKey"));
        }

        [TestMethod]
        public void CreateObjectServiceConfiguration()
        {
        }
    }
}
