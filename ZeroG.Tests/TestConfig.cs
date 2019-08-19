using Microsoft.Extensions.Configuration;
using System.IO;

namespace ZeroG.Tests
{
    public class TestConfig
    {
        private static readonly IConfiguration _config;

        static TestConfig()
        {
            var builder = new ConfigurationBuilder()
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.json");
            _config = builder.Build();
        }

        public static IConfiguration Config => _config;
    }
}
