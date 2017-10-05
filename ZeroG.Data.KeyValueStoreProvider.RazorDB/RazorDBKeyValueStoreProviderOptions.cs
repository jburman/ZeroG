namespace ZeroG.Data.Object
{
    public class RazorDBKeyValueStoreProviderOptions : KeyValueStoreProviderOptions
    {
        private const string ConfigKey_BaseDataPath = "razordb.basedatapath";
        private const string ConfigKey_CacheConfig = "razordb.cacheconfig";
        private const string ConfigKey_CacheSize = "razordb.cachesizebytes";

        public RazorDBKeyValueStoreProviderOptions(string baseDataPath, KeyValueCacheConfiguration cacheConfig, int cacheSizeBytes)
        {
            Set(ConfigKey_BaseDataPath, baseDataPath ?? ".\\");
            Set(ConfigKey_CacheConfig, cacheConfig);
            Set(ConfigKey_CacheSize, cacheSizeBytes);
        }

        public string BaseDataPath => Get<string>(ConfigKey_BaseDataPath);
        public KeyValueCacheConfiguration CacheConfig => Get<KeyValueCacheConfiguration>(ConfigKey_CacheConfig);
        public int CacheSizeBytes => Get<int>(ConfigKey_CacheSize);
    }
}
