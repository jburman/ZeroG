namespace ZeroG.Data.Object.Configure
{
    public class ObjectStoreOptions
    {
        public const int Default_MaxObjectDependencies      = 5;
        public const int Default_AutoCloseTimeoutSeconds    = 300; // seconds
        public const int Default_CacheSizeMB                = 100; // MB

        public ObjectStoreOptions(int maxObjectDependencies = Default_MaxObjectDependencies,
            bool autoClose = true,
            int autoCloseTimeout = Default_AutoCloseTimeoutSeconds,
            int cacheSize = Default_CacheSizeMB)
        {
            MaxObjectDependencies = maxObjectDependencies;
            AutoClose = autoClose;
            AutoCloseTimeout = autoCloseTimeout;
            CacheSize = cacheSize;
        }

        public int MaxObjectDependencies { get; set; }
        public bool AutoClose { get; private set; }
        public int AutoCloseTimeout { get; private set; } // time in seconds
        public int CacheSize { get; private set; } // SizeMB
    }
}
