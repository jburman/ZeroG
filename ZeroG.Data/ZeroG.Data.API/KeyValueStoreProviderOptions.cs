using System;
using System.Collections.Generic;

namespace ZeroG.Data
{
    public class KeyValueStoreProviderOptions
    {
        private Dictionary<string, object> _config;

        public KeyValueStoreProviderOptions()
        {
            _config = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
        }

        public virtual void Set<T>(string name, T value) =>
            _config[name] = value;

        public virtual T Get<T>(string name) =>
            (_config.TryGetValue(name, out object getVal)) ? (T)getVal : default(T);
    }
}
