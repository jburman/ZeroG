#region License, Terms and Conditions
// Copyright (c) 2017 Jeremy Burman
//
// Permission is hereby granted, free of charge, to any person
// obtaining a copy of this software and associated documentation
// files (the "Software"), to deal in the Software without
// restriction, including without limitation the rights to use,
// copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following
// conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
// OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
// HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.
#endregion
using System;
using System.Collections.Generic;

namespace ZeroG.Data.Object.Configure
{
    public class ObjectServiceOptions : IObjectServiceKeyValueStoreOptions<ObjectServiceOptions>
    {
        private const string ConfigKey_Serializer = "serializer";
        private const string ConfigKey_KeyValueStoreProvider = "keyValueStoreProvider";
        private const string ConfigKey_KeyValueStoreProviderOptions = "keyValueStoreProviderOptions";
        private const string ConfigKey_ObjectIndexCache = "objectIndexCache";
        private const string ConfigKey_ObjectStoreOptions = "objectStoreOptions";
        private const string ConfigKey_ObjectVersionChangeHandler = "objectVersionChangeHandler";

        private Dictionary<string, object> _config;

        public ObjectServiceOptions()
        {
            _config = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
            {
                { ConfigKey_Serializer, new ProtobufSerializer() },
                { ConfigKey_KeyValueStoreProvider, null },
                { ConfigKey_ObjectVersionChangeHandler, null },
                { ConfigKey_ObjectIndexCache, null },
                { ConfigKey_ObjectStoreOptions, new ObjectStoreOptions() }
            };
        }

        private T _Get<T>(string key)
        {
            if (_config.TryGetValue(key, out object valLookup))
                return (T)valLookup;
            return default(T);
        }

        public ObjectServiceOptions WithSerializer(ISerializer serializer)
        {
            _config[ConfigKey_Serializer] = serializer;
            return this;
        }

        public ISerializer GetSerializer() =>
            (ISerializer)_config[ConfigKey_Serializer];

        public ObjectServiceOptions WithKeyValueStoreProvider<T>(KeyValueStoreProviderOptions options) where T : IKeyValueStoreProvider
        {
            _config[ConfigKey_KeyValueStoreProvider] = typeof(T);
            if (options != null)
                _config[ConfigKey_KeyValueStoreProviderOptions] = options;
            return this;
        }

        public KeyValueStoreProviderOptions GetKeyValueStoreProviderOptions() => _Get<KeyValueStoreProviderOptions>(ConfigKey_KeyValueStoreProviderOptions);

        public IKeyValueStoreProvider GetKeyValueStoreProviderWithOptions(Func<Type, KeyValueStoreProviderOptions, IKeyValueStoreProvider> creator)
        {
            Type providerType = _config[ConfigKey_KeyValueStoreProvider] as Type;
            KeyValueStoreProviderOptions options = null;
            if (_config.TryGetValue(ConfigKey_KeyValueStoreProviderOptions, out object lookupOptionsBuilder))
                options = lookupOptionsBuilder as KeyValueStoreProviderOptions;

            return creator?.Invoke(providerType, options);
        }

        public ObjectServiceOptions WithObjectVersionChangeHandler(ObjectVersionChangedHandler handler)
        {
            _config[ConfigKey_ObjectVersionChangeHandler] = handler;
            return this;
        }

        public ObjectVersionChangedHandler GetObjectVersionChangeHandler() => _Get<ObjectVersionChangedHandler>(ConfigKey_ObjectVersionChangeHandler);

        public ObjectServiceOptions WithObjectIndexCache(ObjectIndexCacheOptions options)
        {
            _config[ConfigKey_ObjectIndexCache] = options;
            return this;
        }

        public ObjectIndexCacheOptions GetObjectObjectIndexCacheOptions() => _Get<ObjectIndexCacheOptions>(ConfigKey_ObjectIndexCache);

        public ObjectServiceOptions WithObjectStoreOptions(ObjectStoreOptions options)
        {
            _config[ConfigKey_ObjectStoreOptions] = options;
            return this;
        }

        public ObjectStoreOptions GetObjectStoreOptions() => _Get<ObjectStoreOptions>(ConfigKey_ObjectStoreOptions);
    }
}
