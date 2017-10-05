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

using RazorDB;
using System;
using System.Collections.Generic;
using System.IO;

namespace ZeroG.Data.Object
{
    public class RazorDBKeyValueStoreProvider : IKeyValueStoreProvider
    {
        private RazorDBKeyValueStoreProviderOptions _options;
        private string _basePath;
        private RazorCache _sharedCache;

        public RazorDBKeyValueStoreProvider(KeyValueStoreProviderOptions options)
        {
            _options = options as RazorDBKeyValueStoreProviderOptions ?? throw new ArgumentNullException(nameof(options));
            _basePath = _options?.BaseDataPath ?? throw new ArgumentNullException(nameof(_options.BaseDataPath));
        }

        public KeyValueCacheConfiguration CacheConfig => KeyValueCacheConfiguration.Shared;

        //int cacheSizeBytes = (int)config.ObjectStoreCacheSize * 1024 * 1024;

        // Index cache is set to a fifth the size of the data cache size.
        private RazorCache _CreateNewCache() =>
            new RazorCache((int)Math.Ceiling((double)_options.CacheSizeBytes / 5), _options.CacheSizeBytes);

        private RazorCache _GetCache()
        {
            // Create a single cache instance that is shared across all Object Stores
            if (_options.CacheConfig == KeyValueCacheConfiguration.Shared)
                return _sharedCache ?? (_sharedCache = _CreateNewCache());
            else if (_options.CacheConfig == KeyValueCacheConfiguration.Instance)
                return _CreateNewCache();
            else
                return null;
        }

        /// <summary>
        /// initData should be path to RazorDB file
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public IKeyValueStore Get(string name) {

            string fullPath = Path.Combine(_basePath, name);
            string dir = Path.GetDirectoryName(fullPath);
            if (!Directory.Exists(dir))
                Directory.CreateDirectory(dir);

            return new RazorDBKeyValueStore(new KeyValueStore(fullPath, _GetCache()), name);
        }

        public bool Exists(string name)
        {
            string fullPath = Path.Combine(_basePath, name);
            string dir = Path.GetDirectoryName(fullPath);
            return Directory.Exists(dir);
        }

        public void WriteCacheStats(IKeyValueStore store, Dictionary<string, string> report)
        {
            if(CacheConfig == KeyValueCacheConfiguration.Shared)
            {
                report?.Add("ObjectStoreDataSize_SharedCache", _sharedCache.DataCacheSize.ToString());
                report?.Add("ObjectStoreIndexSize_SharedCache", _sharedCache.IndexCacheSize.ToString());
            }
            else if(CacheConfig == KeyValueCacheConfiguration.Instance)
            {
                RazorDBKeyValueStore razorStore = store as RazorDBKeyValueStore;
                if(razorStore != null)
                {
                    report?.Add("ObjectStoreDataSize_" + razorStore.Name, razorStore.DataCacheSize.ToString());
                    report?.Add("ObjectStoreIndexSize_" + razorStore.Name, razorStore.IndexCacheSize.ToString());
                }
            }
        }
    }
}
