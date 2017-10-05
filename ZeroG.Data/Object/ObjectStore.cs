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
using System.IO;
using System.Linq;
using ZeroG.Data.Object.Metadata;
using System.Threading;
using System.Diagnostics;

namespace ZeroG.Data.Object
{
    internal class ObjectStore : IObjectStore
    {
        private class ExpiringObjectStore
        {
            public IKeyValueStore Store;
            public DateTime WhenExpired;
        }

        private ISerializer _serializer;
        private ObjectMetadataStore _objectMetadata;
        private IKeyValueStoreProvider _kvProvider;
        
        private Dictionary<string, ExpiringObjectStore> _stores;
        private Dictionary<string, ExpiringObjectStore> _secondaryStores;
        private Dictionary<string, bool> _secondaryStoreExists;
        private Timer _cleanupTimer;
        private bool _autoClose;
        private int _autoCloseTimeoutSeconds;

        public ObjectStore(ISerializer serializer, ObjectMetadataStore objectMetadata, IKeyValueStoreProvider kvProvider, bool autoClose, int autoCloseTimeoutSeconds)
        {
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            _objectMetadata = objectMetadata ?? throw new ArgumentNullException(nameof(objectMetadata));
            _kvProvider = kvProvider ?? throw new ArgumentNullException(nameof(kvProvider));
            
            _stores = new Dictionary<string, ExpiringObjectStore>(StringComparer.OrdinalIgnoreCase);
            _secondaryStores = new Dictionary<string, ExpiringObjectStore>(StringComparer.OrdinalIgnoreCase);
            _secondaryStoreExists = new Dictionary<string, bool>();

            // Start the Object Store cleanup timer if cleanup is enabled
            _autoClose = autoClose;
            _autoCloseTimeoutSeconds = autoCloseTimeoutSeconds;
            if (_autoClose && _autoCloseTimeoutSeconds > 0)
            {
                _StartObjectStoreCleanup();
            }
        }

        #region Private helpers

        private string _CreateStorePath(string storeName, string objectFullName) => Path.Combine(storeName, objectFullName);

        private IKeyValueStore _EnsureStore(Dictionary<string, ExpiringObjectStore> storeCollection, string path, string objectFullName)
        {
            lock (storeCollection)
            {
                if (storeCollection.ContainsKey(objectFullName))
                {
                    ExpiringObjectStore store = storeCollection[objectFullName];
                    // reset expiration time
                    store.WhenExpired = DateTime.Now.AddSeconds(_autoCloseTimeoutSeconds);
                    return store.Store;
                }
                else
                {
                    var metadata = _objectMetadata.GetMetadata(objectFullName);

                    if (metadata == null)
                    {
                        throw new ArgumentException("Object Store not found: " + objectFullName);
                    }
                    // construct name from stored metadata for consistency
                    objectFullName = ObjectNaming.CreateFullObjectName(metadata.NameSpace, metadata.ObjectName);

                    var store = _kvProvider.Get(_CreateStorePath(path, objectFullName));
                    storeCollection[objectFullName] = new ExpiringObjectStore()
                    {
                        Store = store,
                        WhenExpired = DateTime.Now.AddSeconds(_autoCloseTimeoutSeconds)
                    };
                    return store;
                }
            }
        }

        private IKeyValueStore _EnsureStore(string objectFullName) => _EnsureStore(_stores, "Store", objectFullName);

        private IKeyValueStore _EnsureSecondaryStore(string objectFullName)
        {
            _secondaryStoreExists[objectFullName] = true;
            return _EnsureStore(_secondaryStores, "SecondaryStore", objectFullName);
        }

        private bool _SecondaryStoreExists(string objectFullName)
        {
            if (!_secondaryStoreExists.ContainsKey(objectFullName))
                _secondaryStoreExists[objectFullName] = _kvProvider.Exists(_CreateStorePath("SecondaryStore", objectFullName)); // Directory.Exists(_CreateStorePath("SecondaryStore", objectFullName));

            return _secondaryStoreExists[objectFullName];
        }

        private byte[] _CreateValueForStorage(byte[] value, byte[] secondaryKey)
        {
            // null values are always converted to zero length byte arrays
            if (value == null)
            {
                value = new byte[0];
            }

            if (secondaryKey == null)
            {
                return value;
            }
            else
            {
                var valueLen = value.Length;
                var secondaryKeyLen = secondaryKey.Length;

                var newValue = new byte[valueLen + secondaryKeyLen + 6];

                newValue[0] = 124; // put two leading pipes as an indicator
                newValue[1] = 124;

                var buf = BitConverter.GetBytes(secondaryKeyLen);

                newValue[2] = buf[0];
                newValue[3] = buf[1];
                newValue[4] = buf[2];
                newValue[5] = buf[3];

                Array.Copy(secondaryKey, 0, newValue, 6, secondaryKeyLen);

                Array.Copy(value, 0, newValue, 6 + secondaryKeyLen, value.Length);

                return newValue;
            }
        }

        private byte[] _GetValueFromStoredValue(byte[] value)
        {
            byte[] returnValue = value;

            if (null != value && value.Length > 5)
            {
                // check for leading double pipes indicating that next 4 characters can be treated as the secondary key length
                if (value[0] == 124 && value[1] == 124)
                {
                    var offset = BitConverter.ToInt32(value, 2) + 6;
#if DEBUG
                    if (offset < 0)
                    {
                        Debugger.Break();
                    }
                    
#endif
                    var valLen = value.Length;
                    if (offset < valLen)
                    {
                        var newVal = new byte[value.Length - offset];
                        Array.Copy(value, offset, newVal, 0, newVal.Length);
                        returnValue = newVal;
                    }
                    else
                    {
                        returnValue = new byte[0];
                    }
                }
            }

            return returnValue;
        }

        private byte[] _GetSecondaryKeyFromStoredValue(byte[] value)
        {
            byte[] returnValue = null;

            if (null != value && value.Length > 5)
            {
                // check for leading double pipes indicating that next 4 characters can be treated as the secondary key length
                if (value[0] == 124 && value[1] == 124)
                {
                    var keyLen = BitConverter.ToInt32(value, 2);
                    returnValue = new byte[keyLen];
                    Array.Copy(value, 6, returnValue, 0, keyLen);
                }
            }

            return returnValue;
        }

        //TODO provide background work handler
        private void _StartObjectStoreCleanup()
        {
            _cleanupTimer = new Timer(new TimerCallback((o) => 
            {
                var now = DateTime.Now;

#if DEBUG
                Debug.WriteLine("#### Running Object Store Cleanup");
#endif

                lock (_stores)
                {
                    string[] keys = _stores.Keys.ToArray();
                    foreach (string key in keys)
                    {
                        if (_stores[key].WhenExpired < now)
                        {
                            try
                            {
                                _stores[key].Store.Dispose();
                                _stores.Remove(key);
                            }
                            catch(Exception ex)
                            {
                                Trace.TraceError("Error closing Object Store: {0}", ex);
                                _stores.Remove(key);
                            }
                        }
                    }
                }

                lock (_secondaryStores)
                {
                    string[] keys = _secondaryStores.Keys.ToArray();
                    foreach (string key in keys)
                    {
                        if (_secondaryStores[key].WhenExpired < now)
                        {
                            try
                            {
                                _secondaryStores[key].Store.Dispose();
                                _secondaryStores.Remove(key);
                            }
                            catch (Exception ex)
                            {
                                Trace.TraceError("Error closing Secondary Object Store: {0}", ex);
                                _secondaryStores.Remove(key);
                            }
                        }
                    }
                }
            }), null, _autoCloseTimeoutSeconds * 1000, _autoCloseTimeoutSeconds * 1000);
        }

        #endregion

        public void Set(string nameSpace, PersistentObject obj)
        {
            var objectFullName = ObjectNaming.CreateFullObjectName(nameSpace, obj.Name);
            var store = _EnsureStore(objectFullName);
            var storeValue = _CreateValueForStorage(obj.Value, obj.SecondaryKey);

            store.Set(_serializer.Serialize(obj.ID), storeValue);

            if (obj.HasSecondaryKey())
            {
                var secondaryStore = _EnsureSecondaryStore(objectFullName);
                secondaryStore.Set(obj.SecondaryKey, _serializer.Serialize(obj.ID));
            }
        }

        private byte[] _Get(IKeyValueStore store, byte[] key)
        {
            var val = store.Get(key);

            return _GetValueFromStoredValue(val);
        }

        public byte[] Get(string objectFullName, int id)
        {
            var store = _EnsureStore(objectFullName);
            var key = _serializer.Serialize(id);
            return _Get(store, key);
        }

        public byte[] GetBySecondaryKey(string objectFullName, byte[] key)
        {
            var store = _EnsureSecondaryStore(objectFullName);
            byte[] returnValue = null;

            var primaryKey = store.Get(key);
            if (null != primaryKey)
            {
                returnValue = _Get(_EnsureStore(objectFullName), primaryKey);
            }

            return returnValue;
        }

        public int Count(string objectFullName)
        {
            var store = _EnsureStore(objectFullName);
            return store.Enumerate().Count();
        }

        /// <summary>
        /// Iterates over each object stored in an ObjectStore and returns its 
        /// ID, SecondaryKey, and Value
        /// </summary>
        /// <param name="objectFullName"></param>
        /// <returns></returns>
        public IEnumerable<ObjectStoreRecord> Iterate(string objectFullName)
        {
            var store = _EnsureStore(objectFullName);
            foreach (var entry in store.Enumerate())
            {
                var id = _serializer.DeserializeInt32(entry.Key);
                var value = _GetValueFromStoredValue(entry.Value);
                var secondaryKey = _GetSecondaryKeyFromStoredValue(entry.Value);
                yield return new ObjectStoreRecord(id, secondaryKey, value);
            }
        }

        public int? LookupPrimaryKey(string objectFullName, byte[] secondaryKey)
        {
            int? returnValue = null;

            if (_SecondaryStoreExists(objectFullName))
            {
                IKeyValueStore secondaryStore = _EnsureSecondaryStore(objectFullName);
                byte[] lookup = secondaryStore.Get(secondaryKey);
                if (null != lookup)
                {
                    returnValue = _serializer.DeserializeInt32(lookup);
                }
            }

            return returnValue;
        }

        public void Remove(string objectFullName, int id)
        {
            var store = _EnsureStore(objectFullName);
            var primaryKey = _serializer.Serialize(id);
            if (_SecondaryStoreExists(objectFullName))
            {
                var val = store.Get(primaryKey);
                if (null != val)
                {
                    var secondaryKey = _GetSecondaryKeyFromStoredValue(val);
                    if (null != secondaryKey)
                    {
                        var secondaryStore = _EnsureSecondaryStore(objectFullName);
                        secondaryStore.Delete(secondaryKey);
                    }
                }
            }
            store.Delete(primaryKey);
        }

        public void Truncate(string objectFullName)
        {
            var store = _EnsureStore(objectFullName);
            store.Truncate();

            if (_SecondaryStoreExists(objectFullName))
            {
                store = _EnsureSecondaryStore(objectFullName);
                store.Truncate();
            }
        }

        public Dictionary<string, string> Report()
        {
            var report = new Dictionary<string, string>();
            //TODO fix Report()
            /*
            TODO
            if(_kvProvider.CacheConfig == KeyValueCacheConfiguration.Shared)
            {
                lock (_stores)
                    _stores.Values.FirstOrDefault().Store.WriteCacheStats(report, "Shared_");
            }
            else if(_kvProvider.CacheConfig == KeyValueCacheConfiguration.Instance)
            {
                lock (_stores)
                {
                    foreach (var store in _stores.Values)
                        store.Store.WriteCacheStats(report, string.Empty);
                }
            }
            */

            //if (null != _cache)
            //{
            //    report.Add("ObjectStoreDataSize_SharedCache", _cache.DataCacheSize.ToString());
            //    report.Add("ObjectStoreIndexSize_SharedCache", _cache.IndexCacheSize.ToString());
            //}
            //else
            //{
            //    lock (_stores)
            //    {
            //        foreach (var store in _stores)
            //        {
            //            report.Add("ObjectStoreDataSize_" + store.Key, store.Value.Store.DataCacheSize.ToString());
            //            report.Add("ObjectStoreIndexSize_" + store.Key, store.Value.Store.IndexCacheSize.ToString());
            //        }
            //    }

            //    lock (_secondaryStores)
            //    {
            //        foreach (var store in _secondaryStores)
            //        {
            //            report.Add("ObjectSecondaryStoreDataSize_" + store.Key, store.Value.Store.DataCacheSize.ToString());
            //            report.Add("ObjectSecondaryStoreIndexSize_" + store.Key, store.Value.Store.IndexCacheSize.ToString());
            //        }
            //    }
            //}

            return report;
        }

        #region Dispose implementation
        private bool _disposed;

        public void Dispose()
        {
            _Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void _Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    if (_cleanupTimer != null)
                    {
                        _cleanupTimer.Dispose();
                    }

                    lock (_stores)
                    {
                        foreach (var s in _stores)
                        {
                            s.Value.Store.Dispose();
                        }
                    }

                    lock (_secondaryStores)
                    {
                        foreach (var s in _secondaryStores)
                        {
                            s.Value.Store.Dispose();
                        }
                    }
                }
                _disposed = true;
            }

        }
        #endregion
    }
}
