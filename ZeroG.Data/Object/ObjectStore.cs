#region License, Terms and Conditions
// Copyright (c) 2012 Jeremy Burman
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
using System.Configuration;
using System.IO;
using System.Linq;
using ZeroG.Data.Object.Metadata;
using System.Text;

namespace ZeroG.Data.Object
{
    internal class ObjectStore : IDisposable
    {
        private Config _config;
        private ObjectMetadataStore _objectMetadata;
        private Dictionary<string, KeyValueStore> _stores;
        private Dictionary<string, KeyValueStore> _secondaryStores;
        private Dictionary<string, bool> _secondaryStoreExists;

        public ObjectStore(Config config, ObjectMetadataStore objectMetadata)
        {
            if (null == config)
            {
                throw new ArgumentNullException("config");
            }

            if (null == objectMetadata)
            {
                throw new ArgumentNullException("objectMetadata");
            }

            _config = config;
            _objectMetadata = objectMetadata;
            _stores = new Dictionary<string, KeyValueStore>(StringComparer.OrdinalIgnoreCase);
            _secondaryStores = new Dictionary<string, KeyValueStore>(StringComparer.OrdinalIgnoreCase);
            _secondaryStoreExists = new Dictionary<string, bool>();
        }

        #region Private helpers

        private string _CreateStorePath(string storeName, string objectFullName)
        {
            return Path.Combine(Path.Combine(_config.BaseDataPath, storeName), objectFullName);
        }

        private KeyValueStore _EnsureStore(Dictionary<string, KeyValueStore> storeCollection, string path, string objectFullName)
        {
            lock (storeCollection)
            {
                if (storeCollection.ContainsKey(objectFullName))
                {
                    return storeCollection[objectFullName];
                }
                else
                {
                    var metadata = _objectMetadata.GetMetadata(objectFullName);

                    if (null == metadata)
                    {
                        throw new ArgumentException("Object Store not found: " + objectFullName);
                    }
                    // construct name from stored metadata for consistency
                    objectFullName = ObjectNaming.CreateFullObjectName(metadata.NameSpace, metadata.ObjectName);

                    var store = new KeyValueStore(_CreateStorePath(path, objectFullName));
                    storeCollection[objectFullName] = store;
                    return store;
                }
            }
        }

        private KeyValueStore _EnsureStore(string objectFullName)
        {
            return _EnsureStore(_stores, "Store", objectFullName);
        }

        private KeyValueStore _EnsureSecondaryStore(string objectFullName)
        {
            _secondaryStoreExists[objectFullName] = true;
            return _EnsureStore(_secondaryStores, "SecondaryStore", objectFullName);
        }

        private bool _SecondaryStoreExists(string objectFullName)
        {
            if (!_secondaryStoreExists.ContainsKey(objectFullName))
            {
                _secondaryStoreExists[objectFullName] = Directory.Exists(_CreateStorePath("SecondaryStore", objectFullName));
            }

            return _secondaryStoreExists[objectFullName];
        }

        private byte[] _CreateValueForStorage(byte[] value, byte[] secondaryKey)
        {
            if (null == secondaryKey)
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

            if (null != value && 5 < value.Length)
            {
                // check for leading double pipes indicating that next 4 characters can be treated as the secondary key length
                if (value[0] == 124 && value[1] == 124)
                {
                    var offset = BitConverter.ToInt32(value, 2) + 6;
                    var valLen = value.Length;
                    if (offset < valLen)
                    {
                        var newVal = new byte[value.Length - offset];
                        Array.Copy(value, offset, newVal, 0, newVal.Length);
                        returnValue = newVal;
                    }
                }
            }

            return returnValue;
        }

        private byte[] _GetSecondaryKeyFromStoredValue(byte[] value)
        {
            byte[] returnValue = null;

            if (null != value && 5 < value.Length)
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

        #endregion

        public void Set(string nameSpace, PersistentObject obj)
        {
            var objectFullName = ObjectNaming.CreateFullObjectName(nameSpace, obj.Name);
            var store = _EnsureStore(objectFullName);
            var storeValue = _CreateValueForStorage(obj.Value, obj.SecondaryKey);

            store.Set(SerializerHelper.Serialize(obj.ID), storeValue);

            if (obj.HasSecondaryKey())
            {
                var secondaryStore = _EnsureSecondaryStore(objectFullName);
                secondaryStore.Set(obj.SecondaryKey, SerializerHelper.Serialize(obj.ID));
            }
        }

        private byte[] _Get(KeyValueStore store, byte[] key)
        {
            var val = store.Get(key);

            return _GetValueFromStoredValue(val);
        }

        public byte[] Get(string objectFullName, int id)
        {
            var store = _EnsureStore(objectFullName);
            var key = SerializerHelper.Serialize(id);
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

        public IEnumerable<ObjectStoreRecord> Iterate(string objectFullName)
        {
            var store = _EnsureStore(objectFullName);
            foreach (var entry in store.Enumerate())
            {
                var id = SerializerHelper.DeserializeInt32(entry.Key);
                var value = entry.Value;
                var secondaryKey = _GetSecondaryKeyFromStoredValue(value);
                yield return new ObjectStoreRecord(id, secondaryKey, value);
            }
        }

        public void Remove(string objectFullName, int id)
        {
            var store = _EnsureStore(objectFullName);
            var primaryKey = SerializerHelper.Serialize(id);
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
                    lock (_stores)
                    {
                        foreach (var s in _stores)
                        {
                            s.Value.Dispose();
                        }
                    }

                    lock (_secondaryStores)
                    {
                        foreach (var s in _secondaryStores)
                        {
                            s.Value.Dispose();
                        }
                    }
                }
                _disposed = true;
            }

        }
        #endregion
    }
}
