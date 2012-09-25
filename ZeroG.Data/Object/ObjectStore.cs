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

namespace ZeroG.Data.Object
{
    internal class ObjectStore : IDisposable
    {
        private static readonly string _SecondaryKeyName = "SID";

        private Config _config;
        private ObjectMetadataStore _objectMetadata;
        private Dictionary<string, KeyValueStore> _stores;

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
        }

        private KeyValueStore _EnsureStore(string objectFullName)
        {
            lock (_stores)
            {
                if (_stores.ContainsKey(objectFullName))
                {
                    return _stores[objectFullName];
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

                    var store = new KeyValueStore(Path.Combine(Path.Combine(_config.BaseDataPath, "Store"), objectFullName));
                    _stores[objectFullName] = store;
                    return store;
                }
            }
        }

        public void Set(string nameSpace, PersistentObject obj)
        {
            var fullObjectName = ObjectNaming.CreateFullObjectName(nameSpace, obj.Name);
            var store = _EnsureStore(fullObjectName);

            if (obj.HasSecondaryKey())
            {
                var keyIndex = new Dictionary<string, byte[]>();
                keyIndex[_SecondaryKeyName] = obj.SecondaryKey;

                store.Set(SerializerHelper.Serialize(obj.ID), obj.Value, keyIndex);
            }
            else
            {
                store.Set(SerializerHelper.Serialize(obj.ID), obj.Value);
            }
        }

        public byte[] Get(string objectFullName, int id)
        {
            var store = _EnsureStore(objectFullName);

            return store.Get(SerializerHelper.Serialize(id));
        }

        public IEnumerable<byte[]> GetBySecondaryKey(string objectFullName, byte[] key)
        {
            var store = _EnsureStore(objectFullName);
            foreach (var entry in store.Find(_SecondaryKeyName, key))
            {
                yield return entry.Value;
            }
        }

        public void Remove(string objectFullName, int id)
        {
            var store = _EnsureStore(objectFullName);
            store.Delete(SerializerHelper.Serialize(id));
            store.CleanIndex(_SecondaryKeyName);
        }

        public void Truncate(string objectFullName)
        {
            var store = _EnsureStore(objectFullName);
            store.Truncate();
            store.CleanIndex(_SecondaryKeyName);
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
                }
                _disposed = true;
            }

        }
        #endregion
    }
}
