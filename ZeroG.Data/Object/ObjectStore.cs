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
        private static readonly string _UniqueIDKeyName = "UID";

        private ObjectMetadataStore _objectMetadata;
        private Dictionary<string, KeyValueStore> _stores;

        public ObjectStore(ObjectMetadataStore objectMetadata)
        {
            if (null == objectMetadata)
            {
                throw new ArgumentNullException("objectMetadata");
            }

            _objectMetadata = objectMetadata;
            _stores = new Dictionary<string, KeyValueStore>(StringComparer.OrdinalIgnoreCase);
        }

        private KeyValueStore _EnsureStore(string fullObjectName)
        {
            lock (_stores)
            {
                if (_stores.ContainsKey(fullObjectName))
                {
                    return _stores[fullObjectName];
                }
                else
                {
                    var metadata = _objectMetadata.GetMetadata(fullObjectName);

                    if (null == metadata)
                    {
                        throw new ArgumentException("Object Store not found: " + fullObjectName);
                    }
                    // construct name from stored metadata for consistency
                    fullObjectName = ObjectNaming.CreateFullObjectName(metadata.NameSpace, metadata.ObjectName);

                    var store = new KeyValueStore(Path.Combine(Path.Combine(Config.BaseDataPath, "Store"), fullObjectName));
                    _stores[fullObjectName] = store;
                    return store;
                }
            }
        }

        public void Set(string nameSpace, PersistentObject obj)
        {
            var fullObjectName = ObjectNaming.CreateFullObjectName(nameSpace, obj.Name);
            var store = _EnsureStore(fullObjectName);


            var keyIndex = new Dictionary<string, byte[]>();
            keyIndex[_UniqueIDKeyName] = obj.UniqueID;

            store.Set(SerializerHelper.Serialize(obj.ID), obj.Value, keyIndex);
        }

        public byte[] Get(string nameSpace, string objName, int id)
        {
            var fullObjectName = ObjectNaming.CreateFullObjectName(nameSpace, objName);
            var store = _EnsureStore(fullObjectName);

            return store.Get(SerializerHelper.Serialize(id));
        }

        public byte[] GetByUniqueID(string nameSpace, string objName, byte[] uniqueId)
        {
            var fullObjectName = ObjectNaming.CreateFullObjectName(nameSpace, objName);
            var store = _EnsureStore(fullObjectName);
            return store.Find(_UniqueIDKeyName, uniqueId).FirstOrDefault().Value;
        }

        public void Remove(string nameSpace, string objName, int id)
        {
            var fullObjectName = ObjectNaming.CreateFullObjectName(nameSpace, objName);
            var store = _EnsureStore(fullObjectName);
            store.Delete(SerializerHelper.Serialize(id));
            store.CleanIndex(_UniqueIDKeyName);
        }

        public void Truncate(string nameSpace, string objName)
        {
            var fullObjectName = ObjectNaming.CreateFullObjectName(nameSpace, objName);
            var store = _EnsureStore(fullObjectName);
            store.Truncate();
            store.CleanIndex(_UniqueIDKeyName);
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
