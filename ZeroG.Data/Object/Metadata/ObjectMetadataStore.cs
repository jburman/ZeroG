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

using ProtoBuf;
using RazorDB;
using System;
using System.Linq;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace ZeroG.Data.Object.Metadata
{
    internal class ObjectMetadataStore : IDisposable
    {
        Config _config;

        private KeyValueStore _store;
        private KeyValueStore _nsStore;
        private Dictionary<string, ObjectMetadata> _mdCache;
        private Dictionary<string, ObjectNameSpaceConfig> _nsCache;

        public event ObjectMetadatastoreUpdatedEvent ObjectNameSpaceAdded;
        public event ObjectMetadatastoreUpdatedEvent ObjectNameSpaceRemoved;
        public event ObjectMetadatastoreUpdatedEvent ObjectMetadataAdded;
        public event ObjectMetadatastoreUpdatedEvent ObjectMetadataRemoved;

        public ObjectMetadataStore(Config config)
        {
            if (null == config)
            {
                throw new ArgumentNullException("config");
            }

            _config = config;
            _store = new KeyValueStore(Path.Combine(config.BaseDataPath, "ObjectMetadataStore"));
            _nsStore = new KeyValueStore(Path.Combine(config.BaseDataPath, "ObjectNameSpaceStore"));
            _nsCache = new Dictionary<string, ObjectNameSpaceConfig>(StringComparer.OrdinalIgnoreCase);
            _mdCache = new Dictionary<string, ObjectMetadata>(StringComparer.OrdinalIgnoreCase);

            _InitCache();
        }

        private void _InitCache()
        {
            lock (_nsCache)
            {
                foreach (var e in _nsStore.Enumerate())
                {
                    _nsCache[SerializerHelper.DeserializeString(e.Key)] = SerializerHelper.Deserialize<ObjectNameSpaceConfig>(e.Value);
                }
            }

            lock (_mdCache)
            {
                foreach (var e in _store.Enumerate())
                {
                    _mdCache[SerializerHelper.DeserializeString(e.Key)] = SerializerHelper.Deserialize<ObjectMetadata>(e.Value);
                }
            }
        }

        public void CreateNameSpace(ObjectNameSpaceConfig config)
        {
            var nameSpace = config.Name;

            // check reserved namespace
            if (ObjectNaming.DefaultNameSpace.Equals(nameSpace, StringComparison.InvariantCultureIgnoreCase))
            {
                throw new ArgumentException("The specified name space is reserved.");
            }
            else
            {
                // validate characters and form of the namespace
                if (ObjectNameValidator.IsValidNameSpace(nameSpace))
                {
                    var nsKey = SerializerHelper.Serialize(nameSpace);

                    // only allow namespaces to be created once.
                    // we need to iterate the name spaces in order to do a case insensitive comparison
                    foreach (var storedNameSpace in EnumerateNameSpaces())
                    {
                        if (storedNameSpace.Equals(nameSpace, StringComparison.InvariantCultureIgnoreCase))
                        {
                            throw new ArgumentException("Namespace already exists.");
                        }
                    }
                    // namespace is not already used so go ahead and store it
                    _nsStore.Set(nsKey, SerializerHelper.Serialize<ObjectNameSpaceConfig>(config));
                    lock (_nsCache)
                    {
                        _nsCache[nameSpace] = config;
                    }
                    if (null != ObjectNameSpaceAdded)
                    {
                        ObjectNameSpaceAdded(nameSpace);
                    }
                }
                else
                {
                    throw new ArgumentException("The name space is invalid. Please ensure it contains only alphanumeric characters and periods or underscores.");
                }
            }
        }

        /// <summary>
        /// Updates an existing name space record. Does not create a new record and will throw an exception if the name space does not exist.
        /// Use ObjectMetadataStore Exists method to check if a name space exists already.
        /// </summary>
        /// <param name="config">The name space configuration.</param>
        public void UpdateNameSpace(ObjectNameSpaceConfig config)
        {
            var nameSpace = config.Name;

            // check reserved namespace
            if (ObjectNaming.DefaultNameSpace.Equals(nameSpace, StringComparison.InvariantCultureIgnoreCase))
            {
                throw new ArgumentException("The specified name space is reserved.");
            }
            else
            {
                // validate characters and form of the namespace
                if (ObjectNameValidator.IsValidNameSpace(nameSpace))
                {
                    var nsKey = SerializerHelper.Serialize(nameSpace);

                    // verify that the namespace already exists
                    if (NameSpaceExists(nameSpace))
                    {
                        // overwrite the existing namespace
                        _nsStore.Set(nsKey, SerializerHelper.Serialize<ObjectNameSpaceConfig>(config));
                        lock (_nsCache)
                        {
                            _nsCache[nameSpace] = config;
                        }
                    }
                    else
                    {
                        throw new ArgumentException("The specified name space does not exist: " + nameSpace);
                    }
                }
                else
                {
                    throw new ArgumentException("The name space is invalid. Please ensure it contains only alphanumeric characters and periods or underscores.");
                }
            }
        }

        public ObjectNameSpaceConfig GetNameSpace(string nameSpace)
        {
            // check reserved namespace
            if (ObjectNaming.DefaultNameSpace.Equals(nameSpace, StringComparison.InvariantCultureIgnoreCase))
            {
                throw new ArgumentException("The specified name space is reserved.");
            }
            else
            {
                lock (_nsCache)
                {
                    if (_nsCache.ContainsKey(nameSpace))
                    {
                        return _nsCache[nameSpace];
                    }
                }

                var nsKey = SerializerHelper.Serialize(nameSpace);
                var data = _nsStore.Get(nsKey);
                if (null == data)
                {
                    return null;
                }
                else
                {
                    return SerializerHelper.Deserialize<ObjectNameSpaceConfig>(data);
                }
            }
        }

        public bool NameSpaceExists(string nameSpace)
        {
            lock (_nsCache)
            {
                return _nsCache.ContainsKey(nameSpace);
            }
        }

        public void RemoveNameSpace(string nameSpace)
        {
            // check reserved namespace
            if (ObjectNaming.DefaultNameSpace.Equals(nameSpace, StringComparison.InvariantCultureIgnoreCase))
            {
                throw new ArgumentException("The specified name space is reserved.");
            }
            else
            {
                var nsKey = SerializerHelper.Serialize(nameSpace);

                if (null != _nsStore.Get(nsKey))
                {
                    if (0 == EnumerateObjectNames(nameSpace).Count())
                    {
                        _nsStore.Delete(nsKey);
                        lock (_nsCache)
                        {
                            if (_nsCache.ContainsKey(nameSpace))
                            {
                                _nsCache.Remove(nameSpace);
                            }
                        }
                        if (null != ObjectNameSpaceRemoved)
                        {
                            ObjectNameSpaceRemoved(nameSpace);
                        }
                    }
                    else
                    {
                        throw new InvalidOperationException("Name space still contains objects. All objects must be unprovisioned before removing the name space.");
                    }
                }
            }
        }

        public void StoreMetadata(ObjectMetadata metadata)
        {
            // validate format of the object name
            if (!ObjectNameValidator.IsValidObjectName(metadata.ObjectName))
            {
                throw new ArgumentException("Invalid object name. It should be 3-30 characters long and contain only alphanumeric characters and underscores.");
            }

            if (null != metadata.Indexes)
            {
                // validate format of the index names
                foreach (var idx in metadata.Indexes)
                {
                    if (!ObjectNameValidator.IsValidIndexName(idx.Name))
                    {
                        throw new ArgumentException("Invalid index name found " + idx.Name + ". It should be 1-30 characters long and contain only alphanumeric characters or underscores.");
                    }
                }
            }

            if (null != metadata.Dependencies)
            {
                // validate # of dependencies
                if (metadata.Dependencies.Length > _config.MaxObjectDependencies)
                {
                    throw new ArgumentException("Maximum number of dependencies exceeded.");
                }

                // validate that dependencies exist
                foreach (var dep in metadata.Dependencies)
                {
                    var fullDepObjName = ObjectNaming.CreateFullObjectName(metadata.NameSpace, dep);
                    if (!Exists(fullDepObjName))
                    {
                        throw new ArgumentException("Object dependency does not exist: " + fullDepObjName);
                    }
                }
            }

            var fullObjName = ObjectNaming.CreateFullObjectName(metadata.NameSpace, metadata.ObjectName);
            var objKey = SerializerHelper.Serialize(fullObjName);
            var metadataVal = SerializerHelper.Serialize<ObjectMetadata>(metadata);
            _store.Set(objKey, metadataVal);
            lock (_mdCache)
            {
                _mdCache[fullObjName] = metadata;
            }

            if (null != ObjectMetadataAdded)
            {
                ObjectMetadataAdded(fullObjName);
            }
        }

        public void Remove(string nameSpace, string objectName)
        {
            Remove(ObjectNaming.CreateFullObjectName(nameSpace, objectName));
        }

        public void Remove(string objectFullName)
        {
            var objKey = ObjectNaming.CreateFullObjectKey(objectFullName);
            _store.Delete(objKey);
            lock (_mdCache)
            {
                if (_mdCache.ContainsKey(objectFullName))
                {
                    _mdCache.Remove(objectFullName);
                }
            }
            if (null != ObjectMetadataRemoved)
            {
                ObjectMetadataRemoved(objectFullName);
            }
        }

        public ObjectMetadata GetMetadata(string nameSpace, string objectName)
        {
            return GetMetadata(ObjectNaming.CreateFullObjectName(nameSpace, objectName));
        }

        public ObjectMetadata GetMetadata(string objectFullName)
        {
            lock (_mdCache)
            {
                if (_mdCache.ContainsKey(objectFullName))
                {
                    return _mdCache[objectFullName];
                }
                else
                {
                    return null;
                }
            }
        }

        public IEnumerable<string> EnumerateObjectNames()
        {
            lock (_mdCache)
            {
                foreach (var md in _mdCache.Values)
                {
                    yield return md.ObjectFullName;
                }
            }
        }

        public IEnumerable<string> EnumerateObjectNames(string nameSpace)
        {
            lock (_mdCache)
            {
                foreach (var md in _mdCache.Values)
                {
                    if (nameSpace.Equals(md.NameSpace, StringComparison.OrdinalIgnoreCase))
                    {
                        yield return md.ObjectFullName;
                    }
                }
            }
        }

        public IEnumerable<string> EnumerateNameSpaces()
        {
            lock (_nsCache)
            {
                foreach (string ns in _nsCache.Keys)
                {
                    yield return ns;
                }
            }
        }

        public IEnumerable<string> EnumerateObjectDependencies(string objectFullName)
        {
            var md = GetMetadata(objectFullName);
            if (null != md)
            {
                if (null != md.Dependencies)
                {
                    foreach (var dep in md.Dependencies)
                    {
                        yield return ObjectNaming.CreateFullObjectName(md.NameSpace, dep);
                    }
                }
            }
        }

        public bool Exists(string objectFullName)
        {
            lock (_mdCache)
            {
                return _mdCache.ContainsKey(objectFullName);
            }
        }

        public bool HasObjectDependencies(string objectFullName)
        {
            var md = GetMetadata(objectFullName);
            return (null != md && null != md.Dependencies && 0 != md.Dependencies.Length);
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
                    _store.Dispose();
                    _nsStore.Dispose();
                    lock (_nsCache)
                    {
                        _nsCache.Clear();
                    }
                    lock (_mdCache)
                    {
                        _mdCache.Clear();
                    }
                }
                _disposed = true;
            }

        }
        #endregion
    }
}
