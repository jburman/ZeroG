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
        private KeyValueStore _store;
        private KeyValueStore _nsStore;

        public event ObjectMetadatastoreUpdatedEvent ObjectNameSpaceAdded;
        public event ObjectMetadatastoreUpdatedEvent ObjectNameSpaceRemoved;
        public event ObjectMetadatastoreUpdatedEvent ObjectMetadataAdded;
        public event ObjectMetadatastoreUpdatedEvent ObjectMetadataRemoved;

        public ObjectMetadataStore()
        {
            _store = new KeyValueStore(Path.Combine(Config.BaseDataPath, "ObjectMetadataStore"));
            _nsStore = new KeyValueStore(Path.Combine(Config.BaseDataPath, "ObjectNameSpaceStore"));
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
                if (ObjectNaming.IsValidNameSpace(nameSpace))
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

        public ObjectNameSpaceConfig GetNameSpace(string nameSpace)
        {
            // check reserved namespace
            if (ObjectNaming.DefaultNameSpace.Equals(nameSpace, StringComparison.InvariantCultureIgnoreCase))
            {
                throw new ArgumentException("The specified name space is reserved.");
            }
            else
            {
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
            if (!ObjectNaming.IsValidObjectName(metadata.ObjectName))
            {
                throw new ArgumentException("Invalid object name. It should be 3-30 characters long and contain only alphanumeric characters and underscores.");
            }

            if (null != metadata.Indexes)
            {
                // validate format of the index names
                foreach (var idx in metadata.Indexes)
                {
                    if (!ObjectNaming.IsValidIndexName(idx.Name))
                    {
                        throw new ArgumentException("Invalid index name found " + idx.Name + ". It should be 3-30 characters long and contain only alphanumeric characters or underscores.");
                    }
                }
            }

            if (null != metadata.Dependencies)
            {
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
            var objKey = ObjectNaming.CreateFullObjectKey(objectFullName);
            var value = _store.Get(objKey);
            if (null == value)
            {
                return null;
            }
            else
            {
                return SerializerHelper.Deserialize<ObjectMetadata>(value);
            }
        }

        public IEnumerable<string> EnumerateObjectNames()
        {
            var encoding = Encoding.UTF8;
            foreach (var e in _store.Enumerate())
            {
                yield return encoding.GetString(e.Key);
            }
        }

        public IEnumerable<string> EnumerateObjectNames(string nameSpace)
        {
            var encoding = Encoding.UTF8;
            foreach (var e in _store.Enumerate())
            {
                string objFullName = encoding.GetString(e.Key);
                if (nameSpace.Equals(ObjectNaming.GetNameSpaceFromFullObjectName(objFullName), StringComparison.OrdinalIgnoreCase))
                {
                    yield return objFullName;
                }
            }
        }

        public IEnumerable<string> EnumerateNameSpaces()
        {
            var encoding = Encoding.UTF8;
            foreach (var e in _nsStore.Enumerate())
            {
                yield return encoding.GetString(e.Key);
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
            return null != _store.Get(SerializerHelper.Serialize(objectFullName));
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
                }
                _disposed = true;
            }

        }
        #endregion
    }
}
