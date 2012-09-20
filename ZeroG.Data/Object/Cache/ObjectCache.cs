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

using System;
using System.Collections.Generic;
using ZeroG.Data.Object.Metadata;

namespace ZeroG.Data.Object.Cache
{
    internal class ObjectCache
    {
        private Dictionary<string, ObjectCacheRecord> _cache;
        private ObjectMetadataStore _metadata;
        private ObjectVersionStore _versions;

        public ObjectCache(ObjectMetadataStore metadata, ObjectVersionStore versions)
        {
            _cache = new Dictionary<string, ObjectCacheRecord>(StringComparer.OrdinalIgnoreCase);
            _metadata = metadata;
            _versions = versions;

            metadata.ObjectMetadataAdded += _ObjectMetadataAdded;
            metadata.ObjectMetadataRemoved += _ObjectMetadataRemoved;
        }

        #region Private methods

        /// <summary>
        /// Resets the entire cache
        /// </summary>
        private void _ResetCache()
        {
            _cache.Clear();
        }

        private ObjectCacheRecord _CreateObjectCacheRecord(string objectFullName)
        {
            var metadata = _metadata.GetMetadata(objectFullName);
            var record = new ObjectCacheRecord()
            {
                ObjectFullName = ObjectNaming.CreateFullObjectName(metadata.NameSpace, metadata.ObjectName),
                Version = _versions.Current(objectFullName),
                Count = 0,
                TotalSize = 0,
                Objects = new Dictionary<int,byte[]>()
            };
            _cache[objectFullName] = record;
            return record;
        }

        private void _ObjectMetadataAdded(string value)
        {
            _ResetCache();
        }

        private void _ObjectMetadataRemoved(string value)
        {
            _ResetCache();
        }

        #endregion

        #region Public caching methods

        public byte[] Get(string objectFullName, int objectId)
        {
            byte[] returnValue = null;

            if (IsDirty(objectFullName))
            {
                _CreateObjectCacheRecord(objectFullName);
            }
            else
            {
                var record = _cache[objectFullName];
                if(record.Objects.ContainsKey(objectId)) 
                {
                    returnValue = record.Objects[objectId];
                }
            }

            return returnValue;
        }

        public void Add(string objectFullName, int objectId, byte[] value)
        {
            ObjectCacheRecord record = null;
            if (IsDirty(objectFullName))
            {
                record = _CreateObjectCacheRecord(objectFullName);
            }
            else
            {
                record = _cache[objectFullName];
            }
            record.Objects[objectId] = value;
        }

        public bool IsDirty(string objectFullName)
        {
            bool returnValue = true;

            if (_cache.ContainsKey(objectFullName))
            {
                var currentVersion = _versions.Current(objectFullName);
                returnValue = (currentVersion == _cache[objectFullName].Version);
            }

            return returnValue;
        }
        #endregion
    }
}
