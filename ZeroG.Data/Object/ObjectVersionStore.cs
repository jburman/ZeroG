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
using System.IO;
using System.Linq;
using System.Text;
using ZeroG.Data.Object.Metadata;

namespace ZeroG.Data.Object
{
    internal class ObjectVersionStore : IDisposable
    {
        public static readonly string GlobalObjectVersionName = ObjectNaming.CreateFullObjectName(ObjectNaming.DefaultNameSpace, "GlobalVersion");

        public ObjectVersionChangedEvent VersionChanged;
        public ObjectVersionChangedEvent VersionRemoved;

        private static readonly uint VersionRollover = 10000000;

        private ObjectMetadataStore _metadata;
        private KeyValueStore _store;
        private Dictionary<string, uint> _versions;

        public ObjectVersionStore(Config config, ObjectMetadataStore metadata)
        {
            _metadata = metadata;
            _store = new KeyValueStore(Path.Combine(config.BaseDataPath, "ObjectVersionStore"));
            _versions = new Dictionary<string, uint>(StringComparer.OrdinalIgnoreCase);
        }

        public uint Update(string objectFullName)
        {
            var returnValue = _Update(objectFullName);

            foreach (var dep in _metadata.EnumerateObjectDependencies(objectFullName))
            {
                _Update(dep);
            }

            return returnValue;
        }

        private uint _Update(string objectFullName)
        {
            uint returnValue = Current(objectFullName) + 1;
            if (VersionRollover < returnValue)
            {
                returnValue = 1;
            }

            _store.Set(SerializerHelper.Serialize(objectFullName), BitConverter.GetBytes(returnValue));
            _versions[objectFullName] = returnValue;

            if (null != VersionChanged)
            {
                VersionChanged(objectFullName, returnValue);
            }

            return returnValue;
        }

        public uint Current(string objectFullName)
        {
            uint returnValue = 0;

            if (_versions.ContainsKey(objectFullName))
            {
                returnValue = _versions[objectFullName];
            }
            else
            {
                var val = _store.Get(SerializerHelper.Serialize(objectFullName));
                if (null != val)
                {
                    returnValue = BitConverter.ToUInt32(val, 0);
                    _versions[objectFullName] = returnValue;
                }
            }
            return returnValue;
        }

        public void Remove(string objectFullName)
        {
            foreach (var dep in _metadata.EnumerateObjectDependencies(objectFullName))
            {
                _Update(dep);
            }
            _versions.Remove(objectFullName);
            _store.Delete(SerializerHelper.Serialize(objectFullName));

            if (null != VersionRemoved)
            {
                VersionRemoved(objectFullName, 0);
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
                    _store.Dispose();
                }
                _disposed = true;
            }

        }
        #endregion
    }
}
