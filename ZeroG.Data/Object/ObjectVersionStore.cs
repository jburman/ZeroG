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

        private ObjectMetadataStore _metadata;
        private KeyValueStore _store;
        private Dictionary<string, uint> _versions;

        public ObjectVersionStore(ObjectMetadataStore metadata)
        {
            _metadata = metadata;
            _store = new KeyValueStore(Path.Combine(Config.BaseDataPath, "ObjectVersionStore"));
            _versions = new Dictionary<string, uint>(StringComparer.OrdinalIgnoreCase);
        }

        public uint Update(string fullObjectName)
        {
            uint returnValue = Current(fullObjectName) + 1;

            _store.Set(SerializerHelper.Serialize(fullObjectName), BitConverter.GetBytes(returnValue));
            _versions[fullObjectName] = returnValue;

            return returnValue;
        }

        public uint Current(string fullObjectName)
        {
            uint returnValue = 0;

            if (_versions.ContainsKey(fullObjectName))
            {
                returnValue = _versions[fullObjectName];
            }
            else
            {
                var val = _store.Get(SerializerHelper.Serialize(fullObjectName));
                if (null != val)
                {
                    returnValue = BitConverter.ToUInt32(val, 0);
                    _versions[fullObjectName] = returnValue;
                }
            }
            return returnValue;
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
