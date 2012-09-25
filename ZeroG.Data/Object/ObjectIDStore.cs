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
using System.IO;

namespace ZeroG.Data.Object
{
    internal class ObjectIDStore : IDisposable
    {
        private KeyValueStore _store;
        private object _idLock = new object();

        public ObjectIDStore(Config config)
        {
            _store = new KeyValueStore(Path.Combine(config.BaseDataPath, "ObjectIDStore"));
        }

        public int GetNextID(byte[] key)
        {
            int nextId = 0;
            lock (_idLock)
            {
                var nextIdRaw = _store.Get(key);
                if (null != nextIdRaw)
                {
                    nextId = BitConverter.ToInt32(nextIdRaw, 0);
                    nextId++;
                }
                _store.Set(key, BitConverter.GetBytes(nextId));
            }
            return nextId;
        }

        public int GetCurrentID(byte[] key)
        {
            byte[] val = _store.Get(key);
            if (null == val)
            {
                return 0;
            }
            else
            {
                return BitConverter.ToInt32(val, 0);
            }
        }

        public void Reset(string objectFullName)
        {
            lock (_idLock)
            {
                _store.Set(ObjectNaming.CreateFullObjectKey(objectFullName), BitConverter.GetBytes(0));
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
