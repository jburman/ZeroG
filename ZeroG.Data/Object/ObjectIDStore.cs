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

namespace ZeroG.Data.Object
{
    internal class ObjectIDStore : IDisposable
    {
        private ISerializer _serializer;
        private IKeyValueStore _store;
        private object _idLock = new object();

        public ObjectIDStore(ISerializer serializer, IKeyValueStoreProvider kvProvider)
        {
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            _store = kvProvider.Get("ObjectIDStore");
        }

        public int GetNextID(byte[] key)
        {
            int nextId = 1;
            lock (_idLock)
            {
                var nextIdRaw = _store.Get(key);
                if (null != nextIdRaw)
                {
                    nextId = _serializer.DeserializeInt32(nextIdRaw);
                    nextId++;
                }
                _store.Set(key, _serializer.Serialize(nextId));
            }
            return nextId;
        }

        public int GetCurrentID(byte[] key)
        {
            lock (_idLock)
            {
                byte[] val = _store.Get(key);
                if (null == val)
                {
                    return 0;
                }
                else
                {
                    return _serializer.DeserializeInt32(val);
                }
            }
        }

        public void SetCurrentID(byte[] key, int value)
        {
            lock (_idLock)
            {
                _store.Set(key, _serializer.Serialize(value));
            }
        }

        public void Reset(string objectFullName)
        {
            lock (_idLock)
            {
                _store.Set(_serializer.CreateFullObjectKey(objectFullName), _serializer.Serialize(0));
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
