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

using RazorDB;
using System;
using System.Collections.Generic;

namespace ZeroG.Data.Object
{
    public class RazorDBKeyValueStore : IKeyValueStore
    {
        private KeyValueStore _razorDbStore;
        internal readonly string Name;

        public RazorDBKeyValueStore(KeyValueStore razorDbStore, string name)
        {
            _razorDbStore = razorDbStore ?? throw new ArgumentNullException(nameof(razorDbStore));
            Name = name;
        }

        public byte[] Get(byte[] key) => _razorDbStore.Get(key);

        public void Set(byte[] key, byte[] value) => _razorDbStore.Set(key, value);

        public void Delete(byte[] key) => _razorDbStore.Delete(key);

        public IEnumerable<KeyValuePair> Enumerate()
        {
            var rdbEnum = _razorDbStore.Enumerate();
            foreach(var record in rdbEnum)
            {
                KeyValuePair kvp;
                kvp.Key = record.Key;
                kvp.Value = record.Value;
                yield return kvp;
            }
        }

        public void Truncate() => _razorDbStore.Truncate();

        public int DataCacheSize => _razorDbStore.DataCacheSize;
        public int IndexCacheSize => _razorDbStore.IndexCacheSize;

        #region IDisposable Support
        private bool disposedValue = false;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _razorDbStore?.Dispose();
                }
                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }
        #endregion
    }
}
