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
using System.Diagnostics;
using System.Linq;
using System.Threading;
using ZeroG.Data.Object.Metadata;

namespace ZeroG.Data.Object.Cache
{
    internal class ObjectIndexerCache : IDisposable, ICleanableCache, IObjectIndexerCache
    {
        internal const int MaxCacheKeyLen = 500;

        private const int ReadLockTimeout = 2000;
        private const int WriteLockTimeout = 4000;

        private Dictionary<string, ObjectIndexerCacheRecord> _cache;
        private ReaderWriterLockSlim _cacheLock;
        private ObjectVersionStore _versions;

        public ObjectIndexerCache(ObjectMetadataStore metadata, ObjectVersionStore versions)
        {
            _cache = new Dictionary<string, ObjectIndexerCacheRecord>(StringComparer.OrdinalIgnoreCase);
            _cacheLock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
            _versions = versions;

            metadata.ObjectMetadataAdded += ObjectMetadataAdded;
            metadata.ObjectMetadataRemoved += ObjectMetadataRemoved;

            versions.VersionChanged += ObjectVersionChanged;
            versions.VersionRemoved += ObjectVersionRemoved;
        }

        #region Private methods

        private ObjectIndexerCacheRecord _CreateObjectCacheRecord(string objectFullName, uint hash, int[] values)
        {
            var record = new ObjectIndexerCacheRecord()
            {
                ObjectFullName = objectFullName,
                Version = _versions.Current(objectFullName),
                IsDirty = false
            };
            record.AddToCache(hash, values);
            return record;
        }

        private void _ReplaceObjectInCache(string objectFullName, ObjectIndexerCacheRecord replace)
        {
            if (_cacheLock.TryEnterWriteLock(WriteLockTimeout))
            {
                try
                {
                    if (null != replace)
                    {
                        _cache[objectFullName] = replace;
                    }
                }
                finally
                {
                    _cacheLock.ExitWriteLock();
                }
            }
            else
            {
                // this is an act of desparation to make sure that dirty values won't be read
                Reset();
            }
        }

        #endregion

        public static uint ConstructHash(params object[] parameters)
        {
            uint returnValue = 0;
            int len = parameters.Length;
            int totalLen = 0;
            if (0 < len)
            {
                var s = string.Empty;
                for (int i = 0; len > i; i++)
                {
                    var p = parameters[i];
                    if (null != p)
                    {
                        var nextS = p.ToString();
                        totalLen += nextS.Length;
                        if (totalLen > MaxCacheKeyLen)
                        {
                            break;
                        }
                        s += nextS;
                    }
                }
                if (totalLen <= MaxCacheKeyLen)
                {
                    returnValue = (uint)s.GetHashCode();
                }
            }
            return returnValue;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="parameters">List of parameters to cache against. Note that the first parameter MUST be the full object name.</param>
        /// <returns></returns>
        public int[] Get(params object[] parameters)
        {
            int[] returnValue = null;
            if (0 < parameters.Length)
            {
                string objectFullName = (string)parameters[0];

                if (_cacheLock.TryEnterReadLock(ReadLockTimeout))
                {
                    try
                    {
                        if (_cache.ContainsKey(objectFullName))
                        {
                            ObjectIndexerCacheRecord entry = _cache[objectFullName];

                            if (!entry.IsDirty)
                            {
                                var hash = ConstructHash(parameters);
                                if (0 != hash)
                                {
                                    // try to get from cache
                                    returnValue = entry.GetFromCache(hash);
                                }
                            }
                        }
                    }
                    finally
                    {
                        _cacheLock.ExitReadLock();
                    }
                }
            }
            return returnValue;
        }

        public void Set(int[] values, params object[] parameters)
        {
            if (0 < parameters.Length)
            {
                string objectFullName = (string)parameters[0];

                ObjectIndexerCacheRecord entry = null;
                bool replaceEntry = false;
                var hash = ConstructHash(parameters);

                if (_cacheLock.TryEnterReadLock(ReadLockTimeout))
                {
                    try
                    {
                        if (_cache.ContainsKey(objectFullName))
                        {
                            entry = _cache[objectFullName];

                            if (entry.IsDirty)
                            {
                                // The object version has changed so the cache entry is no longer
                                // valid
                                entry = _CreateObjectCacheRecord(objectFullName, hash, values);
                                replaceEntry = true;
                            }
                        }
                        else
                        {
                            // No cache entry yet so create a new record to store it in.
                            entry = _CreateObjectCacheRecord(objectFullName, hash, values);
                            replaceEntry = true;
                        }
                    }
                    finally
                    {
                        _cacheLock.ExitReadLock();
                    }
                }

                if (replaceEntry)
                {
                    // A new cache entry was created and the old one needs to be replaced.
                    _ReplaceObjectInCache(objectFullName, entry);
                }
                else
                {
                    // Update the exist cache entry
                    if (_cacheLock.TryEnterWriteLock(WriteLockTimeout))
                    {
                        try
                        {
                            entry.AddToCache(hash, values);
                        }
                        finally
                        {
                            _cacheLock.ExitWriteLock();
                        }
                    }
                }
            }
        }

        public void ObjectVersionChanged(string objectFullName, uint newVersion)
        {
            if (_cacheLock.TryEnterReadLock(ReadLockTimeout))
            {
                try
                {
                    if (_cache.ContainsKey(objectFullName))
                    {
                        var entry = _cache[objectFullName];
                        if (newVersion != entry.Version)
                        {
                            entry.IsDirty = true;
                        }
                    }
                }
                finally
                {
                    _cacheLock.ExitReadLock();
                }
            }
            else
            {
                // this is an act of desparation to make sure that dirty values won't be read
                Reset();
            }
        }

        public void ObjectVersionRemoved(string objectFullName, uint newVersion)
        {
            _ReplaceObjectInCache(objectFullName, null);
        }

        public void ObjectMetadataAdded(string objectFullName)
        {
            Reset();
        }

        public void ObjectMetadataRemoved(string objectFullName)
        {
            Reset();
        }

        /// <summary>
        /// Resets the entire cache
        /// </summary>
        public void Reset()
        {
            if (_cacheLock.TryEnterWriteLock(WriteLockTimeout))
            {
                try
                {
                    _cache.Clear();
                }
                finally
                {
                    _cacheLock.ExitWriteLock();
                }
            }
            else
            {
                throw new TimeoutException("Unable to acquire write lock on ObjectIndexerCache. It may contain unreliable information and should be recreated.");
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
                    if (null != _cacheLock)
                    {
                        _cacheLock.Dispose();
                    }
                }

                _disposed = true;
            }
        }
        #endregion

        #region ICleanableCache implementation

        private struct CacheEntry : ICacheEntry
        {
            private string _objectFullName;
            private uint _hash;
            private int _counter, _valueCount;

            public CacheEntry(string objectFullName,
                uint hash,
                int counter,
                int valueCount)
            {
                _objectFullName = objectFullName;
                _hash = hash;
                _counter = counter;
                _valueCount = valueCount;
            }

            public string ObjectFullName
            {
                get { return _objectFullName; }
            }

            public uint Hash
            {
                get { return _hash; }
            }

            public int Counter
            {
                get { return _counter; }
            }

            public int ValueCount
            {
                get { return _valueCount; }
            }

            public int CompareTo(object obj)
            {
                int returnValue = 1;

                if (obj != null && obj is ICacheEntry)
                {
                    ICacheEntry compareTo = (ICacheEntry)obj;
                    // first compare counter
                    returnValue = Counter.CompareTo(compareTo.Counter);
                    // next compare object count
                    if (returnValue == 0)
                    {
                        returnValue = ValueCount.CompareTo(compareTo.ValueCount);
                    }
                }

                return returnValue;
            }
        }

        public CacheTotals Totals
        {
            get 
            {
                int totalQueries = 0;
                int totalValues = 0;

                if (_cacheLock.TryEnterReadLock(ReadLockTimeout))
                {
                    try
                    {
                        foreach (var entry in _cache)
                        {
                            totalQueries += entry.Value.Cache.Count;
                            totalValues += entry.Value.TotalValues;
                        }
                    }
                    finally
                    {
                        _cacheLock.ExitReadLock();
                    }
                }

                return new CacheTotals(totalQueries, totalValues);
            }
        }

        public IEnumerable<ICacheEntry> EnumerateCache()
        {
            if (_cacheLock.IsWriteLockHeld || _cacheLock.TryEnterReadLock(ReadLockTimeout))
            {
                try
                {
                    foreach (var entry in _cache)
                    {
                        string objectName = entry.Value.ObjectFullName;

                        foreach (var cache in entry.Value.Cache)
                        {
                            yield return new CacheEntry(objectName,
                                cache.Key,
                                cache.Value.Counter,
                                cache.Value.Value.Length);
                        }
                    }
                }
                finally
                {
                    if (!_cacheLock.IsWriteLockHeld)
                    {
                        _cacheLock.ExitReadLock();
                    }
                }
            }
        }

        public void Remove(IEnumerable<ICacheEntry> entries)
        {
            if (_cacheLock.TryEnterWriteLock(WriteLockTimeout))
            {
                try
                {
                    ICacheEntry[] entriesToRemove = entries.ToArray();
                    for (int i = 0; i < entriesToRemove.Length; i++)
                    {
                        ICacheEntry entry = entriesToRemove[i];

                        if (_cache.ContainsKey(entry.ObjectFullName))
                        {
                            _cache[entry.ObjectFullName].RemoveFromCache(entry.Hash);
                        }
                    }
                }
                finally
                {
                    _cacheLock.ExitWriteLock();
                }
            }
        }
        #endregion
    }
}
