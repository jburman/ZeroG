#region License, Terms and Conditions
// Copyright (c) 2013 Jeremy Burman
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
using System.Linq;
using System.Threading;

namespace ZeroG.Data.Object.Cache
{
    /// <summary>
    /// Cleans the cache with a more brute force mechanism.
    /// The strategy is to wait until either the Maximum Query Count 
    /// or Maximum Objects in the cache exceeds a threshold.
    /// </summary>
    internal class HardPruneCacheCleaner : ICacheCleaner
    {
        internal const int DefaultMaxQueries = 100000;
        internal const int DefaultMaxObjects = DefaultMaxQueries * 100;
        internal const int DefaultReductionFactor = 2; // Reduces queries by 50%.
        internal const int DefaultCleanFrequency = (5 * 60 * 1000); // 5 minutes

        private ICleanableCache _cache;
        private Timer _cleanTimer; 
        private int _maxQueries, _maxObjects, _reductionFactor, _cleanFrequency;

        public HardPruneCacheCleaner(ICleanableCache cache)
            : this(cache, 
            DefaultMaxQueries, 
            DefaultMaxObjects, 
            DefaultReductionFactor, 
            DefaultCleanFrequency)
        {
        }

        public HardPruneCacheCleaner(ICleanableCache cache, 
            int maximumQueries, 
            int maximumObjects,
            int reductionFactor,
            int cleanFrequency)
        {
            if (null == cache)
            {
                throw new ArgumentNullException("cache");
            }
            _cache = cache;
            _maxQueries = maximumQueries;
            _maxObjects = maximumObjects;
            _reductionFactor = Math.Max(2, reductionFactor);
            _cleanFrequency = cleanFrequency;

            if (cleanFrequency > 0)
            {
                _cleanTimer = new Timer((target) =>
                {
                    CacheTotals totals = _cache.Totals;
                    if (_NeedsCleaning(totals))
                    {
                        _Clean(totals);
                    }
                }, null, cleanFrequency, cleanFrequency);
            }
        }

        public int MaxQueries { get { return _maxQueries; } }
        public int MaxObjects { get { return _maxObjects; } }
        public int ReductionFactor { get { return _reductionFactor; } }
        public int CleanFrequency { get { return _cleanFrequency; } }

        #region Private helpers
        private bool _NeedsCleaning(CacheTotals totals)
        {
            return (totals.TotalQueries > _maxQueries || totals.TotalObjectIDs > _maxObjects);
        }

        private bool _Clean(CacheTotals totals)
        {
            bool itemsRemoved = false;

            int totalToRemove = (int)totals.TotalQueries / _reductionFactor;
            itemsRemoved = totalToRemove > 0;
            ICacheEntry[] entries = _cache.EnumerateCache().ToArray();
            Array.Sort(entries);
            _cache.Remove(entries.Take(totalToRemove));

            return itemsRemoved;
        }
        #endregion

        public bool NeedsCleaning()
        {
            return _NeedsCleaning(_cache.Totals);
        }

        public bool Clean()
        {
            return _Clean(_cache.Totals);
        }

        private bool _disposed;

        private void _Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    if (_cleanTimer != null)
                    {
                        _cleanTimer.Dispose();
                    }
                }

                _disposed = true;
            }
        }

        public void Dispose()
        {
            _Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~HardPruneCacheCleaner()
        {
            _Dispose(false);
        }
    }
}
