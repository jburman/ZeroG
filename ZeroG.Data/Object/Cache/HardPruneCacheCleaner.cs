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

namespace ZeroG.Data.Object.Cache
{
    /// <summary>
    /// Cleans the cache with a more brute force mechanism.
    /// The strategy is to wait until either the Maximum Query Count 
    /// or Maximum Objects in the cache exceeds a threshold.
    /// </summary>
    public class HardPruneCacheCleaner : ICacheCleaner
    {
        private ICleanableCache _cache;
        private uint _maxQueries, _maxObjects;

        public HardPruneCacheCleaner(ICleanableCache cache, uint maximumQueries, uint maximumObjects)
        {
            if (null == cache)
            {
                throw new ArgumentNullException("cache");
            }
            _cache = cache;
            _maxQueries = maximumQueries;
            _maxObjects = maximumObjects;
        }

        public bool NeedsCleaning()
        {
            var totals = _cache.Totals;

            return (totals.TotalQueries > _maxQueries || totals.TotalObjectIDs > _maxObjects);
        }

        public bool Update()
        {
            foreach (ICacheEntry cache in _cache.EnumerateCache())
            {
                // TODO
            }

            return true;
        }

        public void Dispose()
        {
            
        }
    }
}
