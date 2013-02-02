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
using System.Collections.Generic;

namespace ZeroG.Data.Object.Cache
{
    public sealed class ObjectIndexerCacheRecord
    {
        public ObjectIndexerCacheRecord()
        {
            Cache = new Dictionary<uint, CacheValue<int[]>>();
        }

        public string ObjectFullName;
        public bool IsDirty;
        public uint Version;
        public int TotalObjectIDs;
        public Dictionary<uint, CacheValue<int[]>> Cache;

        public void AddToCache(uint hash, int[] objectIds)
        {
            if (Cache.ContainsKey(hash))
            {
                TotalObjectIDs -= Math.Max(0, TotalObjectIDs - Cache[hash].Value.Length);
            }
            Cache[hash] = new CacheValue<int[]>(objectIds);
            TotalObjectIDs += objectIds.Length;
        }

        public int[] GetFromCache(uint hash)
        {
            if (Cache.ContainsKey(hash))
            {
                CacheValue<int[]> value = Cache[hash];
                value.Counter += 1;
                return value.Value;
            }
            else
            {
                return null;
            }
        }

        public void RemoveFromCache(uint hash)
        {
            if (Cache.ContainsKey(hash))
            {
                TotalObjectIDs = Math.Max(0, TotalObjectIDs - Cache[hash].Value.Length);
                Cache.Remove(hash);
            }
        }
    }
}
