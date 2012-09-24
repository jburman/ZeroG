using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using ZeroG.Data.Object.Metadata;

namespace ZeroG.Data.Object.Cache
{
    internal class ObjectIndexerCacheRecord
    {
        public string ObjectFullName;
        public bool IsDirty;
        public uint Version;
        public uint Count;
        public Dictionary<uint, int[]> ObjectIDs;
    }

    public class ObjectIndexerCache
    {
        internal static readonly int MaxCacheKeyLen = 500;

        private Dictionary<string, ObjectIndexerCacheRecord> _cache;
        private ObjectMetadataStore _metadata;
        private ObjectVersionStore _versions;

        internal ObjectIndexerCache(ObjectMetadataStore metadata, ObjectVersionStore versions)
        {
            _cache = new Dictionary<string, ObjectIndexerCacheRecord>(StringComparer.OrdinalIgnoreCase);
            _metadata = metadata;
            _versions = versions;

            metadata.ObjectMetadataAdded += _ObjectMetadataAdded;
            metadata.ObjectMetadataRemoved += _ObjectMetadataRemoved;

            versions.VersionChanged += _ObjectVersionChanged;
            versions.VersionRemoved += _ObjectVersionRemoved;
        }

        #region Private methods

        /// <summary>
        /// Resets the entire cache
        /// </summary>
        private void _ResetCache()
        {
            _cache.Clear();
        }

        private void _ObjectVersionChanged(string value, uint newVersion)
        {
            if (_cache.ContainsKey(value))
            {
                var entry = _cache[value];
                if (newVersion != entry.Version)
                {
                    entry.IsDirty = true;
                }
            }
        }

        private ObjectIndexerCacheRecord _CreateObjectCacheRecord(string objectFullName)
        {
            var metadata = _metadata.GetMetadata(objectFullName);
            var record = new ObjectIndexerCacheRecord()
            {
                ObjectFullName = objectFullName,
                Version = _versions.Current(objectFullName),
                Count = 0,
                ObjectIDs = new Dictionary<uint, int[]>(),
                IsDirty = false
            };
            return record;
        }

        private void _ObjectVersionRemoved(string value, uint newVersion)
        {
            _cache.Remove(value);
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

                if (_cache.ContainsKey(objectFullName))
                {
                    var entry = _cache[objectFullName];

                    if (entry.IsDirty)
                    {
                        _cache[objectFullName] = _CreateObjectCacheRecord(objectFullName);
                    }
                    else
                    {
                        var hash = ConstructHash(parameters);
                        if (0 != hash)
                        {
                            var objIds = entry.ObjectIDs;
                            // try to get from cache
                            if (objIds.ContainsKey(hash))
                            {
                                return objIds[hash]; 
                            }
                        }
                    }
                }
            }
            return returnValue;
        }

        public void Set(int[] objectIds, params object[] parameters)
        {
            if (0 < parameters.Length)
            {
                string objectFullName = (string)parameters[0];

                ObjectIndexerCacheRecord entry = null;

                if (_cache.ContainsKey(objectFullName))
                {
                    entry = _cache[objectFullName];
                    if (entry.IsDirty)
                    {
                        _cache[objectFullName] = _CreateObjectCacheRecord(objectFullName);
                    }
                }
                else
                {
                    entry = _CreateObjectCacheRecord(objectFullName);
                    _cache[objectFullName] = entry;
                }

                var hash = ConstructHash(parameters);
                if (!entry.ObjectIDs.ContainsKey(hash))
                {
                    entry.Count += 1;
                }
                entry.ObjectIDs[hash] = objectIds;
            }
        }
    }
}
