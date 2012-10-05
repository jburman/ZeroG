using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using RazorDB;
using ZeroG.Data.Object.Metadata;

namespace ZeroG.Data.Object.Cache
{
    internal class ObjectVersion : IDisposable
    {
        public static readonly string GlobalObjectVersionName = ObjectNaming.CreateFullObjectName(ObjectNaming.DefaultNameSpace, "GlobalVersion");

        private ObjectMetadataStore _metadata;
        private KeyValueStore _store;
        private Dictionary<string, uint> _versions;

        public ObjectVersion(ObjectMetadataStore metadata)
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

    public interface ICacheDependency
    {
        bool IsDirty(uint version);
    }

    public class GlobalCacheDependency
    {
        private ObjectCache _cache;

        public GlobalCacheDependency(ObjectCache cache)
        {
            _cache = cache;
        }

        public bool IsDirty(uint version)
        {
            
        }
    }

    internal class ObjectCacheRecord
    {
        public byte[] FullObjectKey;
        public uint Version;
        public uint TotalSize;
        public uint Count;
        public Dictionary<int, byte[]> Objects;
    }

    public class ObjectCache
    {
        private Dictionary<string, ObjectCacheRecord> _cache;
        private ObjectVersion _versions;

        public ObjectCache(ObjectVersion versions)
        {
            _cache = new Dictionary<string, ObjectCacheRecord>(StringComparer.OrdinalIgnoreCase);
            _versions = versions;
        }

        public bool IsDirty(string fullObjectName, uint version)
        {
            bool returnValue = true;

            if (_cache.ContainsKey(fullObjectName))
            {
                var currentVersion = _versions.Current(SerializerHelper.Serialize(fullObjectName));
                returnValue = (currentVersion == _cache[fullObjectName].Version);
            }

            return returnValue;
        }

        public bool IsDirty(byte[] fullObjectKey, uint version)
        {
            return IsDirty(SerializerHelper.DeserializeString(fullObjectKey), version);
        }
    }
}
