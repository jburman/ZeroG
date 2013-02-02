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
using System.Threading;

namespace ZeroG.Data.Object
{
    internal class ObjectVersionStore : IDisposable
    {
        private struct ObjectVersionChangeData
        {
            public uint Version;
            public string ObjectFullName;
        }

        public static readonly string GlobalObjectVersionName = ObjectNaming.CreateFullObjectName(ObjectNaming.DefaultNameSpace, "GlobalVersion");

        private const int _ReaderWaitTimeout = 2000;
        private const int _WriterWaitTimeout = 4000;

        public ObjectVersionChangedEvent VersionChanged;
        public ObjectVersionChangedEvent VersionRemoved;

        private static readonly uint VersionRollover = 10000000;

        private ObjectMetadataStore _metadata;
        private KeyValueStore _store;
        private Dictionary<string, uint> _versions;
        private ReaderWriterLockSlim _lock;

        public ObjectVersionStore(Config config, ObjectMetadataStore metadata)
        {
            _metadata = metadata;
            _store = new KeyValueStore(Path.Combine(config.BaseDataPath, "ObjectVersionStore"));
            _versions = new Dictionary<string, uint>(StringComparer.OrdinalIgnoreCase);
            _lock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        }

        public uint Update(string objectFullName)
        {
            uint returnValue = 0;

            // Holds data to be passed to any VersionChanged event listeners
            List<ObjectVersionChangeData> versionChangeData = new List<ObjectVersionChangeData>();

            // The writer lock is obtained so that the version can be written to storage and the _versions cache object
            // can be updated. 
            if (_lock.TryEnterWriteLock(_WriterWaitTimeout))
            {
                try
                {
                    uint newVersion = _Update(objectFullName);
                    versionChangeData.Add(new ObjectVersionChangeData()
                    {
                        Version = newVersion,
                        ObjectFullName = objectFullName
                    });

                    // Dependent objects are automatically updated
                    foreach (var dep in _metadata.EnumerateObjectDependencies(objectFullName))
                    {
                        newVersion = _Update(dep);
                        versionChangeData.Add(new ObjectVersionChangeData()
                        {
                            Version = newVersion,
                            ObjectFullName = dep
                        });
                    }
                }
                finally
                {
                    _lock.ExitWriteLock();
                }

                // Fire off events
                if (VersionChanged != null)
                {
                    foreach (ObjectVersionChangeData changeData in versionChangeData)
                    {
                        VersionChanged(changeData.ObjectFullName, changeData.Version);
                    }
                }
            }
            else
            {
                throw new TimeoutException("Unable to update Object Version. Write lock timeout expired."); 
            }

            return returnValue;
        }

        private uint _Update(string objectFullName)
        {
            uint returnValue = Current(objectFullName) + 1;
            if (VersionRollover < returnValue)
            {
                returnValue = 1;
            }

            _store.Set(SerializerHelper.Serialize(objectFullName), BitConverter.GetBytes(returnValue));
            _versions[objectFullName] = returnValue;

            return returnValue;
        }

        public uint Current(string objectFullName)
        {
            uint returnValue = 0;
            bool notInCache = true;

            // Allow multiple readers of the _versions object, which is treated as a version cache.
            // This method may be called by a thread that has the Write lock already.
            if (_lock.IsWriteLockHeld || _lock.TryEnterReadLock(_WriterWaitTimeout))
            {
                try
                {
                    if (_versions.ContainsKey(objectFullName))
                    {
                        returnValue = _versions[objectFullName];
                        notInCache = false;
                    }
                }
                finally
                {
                    if (_lock.IsReadLockHeld)
                    {
                        _lock.ExitReadLock();
                    }
                }
            }
            else
            {
                throw new TimeoutException("Unable to read Object Version. Read lock timeout expired.");
            }

            if (notInCache)
            {
                // Obtain the writer lock so that the _versions cache object can be updated.
                if (_lock.TryEnterWriteLock(_WriterWaitTimeout))
                {
                    try
                    {
                        var val = _store.Get(SerializerHelper.Serialize(objectFullName));
                        if (null != val)
                        {
                            returnValue = BitConverter.ToUInt32(val, 0);
                            _versions[objectFullName] = returnValue;
                        }
                    }
                    finally
                    {
                        _lock.ExitWriteLock();
                    }
                }
                else
                {
                    throw new TimeoutException("Unable to read Object Version. Write lock timeout expired.");
                }
            }
            return returnValue;
        }

        public void Remove(string objectFullName)
        {
            // Holds data to be passed to any VersionChanged event listeners
            List<ObjectVersionChangeData> versionChangeData = null;

            if (_lock.TryEnterWriteLock(_WriterWaitTimeout))
            {
                try
                {
                    if (_metadata.HasObjectDependencies(objectFullName))
                    {
                        if (VersionChanged != null)
                        {
                            versionChangeData = new List<ObjectVersionChangeData>();
                        }

                        // Dependent objects are automatically updated
                        foreach (var dep in _metadata.EnumerateObjectDependencies(objectFullName))
                        {
                            uint newVersion = _Update(dep);
                            if (versionChangeData != null)
                            {
                                versionChangeData.Add(new ObjectVersionChangeData()
                                {
                                    ObjectFullName = dep,
                                    Version = newVersion
                                });
                            }
                        }
                    }
                    _versions.Remove(objectFullName);
                    _store.Delete(SerializerHelper.Serialize(objectFullName));
                }
                finally
                {
                    _lock.ExitWriteLock();
                }

                // Fire off events
                if (versionChangeData != null)
                {
                    foreach (ObjectVersionChangeData changeData in versionChangeData)
                    {
                        VersionChanged(changeData.ObjectFullName, changeData.Version);
                    }
                }
            }
            else
            {
                throw new TimeoutException("Unable to remove Object Version. Write lock timeout expired.");
            }

            if (VersionRemoved != null)
            {
                VersionRemoved(objectFullName, 0);
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
                    _lock.Dispose();
                    _store.Dispose();
                }
                _disposed = true;
            }

        }
        #endregion
    }
}
