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
using System.IO;
using System.Linq;
using System.Text;
using System.Transactions;
using ZeroG.Data.Object.Backup;
using ZeroG.Data.Object.Cache;
using ZeroG.Data.Object.Index;
using ZeroG.Data.Object.Metadata;

namespace ZeroG.Data.Object
{
    public sealed class ObjectService : IDisposable
    {
        #region ObjectService construction
        private ObjectMetadataStore _objectMetadata;
        private ObjectNaming _objectNaming;
        private ObjectIDStore _objectIDStore;
        private ObjectVersionStore _objectVersions;
        private ObjectStore _objectStore;
        private ObjectIndexer _objectIndexer;
        private ObjectIndexerCache _indexerCache;

        private List<IDisposable> _assignments;

        private static TransactionOptions _DefaultTransactionOptions;

        static ObjectService()
        {
            _DefaultTransactionOptions = new TransactionOptions();
            _DefaultTransactionOptions.IsolationLevel = IsolationLevel.RepeatableRead;
            _DefaultTransactionOptions.Timeout = TransactionManager.DefaultTimeout;
        }

        public ObjectService()
            : this(Config.Default)
        {
        }

        public ObjectService(Config config)
        {
            _assignments = new List<IDisposable>();


            _objectMetadata = new ObjectMetadataStore(config);
            _objectNaming = new ObjectNaming(_objectMetadata);
            _objectIDStore = new ObjectIDStore(config);
            _objectVersions = new ObjectVersionStore(config, _objectMetadata);

            if (config.IndexCacheEnabled)
            {
                _indexerCache = new ObjectIndexerCache(_objectMetadata, _objectVersions);
            }

            _objectStore = new ObjectStore(config, _objectMetadata);
            _objectIndexer = new ObjectIndexer(_indexerCache);

            _assignments.Add(_objectMetadata);
            _assignments.Add(_objectIDStore);
            _assignments.Add(_objectVersions);
            _assignments.Add(_objectStore);
            _assignments.Add(_objectIndexer);
        }

        #endregion

        #region Private helpers
        private void _ValidateArguments(string nameSpace, PersistentObject obj)
        {
            if (string.IsNullOrEmpty(nameSpace))
            {
                throw new ArgumentException("A valid namespace must be supplied.", "nameSpace");
            }
            else
            {
                if (null == obj)
                {
                    throw new ArgumentNullException("obj");
                }
                else
                {
                    if (!_objectNaming.NameSpaceExists(nameSpace))
                    {
                        throw new ArgumentException("Unknown namespace: " + nameSpace);
                    }

                    if (!_objectNaming.ObjectNameExists(nameSpace, obj.Name))
                    {
                        throw new ArgumentException("Object name does not exist: " + ObjectNaming.CreateFullObjectName(nameSpace, obj.Name));
                    }
                }
            }
        }

        private void _ValidateArguments(string nameSpace, string objectName)
        {
            _ValidateArguments(nameSpace, objectName, false);
        }

        private bool _ValidateArguments(string nameSpace, string objectName, bool noThrow)
        {
            if (string.IsNullOrEmpty(nameSpace))
            {
                if (noThrow)
                {
                    return false;
                }
                else
                {
                    throw new ArgumentException("A valid namespace must be supplied.", "nameSpace");
                }
            }
            else
            {
                if (string.IsNullOrEmpty(objectName))
                {
                    if (noThrow)
                    {
                        return false;
                    }
                    else
                    {
                        throw new ArgumentException("A valid object name must be supplied.", "objectName");
                    }
                }
                else
                {
                    if (!_objectNaming.NameSpaceExists(nameSpace))
                    {
                        if (noThrow)
                        {
                            return false;
                        }
                        else
                        {
                            throw new ArgumentException("Unknown namespace: " + nameSpace);
                        }
                    }

                    if (!_objectNaming.ObjectNameExists(nameSpace, objectName))
                    {
                        if (noThrow)
                        {
                            return false;
                        }
                        else
                        {
                            throw new ArgumentException("Object name does not exist: " + ObjectNaming.CreateFullObjectName(nameSpace, objectName));
                        }
                    }
                }
            }

            return true;
        }
        #endregion

        #region Public service methods

        public void BackupNameSpace(string nameSpace, string backupFileName, bool useCompression)
        {
            var ns = GetNameSpace(nameSpace);
            if (null == ns)
            {
                throw new ArgumentException("Name space not found: " + nameSpace);
            }

            using (var writer = new ObjectBackupWriter(backupFileName, useCompression))
            {
                writer.WriteStoreVersion(Config.StoreVersion);

                writer.WriteNameSpace(ns);

                foreach (var objName in _objectMetadata.EnumerateObjectNames(nameSpace))
                {
                    var md = _objectMetadata.GetMetadata(objName);

                    // store the object's metadata
                    writer.WriteObjectMetadata(md);

                    // save the current ObjectID
                    writer.WriteObjectID(_objectIDStore.GetCurrentID(ObjectNaming.CreateFullObjectKey(objName)));

                    // enumerate object values
                    foreach (var objRecord in _objectStore.Iterate(objName))
                    {
                        writer.WriteObject(objRecord);
                    }

                    // enumerate object index values
                    if (null != md.Indexes && 0 < md.Indexes.Length)
                    {
                        foreach (var idxRecord in _objectIndexer.Iterate(objName, md.Indexes))
                        {
                            writer.WriteIndex(ObjectIndexRecord.CreateFromDataRecord(idxRecord));
                        }
                    }
                }
            }
        }

        public void Restore(string backupFileName, bool useCompression)
        {
            using (var reader = new ObjectBackupReader(backupFileName, useCompression))
            {
                string nameSpace = null;
                ObjectMetadata objectMetadata = null;
                var bulkIndexValues = new List<object[]>();

                reader.ReadBackup(
                    (string storeVersion) =>
                    {
                        // ignore version for now
                    },
                    (ObjectNameSpaceConfig nameSpaceConfig) =>
                    {
                        nameSpace = nameSpaceConfig.Name;

                        if (_objectMetadata.NameSpaceExists(nameSpaceConfig.Name))
                        {
                            _objectMetadata.UpdateNameSpace(nameSpaceConfig);
                        }
                        else
                        {
                            _objectMetadata.CreateNameSpace(nameSpaceConfig);
                        }
                    },
                    (ObjectMetadata metadata) =>
                    {
                        // write batched index values if any are queued up
                        if (0 < bulkIndexValues.Count)
                        {
                            _objectIndexer.BulkIndexObject(
                                objectMetadata.ObjectFullName,
                                objectMetadata,
                                bulkIndexValues);

                            bulkIndexValues = new List<object[]>();
                        }

                        // remove all existing data and update the metadata
                        objectMetadata = metadata;

                        Truncate(objectMetadata.NameSpace, objectMetadata.ObjectName, true);

                        _objectMetadata.StoreMetadata(metadata);
                    },
                    (int currentObjectId) =>
                    {
                        // reset the Object Store Identity value
                        _objectIDStore.SetCurrentID(
                            ObjectNaming.CreateFullObjectKey(objectMetadata.ObjectFullName), currentObjectId);
                    },
                    (ObjectStoreRecord obj) =>
                    {
                        _objectStore.Set(objectMetadata.NameSpace,
                            new PersistentObject()
                            {
                                Name = objectMetadata.ObjectName,
                                ID = obj.ID,
                                SecondaryKey = obj.SecondaryKey,
                                Value = obj.Value,
                                Indexes = null // indexes will be applied separately
                            });
                    },
                    (ObjectIndexRecord idx) =>
                    {
                        // first value is always the ID column
                        var idxValues = idx.Values;
                        var values = new object[idxValues.Length];
                        for (int i = 0; idxValues.Length > i; i++)
                        {
                            values[i] = idxValues[i].GetObjectValue();
                        }
                        bulkIndexValues.Add(values);
                    },
                    () =>
                    {
                        // write batched index values if any are queued up
                        if(0 < bulkIndexValues.Count) 
                        {
                            _objectIndexer.BulkIndexObject(
                                objectMetadata.ObjectFullName,
                                objectMetadata,
                                bulkIndexValues);

                            bulkIndexValues = new List<object[]>();
                        }
                    });
            }
        }

        public void CreateNameSpace(ObjectNameSpaceConfig nameSpaceConfig)
        {
            if (null == nameSpaceConfig)
            {
                throw new ArgumentNullException("nameSpaceConfig");
            }
            _objectMetadata.CreateNameSpace(nameSpaceConfig);
        }

        public ObjectNameSpaceConfig GetNameSpace(string nameSpace)
        {
            if (null == nameSpace)
            {
                throw new ArgumentNullException("nameSpace");
            }
            return _objectMetadata.GetNameSpace(nameSpace);
        }

        public void RemoveNameSpace(string nameSpace)
        {
            if (null == nameSpace)
            {
                throw new ArgumentNullException("nameSpace");
            }
            _objectMetadata.RemoveNameSpace(nameSpace);
        }

        public void ProvisionObjectStore(ObjectMetadata metadata)
        {
            if (null == metadata)
            {
                throw new ArgumentNullException("metadata");
            }
            else
            {
                if (string.IsNullOrEmpty(metadata.NameSpace))
                {
                    throw new ArgumentNullException("metadata.NameSpace");
                }
                else if (string.IsNullOrEmpty(metadata.ObjectName))
                {
                    throw new ArgumentNullException("metadata.Name");
                }
                else
                {
                    if (!_objectNaming.NameSpaceExists(metadata.NameSpace))
                    {
                        throw new ArgumentException("Unknown namespace: " + metadata.NameSpace);
                    }

                    // do not allow existing object store to be re-provisioned
                    if (ObjectNameExists(metadata.NameSpace, metadata.ObjectName))
                    {
                        throw new ArgumentException("Object store already exists: " + ObjectNaming.CreateFullObjectKey(metadata.NameSpace, metadata.ObjectName));
                    }

                    using (var trans = new TransactionScope(TransactionScopeOption.Required, _DefaultTransactionOptions))
                    {
                        // This call validates the format of the metadata and will throw and exception
                        // if it is invalid.
                        _objectMetadata.StoreMetadata(metadata);
                        if (null != metadata.Indexes && 0 < metadata.Indexes.Length)
                        {
                            _objectIndexer.ProvisionIndex(metadata);
                        }

                        trans.Complete();
                    }
                }
            }
        }

        public void UnprovisionObjectStore(string nameSpace, string objectName)
        {
            _ValidateArguments(nameSpace, objectName);

            var objectFullName = ObjectNaming.CreateFullObjectName(nameSpace, objectName);

            if (string.IsNullOrEmpty(nameSpace))
            {
                throw new ArgumentNullException("nameSpace");
            }
            else if (string.IsNullOrEmpty(objectName))
            {
                throw new ArgumentNullException("name");
            }
            else
            {
                var metadata = GetObjectMetadata(nameSpace, objectName);
                if (null != metadata)
                {
                    nameSpace = metadata.NameSpace;
                    objectName = metadata.ObjectName;

                    using (var trans = new TransactionScope(TransactionScopeOption.Required, _DefaultTransactionOptions))
                    {
                        if (null != metadata.Indexes && 0 < metadata.Indexes.Length)
                        {
                            if (_objectIndexer.ObjectExists(objectFullName))
                            {
                                _objectIndexer.Truncate(objectFullName);
                                _objectIndexer.UnprovisionIndex(objectFullName);
                            }
                        }

                        _objectStore.Truncate(objectFullName);
                        _objectMetadata.Remove(objectFullName);
                        _objectIDStore.Reset(objectFullName);
                        _objectVersions.Remove(objectFullName);
                        trans.Complete();
                    }
                }
            }
        }

        public ObjectMetadata GetObjectMetadata(string nameSpace, string objectName)
        {
            if (_ValidateArguments(nameSpace, objectName, true))
            {
                return _objectMetadata.GetMetadata(nameSpace, objectName);
            }
            else
            {
                return null;
            }
        }

        public void Truncate(string nameSpace, string objectName, bool resetIdentifiers)
        {
            _ValidateArguments(nameSpace, objectName);

            var objectFullName = ObjectNaming.CreateFullObjectName(nameSpace, objectName);

            using (var trans = new TransactionScope(TransactionScopeOption.Required, _DefaultTransactionOptions))
            {
                if (_objectIndexer.ObjectExists(objectFullName))
                {
                    _objectIndexer.Truncate(objectFullName);
                }

                _objectStore.Truncate(objectFullName);

                _objectVersions.Update(objectFullName);

                if (resetIdentifiers)
                {
                    _objectIDStore.Reset(objectFullName);
                }

                trans.Complete();
            }
        }

        public bool ObjectNameExists(string nameSpace, string objectName)
        {
            if (string.IsNullOrEmpty(nameSpace) || string.IsNullOrEmpty(objectName))
            {
                return false;
            }
            return _objectNaming.NameSpaceExists(nameSpace) & _objectNaming.ObjectNameExists(nameSpace, objectName);
        }

        public bool NameSpaceExists(string nameSpace)
        {
            if (string.IsNullOrEmpty(nameSpace))
            {
                return false;
            }
            return _objectNaming.NameSpaceExists(nameSpace);
        }

        public ObjectID[] BulkStore(string nameSpace, IEnumerable<PersistentObject> objects)
        {
            var returnValue = new List<ObjectID>();

            // group objects by Object Store name
            var objectsGrouped = new Dictionary<string, IList<PersistentObject>>();
            IList<PersistentObject> objectsList = null;

            // holds original object ID values in case a rollback is needed
            var objectIDrollback = new List<int>();

            foreach (var obj in objects)
            {
                // validate here since we're looping through the list already
                _ValidateArguments(nameSpace, obj);

                if (!objectsGrouped.ContainsKey(obj.Name))
                {
                    objectsList = new List<PersistentObject>();
                    objectsGrouped.Add(obj.Name, objectsList);
                }
                objectsList.Add(obj);
                objectIDrollback.Add(obj.ID);
            }

            try
            {
                using (var trans = new TransactionScope(TransactionScopeOption.Required, _DefaultTransactionOptions))
                {
                    // process each group of objects and batch insert the indexes
                    foreach (var objectName in objectsGrouped.Keys)
                    {
                        var group = objectsGrouped[objectName];

                        var objectFullName = ObjectNaming.CreateFullObjectName(nameSpace, objectName);
                        var objectMetadata = _objectMetadata.GetMetadata(objectFullName);
                        var indexValues = new List<object[]>();

                        foreach (var obj in group)
                        {
                            var startingObjId = obj.ID;
                            var objNameKey = ObjectNaming.CreateFullObjectKey(objectFullName);

                            var objId = obj.ID;
                            if (!obj.HasID())
                            {
                                objId = _objectIDStore.GetNextID(objNameKey);
                                obj.ID = objId;
                            }

                            returnValue.Add(new ObjectID()
                            {
                                ID = objId,
                                SecondaryKey = obj.SecondaryKey
                            });

                            if (null != obj.Indexes && 0 < obj.Indexes.Length)
                            {
                                var indexList = obj.Indexes.Select(i => i.GetObjectValue()).ToList();
                                // ID is always first index
                                indexList.Insert(0, obj.ID);
                                indexValues.Add(indexList.ToArray());
                            }
                        }
                        _objectIndexer.BulkIndexObject(objectFullName, objectMetadata, indexValues);
                    }
                    trans.Complete();
                }

                // store all objects

                foreach (var objectName in objectsGrouped.Keys)
                {
                    var group = objectsGrouped[objectName];
                    var objectFullName = ObjectNaming.CreateFullObjectName(nameSpace, objectName);
                    
                    foreach (var obj in group)
                    {
                        _objectStore.Set(nameSpace, obj);
                    }

                    _objectVersions.Update(objectFullName);
                }
            }
            catch (Exception)
            {
                // rollback in-memory IDs
                var objList = objects.ToArray();
                for (int i = 0; objectIDrollback.Count > i; i++)
                {
                    objList[i].ID = objectIDrollback[i];
                }

                throw;
            }

            return returnValue.ToArray();
        }

        public ObjectID Store(string nameSpace, PersistentObject obj)
        {
            ObjectID returnValue = null;

            _ValidateArguments(nameSpace, obj);

            var objectFullName = ObjectNaming.CreateFullObjectName(nameSpace, obj.Name);

            var startingObjId = obj.ID;

            var objNameKey = ObjectNaming.CreateFullObjectKey(objectFullName);

            var objId = obj.ID;

            if (!obj.HasID())
            {
                objId = _objectIDStore.GetNextID(objNameKey);
                obj.ID = objId;
            }

            returnValue = new ObjectID()
            {
                ID = objId,
                SecondaryKey = obj.SecondaryKey
            };

            try
            {
                using (var trans = new TransactionScope(TransactionScopeOption.Required, _DefaultTransactionOptions))
                {
                    if (null != obj.Indexes && 0 < obj.Indexes.Length)
                    {
                        _objectIndexer.IndexObject(objectFullName, obj.ID,
                            obj.Indexes);
                    }

                    _objectStore.Set(nameSpace, obj);
                    _objectVersions.Update(objectFullName);

                    trans.Complete();
                }
            }
            catch (Exception)
            {
                // rollback in memory changes
                obj.ID = startingObjId;

                throw;
            }

            return returnValue;
        }

        public byte[] Get(string nameSpace, string objectName, int id)
        {
            _ValidateArguments(nameSpace, objectName);

            var objectFullName = ObjectNaming.CreateFullObjectName(nameSpace, objectName);

            return _objectStore.Get(objectFullName, id);
        }

        public int GetNextObjectID(string nameSpace, string objectName)
        {
            _ValidateArguments(nameSpace, objectName);

            var objectFullKey = ObjectNaming.CreateFullObjectKey(nameSpace, objectName);

            return _objectIDStore.GetNextID(objectFullKey);
        }

        public byte[] GetBySecondaryKey(string nameSpace, string objectName, byte[] key)
        {
            _ValidateArguments(nameSpace, objectName);

            var objectFullName = ObjectNaming.CreateFullObjectName(nameSpace, objectName);

            if (null == key)
            {
                throw new ArgumentNullException("key");
            }

            return _objectStore.GetBySecondaryKey(objectFullName, key);
        }

        public IEnumerable<ObjectStoreRecord> Iterate(string nameSpace, string objectName)
        {
            _ValidateArguments(nameSpace, objectName);

            var objectFullName = ObjectNaming.CreateFullObjectName(nameSpace, objectName);

            return _objectStore.Iterate(objectFullName);
        }

        public int Count(string nameSpace, string objectName)
        {
            _ValidateArguments(nameSpace, objectName);

            var objectFullName = ObjectNaming.CreateFullObjectName(nameSpace, objectName);

            return _objectStore.Count(objectFullName);
        }

        public IEnumerable<byte[]> Find(string nameSpace, string objectName, string constraint)
        {
            return Find(nameSpace, objectName, constraint, 0, null);
        }

        public IEnumerable<byte[]> Find(string nameSpace, string objectName, string constraint, uint limit, OrderOptions order)
        {
            _ValidateArguments(nameSpace, objectName);

            var objectFullName = ObjectNaming.CreateFullObjectName(nameSpace, objectName);

            var metadata = _objectMetadata.GetMetadata(objectFullName);

            if (null == metadata)
            {
                throw new InvalidOperationException("Metadata not found for object: " + objectFullName);
            }

            var objectIds = _objectIndexer.Find(objectFullName, constraint, limit, order, metadata.Indexes);
            foreach (var objectId in objectIds)
            {
                yield return _objectStore.Get(objectFullName, objectId);
            }
        }

        public IEnumerable<byte[]> Find(string nameSpace, string objectName, ObjectFindOptions options, ObjectIndex[] indexes)
        {
            _ValidateArguments(nameSpace, objectName);

            var objectFullName = ObjectNaming.CreateFullObjectName(nameSpace, objectName);

            var objectIds = _objectIndexer.Find(objectFullName, options, indexes);
            foreach (var objectId in objectIds)
            {
                yield return _objectStore.Get(objectFullName, objectId);
            }
        }

        public void Remove(string nameSpace, string objectName, int id)
        {
            _ValidateArguments(nameSpace, objectName);

            var objectFullName = ObjectNaming.CreateFullObjectName(nameSpace, objectName);

            using (var trans = new TransactionScope(TransactionScopeOption.Required, _DefaultTransactionOptions))
            {
                if (_objectIndexer.ObjectExists(objectFullName))
                {
                    _objectIndexer.RemoveObjectIndex(objectFullName, id);
                }
                _objectStore.Remove(objectFullName, id);

                trans.Complete();
            }
        }

        public void Remove(string nameSpace, string objectName, int[] ids)
        {
            _ValidateArguments(nameSpace, objectName);

            var objectFullName = ObjectNaming.CreateFullObjectName(nameSpace, objectName);

            using (var trans = new TransactionScope(TransactionScopeOption.Required, _DefaultTransactionOptions))
            {
                if (_objectIndexer.ObjectExists(objectFullName))
                {
                    _objectIndexer.RemoveObjectIndexes(objectFullName, ids);
                }
                foreach (var id in ids)
                {
                    _objectStore.Remove(objectFullName, id);
                }
                _objectVersions.Update(objectFullName);

                trans.Complete();
            }
        }

        #endregion

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
                    foreach (var disposable in _assignments)
                    {
                        try
                        {
                            disposable.Dispose();
                        }
                        catch
                        {
                            // ignore here
                        }
                    }
                }

                _disposed = true;
            }

        }
        #endregion
    }
}
