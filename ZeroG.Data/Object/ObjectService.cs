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
using System.Linq;
using System.Text;
using System.Transactions;
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
        private ObjectCache _cache;

        private List<IDisposable> _assignments;

        private static TransactionOptions _DefaultTransactionOptions;

        static ObjectService()
        {
            _DefaultTransactionOptions = new TransactionOptions();
            _DefaultTransactionOptions.IsolationLevel = IsolationLevel.RepeatableRead;
            _DefaultTransactionOptions.Timeout = TransactionManager.DefaultTimeout;
        }

        public ObjectService()
        {
            _assignments = new List<IDisposable>();

            _objectMetadata = new ObjectMetadataStore();
            _objectNaming = new ObjectNaming(_objectMetadata);
            _objectIDStore = new ObjectIDStore();
            _objectVersions = new ObjectVersionStore(_objectMetadata);
            _objectStore = new ObjectStore(_objectMetadata);
            _objectIndexer = new ObjectIndexer();

            _assignments.Add(_objectMetadata);
            _assignments.Add(_objectIDStore);
            _assignments.Add(_objectVersions);
            _assignments.Add(_objectStore);
            _assignments.Add(_objectIndexer);

            if (Config.CacheEnabled)
            {
                _cache = new ObjectCache(_objectMetadata, _objectVersions);
            }
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
                            if (_objectIndexer.Exists(nameSpace, objectName))
                            {
                                _objectIndexer.Truncate(nameSpace, objectName);
                                _objectIndexer.UnprovisionIndex(nameSpace, objectName);
                            }
                        }

                        _objectStore.Truncate(nameSpace, objectName);
                        _objectMetadata.RemoveMetadata(nameSpace, objectName);
                        _objectIDStore.Reset(ObjectNaming.CreateFullObjectKey(nameSpace, objectName));

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

        public ObjectID Store(string nameSpace, PersistentObject obj)
        {
            ObjectID returnValue = null;

            _ValidateArguments(nameSpace, obj);

            var startingObjId = obj.ID;
            var startingObjUniqueId = obj.UniqueID;

            var objNameKey = ObjectNaming.CreateFullObjectKey(nameSpace, obj.Name);

            var objId = obj.ID;
            var objUniqueId = obj.UniqueID;

            if (!obj.HasID())
            {
                objId = _objectIDStore.GetNextID(objNameKey);
                obj.ID = objId;
            }

            if (!obj.HasUniqueID())
            {
                objUniqueId = Guid.NewGuid().ToByteArray();
                obj.UniqueID = objUniqueId;
            }

            returnValue = new ObjectID()
            {
                ID = objId,
                UniqueID = objUniqueId
            };

            try
            {
                using (var trans = new TransactionScope(TransactionScopeOption.Required, _DefaultTransactionOptions))
                {
                    if (null != obj.Indexes && 0 < obj.Indexes.Length)
                    {
                        _objectIndexer.IndexObject(nameSpace, obj);
                    }

                    _objectStore.Set(nameSpace, obj);

                    trans.Complete();
                }
            }
            catch (Exception)
            {
                // rollback in memory changes
                obj.ID = startingObjId;
                obj.UniqueID = startingObjUniqueId;

                throw;
            }

            return returnValue;
        }

        public byte[] Get(string nameSpace, string objectName, int id)
        {
            _ValidateArguments(nameSpace, objectName);

            return _objectStore.Get(nameSpace, objectName, id);
        }

        public byte[] GetByUniqueID(string nameSpace, string objectName, byte[] uniqueId)
        {
            _ValidateArguments(nameSpace, objectName);

            if (null == uniqueId)
            {
                throw new ArgumentNullException("uniqueId");
            }

            return _objectStore.GetByUniqueID(nameSpace, objectName, uniqueId);
        }

        public IEnumerable<byte[]> Find(string nameSpace, string objectName, string constraint)
        {
            _ValidateArguments(nameSpace, objectName);

            var metadata = _objectMetadata.GetMetadata(ObjectNaming.CreateFullObjectName(nameSpace, objectName));

            if (null == metadata)
            {
                throw new InvalidOperationException("Metadata not found for object: " + ObjectNaming.CreateFullObjectName(nameSpace, objectName));
            }

            var objectIds = _objectIndexer.Find(nameSpace, objectName, constraint, metadata.Indexes);
            foreach (var objectId in objectIds)
            {
                yield return _objectStore.Get(nameSpace, objectName, objectId);
            }
        }

        public IEnumerable<byte[]> FindWhereEqualsAnd(string nameSpace, string objectName, ObjectIndex[] indexes)
        {
            _ValidateArguments(nameSpace, objectName);

            var objectIds = _Find(nameSpace, objectName, ObjectFindLogic.And, ObjectFindOperator.Equals, indexes);
            foreach (var objectId in objectIds)
            {
                yield return _objectStore.Get(nameSpace, objectName, objectId);
            }
        }

        public IEnumerable<byte[]> FindWhereLikeAnd(string nameSpace, string objectName, ObjectIndex[] indexes)
        {
            _ValidateArguments(nameSpace, objectName);

            var objectIds = _Find(nameSpace, objectName, ObjectFindLogic.And, ObjectFindOperator.Like, indexes);
            foreach (var objectId in objectIds)
            {
                yield return _objectStore.Get(nameSpace, objectName, objectId);
            }
        }

        public IEnumerable<byte[]> FindWhereEqualsOr(string nameSpace, string objectName, ObjectIndex[] indexes)
        {
            _ValidateArguments(nameSpace, objectName);

            var objectIds = _Find(nameSpace, objectName, ObjectFindLogic.Or, ObjectFindOperator.Equals, indexes);
            foreach (var objectId in objectIds)
            {
                yield return _objectStore.Get(nameSpace, objectName, objectId);
            }
        }

        public IEnumerable<byte[]> FindWhereLikeOr(string nameSpace, string objectName, ObjectIndex[] indexes)
        {
            _ValidateArguments(nameSpace, objectName);

            var objectIds = _Find(nameSpace, objectName, ObjectFindLogic.Or, ObjectFindOperator.Like, indexes);
            foreach (var objectId in objectIds)
            {
                yield return _objectStore.Get(nameSpace, objectName, objectId);
            }
        }

        internal int[] _Find(string nameSpace, string objectName, ObjectFindLogic logic, ObjectFindOperator oper, ObjectIndex[] indexes)
        {
            return _objectIndexer.Find(nameSpace, objectName, logic, oper, indexes);
        }

        public void Remove(string nameSpace, string objectName, int id)
        {
            _ValidateArguments(nameSpace, objectName);

            using (var trans = new TransactionScope(TransactionScopeOption.Required, _DefaultTransactionOptions))
            {
                _objectIndexer.RemoveObjectIndex(nameSpace, objectName, id);

                _objectStore.Remove(nameSpace, objectName, id);

                trans.Complete();
            }
        }

        public void Remove(string nameSpace, string objectName, int[] ids)
        {
            _ValidateArguments(nameSpace, objectName);

            using (var trans = new TransactionScope(TransactionScopeOption.Required, _DefaultTransactionOptions))
            {
                _objectIndexer.RemoveObjectIndexes(nameSpace, objectName, ids);

                foreach (var id in ids)
                {
                    _objectStore.Remove(nameSpace, objectName, id);
                }

                trans.Complete();
            }
        }

        public void Truncate(string nameSpace, string objectName, bool resetIdentifiers)
        {
            _ValidateArguments(nameSpace, objectName);

            using (var trans = new TransactionScope(TransactionScopeOption.Required, _DefaultTransactionOptions))
            {
                _objectIndexer.Truncate(nameSpace, objectName);

                _objectStore.Truncate(nameSpace, objectName);

                if (resetIdentifiers)
                {
                    _objectIDStore.Reset(ObjectNaming.CreateFullObjectKey(nameSpace, objectName));
                }

                trans.Complete();
            }
        }

        public bool ObjectNameExists(string nameSpace, string objName)
        {
            if (string.IsNullOrEmpty(nameSpace) || string.IsNullOrEmpty(objName))
            {
                return false;
            }
            return _objectNaming.NameSpaceExists(nameSpace) & _objectNaming.ObjectNameExists(nameSpace, objName);
        }

        public bool NameSpaceExists(string nameSpace)
        {
            if (string.IsNullOrEmpty(nameSpace))
            {
                return false;
            }
            return _objectNaming.NameSpaceExists(nameSpace);
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
