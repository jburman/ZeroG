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
using System.Configuration;
using System.Data;
using ZeroG.Data.Database;
using ZeroG.Data.Object.Cache;
using ZeroG.Data.Object.Metadata;

namespace ZeroG.Data.Object.Index
{
    internal sealed class ObjectIndexer : IDisposable
    {
        private IObjectIndexProvider _indexer;
        private ObjectIndexerCache _cache;

        private static Type _indexerType = null;

        public ObjectIndexer(ObjectIndexerCache cache) : this()
        {
            _cache = cache;
        }

        public ObjectIndexer()
        {
            if (null == _indexerType)
            {
                var setting = ConfigurationManager.AppSettings[Config.ObjectIndexProviderConfigKey];
                if (!string.IsNullOrEmpty(setting))
                {
                    var objectIndexProviderType = Type.GetType(setting, true);

                    if (typeof(IObjectIndexProvider).IsAssignableFrom(objectIndexProviderType))
                    {
                        _indexer = (IObjectIndexProvider)Activator.CreateInstance(objectIndexProviderType);
                        _indexerType = objectIndexProviderType;
                    }
                    else
                    {
                        throw new InvalidOperationException("Unsupported IObjectIndexProvider type: " + objectIndexProviderType.FullName);
                    }
                }
            }
            else
            {
                _indexer = (IObjectIndexProvider)Activator.CreateInstance(_indexerType);
            }
        }

        private bool _ValidateIndexes(ObjectIndex[] indexes)
        {
            foreach (var idx in indexes)
            {
                var dataType = idx.DataType;
                if (null == idx.Value || 0 == idx.Value.Length)
                {
                    throw new ArgumentNullException("idx.Value", "Index values cannot be null.");
                }
                else if (ObjectIndexType.Unknown == dataType)
                {
                    throw new ArgumentException("ObjectIndexType is Unknown. Unable to store index value.");
                }
            }
            return true;
        }

        private bool _ValidateIndexNames(ObjectIndexMetadata[] indexes, string[] checkNames)
        {
            bool returnValue = true;

            if (null != checkNames)
            {
                var namesLen = checkNames.Length;
                int indexesLen = 0;

                if (null != indexes)
                {
                    indexesLen = indexes.Length;
                }

                for (int i = 0; namesLen > i; i++)
                {
                    var name = checkNames[i];
                    if (!ObjectNameValidator.IsValidIndexName(name))
                    {
                        returnValue = false;
                        break;
                    }

                    // All objects have a built int ID index, which is not part of its declared metadata indexes.
                    if (ObjectIndexProvider.IDColumn.Equals(name, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    if (null != indexes)
                    {
                        bool foundName = false;
                        for (int j = 0; indexesLen > j; j++)
                        {
                            if (name.Equals(indexes[i].Name))
                            {
                                foundName = true;
                            }
                        }
                        // we did not find the index name
                        if (!foundName)
                        {
                            returnValue = false;
                            break;
                        }
                    }
                }
            }

            return returnValue;
        }

        private void _ValidateOrderOptions(ObjectIndexMetadata[] indexes, OrderOptions order)
        {
            if (null != order && null != order.Indexes)
            {
                if (!_ValidateIndexNames(indexes, order.Indexes))
                {
                    throw new ArgumentException("Invalid index name supplied in OrderOptions.");
                }
            }
        }

        public bool ObjectExists(string objectFullName)
        {
            return _indexer.ObjectExists(objectFullName);
        }

        internal int[] Find(string objectFullName, ObjectFindOptions options, ObjectIndex[] indexes)
        {
            int[] returnValue = null;

            if (null != _cache)
            {
                var parameters = new object[2 + ((null == indexes) ? 0 : indexes.Length)];
                parameters[0] = objectFullName;
                parameters[1] = options;
                if (null != indexes)
                {
                    for (int i = 0; indexes.Length > i; i++)
                    {
                        parameters[i + 2] = indexes[i];
                    }
                }
                returnValue = _cache.Get(parameters);
                if (null == returnValue)
                {
                    var order = options.Order;
                    if (null != order && null != order.Indexes)
                    {
                        if (!_ValidateIndexNames(null, order.Indexes))
                        {
                            throw new ArgumentException("Invalid index name supplied in OrderOptions.");
                        }
                    }

                    returnValue = _indexer.Find(objectFullName, options, indexes);
                    _cache.Set(returnValue, parameters);
                }
            }
            else
            {
                returnValue = _indexer.Find(objectFullName, options, indexes);
            }
            return returnValue;
        }

        public int[] Find(string objectFullName, string constraint, ObjectIndexMetadata[] indexes)
        {
            return Find(objectFullName, constraint, 0, null, indexes);
        }

        public int[] Find(string objectFullName, string constraint, uint limit, OrderOptions order, ObjectIndexMetadata[] indexes)
        {
            int[] returnValue = null;

            if (null != _cache)
            {
                var parameters = new object[4 + ((null == indexes) ? 0 : indexes.Length)];
                parameters[0] = objectFullName;
                parameters[1] = constraint;
                parameters[2] = limit;
                parameters[3] = order;
                if (null != indexes)
                {
                    for (int i = 0; indexes.Length > i; i++)
                    {
                        parameters[i + 3] = indexes[i];
                    }
                }
                returnValue = _cache.Get(parameters);
                if (null == returnValue)
                {
                    _ValidateOrderOptions(indexes, order);

                    returnValue = _indexer.Find(objectFullName, constraint, limit, order, indexes);
                    _cache.Set(returnValue, parameters);
                }
            }
            else
            {
                returnValue = _indexer.Find(objectFullName, constraint, limit, order, indexes);
            }
            return returnValue;
        }

        public bool Exists(string objectFullName, string constraint, ObjectIndexMetadata[] indexes)
        {
            return _indexer.Exists(objectFullName, constraint, indexes);
        }

        public IEnumerable<IDataRecord> Iterate(string objectFullName, ObjectIndexMetadata[] indexes)
        {
            return _indexer.Iterate(objectFullName, indexes);
        }

        public IEnumerable<IDataRecord> Iterate(string objectFullName, string constraint, uint limit, OrderOptions order, string[] iterateIndexes, ObjectIndexMetadata[] indexes)
        {
            _ValidateOrderOptions(indexes, order);

            if (null != iterateIndexes && 0 == iterateIndexes.Length)
            {
                _ValidateIndexNames(indexes, iterateIndexes);
            }

            return _indexer.Iterate(objectFullName, constraint, limit, order, iterateIndexes, indexes);
        }

        public void IndexObject(string objectFullName, int objectId, ObjectIndex[] indexes)
        {
            if (_ValidateIndexes(indexes))
            {
                _indexer.UpsertIndexValues(objectFullName, objectId, indexes);
            }
        }

        public void RemoveObjectIndex(string objectFullName, int objectId)
        {
            _indexer.RemoveIndexValue(objectFullName, objectId);
        }

        public void RemoveObjectIndexes(string objectFullName, int[] objectIds)
        {
            _indexer.RemoveIndexValues(objectFullName, objectIds);
        }

        public void ProvisionIndex(ObjectMetadata metadata)
        {
            _indexer.ProvisionIndex(metadata);
        }

        public void UnprovisionIndex(string objectFullName)
        {
            _indexer.UnprovisionIndex(objectFullName);
        }

        public void Truncate(string objectFullName)
        {
            _indexer.Truncate(objectFullName);
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
                }
                _disposed = true;
            }

        }
        #endregion
    }
}
