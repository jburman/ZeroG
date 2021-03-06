﻿#region License, Terms and Conditions
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

using System.Collections.Generic;

namespace ZeroG.Data.Object
{
    public class BulkStore
    {
        public delegate IEnumerable<ObjectID> BulkStoreOperation(IEnumerable<PersistentObject> bulkStoreObjects);

        private IObjectServiceClient _client;
        private BulkStoreOperation _bulkStore;
        private List<PersistentObject> _bulkStoreObjects;

        private BulkStore()
        {
        }

        public BulkStore(IObjectServiceClient client, BulkStoreOperation bulkStore)
        {
            _client = client;
            _bulkStore = bulkStore;
            _bulkStoreObjects = new List<PersistentObject>();
        }

        public void Add(byte[] value, ObjectIndex[] indexes)
        {
            Add(null, value, indexes);
        }

        public void Add(byte[] secondaryKey, byte[] value, ObjectIndex[] indexes)
        {
            _bulkStoreObjects.Add(
                new PersistentObject()
                {
                    Name = _client.ObjectName,
                    SecondaryKey = secondaryKey,
                    Value = value,
                    Indexes = indexes
                });
        }

        public void Add(int objectId, byte[] value)
        {
            Add(objectId, null, value, null);
        }

        public void Add(int objectId, byte[] secondaryKey, byte[] value, ObjectIndex[] indexes)
        {
            _bulkStoreObjects.Add(
                new PersistentObject()
                {
                    Name = _client.ObjectName,
                    ID = objectId,
                    SecondaryKey = secondaryKey,
                    Value = value,
                    Indexes = indexes
                });
        }

        public IEnumerable<ObjectID> Complete()
        {
            return _bulkStore(_bulkStoreObjects);
        }
    }
}
