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
using System.Data;
using ZeroG.Data.Object.Metadata;

namespace ZeroG.Data.Object.Index
{
    public interface IObjectIndexProvider : IDisposable
    {
        bool ObjectExists(string objectFullName);

        bool Exists(string objectFullName, string constraint, ObjectIndexMetadata[] indexes);

        /// <summary>
        /// Counts distinct object IDs stored in the index.
        /// </summary>
        /// <param name="objectFullName"></param>
        /// <returns></returns>
        int CountObjects(string objectFullName);

        int Count(string objectFullName, ObjectFindOptions options, ObjectIndex[] indexes);

        int Count(string objectFullName, string constraint, ObjectIndexMetadata[] indexes);

        ObjectIndexRecord CreateRecord(IDataRecord record);

        int[] Find(string objectFullName, params ObjectIndex[] indexes);

        int[] Find(string objectFullName, ObjectFindOptions options, params ObjectIndex[] indexes);

        int[] Find(string objectFullName, string constraint, ObjectIndexMetadata[] indexes);

        int[] Find(string objectFullName, string constraint, uint limit, OrderOptions order, ObjectIndexMetadata[] indexes);

        IEnumerable<IDataRecord> Iterate(string objectFullName, ObjectIndexMetadata[] indexes);

        IEnumerable<IDataRecord> Iterate(string objectFullName, string constraint, uint limit, OrderOptions order, string[] iterateIndexes, ObjectIndexMetadata[] indexes);

        void ProvisionIndex(ObjectMetadata metadata);

        void UnprovisionIndex(string objectFullName);

        void UpsertIndexValues(string objectFullName, int objectId, params ObjectIndex[] indexes);

        void BulkUpsertIndexValues(string objectFullName, ObjectMetadata metadata, IEnumerable<object[]> indexValues);

        void RemoveIndexValue(string objectFullName, int objectId);

        void RemoveIndexValues(string objectFullName, int[] objectIds);

        void Truncate(string objectFullName);

        void Close();
    }
}
