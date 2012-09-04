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
using ZeroG.Data.Database.Lang;
using ZeroG.Data.Object.Index;
using ZeroG.Data.Object.Metadata;

namespace ZeroG.Data.Database
{
    public abstract class ObjectIndexProvider : IObjectIndexProvider
    {

        #region Config settings
        // TODO: make config settings configurable from app.config

        /// <summary>
        /// Maximum number of IDs that will be passed to a single query.
        /// For example, when deleting a large number of Objects. Queries will be batched if the 
        /// the supplied number of IDs exceeds the constraint limit.
        /// </summary>
        protected static int MaxIDConstraint
        {
            get
            {
                return 100;
            }
        }

        protected static string IDColumn
        {
            get
            {
                return "ID";
            }
        }

        private string _databaseServiceSchema;
        private string _databaseServiceData;

        #endregion

        public ObjectIndexProvider()
            : this("ObjectIndexSchema", "ObjectIndexData")
        {
        }

        public ObjectIndexProvider(string databaseServiceSchema, string databaseServiceData)
        {
            if (string.IsNullOrEmpty(databaseServiceSchema))
            {
                throw new ArgumentException("Missing value for required parameter",  "databaseServiceSchema");
            }

            if (string.IsNullOrEmpty(databaseServiceData))
            {
                throw new ArgumentException("Missing value for required parameter", "databaseServiceData");
            }

            _databaseServiceSchema = databaseServiceSchema;
            _databaseServiceData = databaseServiceData;
        }

        protected virtual IDatabaseService OpenSchema()
        {
            var db = DatabaseService.GetService(_databaseServiceSchema);
            db.Open();
            return db;
        }

        protected virtual IDatabaseService OpenData()
        {
            var db = DatabaseService.GetService(_databaseServiceData);
            db.Open();
            return db;
        }

        protected virtual SQLConstraint CreateSQLConstraint(IDatabaseService db, ObjectIndexMetadata[] indexes, string constraint)
        {
            var typeMappings = new Dictionary<string, Type>(StringComparer.InvariantCultureIgnoreCase);
            foreach (var idx in indexes)
            {
                typeMappings.Add(idx.Name, idx.DataType.GetSystemType());
            }
            return JSONToSQLConstraint.GenerateSQLConstraint(db, typeMappings, constraint);
        }

        public abstract int[] Find(string nameSpace, string objectName, params ObjectIndex[] indexes);
        public abstract int[] Find(string nameSpace, string objectName, string constraint, ObjectIndexMetadata[] indexes);
        public abstract void ProvisionIndex(ObjectMetadata metadata);
        public abstract void UnprovisionIndex(string nameSpace, string objectName);
        public abstract void UpsertIndexValues(string nameSpace, string objectName, int objectId, params ObjectIndex[] indexes);
        public abstract void RemoveIndexValue(string nameSpace, string objectName, int objectId);
        public abstract void RemoveIndexValues(string nameSpace, string objectName, int[] objectIds);
        public abstract void Truncate(string nameSpace, string objectName);
        public abstract void Dispose();
    }
}
