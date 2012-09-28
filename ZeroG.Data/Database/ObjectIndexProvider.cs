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
using ZeroG.Data.Database.Lang;
using ZeroG.Data.Object;
using ZeroG.Data.Object.Index;
using ZeroG.Data.Object.Metadata;

namespace ZeroG.Data.Database
{
    public abstract class ObjectIndexProvider : IObjectIndexProvider
    {
        internal static readonly string DefaultSchemaConnection = "ObjectIndexSchema";
        internal static readonly string DefaultDataAccessConnection = "ObjectIndexData";

        private IDatabaseService _dbSchema, _dbData;

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
            : this(Config.Default)
        {
        }

        public ObjectIndexProvider(Config config)
            : this(config.ObjectIndexSchemaConnection, config.ObjectIndexDataConnection)
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

        public static IDbDataParameter MakeLikeParameter(IDatabaseService db, string paramName, object value)
        {
            IDbDataParameter dbParam = null;

            if (null != value && value is string)
            {
                var strVal = (string)value;
                bool wildCardPrefix = false;
                bool wildCardSuffix = false;
                if (strVal.StartsWith("%"))
                {
                    wildCardPrefix = true;
                    strVal = strVal.TrimStart('%');
                }
                if (strVal.EndsWith("%") && !strVal.EndsWith("\\%"))
                {
                    wildCardSuffix = true;
                    strVal = strVal.TrimEnd('%');
                }
                dbParam = db.MakeLikeParam(paramName, strVal);
                strVal = (string)dbParam.Value;
                if (wildCardPrefix)
                {
                    strVal = "%" + strVal;
                }
                if (wildCardSuffix)
                {
                    strVal = strVal + "%";
                }
                dbParam.Value = strVal;
            }
            else
            {
                dbParam = db.MakeLikeParam(paramName, value);
            }

            return dbParam;
        }

        protected virtual IDatabaseService OpenSchema()
        {
            if (null == _dbSchema || _dbSchema.IsClosed)
            {
                _dbSchema = DatabaseService.GetService(_databaseServiceSchema);
                _dbSchema.Open();
            }
            return _dbSchema;
        }

        protected virtual IDatabaseService OpenData()
        {
            if (null == _dbData || _dbData.IsClosed)
            {
                _dbData = DatabaseService.GetService(_databaseServiceData);
                _dbData.Open();
            }
            return _dbData;
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

        public abstract bool ObjectExists(string objectFullName);
        public abstract bool Exists(string objectFullName, string constraint, ObjectIndexMetadata[] indexes);
        public abstract int Count(string objectFullName, string constraint, ObjectIndexMetadata[] indexes);
        public abstract int[] Find(string objectFullName, params ObjectIndex[] indexes);
        public abstract int[] Find(string objectFullName, ObjectFindLogic logic, ObjectFindOperator oper, params ObjectIndex[] indexes);
        public abstract int[] Find(string objectFullName, string constraint, ObjectIndexMetadata[] indexes);
        public abstract void ProvisionIndex(ObjectMetadata metadata);
        public abstract void UnprovisionIndex(string objectFullName);
        public abstract void UpsertIndexValues(string objectFullName, int objectId, params ObjectIndex[] indexes);
        public abstract void RemoveIndexValue(string objectFullName, int objectId);
        public abstract void RemoveIndexValues(string objectFullName, int[] objectIds);
        public abstract void Truncate(string objectFullName);
        public virtual void Close()
        {
            if (null != _dbSchema)
            {
                _dbSchema.Dispose();
            }
            if (null != _dbData)
            {
                _dbData.Dispose();
            }
        }
        public virtual void Dispose()
        {
            Close();
        }
    }
}
