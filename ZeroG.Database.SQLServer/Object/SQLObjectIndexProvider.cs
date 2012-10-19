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
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using ZeroG.Data.Database;
using ZeroG.Data.Object;
using ZeroG.Data.Object.Index;
using ZeroG.Data.Object.Metadata;

namespace ZeroG.Data.Database.Drivers.Object.Provider
{
    internal class SQLStatements
    {
        public static readonly string TableExists = @"select COUNT(*) from sysobjects where name='{0}' and xtype='U'";

        public static readonly string NoTop = "";

        public static readonly string Top = " TOP {0} ";

        public static readonly string NoOrder = "";

        public static readonly string OrderAsc = " ORDER BY {0} ASC";

        public static readonly string OrderDesc = " ORDER BY {0} DESC";

        public static readonly string RowsExist = @"SELECT 1 FROM [ZeroG].[{0}] WHERE {1}";

        public static readonly string RowsCount = @"SELECT COUNT(1) FROM [ZeroG].[{0}] WHERE {1}";

        public static readonly string RowsCountDistinctObjects = @"SELECT COUNT(DISTINCT {0}) FROM [ZeroG].[{1}]";

        public static readonly string CreateTableIfNotExists = @"IF NOT EXISTS (select * from sysobjects where name='{0}' and xtype='U')
    CREATE TABLE [ZeroG].[{0}](
	    {1}
	CONSTRAINT [PK_{0}] PRIMARY KEY CLUSTERED 
	    ([ID] ASC)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [{2}]
	) ON [{2}]";

        public static readonly string CreateStagingTableIfNotExists = @"IF NOT EXISTS (select * from sysobjects where name='{0}' and xtype='U')
    CREATE TABLE [ZeroG].[{0}](
	    {1}
	) ON [{2}]";

        public static readonly string CreateIndex = @"IF EXISTS (select * from sysobjects where name='{0}' and xtype='U')
	CREATE NONCLUSTERED INDEX [IDX_{0}] ON [ZeroG].[{0}](
		{1}
	)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [{2}]";

        public static readonly string DropTableIfExists = @"IF EXISTS (select * from sysobjects where name='{0}' and xtype='U')
    DROP TABLE [ZeroG].[{0}]";

        public static readonly string Find = @"SELECT {2}[ID] FROM [ZeroG].[{0}]
WHERE {1}{3}";

        public static readonly string Iterate = @"SELECT {4}{1} FROM [ZeroG].[{0}]{2}{3}";

        public static string RemoveIndex = @"DELETE FROM [ZeroG].[{0}] WHERE [{1}] IN ({2})";

        public static readonly string TruncateTable = "TRUNCATE TABLE [ZeroG].[{0}]";
    }

    public class SQLObjectIndexProvider : ObjectIndexProvider
    {

        #region Config settings
        // TODO: make config settings configurable from app.config

        private static string _FileGroup
        {
            get
            {
                return "PRIMARY";
            }
        }

        #endregion

        public SQLObjectIndexProvider()
            : base()
        {
        }

        public SQLObjectIndexProvider(Config config)
            : base(config)
        {
        }

        public SQLObjectIndexProvider(string databaseServiceSchema, string databaseServiceData)
            : base(databaseServiceSchema, databaseServiceData)
        {
        }

        private static string _CreateTableName(IDatabaseService db, string objectFullName)
        {
            return db.EscapeCommandText(objectFullName.Replace('.', '_'));
        }

        private static string _CreateStagingTableName(IDatabaseService db, string objectFullName)
        {
            return _CreateTableName(db, objectFullName + "_stage");
        }

        private static string _CreateColumnDef(IDatabaseService db, ObjectIndexMetadata indexMetadata)
        {
            string name = db.EscapeCommandText(indexMetadata.Name);
            string type = "nvarchar";
            string length = "(30)";

            switch (indexMetadata.DataType)
            {
                case ObjectIndexType.Integer:
                    type = "int";
                    length = "";
                    break;
                case ObjectIndexType.Binary:
                    type = "binary";
                    length = "(" + indexMetadata.Precision + ")";
                    break;
                case ObjectIndexType.DateTime:
                    type = "datetime";
                    length = "";
                    break;
                case ObjectIndexType.Decimal:
                    type = "decimal";
                    length = "(" + indexMetadata.Precision + "," + indexMetadata.Scale + ")";
                    break;
                default:
                    length = "(" + indexMetadata.Precision + ")";
                    break;
            }

            return string.Format("[{0}] [{1}]{2} NOT NULL", name, type, length);
        }

        private static string _CreateOrderBySQL(IDatabaseService db, OrderOptions order)
        {
            var orderBySql = SQLStatements.NoOrder;
            if (null != order)
            {
                string orderColSql = string.Join(",", order.Indexes.Select(i => db.MakeQuotedName(i)).ToArray());

                if (order.Descending)
                {
                    orderBySql = string.Format(SQLStatements.OrderDesc, orderColSql);
                }
                else
                {
                    orderBySql = string.Format(SQLStatements.OrderAsc, orderColSql);
                }
            }
            return orderBySql;
        }

        private bool _TableExists(IDatabaseService db, string tableName)
        {
            return 0 < db.ExecuteScalar<int>(string.Format(SQLStatements.TableExists, tableName), 0);
        }

        private void _ProvisionIndexStaging(IDatabaseService db, ObjectMetadata metadata)
        {
            var tableName = _CreateStagingTableName(db, metadata.ObjectFullName);
            string idColName = db.MakeQuotedName(IDColumn);
            string colDefs = idColName + " [int] NOT NULL";
            string colIndexNames = idColName;

            if (null != metadata.Indexes && 0 < metadata.Indexes.Length)
            {
                colDefs += "," + string.Join(",", metadata.Indexes.Select(i => _CreateColumnDef(db, i)).ToArray());
                colIndexNames += "," + string.Join(",", metadata.Indexes.Select(i => db.MakeQuotedName(i.Name)).ToArray());
            }

            var createTableSQL = string.Format(SQLStatements.CreateStagingTableIfNotExists, tableName, colDefs, _FileGroup);
            db.ExecuteNonQuery(createTableSQL);
        }

        private void _UnprovisionIndexStaging(IDatabaseService db, string objectFullName)
        {
            var tableName = _CreateStagingTableName(db, objectFullName);

            if (_TableExists(db, tableName))
            {
                var dropTableSQL = string.Format(SQLStatements.DropTableIfExists, tableName);
                db.ExecuteNonQuery(dropTableSQL);
            }
        }

        private void _StageBulkIndexValues(string objectFullName, ObjectMetadata metadata, IEnumerable<object[]> records)
        {
            using (var db = OpenSchema())
            {
                var stagingTableName = _CreateStagingTableName(db, objectFullName);

                if (!_TableExists(db, stagingTableName))
                {
                    _ProvisionIndexStaging(db, metadata);
                }

                var colNames = new List<string>();
                colNames.Add(db.MakeQuotedName(IDColumn));
                foreach (var idx in metadata.Indexes)
                {
                    colNames.Add(db.MakeQuotedName(idx.Name));
                }

                db.ExecuteBulkInsert(
                    records,
                    "[ZeroG]." + db.MakeQuotedName(stagingTableName),
                    colNames.ToArray());
            }
        }

        private void _CleanStageBulkIndexValues(string objectFullName)
        {
            using (var db = OpenSchema())
            {
                var stagingTableName = _CreateStagingTableName(db, objectFullName);

                if (_TableExists(db, stagingTableName))
                {
                    db.ExecuteNonQuery("TRUNCATE TABLE [ZeroG]." + db.MakeQuotedName(stagingTableName));
                }
            }
        }

        private void _MergeBulkIndexValues(string objectFullName, ObjectMetadata metadata)
        {
            using (var db = OpenData())
            {
                string tableName = _CreateTableName(db, objectFullName);
                string stagingTableName = _CreateStagingTableName(db, objectFullName);

                var colNameList = new List<string>();
                var idCol = db.MakeQuotedName(IDColumn);
                colNameList.Add(idCol);
                string setStatement = idCol + " = source." + idCol;
                string valuesStatement = "source." + idCol;
                foreach (var idx in metadata.Indexes)
                {
                    var idxName = db.MakeQuotedName(idx.Name);
                    colNameList.Add(idxName);
                    setStatement += "," + idxName + " = source." + idxName;
                    valuesStatement += ",source." + idxName;
                }

                var sql = string.Format(@"MERGE [ZeroG].{0} AS target
    USING (SELECT {1} FROM [ZeroG].{2}) AS source ({1})
    ON (target.{3} = source.{3})
    WHEN MATCHED THEN 
        UPDATE SET {4}
	WHEN NOT MATCHED THEN	
	    INSERT ({1})
	    VALUES ({5});",
                    db.MakeQuotedName(tableName),                   // 0
                    string.Join(",", colNameList.ToArray()),        // 1
                    db.MakeQuotedName(stagingTableName),            // 2
                    idCol,                                          // 3
                    setStatement,                                   // 4
                    valuesStatement                                 // 5
                    );

                db.ExecuteNonQuery(sql);
            }
        }

        public override bool ObjectExists(string objectFullName)
        {
            using (var db = OpenSchema())
            {
                return _TableExists(db, _CreateTableName(db, objectFullName));
            }
        }

        public override bool Exists(string objectFullName, string constraint, ObjectIndexMetadata[] indexes)
        {
            using (var db = OpenData())
            {
                var sqlConstraint = CreateSQLConstraint(db, indexes, constraint);
                var tableName = _CreateTableName(db, objectFullName);
                return 1 == db.ExecuteScalar<int>(string.Format(SQLStatements.RowsExist, tableName, sqlConstraint.SQL), 0, sqlConstraint.Parameters.ToArray());
            }
        }

        public override int CountObjects(string objectFullName)
        {
            using (var db = OpenData())
            {
                var tableName = _CreateTableName(db, objectFullName);
                return db.ExecuteScalar<int>(string.Format(SQLStatements.RowsCountDistinctObjects, db.MakeQuotedName(IDColumn), tableName), 0);
            }
        }

        public override int Count(string objectFullName, string constraint, ObjectIndexMetadata[] indexes)
        {
            using (var db = OpenData())
            {
                var sqlConstraint = CreateSQLConstraint(db, indexes, constraint);
                var tableName = _CreateTableName(db, objectFullName);
                return db.ExecuteScalar<int>(string.Format(SQLStatements.RowsCount, tableName, sqlConstraint.SQL), 0, sqlConstraint.Parameters.ToArray());
            }
        }

        public override int[] Find(string objectFullName, ObjectFindOptions options, params ObjectIndex[] indexes)
        {
            int[] returnValue = null;
            var logic = options.Logic;
            var oper = options.Operator;
            var limit = options.Limit;

            bool useOr = ObjectFindLogic.Or == logic;
            bool useLike = ObjectFindOperator.Like == oper;

            using (var db = OpenData())
            {
                var tableName = _CreateTableName(db, objectFullName);
                var topSql = (0 == limit) ? SQLStatements.NoTop : string.Format(SQLStatements.Top, limit);

                var parameters = new List<IDbDataParameter>();
                var sqlConstraint = new StringBuilder();
                for (int i = 0; indexes.Length > i; i++)
                {
                    if (0 < i)
                    {
                        if (useOr)
                        {
                            sqlConstraint.Append(" OR ");
                        }
                        else
                        {
                            sqlConstraint.Append(" AND ");
                        }
                    }

                    var idx = indexes[i];
                    var paramName = "p" + i + idx.Name;
                    var value = idx.GetObjectValue();
                    sqlConstraint.Append(db.MakeQuotedName(idx.Name));

                    if (useLike)
                    {
                        sqlConstraint.Append(' ');
                        sqlConstraint.Append(db.MakeLikeParamReference(paramName));
                        parameters.Add(ObjectIndexProvider.MakeLikeParameter(db, paramName, value));
                    }
                    else
                    {
                        sqlConstraint.Append(" = ");
                        sqlConstraint.Append(db.MakeParamReference(paramName));
                        parameters.Add(db.MakeParam(paramName, value));
                    }
                }

                var orderBySql = _CreateOrderBySQL(db, options.Order);

                returnValue = db.GetValues<int>(string.Format(SQLStatements.Find, tableName, sqlConstraint.ToString(), topSql, orderBySql), parameters.ToArray());
            }

            return returnValue;
        }

        public override int[] Find(string objectFullName, params ObjectIndex[] indexes)
        {
            return Find(objectFullName, new ObjectFindOptions()
            {
                Logic = ObjectFindLogic.And,
                Operator = ObjectFindOperator.Equals
            }, indexes);
        }

        public override int[] Find(string objectFullName, string constraint, ObjectIndexMetadata[] indexes)
        {
            return Find(objectFullName, constraint, 0, null, indexes);
        }

        public override int[] Find(string objectFullName, string constraint, uint limit, OrderOptions order, ObjectIndexMetadata[] indexes)
        {
            using (var db = OpenData())
            {
                var sqlConstraint = CreateSQLConstraint(db, indexes, constraint);
                var tableName = _CreateTableName(db, objectFullName);
                var topSql = (0 == limit) ? SQLStatements.NoTop : string.Format(SQLStatements.Top, limit);

                var orderBySql = _CreateOrderBySQL(db, order);

                return db.GetValues<int>(string.Format(SQLStatements.Find, tableName, sqlConstraint.SQL, topSql, orderBySql), sqlConstraint.Parameters.ToArray());
            }
        }

        public override IEnumerable<IDataRecord> Iterate(string objectFullName, string constraint, uint limit, OrderOptions order, string[] iterateIndexes, ObjectIndexMetadata[] indexes)
        {
            using (var db = OpenData())
            {
                var tableName = _CreateTableName(db, objectFullName);
                var whereSql = string.Empty;
                IDataParameter[] parameters = null;
                if (!string.IsNullOrEmpty(constraint))
                {
                    var sqlConstraint = CreateSQLConstraint(db, indexes, constraint);
                    whereSql = " WHERE " + sqlConstraint.SQL;
                    parameters = sqlConstraint.Parameters.ToArray();
                }
                var orderBySql = _CreateOrderBySQL(db, order);
                var topSql = (0 == limit) ? SQLStatements.NoTop : string.Format(SQLStatements.Top, limit);

                string[] selectNames = null;

                if (null == iterateIndexes || 0 == iterateIndexes.Length)
                {
                    selectNames = new string[indexes.Length + 1];
                    selectNames[0] = db.MakeQuotedName(IDColumn);
                    for (int i = 0; indexes.Length > i; i++)
                    {
                        selectNames[i + 1] = db.MakeQuotedName(indexes[i].Name);
                    }
                }
                else
                {
                    selectNames = new string[iterateIndexes.Length];
                    for (int i = 0; iterateIndexes.Length > i; i++)
                    {
                        selectNames[i] = db.MakeQuotedName(iterateIndexes[i]);
                    }
                }

                string selectNamesSql = string.Join(",", selectNames);

                var reader = db.ExecuteReader(string.Format(SQLStatements.Iterate, tableName, selectNamesSql, whereSql, orderBySql, topSql), parameters);
                while (reader.Read())
                {
                    yield return reader;
                }
            }
        }

        public override void ProvisionIndex(ObjectMetadata metadata)
        {
            using (var db = OpenSchema())
            {
                var tableName = _CreateTableName(db, metadata.ObjectFullName);
                string idColName = db.MakeQuotedName(IDColumn);
                string colDefs = idColName + " [int] NOT NULL";
                string colIndexNames = idColName;

                if (null != metadata.Indexes && 0 < metadata.Indexes.Length)
                {
                    colDefs += "," + string.Join(",", metadata.Indexes.Select(i => _CreateColumnDef(db, i)).ToArray());
                    colIndexNames += "," + string.Join(",", metadata.Indexes.Select(i => db.MakeQuotedName(i.Name)).ToArray());
                }

                var createTableSQL = string.Format(SQLStatements.CreateTableIfNotExists, tableName, colDefs, _FileGroup);
                db.ExecuteNonQuery(createTableSQL);

                var indexTableSQL = string.Format(SQLStatements.CreateIndex, tableName, colIndexNames, _FileGroup);
                db.ExecuteNonQuery(indexTableSQL);
            }
        }

        public override void UnprovisionIndex(string objectFullName)
        {
            using (var db = OpenSchema())
            {
                var tableName = _CreateTableName(db, objectFullName);

                var dropTableSQL = string.Format(SQLStatements.DropTableIfExists, tableName);
                db.ExecuteNonQuery(dropTableSQL);

                // Remove the staging table as well
                _UnprovisionIndexStaging(db, objectFullName);
            }
        }

        public override void UpsertIndexValues(string objectFullName, int objectId, params ObjectIndex[] indexes)
        {
            using (var db = OpenData())
            {
                var tableName = _CreateTableName(db, objectFullName);

                var parameters = new List<IDataParameter>();
                for (int i = 0; indexes.Length > i; i++)
                {
                    var idx = indexes[i];
                    var param = db.MakeParam(idx.Name + "_param", idx.GetObjectValue());
                    parameters.Add(param);
                }

                var sql = new StringBuilder();

                sql.Append(@"MERGE [ZeroG].");
                sql.Append(db.MakeQuotedName(tableName));
                sql.Append(@" WITH(HOLDLOCK) AS mergeTo
USING (VALUES (");

                int paramCount = parameters.Count;

                // 1. generate set of values for USING VALUES clause
                if (0 < paramCount)
                {
                    for (int i = 0; paramCount > i; i++)
                    {
                        sql.Append(db.MakeParamReference(parameters[i].ParameterName));
                        sql.Append(',');
                    }
                    sql.Remove(sql.Length - 1, 1);
                }

                sql.Append(@"))
    AS source (");

                // 2. generate set of field names for USING AS clause
                if (0 < paramCount)
                {
                    for (int i = 0; paramCount > i; i++)
                    {
                        var idx = indexes[i];
                        sql.Append(db.MakeQuotedName(idx.Name));
                        sql.Append(',');
                    }
                    sql.Remove(sql.Length - 1, 1);
                }

                sql.Append(@")
    ON mergeTo.");

                sql.Append(db.MakeQuotedName(IDColumn));

                sql.Append(@" = ");
                sql.Append(db.MakeParamReference("recordId"));
                sql.Append(@"
WHEN MATCHED THEN
    UPDATE
    SET ");

                if (0 < paramCount)
                {
                    for (int i = 0; paramCount > i; i++)
                    {
                        var idx = indexes[i];
                        sql.Append(db.MakeQuotedName(idx.Name));
                        sql.Append(" = source.");
                        sql.Append(db.MakeQuotedName(idx.Name));
                        sql.Append(",");
                    }
                    sql.Remove(sql.Length - 1, 1);
                }

                // 3. generate set of fields for UPDATE SET clause
                sql.Append(@"
WHEN NOT MATCHED THEN
    INSERT (");

                sql.Append(db.MakeQuotedName(IDColumn));
                sql.Append(@",");

                // 4. generate set of fields for INSERT clause
                if (0 < paramCount)
                {
                    for (int i = 0; paramCount > i; i++)
                    {
                        var idx = indexes[i];
                        sql.Append(db.MakeQuotedName(idx.Name));
                        sql.Append(",");
                    }
                    sql.Remove(sql.Length - 1, 1);
                }

                sql.Append(@")
    VALUES (");

                sql.Append(db.MakeParamReference("recordId"));

                sql.Append(@",");

                // 5. generate set of fields for INSERT VALUES clause
                if (0 < paramCount)
                {
                    for (int i = 0; paramCount > i; i++)
                    {
                        var idx = indexes[i];
                        sql.Append("source.");
                        sql.Append(db.MakeQuotedName(idx.Name));
                        sql.Append(",");
                    }
                    sql.Remove(sql.Length - 1, 1);
                }
                sql.Append(");");

                parameters.Add(db.MakeParam("recordId", objectId));

                db.ExecuteNonQuery(sql.ToString(), parameters.ToArray());
            }
        }

        public override void BulkUpsertIndexValues(string objectFullName, ObjectMetadata metadata, IEnumerable<object[]> indexValues)
        {
            // first, stage the values
            _StageBulkIndexValues(objectFullName, metadata, indexValues);

            try
            {
                // second, merge them into the primary index table
                _MergeBulkIndexValues(objectFullName, metadata);
            }
            finally
            {
                _CleanStageBulkIndexValues(objectFullName);
            }
        }

        public override void RemoveIndexValue(string objectFullName, int objectId)
        {
            using (var db = OpenData())
            {
                var tableName = _CreateTableName(db, objectFullName);

                db.ExecuteNonQuery(string.Format(SQLStatements.RemoveIndex, tableName, IDColumn, objectId));
            }
        }

        public override void RemoveIndexValues(string objectFullName, int[] objectIds)
        {
            using (var db = OpenData())
            {
                if (1 == objectIds.Length)
                {
                    RemoveIndexValue(objectFullName, objectIds[0]);
                }
                else
                {
                    var tableName = _CreateTableName(db, objectFullName);

                    var objectIdConstraint = new StringBuilder();
                    for (int i = 0; objectIds.Length > i; i++)
                    {
                        objectIdConstraint.Append(objectIds[i]);
                        objectIdConstraint.Append(',');

                        if (0 < i && (i % MaxIDConstraint) == 0)
                        {
                            objectIdConstraint.Length -= 1; // trim trailing comma

                            db.ExecuteNonQuery(string.Format(SQLStatements.RemoveIndex, tableName, IDColumn, objectIdConstraint.ToString()));

                            objectIdConstraint.Length = 0;
                        }
                    }

                    if (0 < objectIdConstraint.Length)
                    {
                        objectIdConstraint.Length -= 1; // trim trailing comma

                        db.ExecuteNonQuery(string.Format(SQLStatements.RemoveIndex, tableName, IDColumn, objectIdConstraint.ToString()));
                    }
                }
            }
        }

        public override void Truncate(string objectFullName)
        {
            using (var db = OpenSchema())
            {
                var tableName = _CreateTableName(db, objectFullName);
                var sql = string.Format(SQLStatements.TruncateTable, tableName);
                db.ExecuteNonQuery(sql);
            }
        }

        public override void Dispose()
        {
        }
    }
}