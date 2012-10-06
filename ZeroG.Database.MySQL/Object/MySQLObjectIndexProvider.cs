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
using ZeroG.Lang;

namespace ZeroG.Data.Database.Drivers.Object.Provider
{
    internal class SQLStatements
    {
        public static readonly string NoLimit = "";

        public static readonly string Limit = " LIMIT {0} ";

        public static readonly string NoOrder = "";

        public static readonly string OrderAsc = " ORDER BY {0} ASC";

        public static readonly string OrderDesc = " ORDER BY {0} DESC";

        public static readonly string TableExists = @"SHOW TABLES LIKE '{0}'";

        public static readonly string RowsExist = @"SELECT 1 FROM `{0}` WHERE {1}";

        public static readonly string RowsCount = @"SELECT COUNT(1) FROM `{0}` WHERE {1}";

        public static readonly string CreateTableIfNotExists = @"CREATE TABLE IF NOT EXISTS `{0}`(
	    {1}
	) ENGINE = InnoDB CHARACTER SET utf8 COLLATE utf8_general_ci";

        public static readonly string CreateIndex = @"CREATE INDEX `IDX_{0}` ON `{0}` ({1})";

        public static readonly string DropTableIfExists = @"DROP TABLE IF EXISTS `{0}`";

        public static readonly string Find = @"SELECT `ID` FROM `{0}`
WHERE {1}{2}{3}";

        public static readonly string Iterate = @"SELECT {1} FROM `{0}`{2}{3}{4}";

        public static string RemoveIndex = @"DELETE FROM `{0}` WHERE `{1}` IN ({2})";

        public static readonly string TruncateTable = "TRUNCATE TABLE `{0}`";
    }

    public class MySQLObjectIndexProvider : ObjectIndexProvider
    {
        public MySQLObjectIndexProvider()
            : base()
        {
        }

        public MySQLObjectIndexProvider(Config config)
            : base(config)
        {
        }

        public MySQLObjectIndexProvider(string databaseServiceSchema, string databaseServiceData)
            : base (databaseServiceSchema, databaseServiceData)
        {    
        }

        private static string _CreateTableName(IDatabaseService db, string objectFullName)
        {
            return db.EscapeCommandText(objectFullName.Replace('.', '_'));
        }

        private static string _CreateColumnDef(IDatabaseService db, ObjectIndexMetadata indexMetadata)
        {
            string name = db.EscapeCommandText(indexMetadata.Name);
            string type = "VARCHAR";
            string length = "(30)";

            switch (indexMetadata.DataType)
            {
                case ObjectIndexType.Integer:
                    type = "INT";
                    length = "";
                    break;
                case ObjectIndexType.Binary:
                    type = "BINARY";
                    length = "(" + indexMetadata.Precision + ")";
                    break;
                case ObjectIndexType.DateTime:
                    type = "DATETIME";
                    length = "";
                    break;
                case ObjectIndexType.Decimal:
                    type = "DECIMAL";
                    length = "(" + indexMetadata.Precision + "," + indexMetadata.Scale + ")";
                    break;
                default:
                    length = "(" + indexMetadata.Precision + ")";
                    break;
            }

            return string.Format("`{0}` {1}{2} NOT NULL", name, type, length);
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

        public override bool ObjectExists(string objectFullName)
        {
            bool returnValue = false;

            using (var db = OpenSchema())
            {
                var tableName = _CreateTableName(db, objectFullName);

                using (var reader = db.ExecuteReader(string.Format(SQLStatements.TableExists, tableName)))
                {
                    if (reader.Read())
                    {
                        returnValue = true;
                    }
                }
            }

            return returnValue;
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

            var limitSql = (0 == limit) ? SQLStatements.NoLimit : string.Format(SQLStatements.Limit, limit);

            using (var db = OpenData())
            {
                var tableName = _CreateTableName(db, objectFullName);

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

                returnValue = db.GetValues<int>(string.Format(SQLStatements.Find, tableName, sqlConstraint.ToString(), orderBySql, limitSql), parameters.ToArray());
            }

            return returnValue;
        }

        public override int[] Find(string objectFullName, params ObjectIndex[] indexes)
        {
            return Find(objectFullName,
                new ObjectFindOptions()
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
                var tableName = _CreateTableName(db, objectFullName);
                var sqlConstraint = CreateSQLConstraint(db, indexes, constraint);
                var orderBySql = _CreateOrderBySQL(db, order);
                var limitSql = (0 == limit) ? SQLStatements.NoLimit : string.Format(SQLStatements.Limit, limit);
                return db.GetValues<int>(string.Format(SQLStatements.Find, tableName, sqlConstraint.SQL, orderBySql, limitSql), sqlConstraint.Parameters.ToArray());
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
                var limitSql = (0 == limit) ? SQLStatements.NoLimit : string.Format(SQLStatements.Limit, limit);

                string[] selectNames = null;

                // if no iterate indexes are specified, then iterate overall index names from the metadata
                if (null == iterateIndexes || 0 == iterateIndexes.Length)
                {
                    if (null == indexes || 0 == indexes.Length)
                    {
                        // If not iterate indexes or metadataColumns provided, then only select the ID column
                        selectNames = new string[] { db.MakeQuotedName(IDColumn) };
                    }
                    else
                    {
                        selectNames = new string[indexes.Length + 1];
                        selectNames[0] = db.MakeQuotedName(IDColumn);
                        for (int i = 0; indexes.Length > i; i++)
                        {
                            selectNames[i + 1] = db.MakeQuotedName(indexes[i].Name);
                        }
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

                var reader = db.ExecuteReader(string.Format(SQLStatements.Iterate, tableName, selectNamesSql, whereSql, orderBySql, limitSql), parameters);
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
                string colDefs = idColName + " INT NOT NULL PRIMARY KEY";
                string colIndexNames = idColName;

                if (null != metadata.Indexes && 0 < metadata.Indexes.Length)
                {
                    colDefs += "," + string.Join(",", metadata.Indexes.Select(i => _CreateColumnDef(db, i)).ToArray());
                    colIndexNames += "," + string.Join(",", metadata.Indexes.Select(i => db.MakeQuotedName(i.Name)).ToArray());
                }

                var createTableSQL = string.Format(SQLStatements.CreateTableIfNotExists, tableName, colDefs);
                db.ExecuteNonQuery(createTableSQL);

                var indexTableSQL = string.Format(SQLStatements.CreateIndex, tableName, colIndexNames);
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
                    var value = idx.GetObjectValue();
                    var param = db.MakeParam(idx.Name + "_param", value);
                    parameters.Add(param);
                }
                
                var sql = new StringBuilder();

                sql.Append(@"INSERT INTO ");
                sql.Append(db.MakeQuotedName(tableName));
                sql.Append(@" (");
                sql.Append(db.MakeQuotedName(IDColumn));
                
                int paramCount = parameters.Count;

                // 1. generate set of field names for INTO clause
                if(0 < paramCount) 
                {
                    for(int i = 0; paramCount > i; i++)
                    {
                        var idx = indexes[i];
                        sql.Append(',');
                        sql.Append(db.MakeQuotedName(idx.Name));
                    }
                }
                sql.Append(") VALUES (");
                sql.Append(db.MakeParamReference("recordId"));

                // 2. generate set of values for VALUES clause
                if(0 < paramCount) 
                {
                    for(int i = 0; paramCount > i; i++)
                    {
                        sql.Append(',');
                        sql.Append(db.MakeParamReference(parameters[i].ParameterName));
                    }
                }
                
                sql.Append(@")
    ON DUPLICATE KEY UPDATE
");
                
                if(0 < paramCount) 
                {
                    for(int i = 0; paramCount > i; i++)
                    {
                        var idx = indexes[i];
                        sql.Append(db.MakeQuotedName(idx.Name));
                        sql.Append(" = ");
                        sql.Append(db.MakeParamReference(parameters[i].ParameterName));
                        sql.Append(',');
                    }
                    sql.Remove(sql.Length - 1,1);
                }
                
                parameters.Add(db.MakeParam("recordId", objectId));

                db.ExecuteNonQuery(sql.ToString(), parameters.ToArray());
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
