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

        public static readonly string RowsExist = @"SELECT 1 FROM [ZeroG].[{0}] WHERE {1}";

        public static readonly string RowsCount = @"SELECT COUNT(1) FROM [ZeroG].[{0}] WHERE {1}";

        public static readonly string CreateTableIfNotExists = @"IF NOT EXISTS (select * from sysobjects where name='{0}' and xtype='U')
    CREATE TABLE [ZeroG].[{0}](
	    {1}
	CONSTRAINT [PK_{0}] PRIMARY KEY CLUSTERED 
	    ([ID] ASC)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [{2}]
	) ON [{2}]";

        public static readonly string CreateIndex = @"IF EXISTS (select * from sysobjects where name='{0}' and xtype='U')
	CREATE NONCLUSTERED INDEX [IDX_{0}] ON [ZeroG].[{0}](
		{1}
	)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [{2}]";

        public static readonly string DropTableIfExists = @"IF EXISTS (select * from sysobjects where name='{0}' and xtype='U')
    DROP TABLE [ZeroG].[{0}]";

        public static readonly string Find = @"SELECT DISTINCT [ID] FROM [ZeroG].[{0}]
WHERE {1}";

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

        public override bool ObjectExists(string objectFullName)
        {
            bool returnValue = false;

            using (var db = OpenSchema())
            {
                returnValue = 0 < db.ExecuteScalar<int>(string.Format(SQLStatements.TableExists, _CreateTableName(db, objectFullName)), 0);
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

            bool useOr = ObjectFindLogic.Or == logic;
            bool useLike = ObjectFindOperator.Like == oper;

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
                    if (value is byte[])
                    {
                        string hexStr = DatabaseHelper.ByteToHexString((byte[])value);
                        value = hexStr;
                        sqlConstraint.Append(db.MakeQuotedName(idx.Name));
                    }
                    else
                    {
                        sqlConstraint.Append(db.MakeQuotedName(idx.Name));
                    }
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

                returnValue = db.GetValues<int>(string.Format(SQLStatements.Find, tableName, sqlConstraint.ToString()), parameters.ToArray());
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
            return Find(objectFullName, constraint, 0, indexes);
        }

        public override int[] Find(string objectFullName, string constraint, uint limit, ObjectIndexMetadata[] indexes)
        {
            using (var db = OpenData())
            {
                var sqlConstraint = CreateSQLConstraint(db, indexes, constraint);
                var tableName = _CreateTableName(db, objectFullName);
                return db.GetValues<int>(string.Format(SQLStatements.Find, tableName, sqlConstraint.SQL), sqlConstraint.Parameters.ToArray());
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