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
        public static readonly string CreateTableIfNotExists = @"CREATE TABLE IF NOT EXISTS `{0}`(
	    {1}
	) ENGINE = InnoDB CHARACTER SET utf8 COLLATE utf8_general_ci";

        public static readonly string CreateIndex = @"CREATE INDEX `IDX_{0}` ON `{0}` ({1})";

        public static readonly string DropTableIfExists = @"DROP TABLE IF EXISTS `{0}`";

        public static readonly string Find = @"SELECT `ID` FROM `{0}`
WHERE {1}";

        public static string RemoveIndex = @"DELETE FROM `{0}` WHERE `{1}` IN ({2})";

        public static readonly string TruncateTable = "TRUNCATE TABLE `{0}`";
    }

    public class MySQLObjectIndexProvider : ObjectIndexProvider
    {

        public MySQLObjectIndexProvider()
            : base()
        {

        }

        public MySQLObjectIndexProvider(string databaseServiceSchema, string databaseServiceData)
            : base (databaseServiceSchema, databaseServiceData)
        {
            
        }

        private static string _CreateTableName(IDatabaseService db, string nameSpace, string objectName)
        {
            return db.EscapeCommandText(nameSpace + "_" + objectName);
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

        public override int[] Find(string nameSpace, string objectName, ObjectFindLogic logic, ObjectFindOperator oper, params ObjectIndex[] indexes)
        {
            int[] returnValue = null;
            bool useOr = ObjectFindLogic.Or == logic;
            bool useLike = ObjectFindOperator.Like == oper;

            using (var db = OpenData())
            {
                var tableName = _CreateTableName(db, nameSpace, objectName);

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
                    var value = idx.Value;
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

        public override int[] Find(string nameSpace, string objectName, params ObjectIndex[] indexes)
        {
            return Find(nameSpace, objectName, ObjectFindLogic.And, ObjectFindOperator.Equals, indexes);
        }

        public override int[] Find(string nameSpace, string objectName, string constraint, ObjectIndexMetadata[] indexes)
        {
            using (var db = OpenData())
            {
                var sqlConstraint = CreateSQLConstraint(db, indexes, constraint);
                var tableName = _CreateTableName(db, nameSpace, objectName);
                return db.GetValues<int>(string.Format(SQLStatements.Find, tableName, sqlConstraint.SQL), sqlConstraint.Parameters.ToArray());
            }
        }

        public override void ProvisionIndex(ObjectMetadata metadata)
        {
            using (var db = OpenSchema())
            {
                var tableName = _CreateTableName(db, metadata.NameSpace, metadata.ObjectName);
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

        public override void UnprovisionIndex(string nameSpace, string objectName)
        {
            using (var db = OpenSchema())
            {
                var tableName = _CreateTableName(db, nameSpace, objectName);

                var dropTableSQL = string.Format(SQLStatements.DropTableIfExists, tableName);
                db.ExecuteNonQuery(dropTableSQL);
            }
        }

        public override void UpsertIndexValues(string nameSpace, string objectName, int objectId, params ObjectIndex[] indexes)
        {
            using (var db = OpenData())
            {
                var tableName = _CreateTableName(db, nameSpace, objectName);

                var parameters = new List<IDataParameter>();
                for (int i = 0; indexes.Length > i; i++)
                {
                    var idx = indexes[i];
                    var value = idx.Value;
                    if (value is byte[])
                    {
                        value = DatabaseHelper.ByteToHexString((byte[])value);
                    }
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

        public override void RemoveIndexValue(string nameSpace, string objectName, int objectId)
        {
            using (var db = OpenData())
            {
                var tableName = _CreateTableName(db, nameSpace, objectName);

                db.ExecuteNonQuery(string.Format(SQLStatements.RemoveIndex, tableName, IDColumn, objectId));
            }
        }

        public override void RemoveIndexValues(string nameSpace, string objectName, int[] objectIds)
        {
            using (var db = OpenData())
            {
                if (1 == objectIds.Length)
                {
                    RemoveIndexValue(nameSpace, objectName, objectIds[0]);
                }
                else
                {
                    var tableName = _CreateTableName(db, nameSpace, objectName);

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

        public override void Truncate(string nameSpace, string objectName)
        {
            using (var db = OpenData())
            {
                var tableName = _CreateTableName(db, nameSpace, objectName);
                var sql = string.Format(SQLStatements.TruncateTable, tableName);
                db.ExecuteNonQuery(sql);
            }
        }

        public override void Dispose()
        {
        }
    }
}
