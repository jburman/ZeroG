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
`ID` INT NOT NULL PRIMARY KEY
	    {1})
	) ENGINE = InnoDB CHARACTER SET utf8 COLLATE utf8_general_ci";

        public static readonly string CreateIndex = @"CREATE INDEX IF NOT EXISTS `IDX_{0}` USING HASH ({1})";

        public static readonly string DropTableIfExists = @"DROP TABLE IF EXISTS `{0}`";

        public static readonly string Find = @"SELECT `ID` FROM `{0}`
WHERE {1}";

        public static string RemoveIndex = @"DELETE FROM `{0}` WHERE `{1}` IN ({2})";

        public static readonly string TruncateTable = "TRUNCATE TABLE `{0}`";
    }

    public class MySQLObjectIndexProvider : IObjectIndexProvider
    {

        #region Config settings
        // TODO: make config settings configurable from app.config

        /// <summary>
        /// Maximum number of IDs that will be passed to a single query.
        /// For example, when deleting a large number of Objects. Queries will be batched if the 
        /// the supplied number of IDs exceeds the constraint limit.
        /// </summary>
        private static int _MaxIDConstraint
        {
            get
            {
                return 100;
            }
        }

        private static string _FileGroup
        {
            get
            {
                return "PRIMARY";
            }
        }

        private static string _IDColumn
        {
            get
            {
                return "ID";
            }
        }

        #endregion

        public MySQLObjectIndexProvider()
        {
        }

        private IDatabaseService _OpenSchema()
        {
            var db = DatabaseService.GetService("ObjectIndexSchema");
            db.Open();
            return db;
        }

        private IDatabaseService _OpenData()
        {
            var db = DatabaseService.GetService("ObjectIndexData");
            db.Open();
            return db;
        }

        private static string _CreateTableName(IDatabaseService db, string nameSpace, string objectName)
        {
            return db.EscapeCommandText(nameSpace + "_" + objectName);
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

        public int[] Find(string nameSpace, string objectName, ObjectIndex[] indexes)
        {
            int[] returnValue = null;

            using (var db = _OpenData())
            {
                var tableName = _CreateTableName(db, nameSpace, objectName);

                var parameters = new List<IDbDataParameter>();
                var sqlConstraint = new StringBuilder();
                for (int i = 0; indexes.Length > i; i++)
                {
                    var idx = indexes[i];
                    var paramName = "p" + i + idx.Name;
                    parameters.Add(db.MakeParam(paramName, idx.Value));
                    if(0 < i) 
                    {
                        sqlConstraint.Append(" AND ");
                    }
                    sqlConstraint.Append('[');
                    sqlConstraint.Append(db.EscapeCommandText(idx.Name));
                    sqlConstraint.Append("] = @");
                    sqlConstraint.Append(paramName);
                }

                returnValue = db.GetValues<int>(string.Format(SQLStatements.Find, tableName, sqlConstraint.ToString()), parameters.ToArray());
            }

            return returnValue;
        }

        public void ProvisionIndex(ObjectMetadata metadata)
        {
            using (var db = _OpenSchema())
            {
                var tableName = _CreateTableName(db, metadata.NameSpace, metadata.ObjectName);
                string idColName = "[" + _IDColumn + "]";
                string colDefs = idColName + " [int] NOT NULL";
                string colIndexNames = idColName;

                if (null != metadata.Indexes && 0 < metadata.Indexes.Length)
                {
                    colDefs += "," + string.Join(",", metadata.Indexes.Select(i => _CreateColumnDef(db, i)).ToArray());
                    colIndexNames += "," + string.Join(",", metadata.Indexes.Select(i => "[" + db.EscapeCommandText(i.Name) + "]").ToArray());
                }

                var createTableSQL = string.Format(SQLStatements.CreateTableIfNotExists, tableName, colDefs, _FileGroup);
                db.ExecuteNonQuery(createTableSQL);

                var indexTableSQL = string.Format(SQLStatements.CreateIndex, tableName, colIndexNames, _FileGroup);
                db.ExecuteNonQuery(indexTableSQL);
            }
        }

        public void UnprovisionIndex(string nameSpace, string objectName)
        {
            using (var db = _OpenSchema())
            {
                var tableName = _CreateTableName(db, nameSpace, objectName);

                var dropTableSQL = string.Format(SQLStatements.DropTableIfExists, tableName);
                db.ExecuteNonQuery(dropTableSQL);
            }
        }

        public void UpsertIndexValues(string nameSpace, string objectName, int objectId, ObjectIndex[] indexes)
        {
            using (var db = _OpenData())
            {
                var tableName = _CreateTableName(db, nameSpace, objectName);

                var parameters = new List<IDataParameter>();
                for (int i = 0; indexes.Length > i; i++)
                {
                    var idx = indexes[i];
                    var param = db.MakeParam(idx.Name + "_param", idx.Value);
                    parameters.Add(param);
                }
                
                var sql = new StringBuilder();

                sql.Append(@"MERGE [");
                sql.Append(tableName);
                sql.Append("]");
                sql.Append(@" WITH(HOLDLOCK) AS mergeTo
USING (VALUES (");
                
                int paramCount = parameters.Count;

                // 1. generate set of values for USING VALUES clause
                if(0 < paramCount) 
                {
                    for(int i = 0; paramCount > i; i++)
                    {
                        sql.Append('@');
                        sql.Append(db.EscapeCommandText(parameters[i].ParameterName));
                        sql.Append(',');
                    }
                    sql.Remove(sql.Length - 1,1);
                }
                
                sql.Append(@"))
    AS source (");
                
                // 2. generate set of field names for USING AS clause
                if(0 < paramCount) 
                {
                    for(int i = 0; paramCount > i; i++)
                    {
                        var idx = indexes[i];
                        sql.Append('[');
                        sql.Append(db.EscapeCommandText(idx.Name));
                        sql.Append("],");
                    }
                    sql.Remove(sql.Length - 1,1);
                }
                
                sql.Append(@")
    ON mergeTo.[");
                
                sql.Append(_IDColumn);

                sql.Append(@"] = @recordId
WHEN MATCHED THEN
    UPDATE
    SET ");

                if(0 < paramCount) 
                {
                    for(int i = 0; paramCount > i; i++)
                    {
                        var idx = indexes[i];
                        sql.Append('[');
                        sql.Append(db.EscapeCommandText(idx.Name));
                        sql.Append("] = source.[");
                        sql.Append(db.EscapeCommandText(idx.Name));
                        sql.Append("],");
                    }
                    sql.Remove(sql.Length - 1,1);
                }

                // 3. generate set of fields for UPDATE SET clause
                sql.Append(@"
WHEN NOT MATCHED THEN
    INSERT ([");
                
                sql.Append(_IDColumn);
                sql.Append(@"],");
                
                // 4. generate set of fields for INSERT clause
                if (0 < paramCount)
                {
                    for (int i = 0; paramCount > i; i++)
                    {
                        var idx = indexes[i];
                        sql.Append('[');
                        sql.Append(db.EscapeCommandText(idx.Name));
                        sql.Append("],");
                    }
                    sql.Remove(sql.Length - 1, 1);
                }

                sql.Append(@")
    VALUES (@recordId,");

                // 5. generate set of fields for INSERT VALUES clause
                if (0 < paramCount)
                {
                    for (int i = 0; paramCount > i; i++)
                    {
                        var idx = indexes[i];
                        sql.Append("source.[");
                        sql.Append(db.EscapeCommandText(idx.Name));
                        sql.Append("],");
                    }
                    sql.Remove(sql.Length - 1, 1);
                }
                sql.Append(");");
                
                parameters.Add(db.MakeParam("recordId", objectId));

                db.ExecuteNonQuery(sql.ToString(), parameters.ToArray());
            }
        }

        public void RemoveIndexValue(string nameSpace, string objectName, int objectId)
        {
            using (var db = _OpenData())
            {
                var tableName = _CreateTableName(db, nameSpace, objectName);

                db.ExecuteNonQuery(string.Format(SQLStatements.RemoveIndex, tableName, _IDColumn, objectId));
            }
        }

        public void RemoveIndexValues(string nameSpace, string objectName, int[] objectIds)
        {
            using (var db = _OpenData())
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

                        if (0 < i && (i % _MaxIDConstraint) == 0)
                        {
                            objectIdConstraint.Length -= 1; // trim trailing comma

                            db.ExecuteNonQuery(string.Format(SQLStatements.RemoveIndex, tableName, _IDColumn, objectIdConstraint.ToString()));

                            objectIdConstraint.Length = 0;
                        }
                    }

                    if (0 < objectIdConstraint.Length)
                    {
                        objectIdConstraint.Length -= 1; // trim trailing comma

                        db.ExecuteNonQuery(string.Format(SQLStatements.RemoveIndex, tableName, _IDColumn, objectIdConstraint.ToString()));
                    }
                }
            }
        }

        public void Truncate(string nameSpace, string objectName)
        {
            using (var db = _OpenData())
            {
                var tableName = _CreateTableName(db, nameSpace, objectName);
                var sql = string.Format(SQLStatements.TruncateTable, tableName);
                db.ExecuteNonQuery(sql);
            }
        }

        public void Dispose()
        {
        }
    }
}
