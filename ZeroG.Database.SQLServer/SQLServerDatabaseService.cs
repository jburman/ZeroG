#region License, Terms and Conditions
// Copyright (c) 2010 Jeremy Burman
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
using System.Linq;
using System.Text;
using System.Configuration;
using System.Collections.Specialized;
using System.Data;
using System.Data.SqlClient;
using System.Data.Common;
using System.Text.RegularExpressions;
using System.IO;

namespace ZeroG.Data.Database.Drivers
{
    public sealed class SQLServerDatabaseService : DatabaseService
    {
        #region Constants
        public const string ParameterQualifier = "@";
        public const uint DefaultSQLBatchSize = 10;
        public const string AttributeClientBulkInsertPath = "ClientBulkInsertPath";
        public const string AttributeServerBulkInsertPath = "ServerBulkInsertPath";
        public const string AttributeSQLBatchSize = "SQLBatchSize";
        #endregion

        #region Constructors/Destructors
        public SQLServerDatabaseService()
            : this(null) 
        {
        }

        internal SQLServerDatabaseService(string connStr)
            : base(connStr)
        {
            _sqlBatchSize = DefaultSQLBatchSize;
        }
        #endregion

        #region Private
        private string _clientBulkInsertPath, _serverBulkInsertPath;
        private uint _sqlBatchSize;

        private static readonly byte[] NullFieldValue = Encoding.UTF8.GetBytes("\\N");
        private static readonly byte[] FieldDelim = Encoding.UTF8.GetBytes("\t");
        private static readonly byte[] RowDelim = Encoding.UTF8.GetBytes("\r\n");

        private static void _CreateFileRow(Stream output, object[] row, HashSet<string> cleanupCols, string[] columns)
        {
            if (null != row)
            {
                for (int j = 0; row.Length > j; j++)
                {
                    var val = row[j];
                    byte[] fieldVal = NullFieldValue;
                    if (null != val)
                    {
                        // strings need to have escaped characters restored once the bulk insert is complete
                        if (val is string)
                        {
                            if (!cleanupCols.Contains(columns[j]))
                            {
                                cleanupCols.Add(columns[j]);
                            }
                        }

                        fieldVal = FileFieldConverter.ToFileFieldString(val);
                    }

                    output.Write(fieldVal, 0, fieldVal.Length);
                    if (j != row.Length - 1)
                    {
                        output.Write(FieldDelim, 0, FieldDelim.Length);
                    }
                }
            }
        }
        #endregion

        #region Public

        #region Properties
        #endregion // end Properties

        #region Methods
        public override IDbTransaction BeginTransaction()
        {
            _IsConnAvailable();
            return _dbConn.BeginTransaction();
        }

        public override IDbTransaction BeginTransaction(IsolationLevel isolation)
        {
            _IsConnAvailable();
            return _dbConn.BeginTransaction(isolation);
        }

        public override void Configure(DatabaseServiceConfiguration config)
        {
            _connString = config.ConnectionString;
            if (null != config.Properties)
            {
                foreach (var prop in config.Properties)
                {
                    if (prop.Name == AttributeClientBulkInsertPath)
                    {
                        _clientBulkInsertPath = prop.Value;
                    }
                    else if (prop.Name == AttributeServerBulkInsertPath)
                    {
                        _serverBulkInsertPath = prop.Value;
                    }
                    else if (prop.Name == AttributeSQLBatchSize)
                    {
                        uint tryVal = 0;
                        uint.TryParse(prop.Value, out tryVal);
                        if (0 == tryVal)
                        {
                            tryVal = DefaultSQLBatchSize;
                        }
                        _sqlBatchSize = tryVal;
                    }
                }
            }
        }

        public override IDbDataAdapter CreateDataAdapter(string commandText, params IDataParameter[] parameters)
        {
            return CreateDataAdapter(commandText, null, parameters);
        }

        public override IDbDataAdapter CreateDataAdapter(string commandText, IDbTransaction trans, params IDataParameter[] parameters)
        {
            SqlCommand cmd = (SqlCommand)_PrepareCommand(trans, commandText, parameters);
            return new SqlDataAdapter(cmd);
        }

        public override string EscapeCommandText(string commandText)
        {
            if (null != commandText)
            {
                return commandText.Replace("'", "''");
            }
            else
            {
                return commandText;
            }
        }

        public override string EscapeNameForLike(string name)
        {
            return EscapeCommandText(name) + " ESCAPE '\\'";
        }

        public override string EscapeValueForLike(string value)
        {
            if (!string.IsNullOrEmpty(value))
            {
                value = value.Replace("\\", "\\\\");
                value = value.Replace("%", "\\%");
                value = value.Replace("_", "\\_");
                value = value.Replace("[", "\\[");
            }
            return value;
        }

        public override void ExecuteBulkCopy(DataTable copyData, string copyToTable, Dictionary<string, string> columnMap)
        {
            ExecuteBulkCopy(null, copyData, copyToTable, columnMap);
        }

        public override void ExecuteBulkCopy(IDbTransaction transaction, DataTable copyData, string copyToTable, Dictionary<string, string> columnMap)
        {
            _IsConnAvailable();

            SqlBulkCopy bulkCopy = null;
            if (null != transaction)
            {
                bulkCopy = new SqlBulkCopy((SqlConnection)_dbConn, SqlBulkCopyOptions.Default, (SqlTransaction)transaction);
            }
            else
            {
                bulkCopy = new SqlBulkCopy((SqlConnection)_dbConn);
            }

            if (null != columnMap)
            {
                Dictionary<string, string>.KeyCollection keys = columnMap.Keys;

                foreach (string key in keys)
                {
                    bulkCopy.ColumnMappings.Add(key, columnMap[key]);
                }
            }

            bulkCopy.DestinationTableName = copyToTable;

            bulkCopy.WriteToServer(copyData);
        }

        private void _BulkInsertFallback(IEnumerable<object[]> insertData, string insertToTable, string[] columns)
        {
            var sqlBatch = new StringBuilder();
            var paramList = new List<IDataParameter>();
            
            int colCount = columns.Length;
            int count = 0;

            var paramNameList = new string[colCount];

            string insertTemplate = "INSERT INTO " + insertToTable + " (" + 
                string.Join(",", columns) +
                ") VALUES ({0});";

            foreach (var dataRow in insertData)
            {
                // create each parameter
                for (int i = 0; colCount > i; i++)
                {
                    var paramName = "p" + count + "_" + i;
                    paramNameList[i] = "@" + paramName;
                    paramList.Add(MakeParam(paramName, dataRow[i]));
                }

                sqlBatch.Append(
                    string.Format(insertTemplate, string.Join(",", paramNameList)));

                if (0 < count && (0 == (count % _sqlBatchSize)))
                {
                    using(var cmd = _PrepareCommand(
                        null,
                        sqlBatch.ToString(),
                        paramList.ToArray()))
                    {
                        cmd.ExecuteNonQuery();
                    }

                    paramList.Clear();
                    sqlBatch.Length = 0;
                    count = 0;
                }

                ++count;
            }

            if (0 < paramList.Count)
            {
                using (var cmd = _PrepareCommand(
                        null,
                        sqlBatch.ToString(),
                        paramList.ToArray()))
                {
                    cmd.ExecuteNonQuery();
                }
            }
        }

        public override void ExecuteBulkInsert(IEnumerable<object[]> insertData, string insertToTable, string[] columns)
        {
            // if bulk insert is not configured to allow file transfer between  the client and the server 
            // then use a slower fallback method
            if (string.IsNullOrEmpty(_clientBulkInsertPath) || string.IsNullOrEmpty(_serverBulkInsertPath))
            {
                _BulkInsertFallback(insertData, insertToTable, columns);
            }
            else
            {
                var fileName = Guid.NewGuid().ToString("N") + ".txt";
                var filePath = Path.Combine(_clientBulkInsertPath, fileName);
                var serverFilePath = Path.Combine(_serverBulkInsertPath, fileName);
                var cleanupCols = new HashSet<string>();

                try
                {
                    using (var fstream = new FileStream(filePath, FileMode.Create))
                    {
                        foreach (var dataRow in insertData)
                        {
                            _CreateFileRow(fstream, dataRow, cleanupCols, columns);
                            fstream.Write(RowDelim, 0, RowDelim.Length);
                        }
                        fstream.Flush();
                    }
                    using (var cmd = _PrepareCommand(null,
                        string.Format(@"BULK INSERT {0} FROM '{1}'
WITH (
FIELDTERMINATOR = '\t',
DATAFILETYPE='widechar')",
                        insertToTable,
                        filePath),
                        null))
                    {
                        cmd.ExecuteNonQuery();
                    }

                    // new lines and tabs get collapsed during import so they need to replaced with their original values.
                    if (0 < cleanupCols.Count)
                    {
                        var updateSql = string.Format(@"UPDATE {0} SET {1}",
                            insertToTable,
                            string.Join(",", cleanupCols.Select(c => string.Format(@"[{0}] = REPLACE(REPLACE(REPLACE([{0}], '\t', CHAR(9)), '\n', CHAR(10)), '\\', '\')", c))
                                .ToArray()));

                        using (var cmdUpdate = _PrepareCommand(null,
                            updateSql,
                            null))
                        {
                            cmdUpdate.ExecuteNonQuery();
                        }
                    }
                }
                finally
                {
                    File.Delete(filePath);
                }
            }
        }

        public override int ExecuteNonQuery(string commandText, params IDataParameter[] parameters)
        {
            return ExecuteNonQuery(null, commandText, parameters);
        }

        public override int ExecuteNonQuery(IDbTransaction trans, string commandText, params IDataParameter[] parameters)
        {
            using (IDbCommand cmd = _PrepareCommand(trans, commandText, parameters))
            {
                try
                {
                    return cmd.ExecuteNonQuery();

                }
                finally
                {
                    cmd.Parameters.Clear();
                }

            }
        }

        public override T ExecuteScalar<T>(string commandText, T defaultValue, params IDataParameter[] parameters)
        {
            return ExecuteScalar<T>(null, commandText, defaultValue, parameters);
        }

        public override T ExecuteScalar<T>(IDbTransaction trans, string commandText, T defaultValue, params IDataParameter[] parameters)
        {
            using (IDbCommand cmd = _PrepareCommand(trans, commandText, parameters))
            {
                object val = cmd.ExecuteScalar();
                if (val is DBNull || null == val)
                {
                    return defaultValue;
                }
                else
                {
                    return (T)val;
                }
            }
        }

        public override IDataReader ExecuteReader(string commandText, params IDataParameter[] parameters)
        {
            IDbCommand cmd = _PrepareCommand(null, commandText, parameters);
            return cmd.ExecuteReader(CommandBehavior.SingleResult);
        }

        public override IDataReader ExecuteReader(IDbTransaction trans, string commandText, params IDataParameter[] parameters)
        {
            IDbCommand cmd = _PrepareCommand(trans, commandText, parameters);
            return cmd.ExecuteReader(CommandBehavior.SingleResult);
        }

        public override void FillDataSet(DataSet ds, string tableName, string commandText, params IDataParameter[] parameters)
        {
            DataTable dt = GetDataTable(commandText, parameters);
            dt.TableName = tableName;
            ds.Tables.Add(dt);
        }

        public override DataTable GetDataTable(string commandText, params IDataParameter[] parameters)
        {
            return GetDataTable(commandText, null, parameters);
        }

        public override DataTable GetDataTable(string commandText, IDbTransaction trans, params IDataParameter[] parameters)
        {
            DataTable dt = new DataTable();
            using (SqlDataAdapter sqlAdapter = (SqlDataAdapter)CreateDataAdapter(commandText, trans, parameters))
            {
                sqlAdapter.Fill(dt);
            }
            return dt;
        }

        public override string GetDriverName()
        {
            return "SQLServer";
        }

        public override KeyValuePair<TKey, TValue>[] GetKeyValuePairs<TKey, TValue>(string commandText, params IDataParameter[] parameters)
        {
            using (IDataReader rdr = ExecuteReader(commandText, parameters))
            {
                var vals = new List<KeyValuePair<TKey, TValue>>();
                while (rdr.Read())
                {
                    vals.Add(new KeyValuePair<TKey, TValue>((TKey)rdr[0], (TValue)rdr[1]));
                }

                return vals.ToArray();
            }
        }

        public override Dictionary<TKey, TValue> GetDictionary<TKey, TValue>(string commandText, params IDataParameter[] parameters)
        {
            using (IDataReader rdr = ExecuteReader(commandText, parameters))
            {
                var vals = new Dictionary<TKey, TValue>();
                while (rdr.Read())
                {
                    vals[(TKey)rdr[0]] = (TValue)rdr[1];
                }

                return vals;
            }
        }

        public override T[] GetValues<T>(string commandText, params IDataParameter[] parameters)
        {
            return GetValues<T>(null, commandText, parameters);
        }

        public override T[] GetValues<T>(IDbTransaction trans, string commandText, params IDataParameter[] parameters)
        {
            using (IDataReader rdr = ExecuteReader(trans, commandText, parameters))
            {

                List<T> vals = new List<T>();
                while (rdr.Read())
                {
                    vals.Add((T)rdr[0]);
                }

                return vals.ToArray();
            }
        }

        public override IDbDataParameter MakeParam(string name, object value)
        {
            return new SqlParameter(name, value);
        }

        public override string MakeParamReference(string paramName)
        {
            return ParameterQualifier + EscapeCommandText(paramName);
        }

        public override IDbDataParameter MakeLikeParam(string name, object value)
        {
            if (null != value && value is string)
            {
                string val = value.ToString();
                if (0 < val.Length)
                {
                    value = EscapeValueForLike(val);
                }
            }
            return new SqlParameter(name, value);
        }

        public override string MakeLikeParamReference(string paramName)
        {
            return "LIKE " + ParameterQualifier + EscapeNameForLike(paramName);
        }

        public override IDbDataParameter MakeReturnValueParam()
        {
            SqlParameter parameter = new SqlParameter("RETURN_VALUE", null);
            parameter.Direction = ParameterDirection.ReturnValue;
            return parameter;
        }

        public override IDbDataParameter MakeOutputParam(string paramName, DbType type)
        {
            SqlParameter parameter = new SqlParameter(paramName, null);
            parameter.Direction = ParameterDirection.Output;
            parameter.DbType = type;
            return parameter;
        }

        public override string MakeQuotedName(string name)
        {
            if(name.Length > 0 && !(name[0] == '[' && name[name.Length-1] == ']'))
            {
                return "[" + EscapeCommandText(name) + "]";
            }
            else
            {
                return EscapeCommandText(name);
            }
        }

        public override void Open()
        {
            // JB: 9/25/09 - Throw an exception here.  I think it is a better alternative than simply closing and 
            // opening a new one, as it is probably a faulty design that would call Open multiple times and should be addressed.
            if (null != _dbConn)
            {
                throw new InvalidOperationException("A connection is already open.");
            }
            _dbConn = new SqlConnection(_connString);
            _dbConn.Open();
        }
        #region Async methods
        public override DatabaseAsyncResult BeginExecuteReader(string commandText, params IDataParameter[] parameters)
        {
            SqlCommand cmd = (SqlCommand)_PrepareCommand(null, commandText, parameters);
            return new DatabaseAsyncResult(cmd.BeginExecuteReader(CommandBehavior.SingleResult), cmd);
        }
        public override DatabaseAsyncResult BeginExecuteReader(IDbTransaction trans, string commandText, params IDataParameter[] parameters)
        {
            SqlCommand cmd = (SqlCommand)_PrepareCommand(null, commandText, parameters);
            return new DatabaseAsyncResult(cmd.BeginExecuteReader(CommandBehavior.SingleResult), cmd);
        }
        public override IDataReader EndExecuteReader(DatabaseAsyncResult result)
        {
            var cmd = (SqlCommand)result.Command;
            return cmd.EndExecuteReader(result.Result);
        }
        #endregion
        #endregion // end Methods

        #endregion // end Public
    }
}

