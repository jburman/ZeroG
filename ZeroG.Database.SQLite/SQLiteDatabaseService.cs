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
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using ZeroG.Data.Database;
using ZeroG.Data.Database.Drivers;

namespace ZeroG.Data.Database.Drivers
{
    public class SQLiteDatabaseService : DatabaseService
    {
        #region Constants
        public static readonly string ParameterQualifier = "@";
        #endregion

        #region Constructors/Destructors

        public SQLiteDatabaseService()
            : base() 
        {
        }

        internal SQLiteDatabaseService(string connStr)
            : base(_SubConnStr(connStr))
        {
        }

        #endregion
        
        #region Public

        #region Properties
        public string CurrentConnectionString
        {
            get
            {
                return _connString;
            }
        }
        #endregion // end Properties

        #region Methods

        private static string _appDir;

        /// <summary>
        /// Processes substitution strings in the connection string.
        /// Supported substitutions are:
        /// {AppDir} - this is replaced with the application's current directy.
        /// </summary>
        /// <param name="connStr"></param>
        private static string _SubConnStr(string connStr)
        {
            if (!string.IsNullOrEmpty(connStr))
            {
                var index = -1;
                if (-1 != (index = connStr.IndexOf("{appdir}", StringComparison.OrdinalIgnoreCase)))
                {
                    var sub = connStr.Substring(index, "{appdir}".Length);

                    if (null == _appDir)
                        _appDir = AppDomain.CurrentDomain.BaseDirectory.TrimEnd('\\').TrimEnd('/');
                    connStr = connStr.Replace(sub, _appDir);
                }
            }
            return connStr;
        }

        public override IDbTransaction BeginTransaction()
        {
            _IsConnAvailable();
            return _dbConn.BeginTransaction();
        }

        public override IDbTransaction BeginTransaction(System.Data.IsolationLevel isolation)
        {
            _IsConnAvailable();
            return _dbConn.BeginTransaction(isolation);
        }

        public override void ConfigureDriver(DatabaseServiceConfig config)
        {
            _connString = _SubConnStr(config.ConnectionString);
        }

        public override IDbDataAdapter CreateDataAdapter(string commandText, params IDataParameter[] parameters)
        {
            return CreateDataAdapter(commandText, null, parameters);
        }

        public override IDbDataAdapter CreateDataAdapter(string commandText, IDbTransaction trans, params IDataParameter[] parameters)
        {
            SQLiteCommand cmd = (SQLiteCommand)_PrepareCommand(trans, commandText, parameters);
            return new SQLiteDataAdapter(cmd);
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
            throw new NotSupportedException("Bulk Copy is not supported by the SQLite Database Server.");
        }

        public override void ExecuteBulkInsert(IEnumerable<object[]> insertData, string insertToTable, string[] columns)
        {
            var trans = BeginTransaction();

            try
            {
                var parameters = new List<IDataParameter>();
                var firstRow = insertData.FirstOrDefault();
                var sql = "INSERT OR REPLACE INTO " + MakeQuotedName(insertToTable) + " (" + string.Join(",", columns.Select(c => MakeQuotedName(c)).ToArray()) + ") VALUES({0})";
                if (null != firstRow)
                {
                    string paramStr = "";
                    var len = columns.Length;
                    for (int i = 0; len > i; i++)
                    {
                        parameters.Add(MakeParam("p" + i, firstRow[i]));
                        paramStr = paramStr + MakeParamReference("p" + i);
                        if (i != len - 1)
                        {
                            paramStr = paramStr + ",";
                        }
                    }
                    sql = string.Format(sql, paramStr);

                    using (IDbCommand cmd = _PrepareCommand(trans, sql, parameters))
                    {
                        cmd.Prepare();

                        foreach (var row in insertData)
                        {
                            for (int i = 0; len > i; i++)
                            {
                                parameters[i].Value = row[i];
                            }

                            cmd.ExecuteNonQuery();
                        }
                    }
                }

                trans.Commit();
            }
            catch
            {
                trans.Rollback();

                throw;
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
                    return (T)Convert.ChangeType(val, typeof(T));
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
            using (SQLiteDataAdapter sqlAdapter = (SQLiteDataAdapter)CreateDataAdapter(commandText, trans, parameters))
            {
                sqlAdapter.Fill(dt);
            }
            return dt;
        }

        public override string GetDriverName()
        {
            return "SQLite";
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
            return new SQLiteParameter(name, value);
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
            return new SQLiteParameter(name, value);
        }

        public override string MakeLikeParamReference(string paramName)
        {
            return "LIKE " + ParameterQualifier + EscapeNameForLike(paramName);
        }

        public override IDbDataParameter MakeReturnValueParam()
        {
            SQLiteParameter parameter = new SQLiteParameter(ParameterQualifier + "RETURN_VALUE", null);
            parameter.Direction = ParameterDirection.ReturnValue;
            return parameter;
        }

        public override IDbDataParameter MakeOutputParam(string paramName, DbType type)
        {
            SQLiteParameter parameter = new SQLiteParameter(ParameterQualifier + paramName, null);
            parameter.Direction = ParameterDirection.Output;
            parameter.DbType = type;
            return parameter;
        }

        public override string MakeQuotedName(string name)
        {
            if (name.Length > 0 && !(name[0] == '[' && name[name.Length - 1] == ']'))
            {
                return "[" + EscapeCommandText(name) + "]";
            }
            else
            {
                return EscapeCommandText(name);
            }
        }

        private static Dictionary<string, IDbConnection> _transDBs = new Dictionary<string, IDbConnection>();

        private IDbConnection _GetTransactionDB(string connStr)
        {
            IDbConnection returnValue = null;

            var trans = Transaction.Current;
            if (null != trans && (TransactionStatus.Active == trans.TransactionInformation.Status))
            {
                var id = trans.TransactionInformation.LocalIdentifier;
                id = connStr + ":" + id;

                lock (_transDBs)
                {
                    if (_transDBs.ContainsKey(id))
                    {
                        returnValue = _transDBs[id];
                    }
                }
            }

            return returnValue;
        }

        private void _StoreTransactionDB(IDbConnection conn, string connStr)
        {
            var trans = Transaction.Current;
            if (null != trans && (TransactionStatus.Active == trans.TransactionInformation.Status))
            {
                var id = trans.TransactionInformation.LocalIdentifier;
                id = connStr + ":" + id;
                lock (_transDBs)
                {
                    _transDBs[id] = conn;
                }

                trans.TransactionCompleted += (object sender, TransactionEventArgs e) => 
                {
                    base.Dispose();
                };
            }
        }

        private bool _IsInTransaction(string connStr)
        {
            var returnValue = false;

            var trans = Transaction.Current;
            if (null != trans && (TransactionStatus.Active == trans.TransactionInformation.Status))
            {
                var id = trans.TransactionInformation.LocalIdentifier;
                id = connStr + ":" + id;
                lock (_transDBs)
                {
                    returnValue = _transDBs.ContainsKey(id);
                }
            }

            return returnValue;
        }

        public override void Open()
        {
            // JB: 9/25/09 - Throw an exception here.  I think it is a better alternative than simply closing and 
            // opening a new one, as it is probably a faulty design that would call Open multiple times and should be addressed.
            if (null != _dbConn)
            {
                throw new InvalidOperationException("A connection is already open.");
            }

            _dbConn = _GetTransactionDB(_connString);
            if (null == _dbConn)
            {
                _dbConn = new SQLiteConnection(_connString);
                _dbConn.Open();
                _StoreTransactionDB(_dbConn, _connString);
            }
        }

        public override void Dispose()
        {
            if (!_IsInTransaction(_connString))
            {
                base.Dispose();
            }
        }

        #region Async methods
        public override async Task<IDataReader> ExecuteReaderAsync(string commandText, CancellationToken cancellationToken, params IDataParameter[] parameters)
        {
            SQLiteCommand cmd = (SQLiteCommand)_PrepareCommand(null, commandText, parameters);
            return await cmd.ExecuteReaderAsync(CommandBehavior.SingleResult, cancellationToken);
        }
        public override async Task<IDataReader> ExecuteReaderAsync(IDbTransaction trans, string commandText, CancellationToken cancellationToken, params IDataParameter[] parameters)
        {
            SQLiteCommand cmd = (SQLiteCommand)_PrepareCommand(null, commandText, parameters);
            return await cmd.ExecuteReaderAsync(CommandBehavior.SingleResult);
        }
        #endregion
        #endregion // end Methods

        #endregion // end Public
    }
}
