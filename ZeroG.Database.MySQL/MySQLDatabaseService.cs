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
using System.Data.Common;
using System.Text.RegularExpressions;
using MySql.Data.MySqlClient;

namespace ZeroG.Data.Database.Drivers
{

    public sealed class MySQLDatabaseService : DatabaseService
    {
        #region Constructors/Destructors

        public MySQLDatabaseService()
            : base() 
        {
        }

        internal MySQLDatabaseService(string connStr)
            : base(connStr)
        {
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
        }

        public override IDbDataAdapter CreateDataAdapter(string commandText, params IDataParameter[] parameters)
        {
            return CreateDataAdapter(commandText, null, parameters);
        }

        public override IDbDataAdapter CreateDataAdapter(string commandText, IDbTransaction trans, params IDataParameter[] parameters)
        {
            MySqlCommand cmd = (MySqlCommand)_PrepareCommand(trans, commandText, parameters);
            return new MySqlDataAdapter(cmd);
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
            return name + " ESCAPE '\\\\'";
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
            throw new NotSupportedException("Bulk Copy is not supported by the MySQL Database Server.");
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
            using (MySqlDataAdapter sqlAdapter = (MySqlDataAdapter)CreateDataAdapter(commandText, trans, parameters))
            {
                sqlAdapter.Fill(dt);
            }
            return dt;
        }

        public override string GetDriverName()
        {
            return "MySQL";
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
            return new MySqlParameter(name, value);
        }

        public override string MakeParamReference(string paramName)
        {
            return "?" + paramName;
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
            return new MySqlParameter(name, value);
        }

        public override string MakeLikeParamReference(string paramName)
        {
            return "like ?" + EscapeNameForLike(paramName);
        }

        public override IDbDataParameter MakeReturnValueParam()
        {
            MySqlParameter parameter = new MySqlParameter("?RETURN_VALUE", null);
            parameter.Direction = ParameterDirection.ReturnValue;
            return parameter;
        }

        public override IDbDataParameter MakeOutputParam(string paramName, DbType type)
        {
            MySqlParameter parameter = new MySqlParameter("?" + paramName, null);
            parameter.Direction = ParameterDirection.Output;
            parameter.DbType = type;
            return parameter;
        }

        public override string MakeQuotedName(string name)
        {
            return "`" + name + "`";
        }

        public override void Open()
        {
            // JB: 9/25/09 - Throw an exception here.  I think it is a better alternative than simply closing and 
            // opening a new one, as it is probably a faulty design that would call Open multiple times and should be addressed.
            if (null != _dbConn)
            {
                throw new InvalidOperationException("A connection is already open.");
            }
            _dbConn = new MySqlConnection(_connString);
            _dbConn.Open();
        }
        #region Async methods
        public override DatabaseAsyncResult BeginExecuteReader(string commandText, params IDataParameter[] parameters)
        {
            MySqlCommand cmd = (MySqlCommand)_PrepareCommand(null, commandText, parameters);
            return new DatabaseAsyncResult(cmd.BeginExecuteReader(CommandBehavior.SingleResult), cmd);
        }
        public override DatabaseAsyncResult BeginExecuteReader(IDbTransaction trans, string commandText, params IDataParameter[] parameters)
        {
            MySqlCommand cmd = (MySqlCommand)_PrepareCommand(null, commandText, parameters);
            return new DatabaseAsyncResult(cmd.BeginExecuteReader(CommandBehavior.SingleResult), cmd);
            
        }
        public override IDataReader EndExecuteReader(DatabaseAsyncResult result)
        {
            var cmd = (MySqlCommand)result.Command;
            return cmd.EndExecuteReader(result.Result);
        }
        #endregion
        #endregion // end Methods

        #endregion // end Public
    }
}

