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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

[assembly: InternalsVisibleTo("ZeroG.Tests")]

namespace ZeroG.Data.Database
{
    /// <summary>
    /// Provides a helper layer for interacting with databases.  The DatabaseService is abstract and must 
    /// have a driver loaded for it to be used.  The normal method for loading a driver is via the config file.
    /// The following config section must be added to the config file.
    /// <![CDATA[
    /// <configSections>
    ///    <section name="databaseServiceConfigs" type="ZeroG.Data.Database.DatabaseServiceSection, ZeroG.Data, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null"
    ///             allowDefinition="Everywhere" allowExeDefinition="MachineToApplication" restartOnExternalChanges="true" />
    ///    </configSections>
    /// ]]>
    /// To add a DatabaseService driver configuration entry add the following into the config file.
    /// A SQLServer driver is also available (change "MySQL" to "SQLServer" below)
    /// <![CDATA[
    /// <databaseServiceConfigs>
    ///    <config name="MyDbConn" 
    ///            type="ZeroG.Data.Database.Drivers.MySQLDatabaseService, ZeroG.Database.MySQL, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null"
    ///            connStr="Server=hostname;Database=databasename;Uid=***;pwd=***;CharSet=utf8" />
    /// </databaseServiceConfigs>
    /// ]]>
    /// To load and use the above database configuration use the following code.
    /// using(DatabaseService db = DatabaseService.GetService("MyDbConn")) 
    /// {
    ///     db.Open();
    ///     
    ///     //...
    /// }
    /// </summary>
    public abstract class DatabaseService : IDatabaseService
    {
        /// <summary>
        /// Timeout for SQL commands defined in seconds.
        /// </summary>
        protected int _commandTimeout;
        protected string _connString;
        protected IDbConnection _dbConn;
        private bool _disposed;

        #region Constructors/Destructors
        static DatabaseService()
        {
        }

        protected DatabaseService()
        {
            _commandTimeout = 300;
            _disposed = false;
        }

        public DatabaseService(string connStr)
            : this()
        {
            _connString = connStr;
        }

        ~DatabaseService()
        {
            _Dispose(false);
        }
        #endregion

        #region Private
        #region Properties
        #endregion // end Properties

        #region Methods
        private void _Dispose(bool disposing)
        {
            if (!this._disposed)
            {
                if (disposing)
                {
                    if (null != _dbConn)
                    {
                        if (ConnectionState.Closed != _dbConn.State)
                        {
                            _dbConn.Close();
                        }
                        _dbConn.Dispose();
                    }
                }

                _disposed = true;
            }
        }

        private static ConcurrentDictionary<string, DatabaseServiceConfig> _configs = new ConcurrentDictionary<string, DatabaseServiceConfig>();
        public static void Configure(DatabaseServiceConfig config)
        {
            _configs.TryAdd(config.Name, config);
        }

        public static DatabaseService GetService(string name)
        {
            if (_configs.TryGetValue(name, out DatabaseServiceConfig config))
            {
                DatabaseService db = (DatabaseService)Activator.CreateInstance(config.DriverType);
                db.ConfigureDriver(config);
                return db;
            }
            return null;
        }

        protected bool _IsConnAvailable()
        {
            if (null == _dbConn)
            {
                throw new InvalidOperationException("The CurrentConnection property is not available before Open() has been called.");
            }
            if (_disposed)
            {
                throw new ObjectDisposedException("The DatabaseService instance has alredy been exposed.");
            }
            if (ConnectionState.Closed == _dbConn.State)
            {
                throw new InvalidOperationException("The database connection has been closed and is no longer available.  Please call Open() to open a new connection.");
            }
            return true;
        }

        public static readonly Regex StoreProcPattern = new Regex("^([\\[]?[a-zA-Z0-9]+[\\]]?\\.)?[\\[]?sp_",
            RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace);

        protected IDbCommand _PrepareCommand(IDbTransaction trans, string commandText, IEnumerable<IDataParameter> parameters)
        {
            _IsConnAvailable();
            IDbCommand cmd = _dbConn.CreateCommand();
            if (null != trans)
            {
                cmd.Transaction = trans;
            }
            cmd.CommandText = commandText;
            if (StoreProcPattern.IsMatch(commandText))
            {
                cmd.CommandType = CommandType.StoredProcedure;
            }
            if (null != parameters)
            {
                foreach (IDataParameter param in parameters)
                {
                    cmd.Parameters.Add(param);
                }
            }
            cmd.CommandTimeout = _commandTimeout;
            return cmd;
        }
        #endregion // end Methods

        #endregion

        #region Public

        #region Properties
        public IDbConnection CurrentConnection
        {
            get
            {
                return _dbConn;
            }
        }

        public string DatabaseName
        {
            get
            {
                return (null == _dbConn) ? null : _dbConn.Database;
            }
        }

        public bool IsClosed
        {
            get
            {
                try
                {
                    return null == _dbConn || ConnectionState.Closed == _dbConn.State;
                }
                catch (ObjectDisposedException)
                {
                    return true;
                }
            }
        }

        public bool IsOpen
        {
            get
            {
                return (null != _dbConn && ConnectionState.Closed != _dbConn.State);
            }
        }
        #endregion // end Properties

        #region Methods
        public abstract void ConfigureDriver(DatabaseServiceConfig config);
        public abstract IDbTransaction BeginTransaction();
        public abstract IDbTransaction BeginTransaction(IsolationLevel isolation);
        public abstract IDbDataAdapter CreateDataAdapter(string commandText, params IDataParameter[] parameters);
        public abstract IDbDataAdapter CreateDataAdapter(string commandText, IDbTransaction trans, params IDataParameter[] parameters);

        public virtual void Dispose()
        {
            _Dispose(true);
            GC.SuppressFinalize(this);
        }

        public abstract string EscapeCommandText(string commandText);
        public abstract string EscapeNameForLike(string name);
        public abstract string EscapeValueForLike(string value);
        public abstract void ExecuteBulkCopy(DataTable copyData, string copyToTable, Dictionary<string, string> columnMap);
        public abstract void ExecuteBulkCopy(IDbTransaction transaction, DataTable copyData, string copyToTable, Dictionary<string, string> columnMap);
        public abstract void ExecuteBulkInsert(IEnumerable<object[]> insertData, string insertToTable, string[] columns);
        public abstract int ExecuteNonQuery(string commandText, params IDataParameter[] parameters);
        public abstract int ExecuteNonQuery(IDbTransaction trans, string commandText, params IDataParameter[] parameters);
        public abstract T ExecuteScalar<T>(string commandText, T defaultValue, params IDataParameter[] parameters);
        public abstract T ExecuteScalar<T>(IDbTransaction trans, string commandText, T defaultValue, params IDataParameter[] parameters);
        public abstract IDataReader ExecuteReader(string commandText, params IDataParameter[] parameters);
        public abstract IDataReader ExecuteReader(IDbTransaction trans, string commandText, params IDataParameter[] parameters);
        public abstract void FillDataSet(DataSet ds, string tableName, string commandText, params IDataParameter[] parameters);
        public abstract DataTable GetDataTable(string commandText, params IDataParameter[] parameters);
        public abstract DataTable GetDataTable(string commandText, IDbTransaction trans, params IDataParameter[] parameters);
        public abstract string GetDriverName();
        public abstract KeyValuePair<TKey, TValue>[] GetKeyValuePairs<TKey, TValue>(string commandText, params IDataParameter[] parameters);
        public abstract Dictionary<TKey, TValue> GetDictionary<TKey, TValue>(string commandText, params IDataParameter[] parameters);
        public abstract T[] GetValues<T>(string commandText, params IDataParameter[] parameters);
        public abstract T[] GetValues<T>(IDbTransaction trans, string commandText, params IDataParameter[] parameters);
        public abstract IDbDataParameter MakeParam(string name, object value);
        public abstract string MakeParamReference(string paramName);
        public abstract IDbDataParameter MakeLikeParam(string name, object value);
        public abstract string MakeLikeParamReference(string paramName);
        public abstract IDbDataParameter MakeReturnValueParam();
        public abstract IDbDataParameter MakeOutputParam(string paramName, DbType type);
        public abstract string MakeQuotedName(string name);
        public abstract void Open();

        #region Async methods
        public abstract Task<IDataReader> ExecuteReaderAsync(string commandText, CancellationToken cancellationToken, params IDataParameter[] parameters);
        public abstract Task<IDataReader> ExecuteReaderAsync(IDbTransaction trans, string commandText, CancellationToken cancellationToken, params IDataParameter[] parameters);
        #endregion
        #endregion // end Methods

        #endregion // end Public
    }
}
