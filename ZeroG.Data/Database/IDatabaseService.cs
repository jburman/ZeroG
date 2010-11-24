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
using System.Data;

namespace ZeroG.Data.Database
{
    public interface IDatabaseService : IDisposable
    {
        IDbConnection CurrentConnection { get; }
        bool IsClosed { get; }
        bool IsOpen { get; }
        IDbTransaction BeginTransaction();
        IDbTransaction BeginTransaction(IsolationLevel isolation);
        IDbDataAdapter CreateDataAdapter(string commandText, params IDataParameter[] parameters);
        IDbDataAdapter CreateDataAdapter(string commandText, IDbTransaction trans, params IDataParameter[] parameters);
        string EscapeCommandText(string commandText);
        string EscapeNameForLike(string name);
        string EscapeValueForLike(string value);
        void ExecuteBulkCopy(DataTable copyData, string copyToTable, Dictionary<string, string> columnMap);
        void ExecuteBulkCopy(IDbTransaction transaction, DataTable copyData, string copyToTable, Dictionary<string, string> columnMap);
        int ExecuteNonQuery(string commandText, params IDataParameter[] parameters);
        int ExecuteNonQuery(IDbTransaction trans, string commandText, params IDataParameter[] parameters);
        T ExecuteScalar<T>(string commandText, T defaultValue, params IDataParameter[] parameters);
        T ExecuteScalar<T>(IDbTransaction trans, string commandText, T defaultValue, params IDataParameter[] parameters);
        IDataReader ExecuteReader(string commandText, params IDataParameter[] parameters);
        IDataReader ExecuteReader(IDbTransaction trans, string commandText, params IDataParameter[] parameters);
        void FillDataSet(DataSet ds, string tableName, string commandText, params IDataParameter[] parameters);
        DataTable GetDataTable(string commandText, params IDataParameter[] parameters);
        DataTable GetDataTable(string commandText, IDbTransaction trans, params IDataParameter[] parameters);
        string GetDriverName();
        KeyValuePair<TKey, TValue>[] GetKeyValuePairs<TKey, TValue>(string commandText, params IDataParameter[] parameters);
        Dictionary<TKey, TValue> GetDictionary<TKey, TValue>(string commandText, params IDataParameter[] parameters);
        T[] GetValues<T>(string commandText, params IDataParameter[] parameters);
        T[] GetValues<T>(IDbTransaction trans, string commandText, params IDataParameter[] parameters);
        IDbDataParameter MakeParam(string name, object value);
        IDbDataParameter MakeLikeParam(string name, object value);
        string MakeLikeParamReference(string paramName);
        IDbDataParameter MakeReturnValueParam();
        IDbDataParameter MakeOutputParam(string paramName, DbType type);
        void Open();
    }
}
