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
using System.Collections.Specialized;
using System.Configuration;
using ZeroG.Data.Database;

namespace ZeroG.Data.Object
{
    /// <summary>
    /// Stores run-time configurable settings for the Object Service and related code.
    /// The configuration values may be initialized either via constructor or via App Settings in the configuration file.
    /// </summary>
    public class Config
    {
        public static readonly string StoreVersion = "1.0";
        private static string _appDir;

        /// <summary>
        /// Allows Config values to be specified without using the App config file.
        /// </summary>
        /// <param name="baseDataPath"></param>
        /// <param name="indexCacheEnabled"></param>
        /// <param name="objectIndexSchemaConn"></param>
        /// <param name="objectIndexDataConn"></param>
        /// <param name="maxObjectDependences"></param>
        public Config(string baseDataPath = ".",
            bool indexCacheEnabled = true,
            int indexCacheMaxQueries = 100_000,
            int indexCacheMaxValues = 10_000_000,
            string objectIndexSchemaConn = ObjectIndexProvider.DefaultSchemaConnection,
            string objectIndexDataConn = ObjectIndexProvider.DefaultDataAccessConnection,
            int maxObjectDependences = 5,
            bool objectStoreAutoClose = true,
            int objectStoreAutoCloseTimeout = 300,
            int objectStoreCacheSize = 100)
        {
            _baseDataPath = baseDataPath;
            _indexCacheEnabled = indexCacheEnabled;
            _indexCacheMaxQueries = indexCacheMaxQueries;
            _indexCacheMaxValues = indexCacheMaxValues;
            _objectIndexSchemaConn = objectIndexSchemaConn;
            _objectIndexDataConn = objectIndexDataConn;
            _maxObjectDependencies = maxObjectDependences;
            _objectStoreAutoClose = objectStoreAutoClose;
            _objectStoreAutoCloseTimeout = objectStoreAutoCloseTimeout;
            _objectStoreCacheSize = objectStoreCacheSize;

            _FinalizeProperties();
        }

        #region Private helpers


        /// <summary>
        /// Performs final processing on configuration property values before they are used.
        /// For example, place holder values are substituted into the BaseDataPath value.
        /// </summary>
        private void _FinalizeProperties()
        {
            if (null != _baseDataPath)
            {
                var index = -1;
                if (-1 != (index = _baseDataPath.IndexOf("{appdir}", StringComparison.OrdinalIgnoreCase)))
                {
                    var sub = _baseDataPath.Substring(index, "{appdir}".Length);

                    if (_appDir == null)
                    {
                        _appDir = AppDomain.CurrentDomain.BaseDirectory;
                    }
                    _baseDataPath = _baseDataPath.Replace(sub, _appDir);
                }
            }
        }

        #endregion

        private static Config _default;
        /// <summary>
        /// A static Config instance initialized from the application's configuration file.
        /// </summary>
        public static Config Default
        {
            get
            {
                if (_default is null)
                    _default = new Config();
                return _default;
            }
        }

        private string _baseDataPath;
        /// <summary>
        /// The base directory to store Object Store values under.
        /// It may include the {appdir} token to allow it to be dynamically rebased to the 
        /// directory that the process is running under.
        /// </summary>
        public string BaseDataPath
        {
            get
            {
                return _baseDataPath;
            }
        }

        private bool _indexCacheEnabled;
        /// <summary>
        /// Enable caching of index query results.
        /// </summary>
        /// <remarks>Defaults to False</remarks>
        public bool IndexCacheEnabled
        {
            get
            {
                return _indexCacheEnabled;
            }
        }

        private int _indexCacheMaxQueries;
        /// <summary>
        /// Specify the maximum number of queries to store in the cache before pruning.
        /// </summary>
        /// <remarks>Defaults to 100,000</remarks>
        public int IndexCacheMaxQueries
        {
            get
            {
                return _indexCacheMaxQueries;
            }
        }

        private int _indexCacheMaxValues;
        /// <summary>
        /// Specify the maximum number of object IDs to store in the cache before pruning
        /// </summary>
        /// <remarks>Defaults to 10,000,000</remarks>
        public int IndexCacheMaxValues
        {
            get
            {
                return _indexCacheMaxValues;
            }
        }

        private string _objectIndexSchemaConn;
        /// <summary>
        /// The database connection string that the Object Service will use when 
        /// updating database schemas.
        /// </summary>
        public string ObjectIndexSchemaConnection
        {
            get
            {
                return _objectIndexSchemaConn;
            }
        }

        private string _objectIndexDataConn;
        /// <summary>
        /// The database connection string that the Object Service will use when 
        /// retrieving or updating data.
        /// </summary>
        public string ObjectIndexDataConnection
        {
            get
            {
                return _objectIndexDataConn;
            }
        }

        private bool _objectStoreAutoClose;
        /// <summary>
        /// Specifies whether or not inactive Object Store instances are automatically closed after a specified time period.
        /// By default, they are retained for future requests and released when the ObjectService is disposed.
        /// </summary>
        /// <seealso cref="ZeroG.Data.Config.ObjectStoreAutoCloseTimeout"/>
        /// <remarks>Defaults to True</remarks>
        public bool ObjectStoreAutoClose
        {
            get
            {
                return _objectStoreAutoClose;
            }
        }

        private int _objectStoreAutoCloseTimeout;
        /// <summary>
        /// Specifies the number of seconds that an Object Store instance is left open before it is closed.
        /// </summary>
        /// <seealso cref="ZeroG.Data.Config.ObjectStoreAutoClose"/>
        /// <remarks>Defaults to 300 seconds</remarks>
        public int ObjectStoreAutoCloseTimeout
        {
            get
            {
                return _objectStoreAutoCloseTimeout;
            }
        }

        private int _objectStoreCacheSize;
        /// <summary>
        /// Specifies size of ObjectStore data cache in MB.
        /// </summary>
        public int ObjectStoreCacheSize 
        {
            get
            {
                return _objectStoreCacheSize;
            }
        }

        private int _maxObjectDependencies;
        /// <summary>
        /// The maximum number of Objects that that another Object can be dependent on.
        /// </summary>
        /// <remarks>Defaults to 5</remarks>
        public int MaxObjectDependencies
        {
            get
            {
                return _maxObjectDependencies;
            }
        }
    }
}
