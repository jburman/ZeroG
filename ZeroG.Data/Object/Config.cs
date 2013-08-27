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
        public static readonly string ObjectIndexProviderConfigKey = "ObjectIndexProvider";

        private static string _appDir;

        /// <summary>
        /// Constructs a new Config instance with all properties initialized from the App config file.
        /// </summary>
        public Config()
        {
            _baseDataPath = ConfigurationManager.AppSettings["ObjectServiceDataDir"];

            _LoadPropertiesFromConfig();

            _FinalizeProperties();
        }


        /// <summary>
        /// Constructs a new Config instance with all properties initialized from the App config file,
        /// except for the base data path property.
        /// </summary>
        public Config(string baseDataPath) : this()
        {
            _baseDataPath = baseDataPath;

            _LoadPropertiesFromConfig();

            _FinalizeProperties();
        }

        /// <summary>
        /// Allows Config values to be specified without using the App config file.
        /// </summary>
        /// <param name="baseDataPath"></param>
        /// <param name="indexCacheEnabled"></param>
        /// <param name="objectIndexSchemaConn"></param>
        /// <param name="objectIndexDataConn"></param>
        /// <param name="maxObjectDependences"></param>
        public Config(string baseDataPath,
            bool indexCacheEnabled,
            string objectIndexSchemaConn,
            string objectIndexDataConn,
            uint maxObjectDependences,
            bool objectStoreAutoClose,
            uint objectStoreAutoCloseTimeout,
            uint objectStoreCacheSize)
        {
            _baseDataPath = baseDataPath;
            _indexCacheEnabled = indexCacheEnabled;
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
        /// Initializes all properties from the App config file except for ObjectServiceDataDir.
        /// </summary>
        private void _LoadPropertiesFromConfig()
        {
            NameValueCollection appSettings = ConfigurationManager.AppSettings;

            bool boolParse = false;
            bool.TryParse(appSettings["ObjectIndexCacheEnabled"], out boolParse);
            _indexCacheEnabled = boolParse;

            _objectIndexSchemaConn = appSettings["ObjectIndexSchemaConnection"];
            _objectIndexSchemaConn = _objectIndexSchemaConn ?? ObjectIndexProvider.DefaultSchemaConnection;

            _objectIndexDataConn = appSettings["ObjectIndexDataConnection"];
            _objectIndexDataConn = _objectIndexDataConn ?? ObjectIndexProvider.DefaultDataAccessConnection;

            uint intParse = 0;
            uint.TryParse(appSettings["MaxObjectDependencies"], out intParse);
            if (0 == intParse)
            {
                intParse = 5;
            }
            _maxObjectDependencies = intParse;

            boolParse = true;
            bool.TryParse(appSettings["ObjectStoreAutoClose"], out boolParse);
            _objectStoreAutoClose = boolParse;

            intParse = 300;
            uint.TryParse(appSettings["ObjectStoreAutoCloseTimeout"], out intParse);
            _objectStoreAutoCloseTimeout = intParse;
            if (0 == intParse)
            {
                _objectStoreAutoCloseTimeout = 300; // reset to the default value
            }

            intParse = 100;
            uint.TryParse(appSettings["ObjectStoreCacheSize"], out intParse);
            _objectStoreCacheSize = intParse;
            if (0 == intParse)
            {
                _objectStoreCacheSize = 100; // reset to the default value
            }
        }

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
                if (null == _default)
                {
                    _default = new Config();
                }
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

        private uint _objectStoreAutoCloseTimeout;
        /// <summary>
        /// Specifies the number of seconds that an Object Store instance is left open before it is closed.
        /// </summary>
        /// <seealso cref="ZeroG.Data.Config.ObjectStoreAutoClose"/>
        /// <remarks>Defaults to 300 seconds</remarks>
        public uint ObjectStoreAutoCloseTimeout
        {
            get
            {
                return _objectStoreAutoCloseTimeout;
            }
        }

        private uint _objectStoreCacheSize;
        /// <summary>
        /// Specifies size of ObjectStore data cache in MB.
        /// </summary>
        public uint ObjectStoreCacheSize 
        {
            get
            {
                return _objectStoreCacheSize;
            }
        }

        private uint _maxObjectDependencies;
        /// <summary>
        /// The maximum number of Objects that that another Object can be dependent on.
        /// </summary>
        /// <remarks>Defaults to 5</remarks>
        public uint MaxObjectDependencies
        {
            get
            {
                return _maxObjectDependencies;
            }
        }
    }
}
