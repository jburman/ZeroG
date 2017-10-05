#region License, Terms and Conditions
// Copyright (c) 2017 Jeremy Burman
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
        public static readonly string ObjectIndexProviderConfigKey = "ObjectIndexProvider";

        private static string _appDir;

        /// <summary>
        /// Constructs a new Config instance with all properties initialized from the App config file.
        /// </summary>
        public Config()
        {
            BaseDataPath = ConfigurationManager.AppSettings["ObjectServiceDataDir"];

            _LoadPropertiesFromConfig();

            _FinalizeProperties();
        }


        /// <summary>
        /// Constructs a new Config instance with all properties initialized from the App config file,
        /// except for the base data path property.
        /// </summary>
        public Config(string baseDataPath) : this()
        {
            BaseDataPath = baseDataPath;

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
        /// <param name="maxObjectDependencies"></param>
        public Config(string baseDataPath,
            bool indexCacheEnabled,
            int indexCacheMaxQueries,
            int indexCacheMaxValues,
            string objectIndexSchemaConn,
            string objectIndexDataConn,
            int maxObjectDependencies,
            bool objectStoreAutoClose,
            int objectStoreAutoCloseTimeout,
            int objectStoreCacheSize,
            Type serializer,
            Type keyValueStoreProvider)
        {
            BaseDataPath = baseDataPath;
            IndexCacheEnabled = indexCacheEnabled;
            IndexCacheMaxQueries = indexCacheMaxQueries;
            IndexCacheMaxValues = indexCacheMaxValues;
            ObjectIndexSchemaConnection = objectIndexSchemaConn;
            ObjectIndexDataConnection = objectIndexDataConn;
            MaxObjectDependencies = maxObjectDependencies;
            ObjectStoreAutoClose = objectStoreAutoClose;
            ObjectStoreAutoCloseTimeout = objectStoreAutoCloseTimeout;
            ObjectStoreCacheSize = objectStoreCacheSize;
            Serializer = serializer;
            KeyValueStoreProvider = keyValueStoreProvider;

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
            int intParse = 0;

            bool.TryParse(appSettings["ObjectIndexCacheEnabled"], out boolParse);
            IndexCacheEnabled = boolParse;

            intParse = -1;
            if (!int.TryParse(appSettings["ObjectIndexCacheMaxQueries"], out intParse) || intParse < 0)
            {
                intParse = 100000;
            }
            IndexCacheMaxQueries = intParse;

            intParse = -1;
            if (!int.TryParse(appSettings["ObjectIndexCacheMaxValues"], out intParse) || intParse < 0)
            {
                intParse = 10000000;
            }
            IndexCacheMaxValues = intParse;

            ObjectIndexSchemaConnection = appSettings["ObjectIndexSchemaConnection"];
            ObjectIndexSchemaConnection = ObjectIndexSchemaConnection ?? ObjectIndexProvider.DefaultSchemaConnection;

            ObjectIndexDataConnection = appSettings["ObjectIndexDataConnection"];
            ObjectIndexDataConnection = ObjectIndexDataConnection ?? ObjectIndexProvider.DefaultDataAccessConnection;

            intParse = -1;
            if (!int.TryParse(appSettings["MaxObjectDependencies"], out intParse) || intParse < 0)
            {
                intParse = 5;
            }
            MaxObjectDependencies = intParse;

            boolParse = true;
            bool.TryParse(appSettings["ObjectStoreAutoClose"], out boolParse);
            ObjectStoreAutoClose = boolParse;

            intParse = -1;
            if (!int.TryParse(appSettings["ObjectStoreAutoCloseTimeout"], out intParse) || intParse < 0)
            {
                intParse = 300; // reset to the default value
            }
            ObjectStoreAutoCloseTimeout = intParse;

            intParse = -1;
            if (!int.TryParse(appSettings["ObjectStoreCacheSize"], out intParse) || intParse < 0)
            {
                ObjectStoreCacheSize = 100; // reset to the default value
            }
            ObjectStoreCacheSize = intParse;
        }

        /// <summary>
        /// Performs final processing on configuration property values before they are used.
        /// For example, place holder values are substituted into the BaseDataPath value.
        /// </summary>
        private void _FinalizeProperties()
        {
            if (null != BaseDataPath)
            {
                var index = -1;
                if ((index = BaseDataPath.IndexOf("{appdir}", StringComparison.OrdinalIgnoreCase)) != -1)
                {
                    var sub = BaseDataPath.Substring(index, "{appdir}".Length);

                    if (_appDir == null)
                        _appDir = AppDomain.CurrentDomain.BaseDirectory;
                    BaseDataPath = BaseDataPath.Replace(sub, _appDir);
                }
            }
        }

        #endregion

        //private static Config _default;
        ///// <summary>
        ///// A static Config instance initialized from the application's configuration file.
        ///// </summary>
        //public static Config Default
        //{
        //    get
        //    {
        //        if (null == _default)
        //        {
        //            _default = new Config();
        //        }
        //        return _default;
        //    }
        //}

        /// <summary>
        /// The base directory to store Object Store values under.
        /// It may include the {appdir} token to allow it to be dynamically rebased to the 
        /// directory that the process is running under.
        /// </summary>
        public string BaseDataPath { get; private set; }

        /// <summary>
        /// Enable caching of index query results.
        /// </summary>
        /// <remarks>Defaults to False</remarks>
        public bool IndexCacheEnabled { get; private set; }

        /// <summary>
        /// Specify the maximum number of queries to store in the cache before pruning.
        /// </summary>
        /// <remarks>Defaults to 100,000</remarks>
        public int IndexCacheMaxQueries { get; private set; }

        /// <summary>
        /// Specify the maximum number of object IDs to store in the cache before pruning
        /// </summary>
        /// <remarks>Defaults to 10,000,000</remarks>
        public int IndexCacheMaxValues { get; private set; }

        /// <summary>
        /// The database connection string that the Object Service will use when 
        /// updating database schemas.
        /// </summary>
        public string ObjectIndexSchemaConnection { get; private set; }

        /// <summary>
        /// The database connection string that the Object Service will use when 
        /// retrieving or updating data.
        /// </summary>
        public string ObjectIndexDataConnection { get; private set; }

        /// <summary>
        /// Specifies whether or not inactive Object Store instances are automatically closed after a specified time period.
        /// By default, they are retained for future requests and released when the ObjectService is disposed.
        /// </summary>
        /// <seealso cref="ZeroG.Data.Config.ObjectStoreAutoCloseTimeout"/>
        /// <remarks>Defaults to True</remarks>
        public bool ObjectStoreAutoClose { get; private set; }

        /// <summary>
        /// Specifies the number of seconds that an Object Store instance is left open before it is closed.
        /// </summary>
        /// <seealso cref="ZeroG.Data.Config.ObjectStoreAutoClose"/>
        /// <remarks>Defaults to 300 seconds</remarks>
        public int ObjectStoreAutoCloseTimeout { get; private set; }

        /// <summary>
        /// Specifies size of ObjectStore data cache in MB.
        /// </summary>
        public int ObjectStoreCacheSize { get; private set; }


        /// <summary>
        /// The maximum number of Objects that that another Object can be dependent on.
        /// </summary>
        /// <remarks>Defaults to 5</remarks>
        public int MaxObjectDependencies { get; private set; }

        public Type Serializer { get; private set; }
        public Type KeyValueStoreProvider { get; private set; }
    }
}
