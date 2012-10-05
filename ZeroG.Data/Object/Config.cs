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

using System.Configuration;
using ZeroG.Data.Database;

namespace ZeroG.Data.Object
{
    public class Config
    {
        public static readonly string StoreVersion = "1.0";

        public static readonly string ObjectIndexProviderConfigKey = "ObjectIndexProvider";

        public Config()
        {
            _baseDataPath = ConfigurationManager.AppSettings["ObjectServiceDataDir"];
            
            bool boolParse = false;
            bool.TryParse(ConfigurationManager.AppSettings["ObjectIndexCacheEnabled"], out boolParse);
            _indexCacheEnabled = boolParse;

            _objectIndexSchemaConn = ConfigurationManager.AppSettings["ObjectIndexSchemaConnection"];
            _objectIndexSchemaConn = _objectIndexSchemaConn ?? ObjectIndexProvider.DefaultSchemaConnection;

            _objectIndexDataConn = ConfigurationManager.AppSettings["ObjectIndexDataConnection"];
            _objectIndexDataConn = _objectIndexDataConn ?? ObjectIndexProvider.DefaultDataAccessConnection;

            uint intParse = 0;
            uint.TryParse(ConfigurationManager.AppSettings["MaxObjectDependencies"], out intParse);
            if(0 == intParse) 
            {
                intParse = 5;
            }
            _maxObjectDependencies = intParse;
        }

        public Config(string baseDataPath) : this()
        {
            _baseDataPath = baseDataPath;
        }

        public Config(string baseDataPath,
            bool indexCacheEnabled,
            string objectIndexSchemaConn,
            string objectIndexDataConn,
            uint maxObjectDependences)
        {
            _baseDataPath = baseDataPath;
            _indexCacheEnabled = indexCacheEnabled;
            _objectIndexSchemaConn = objectIndexSchemaConn;
            _objectIndexDataConn = objectIndexDataConn;
            _maxObjectDependencies = maxObjectDependences;
        }

        private static Config _default;
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
        public string BaseDataPath
        {
            get
            {
                return _baseDataPath;
            }
        }

        private bool _indexCacheEnabled;
        public bool IndexCacheEnabled
        {
            get
            {
                return _indexCacheEnabled;
            }
        }

        private string _objectIndexSchemaConn;
        public string ObjectIndexSchemaConnection
        {
            get
            {
                return _objectIndexSchemaConn;
            }
        }

        private string _objectIndexDataConn;
        public string ObjectIndexDataConnection
        {
            get
            {
                return _objectIndexDataConn;
            }
        }

        private uint _maxObjectDependencies;
        public uint MaxObjectDependencies
        {
            get
            {
                return _maxObjectDependencies;
            }
        }
    }
}
