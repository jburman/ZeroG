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
        public static readonly string ObjectIndexProviderConfigKey = "ObjectIndexProvider";


        private static string _baseDataPath;
        public static string BaseDataPath
        {
            get
            {
                if (null == _baseDataPath)
                {
                    _baseDataPath = ConfigurationManager.AppSettings["ObjectServiceDataDir"];
                }
                return _baseDataPath;
            }
        }

        private static string _objectIndexSchemaConn;
        public static string ObjectIndexSchemaConnection
        {
            get
            {
                if (null == _objectIndexSchemaConn)
                {
                    _objectIndexSchemaConn = ConfigurationManager.AppSettings["ObjectIndexSchemaConnection"];
                    _objectIndexSchemaConn = _objectIndexSchemaConn ?? ObjectIndexProvider.DefaultSchemaConnection;
                }
                return _objectIndexSchemaConn;
            }
        }

        private static string _objectIndexDataConn;
        public static string ObjectIndexDataConnection
        {
            get
            {
                if (null == _objectIndexDataConn)
                {
                    _objectIndexDataConn = ConfigurationManager.AppSettings["ObjectIndexDataConnection"];
                    _objectIndexDataConn = _objectIndexDataConn ?? ObjectIndexProvider.DefaultDataAccessConnection;
                }
                return _objectIndexDataConn;
            }
        }
    }
}
