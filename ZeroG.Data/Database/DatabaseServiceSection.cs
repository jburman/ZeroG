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
using System.Xml;

namespace ZeroG.Data.Database
{
    public class DatabaseServiceSection : IConfigurationSectionHandler
    {
        private Dictionary<string, DatabaseServiceConfiguration> _configs;

        public object Create(object parent, object configContext, XmlNode node)
        {
            Dictionary<string, DatabaseServiceConfiguration> configs = node.ChildNodes.OfType<XmlNode>()
                .Select(n => new DatabaseServiceConfiguration(
                    n.Attributes["name"].Value,
                    n.Attributes["type"].Value,
                    n.Attributes["connStr"].Value))
                .ToDictionary(dbConfig => dbConfig.Name, StringComparer.InvariantCultureIgnoreCase);

            return new DatabaseServiceSection(configs);
        }

        public DatabaseServiceSection()
        {
        }

        public DatabaseServiceSection(Dictionary<string, DatabaseServiceConfiguration> configs)
        {
            _configs = configs;
        }

        public Dictionary<string, DatabaseServiceConfiguration> Configs
        {
            get
            {
                return (null == _configs) ?
                    new Dictionary<string, DatabaseServiceConfiguration>() :
                    new Dictionary<string, DatabaseServiceConfiguration>(_configs); // copy the configs
            }
        }
    }
}
