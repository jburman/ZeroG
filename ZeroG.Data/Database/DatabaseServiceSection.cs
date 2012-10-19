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
            var configs = new Dictionary<string, DatabaseServiceConfiguration>();

            foreach (XmlNode configNode in node.ChildNodes)
            {
                if (XmlNodeType.Element == configNode.NodeType)
                {
                    string name = null, type = null, connStr = null;

                    XmlAttribute nameAttr = configNode.Attributes["name"];
                    XmlAttribute typeAttr = configNode.Attributes["type"];
                    XmlAttribute connStrAttr = configNode.Attributes["connStr"];

                    if(null != nameAttr) 
                    {
                        name = nameAttr.Value;
                    }
                    if (null != typeAttr)
                    {
                        type = typeAttr.Value;
                    }
                    if (null != connStrAttr)
                    {
                        connStr = connStrAttr.Value;
                    }

                    if(string.IsNullOrEmpty(name))
                    {
                        throw new ArgumentException("Required \"name\" attribute missing from DatabaseServiceSection");
                    }
                    if (string.IsNullOrEmpty(type))
                    {
                        throw new ArgumentException("Required \"type\" attribute missing from DatabaseServiceSection");
                    }
                    if (string.IsNullOrEmpty(connStr))
                    {
                        throw new ArgumentException("Required \"connStr\" attribute missing from DatabaseServiceSection");
                    }

                    // load in any <prop name="" value="" /> nodes and store them in a dictionary
                    Dictionary<string, string> additionalProperties = null;
                    if (0 < configNode.ChildNodes.Count)
                    {
                        additionalProperties = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

                        foreach (XmlNode propNode in configNode.ChildNodes)
                        {
                            if (XmlNodeType.Element == propNode.NodeType && propNode.LocalName == "prop")
                            {
                                string propName = null, propVal = null;

                                XmlAttribute propNameAttr = propNode.Attributes["name"];
                                XmlAttribute propValAttr = propNode.Attributes["value"];

                                if (null != propNameAttr)
                                {
                                    propName = propNameAttr.Value;
                                }
                                if (null != propValAttr)
                                {
                                    propVal = propValAttr.Value;
                                }
                                
                                if (!string.IsNullOrEmpty(propName))
                                {
                                    additionalProperties.Add(propName, propVal);
                                }
                            }
                        }
                    }

                    configs.Add(name, new DatabaseServiceConfiguration(name, type, connStr, additionalProperties));
                }
            }
                /*
            Dictionary<string, DatabaseServiceConfiguration> configs = node.ChildNodes.OfType<XmlNode>()
                .Where(n => XmlNodeType.Element == n.NodeType)
                .Select(n => new DatabaseServiceConfiguration(
                    n.Attributes["name"].Value,
                    n.Attributes["type"].Value,
                    n.Attributes["connStr"].Value))
                .ToDictionary(dbConfig => dbConfig.Name, StringComparer.InvariantCultureIgnoreCase);
                */
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
