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
using System.Collections.Generic;
using System.Text.RegularExpressions;
using ZeroG.Data.Object.Metadata;

namespace ZeroG.Data.Object
{
    public sealed class ObjectNaming
    {
        public const string DefaultNameSpace = "System.Default";

        private ObjectMetadataStore _objectMetadata;

        private HashSet<string> _validNameSpaces;
        private HashSet<string> _validNames;

        internal ObjectNaming(ObjectMetadataStore objectMetadata)
        {
            if (null == objectMetadata)
            {
                throw new ArgumentNullException("objectMetadata");
            }
            _objectMetadata = objectMetadata;
            _validNameSpaces = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _validNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            _validNameSpaces.Add(DefaultNameSpace);

            foreach (string name in _objectMetadata.EnumerateObjectNames())
            {
                _validNames.Add(name);
            }

            foreach (string ns in _objectMetadata.EnumerateNameSpaces())
            {
                _validNameSpaces.Add(ns);
            }

            _objectMetadata.ObjectNameSpaceAdded += new ObjectMetadatastoreUpdatedEvent((string name) =>
            {
                _validNameSpaces.Add(name);
            });

            _objectMetadata.ObjectNameSpaceRemoved += new ObjectMetadatastoreUpdatedEvent((string name) =>
            {
                _validNameSpaces.Remove(name);
            });

            _objectMetadata.ObjectMetadataAdded += new ObjectMetadatastoreUpdatedEvent((string name) =>
            {
                _validNames.Add(name);
            });

            _objectMetadata.ObjectMetadataRemoved += new ObjectMetadatastoreUpdatedEvent((string name) =>
            {
                _validNames.Remove(name);
            });
        }

        internal bool NameSpaceExists(string nameSpace)
        {
            if (string.IsNullOrEmpty(nameSpace))
            {
                return false;
            }
            else
            {
                return _validNameSpaces.Contains(nameSpace);
            }
        }

        internal bool ObjectNameExists(string nameSpace, string objectName)
        {
            if (string.IsNullOrEmpty(nameSpace))
            {
                return false;
            }

            if (string.IsNullOrEmpty(objectName))
            {
                return false;
            }
            else
            {
                return _validNames.Contains(CreateFullObjectName(nameSpace, objectName));
            }
        }

        public static string CreateFullObjectName(string nameSpace, string objectName)
        {
            return ObjectNameFormatter.CreateFullObjectName(nameSpace, objectName);
        }

        public static byte[] CreateFullObjectKey(string nameSpace, string objectName)
        {
            return ObjectNameFormatter.CreateFullObjectKey(nameSpace, objectName);
        }

        public static byte[] CreateFullObjectKey(string objectFullName)
        {
            return ObjectNameFormatter.CreateFullObjectKey(objectFullName);
        }

        public static string GetNameSpaceFromFullObjectName(string fullObjectName)
        {
            string returnVal = null;

            if (!string.IsNullOrEmpty(fullObjectName))
            {
                var idx = fullObjectName.IndexOf('.');
                if (-1 != idx)
                {
                    returnVal = fullObjectName.Substring(0, idx);
                }
            }

            return returnVal;
        }

        internal static string GetCacheKeyName(string objectName)
        {
            if (null != objectName)
            {
                return objectName + "_CK";
            }
            else
            {
                throw new ArgumentNullException("objectName");
            }
        }
    }
}
