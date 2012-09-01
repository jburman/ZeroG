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
    internal sealed class ObjectNaming
    {
        public const string DefaultNameSpace = "System.Default";

        private ObjectMetadataStore _objectMetadata;

        private HashSet<string> _validNameSpaces;
        private HashSet<string> _validNames;

        public ObjectNaming(ObjectMetadataStore objectMetadata)
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

            _objectMetadata.ObjectMetadataAdded += new ObjectMetadatastoreUpdatedEvent((string name) =>
            {
                _validNames.Add(name);
            });

            _objectMetadata.ObjectMetadataRemoved += new ObjectMetadatastoreUpdatedEvent((string name) =>
            {
                _validNames.Remove(name);
            });
        }

        private static Regex _NameSpaceValidator = new Regex("[a-zA-Z0-9]+(([\\._])[a-zA-Z0-9])*", RegexOptions.Compiled);
        public static bool IsValidNameSpace(string nameSpace)
        {
            return !string.IsNullOrEmpty(nameSpace) && 3 < nameSpace.Length && 30 > nameSpace.Length && _NameSpaceValidator.IsMatch(nameSpace);
        }

        private static Regex _ObjectNameValidator = new Regex("[a-zA-Z0-9_]+", RegexOptions.Compiled);
        public static bool IsValidObjectName(string objectName)
        {
            return !string.IsNullOrEmpty(objectName) && 3 < objectName.Length && 30 > objectName.Length && _ObjectNameValidator.IsMatch(objectName);
        }

        public static bool IsValidIndexName(string indexName)
        {
            return !string.IsNullOrEmpty(indexName) && 3 < indexName.Length && 30 > indexName.Length && _ObjectNameValidator.IsMatch(indexName);
        }

        public bool NameSpaceExists(string nameSpace)
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

        public bool ObjectNameExists(string nameSpace, string objectName)
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
            return nameSpace + "." + objectName;
        }

        public static byte[] CreateFullObjectKey(string nameSpace, string objectName)
        {
            return SerializerHelper.Serialize(nameSpace + "." + objectName);
        }

        public static byte[] CreateFullObjectKey(string objectFullName)
        {
            return SerializerHelper.Serialize(objectFullName);
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
