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

using System.Text.RegularExpressions;

namespace ZeroG.Data.Object
{
    public class ObjectNameValidator
    {
        private static Regex _NameSpaceValidator = new Regex("^([a-zA-Z0-9]+([\\._]?[a-zA-Z0-9])*)$", RegexOptions.Compiled);
        public static bool IsValidNameSpace(string nameSpace)
        {
            return !string.IsNullOrEmpty(nameSpace) && 3 < nameSpace.Length && 30 > nameSpace.Length && _NameSpaceValidator.IsMatch(nameSpace);
        }

        private static Regex _ObjectNameValidator = new Regex("^([a-zA-Z0-9_]+)$", RegexOptions.Compiled);
        public static bool IsValidObjectName(string objectName)
        {
            return !string.IsNullOrEmpty(objectName) && 3 < objectName.Length && 30 > objectName.Length && _ObjectNameValidator.IsMatch(objectName);
        }

        public static bool IsValidIndexName(string indexName)
        {
            return !string.IsNullOrEmpty(indexName) && 3 < indexName.Length && 30 > indexName.Length && _ObjectNameValidator.IsMatch(indexName);
        }
    }
}
