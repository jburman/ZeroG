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

namespace ZeroG.Data.Object.Index
{
    public static class ObjectIndexTypeExtensions
    {
        private static Dictionary<ObjectIndexType, Type> _typeMappings;

        static ObjectIndexTypeExtensions()
        {
            _typeMappings = new Dictionary<ObjectIndexType, Type>();
            _typeMappings.Add(ObjectIndexType.String, typeof(string));
            _typeMappings.Add(ObjectIndexType.Integer, typeof(Int32));
            _typeMappings.Add(ObjectIndexType.Binary, typeof(byte[]));
            _typeMappings.Add(ObjectIndexType.Decimal, typeof(decimal));
            _typeMappings.Add(ObjectIndexType.DateTime, typeof(DateTime));
            _typeMappings.Add(ObjectIndexType.Unknown, typeof(string));
        }

        public static ObjectIndexType GetDataType(this ObjectIndexType objectIndexType, object value)
        {
            if (value is byte[])
            {
                return ObjectIndexType.Binary;
            }
            else if (value is string)
            {
                return ObjectIndexType.String;
            }
            else if (value is DateTime)
            {
                return ObjectIndexType.DateTime;
            }
            else if (value is decimal || value is float)
            {
                return ObjectIndexType.Decimal;
            }
            else if (value is int || value is uint || value is short || value is ushort || value is long || value is ulong)
            {
                return ObjectIndexType.Integer;
            }
            else
            {
                return ObjectIndexType.Unknown;
            }
        }

        public static Type GetSystemType(this ObjectIndexType objectIndexType)
        {
            return _typeMappings[objectIndexType];
        }
    }
}
