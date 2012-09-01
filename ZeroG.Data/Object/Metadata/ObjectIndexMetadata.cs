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
using System.Runtime.Serialization;
using ZeroG.Data.Object.Index;

namespace ZeroG.Data.Object.Metadata
{
    [DataContract]
    public sealed class ObjectIndexMetadata
    {
        public ObjectIndexMetadata()
        {
        }

        public ObjectIndexMetadata(string name, ObjectIndexType dataType)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentException("Name is a required parameter.");
            }

            Name = name;
            DataType = dataType;
        }

        public ObjectIndexMetadata(string name, ObjectIndexType dataType, uint precision)
            : this(name, dataType)
        {
            Precision = precision;
        }

        public ObjectIndexMetadata(string name, ObjectIndexType dataType, uint precision, uint scale)
            : this(name, dataType, precision)
        {
            Scale = scale;
        }

        [DataMember(Order = 1)]
        public string Name;
        [DataMember(Order = 2)]
        public ObjectIndexType DataType;
        [DataMember(Order = 3)]
        public uint Precision;
        [DataMember(Order = 4)]
        public uint Scale;
    }
}
