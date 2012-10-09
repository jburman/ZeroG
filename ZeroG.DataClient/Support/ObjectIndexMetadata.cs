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

namespace ZeroG.Data.Object
{
    [DataContract]
    public sealed class ObjectIndexMetadata
    {
        private ObjectIndexMetadata()
        {
        }

        public ObjectIndexMetadata(string name, ObjectIndexType dataType)
            : this(name, dataType, 7, 0)
        {
        }

        public ObjectIndexMetadata(string name, ObjectIndexType dataType, uint precision)
            : this(name, dataType, precision, 0)
        {
        }

        public ObjectIndexMetadata(string name, ObjectIndexType dataType, uint precision, uint scale)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentException("Name is a required parameter.");
            }

            if (ObjectIndexType.Unknown == dataType)
            {
                throw new ArgumentException("Data type is required to specified. Found: " + dataType);
            }

            Name = name;
            DataType = dataType;
            Precision = precision;
            Scale = scale;
        }

        [DataMember(Order = 1)]
        public string Name { get; private set; }
        [DataMember(Order = 2)]
        public ObjectIndexType DataType { get; private set; }
        [DataMember(Order = 3)]
        public uint Precision { get; private set; }
        [DataMember(Order = 4)]
        public uint Scale { get; private set; }
    }
}
