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
    public sealed class ObjectIndex
    {
        public static ObjectIndexType DefaultDataType = default(ObjectIndexType);

        private object _deserializedValue;

        #region ObjectIndex construction

        public ObjectIndex() { }

        private ObjectIndex(string name, byte[] value, object deserializedValue, ObjectIndexType dataType)
        {
            Name = name;
            Value = value;
            DataType = dataType;
            _deserializedValue = deserializedValue;
        }

        public static ObjectIndex Create(string name, object value)
        {
            ObjectIndex returnValue = null;

            if (null == name)
            {
                throw new ArgumentNullException("name");
            }

            if (!ObjectNameValidator.IsValidIndexName(name))
            {
                throw new ArgumentException("Invalid index name found " + name + ". It should be 1-30 characters long and contain only alphanumeric characters or underscores.");
            }

            if (null != value)
            {
                var dataType = DefaultDataType.GetDataType(value);
                if (ObjectIndexType.Unknown == dataType)
                {
                    throw new ArgumentException("Unsupported Type: " + value.GetType().Name);
                }

                returnValue = new ObjectIndex(name, dataType.ConvertToBinary(value), value, dataType);
            }
            else
            {
                returnValue = new ObjectIndex(name, null, null, ObjectIndexType.Unknown);
            }

            return returnValue;
        }

        #endregion

        [DataMember(Order = 1)]
        public ObjectIndexType DataType;
        [DataMember(Order = 2)]
        public string Name;
        [DataMember(Order = 3)]
        public byte[] Value;

        public object GetObjectValue()
        {
            if (null == _deserializedValue)
            {
                _deserializedValue = DataType.ConvertToObject(Value);
            }
            return _deserializedValue;
        }

        public override string ToString()
        {
            return Name + "=" + GetObjectValue();
        }

        public override int GetHashCode()
        {
            return ToString().GetHashCode();
        }

        public override bool Equals(object obj)
        {
            if (null == obj) { return false; }

            return ToString() == obj.ToString();
        }
    }
}
