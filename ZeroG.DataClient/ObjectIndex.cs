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

using System.Runtime.Serialization;

namespace ZeroG.Data.Object
{
    [DataContract]
    public sealed class ObjectIndex
    {
        public ObjectIndex() { }

        public ObjectIndex(string name, object value)
        {
            Name = name;
            Value = value;
        }

        [DataMember(Order = 1)]
        public string Name;
        [DataMember(Order = 2)]
        public object Value;

        public ObjectIndexType GetDataType()
        {
            var dataType = default(ObjectIndexType);
            return dataType.GetDataType(Value);
        }

        public override string ToString()
        {
            return Name + "=" + Value;
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
