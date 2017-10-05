#region License, Terms and Conditions
// Copyright (c) 2017 Jeremy Burman
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

using ProtoBuf;
using System;
using System.IO;
using System.Text;

namespace ZeroG.Data.Object
{
    public class ProtobufSerializer : ISerializer
    {
        private static Encoding _utf8 = Encoding.UTF8;

        public byte[] Serialize(string value)
        {
            if (null != value)
            {
                return _utf8.GetBytes(value);
            }
            else
            {
                return null;
            }
        }

        public byte[] Serialize(uint id) => BitConverter.GetBytes(id);

        public byte[] Serialize(int id) => BitConverter.GetBytes(id);

        public byte[] Serialize(Guid id) => id.ToByteArray();

        public byte[] Serialize<T>(T value)
        {
            var buffer = new MemoryStream();
            Serializer.Serialize<T>(buffer, value);
            return buffer.ToArray();
        }

        public byte[] Deserialize(string value) => _utf8.GetBytes(value);

        public string DeserializeString(byte[] val) => _utf8.GetString(val);

        public int DeserializeInt32(byte[] val) => BitConverter.ToInt32(val, 0);

        public uint DeserializeUInt32(byte[] val) => BitConverter.ToUInt32(val, 0);

        public Guid DeserializeGuid(byte[] val) => new Guid(val);

        public T Deserialize<T>(byte[] value)
        {
            var buffer = new MemoryStream(value);
            return Serializer.Deserialize<T>(buffer);
        }

        public byte[] CreateFullObjectKey(string nameSpace, string objectName) => Serialize(nameSpace + "." + objectName);

        public byte[] CreateFullObjectKey(string objectFullName) => Serialize(objectFullName);
    }
}
