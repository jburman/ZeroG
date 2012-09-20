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

using ProtoBuf;
using System;
using System.IO;
using System.Text;

namespace ZeroG.Data.Object
{
    public class SerializerHelper
    {
        private static Encoding _utf8 = Encoding.UTF8;

        public static byte[] Serialize(string value)
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

        public static byte[] Serialize(uint id)
        {
            return BitConverter.GetBytes(id);
        }

        public static byte[] Serialize(int id)
        {
            return BitConverter.GetBytes(id);
        }

        public static byte[] Serialize(Guid id)
        {
            if (null == id)
            {
                return null;
            }
            else
            {
                return id.ToByteArray();
            }
        }

        public static byte[] Serialize<T>(T value)
        {
            var buffer = new MemoryStream();
            Serializer.Serialize<T>(buffer, value);
            return buffer.ToArray();
        }

        public static byte[] Deserialize(string value)
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

        public static string DeserializeString(byte[] val)
        {
            return _utf8.GetString(val);
        }

        public static int DeserializeInt32(byte[] val)
        {
            return BitConverter.ToInt32(val, 0);
        }

        public static uint DeserializeUInt32(byte[] val)
        {
            return BitConverter.ToUInt32(val, 0);
        }

        public static Guid DeserializeGuid(byte[] val)
        {
            return new Guid(val);
        }

        public static T Deserialize<T>(byte[] value)
        {
            var buffer = new MemoryStream(value);
            return Serializer.Deserialize<T>(buffer);
        }
    }
}
