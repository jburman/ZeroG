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

using System;
using System.IO;
using System.IO.Compression;
using ZeroG.Data.Object.Metadata;
using ZeroG.Lang;

namespace ZeroG.Data.Object.Backup
{
    public class ObjectBackupWriter : IDisposable
    {
        // backup file line prefixes

        internal static readonly string VersionPrefix = "VERSION: ";
        internal static readonly string NameSpacePrefix = "NAMESPACE: ";
        internal static readonly string ObjectMetadataPrefix = "OBJECTMETADATA: ";
        internal static readonly string ObjectIDPrefix = "ObjectID: ";
        internal static readonly string ObjectPrefix = "O: ";
        internal static readonly string IndexPrefix = "I: ";

        private ISerializer _serializer;
        private bool _compress;
        private StreamWriter _out;

        public ObjectBackupWriter(ISerializer serializer, string path, bool useCompression)
        {
            _serializer = serializer;
            _compress = useCompression;
            var fs = new FileStream(path, FileMode.Create, FileAccess.ReadWrite);
            if (_compress)
            {
                _out = new StreamWriter(
                    new GZipStream(fs, CompressionMode.Compress));
            }
            else
            {
                _out = new StreamWriter(fs);
            }
        }

        public void WriteStoreVersion(string version)
        {
            _out.WriteLine(VersionPrefix + version);
        }

        public void WriteNameSpace(ObjectNameSpaceConfig nameSpace)
        {
            _out.WriteLine(NameSpacePrefix + BinaryHelper.ByteToHexString(_serializer.Serialize(nameSpace)));
        }

        public void WriteObjectMetadata(ObjectMetadata metadata)
        {
            _out.WriteLine(ObjectMetadataPrefix + BinaryHelper.ByteToHexString(_serializer.Serialize(metadata)));
        }

        public void WriteObjectID(int id)
        {
            _out.WriteLine(ObjectIDPrefix + id);
        }

        public void WriteObject(ObjectStoreRecord obj)
        {
            _out.WriteLine(ObjectPrefix + BinaryHelper.ByteToHexString(_serializer.Serialize(obj)));
        }

        public void WriteIndex(ObjectIndexRecord idx)
        {
            _out.WriteLine(IndexPrefix + BinaryHelper.ByteToHexString(_serializer.Serialize(idx)));
        }

        #region Dispose implementation
        private bool _disposed;

        public void Dispose()
        {
            _Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void _Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    if (null != _out)
                    {
                        _out.Flush();
                        _out.Dispose();
                    }
                }
                _disposed = true;
            }

        }
        #endregion
    }
}
