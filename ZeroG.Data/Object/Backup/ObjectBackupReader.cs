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
using System.IO;
using System.IO.Compression;
using ZeroG.Data.Object.Metadata;
using ZeroG.Lang;

namespace ZeroG.Data.Object.Backup
{
    public class ObjectBackupReader : IDisposable
    {
        public delegate void StoreVersionHandler(string versionString);
        public delegate void NameSpaceHandler(ObjectNameSpaceConfig nameSpace);
        public delegate void ObjectMetadataHandler(ObjectMetadata metadata);
        public delegate void ObjectIDHandler(int objectId);
        public delegate void ObjectHandler(ObjectStoreRecord obj);
        public delegate void IndexHandler(ObjectIndexRecord index);

        private bool _compress;
        private StreamReader _in;

        public ObjectBackupReader(string path, bool useCompression)
        {
            _compress = useCompression;
            var fs = new FileStream(path, FileMode.Open, FileAccess.Read);
            if (_compress)
            {
                _in = new StreamReader(
                    new GZipStream(fs, CompressionMode.Compress));
            }
            else
            {
                _in = new StreamReader(fs);
            }
        }

        #region Private reader methods
        private string _ReadAndExpectLine()
        {
            if (_in.EndOfStream)
            {
                throw new IOException("Unexpected end of file.");
            }
            return _in.ReadLine();
        }

        private string _ReadIfNotEOF()
        {
            if (!_in.EndOfStream)
            {
                return _in.ReadLine();
            }
            else
            {
                return null;
            }
        }

        private void _ReadStoreVersion(StoreVersionHandler handler)
        {
            string line = _ReadAndExpectLine();
            if (null != handler)
            {
                if (!line.StartsWith(ObjectBackupWriter.VersionPrefix))
                {
                    throw new IOException("Expected Object Store Version");
                }
                else
                {
                    handler(line.Substring(ObjectBackupWriter.VersionPrefix.Length));
                }
            }
        }

        private bool _ReadNameSpace(NameSpaceHandler handler)
        {
            var returnValue = false;

            var line = _ReadIfNotEOF();

            if (null != line)
            {
                if (!line.StartsWith(ObjectBackupWriter.NameSpacePrefix))
                {
                    throw new IOException("Expected Name Space");
                }
                else
                {
                    if (null != handler)
                    {
                        handler(SerializerHelper.Deserialize<ObjectNameSpaceConfig>(
                            BinaryHelper.HexStringToByte(line.Substring(ObjectBackupWriter.NameSpacePrefix.Length))));
                    }
                    returnValue = true;
                }
            }

            return returnValue;
        }
        #endregion

        public void ReadBackup(
            StoreVersionHandler storeVersionRead,
            NameSpaceHandler nameSpaceRead,
            ObjectMetadataHandler objectMetadataRead,
            ObjectIDHandler objectIDRead,
            ObjectHandler objectRead,
            IndexHandler indexRead)
        {
            _ReadStoreVersion(storeVersionRead);

            if (_ReadNameSpace(nameSpaceRead))
            {
                // if read metadata

                // then read objects

                // read indexes

                // ... read metadata ... read name spaces ...
            }
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
                    if (null != _in)
                    {
                        _in.Dispose();
                    }
                }
                _disposed = true;
            }

        }
        #endregion
    }
}
