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
using ZeroG.Lang;

namespace ZeroG.Data.Object.Backup
{
    public class ObjectBackupReader : IDisposable
    {
        private ISerializer _serializer;
        private bool _compress;
        private StreamReader _in;

        public ObjectBackupReader(ISerializer serializer, string path, bool useCompression)
        {
            _serializer = serializer;
            _compress = useCompression;
            var fs = new FileStream(path, FileMode.Open, FileAccess.Read);
            if (_compress)
            {
                _in = new StreamReader(
                    new GZipStream(fs, CompressionMode.Decompress));
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

        private void _ReadStoreVersion(Action<string> handler)
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

        private bool _ReadNameSpace(Action<ObjectNameSpaceConfig> handler, string line)
        {
            var returnValue = false;

            if (!line.StartsWith(ObjectBackupWriter.NameSpacePrefix))
            {
                throw new IOException("Expected Name Space");
            }
            else
            {
                handler?.Invoke(_serializer.Deserialize<ObjectNameSpaceConfig>(
                    BinaryHelper.HexStringToByte(line.Substring(ObjectBackupWriter.NameSpacePrefix.Length))));
                returnValue = true;
            }

            return returnValue;
        }

        private bool _ReadObjectMetadata(Action<ObjectMetadata> handler, string line)
        {
            var returnValue = false;

            if (line.StartsWith(ObjectBackupWriter.ObjectMetadataPrefix))
            {
                handler?.Invoke(_serializer.Deserialize<ObjectMetadata>(
                    BinaryHelper.HexStringToByte(line.Substring(ObjectBackupWriter.ObjectMetadataPrefix.Length))));
                returnValue = true;
            }

            return returnValue;
        }

        private bool _ReadObjectID(Action<int> handler, string line)
        {
            var returnValue = false;

            if (!line.StartsWith(ObjectBackupWriter.ObjectIDPrefix))
            {
                throw new IOException("Expected Object ID");
            }
            else
            {
                handler?.Invoke(int.Parse(line.Substring(ObjectBackupWriter.ObjectIDPrefix.Length)));
                returnValue = true;
            }

            return returnValue;
        }

        private bool _ReadObject(Action<ObjectStoreRecord> handler, string line)
        {
            var returnValue = false;

            if (line.StartsWith(ObjectBackupWriter.ObjectPrefix))
            {
                handler?.Invoke(_serializer.Deserialize<ObjectStoreRecord>(
                    BinaryHelper.HexStringToByte(line.Substring(ObjectBackupWriter.ObjectPrefix.Length))));
                returnValue = true;
            }

            return returnValue;
        }

        private bool _ReadIndex(Action<ObjectIndexRecord> handler, string line)
        {
            var returnValue = false;

            if (line.StartsWith(ObjectBackupWriter.IndexPrefix))
            {
                handler?.Invoke(_serializer.Deserialize<ObjectIndexRecord>(
                    BinaryHelper.HexStringToByte(line.Substring(ObjectBackupWriter.IndexPrefix.Length))));
                returnValue = true;
            }

            return returnValue;
        }

        #endregion

        public void ReadBackup(
            Action<string> storeVersionRead,
            Action<ObjectNameSpaceConfig> nameSpaceRead,
            Action<ObjectMetadata> objectMetadataRead,
            Action<int> objectIDRead,
            Action<ObjectStoreRecord> objectRead,
            Action<ObjectIndexRecord> indexRead,
            Action completed)
        {
            _ReadStoreVersion(storeVersionRead);

            string line = null;

            while(!_in.EndOfStream) 
            {
                line = _in.ReadLine();

                if(null != line) 
                {
                    if (line.StartsWith(ObjectBackupWriter.NameSpacePrefix))
                    {
                        _ReadNameSpace(nameSpaceRead, line);
                    }
                    else if (line.StartsWith(ObjectBackupWriter.ObjectMetadataPrefix))
                    {
                        _ReadObjectMetadata(objectMetadataRead, line);
                    }
                    else if (line.StartsWith(ObjectBackupWriter.ObjectIDPrefix))
                    {
                        _ReadObjectID(objectIDRead, line);
                    }
                    else if (line.StartsWith(ObjectBackupWriter.ObjectPrefix))
                    {
                        _ReadObject(objectRead, line);
                    }
                    else if (line.StartsWith(ObjectBackupWriter.IndexPrefix))
                    {
                        _ReadIndex(indexRead, line);
                    }
                }
            }
            completed?.Invoke();
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
