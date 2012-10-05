using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using ZeroG.Data.Database;
using ZeroG.Data.Object.Metadata;
using ZeroG.Lang;

namespace ZeroG.Data.Object.Backup
{
    public class ObjectBackupWriter : IDisposable
    {
        private bool _compress;
        private StreamWriter _out;

        public ObjectBackupWriter(string path, bool useCompression)
        {
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
            _out.WriteLine("VERSION: " + version);
        }

        public void WriteNameSpace(ObjectNameSpaceConfig nameSpace)
        {
            _out.WriteLine("NAMESPACE: " + BinaryHelper.ByteToHexString(SerializerHelper.Serialize<ObjectNameSpaceConfig>(nameSpace)));
        }

        public void WriteObjectMetadata(ObjectMetadata metadata)
        {
            _out.WriteLine("OBJECTMETADATA: " + BinaryHelper.ByteToHexString(SerializerHelper.Serialize<ObjectMetadata>(metadata)));
        }

        public void WriteObjectID(int id)
        {
            _out.WriteLine("OBJECTID: " + id);
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
                        _out.Dispose();
                    }
                }
                _disposed = true;
            }

        }
        #endregion
    }
}
