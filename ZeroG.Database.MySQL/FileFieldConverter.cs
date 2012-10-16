using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ZeroG.Lang;

namespace ZeroG.Data.Database.Drivers
{
    internal sealed class FileFieldConverter
    {
        private static readonly Encoding _utf8 = Encoding.UTF8;

        public static byte[] ToFileFieldString(object val)
        {
            if (val == null)
            {
                return null;
            }
            else if (val is string)
            {
                return _utf8.GetBytes(DatabaseHelper.EscapeValueForFile((string)val));
            }
            else if (val is int)
            {
                return _utf8.GetBytes(val.ToString());// BitConverter.GetBytes((int)val);
            }
            else if (val is byte[])
            {
                return DatabaseHelper.EscapeValueForFile((byte[])val);
            }
            else if (val is DateTime)
            {
                return _utf8.GetBytes(((DateTime)val).ToString("u").TrimEnd('Z'));
            }
            else
            {
                return _utf8.GetBytes(DatabaseHelper.EscapeValueForFile(val.ToString()));

            }
        }
    }
}
