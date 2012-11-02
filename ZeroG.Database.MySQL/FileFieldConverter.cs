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
