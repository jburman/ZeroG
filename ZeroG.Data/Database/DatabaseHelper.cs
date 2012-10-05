#region License, Terms and Conditions
// Copyright (c) 2010 Jeremy Burman
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
using System.Text;
using System.Data;

namespace ZeroG.Data.Database
{
    public class DatabaseHelper
    {
        /// <summary>
        /// This escapes a value for storing into a data file that may be used with a database 
        /// like MySQL.
        /// It uses the backslash (\) as the escape character for other backslashes,tabs, and newline characters.
        /// </summary>
        /// <returns>An escaped version of the passed in value.</returns>
        public static string EscapeValueForFile(string val)
        {
            if (!string.IsNullOrEmpty(val))
            {
                val = val.Replace("\\", "\\\\").Replace("\n", "\\n").Replace("\t", "\\t");
            }
            return val;
        }

        public static byte[] GetBytes(IDataRecord reader, string fieldName, byte[] defaultValue)
        {
            object val = reader[fieldName];
            if (val is DBNull)
            {
                return defaultValue;
            }
            return (byte[])val;
        }

        public static string GetString(IDataRecord reader, int index, string defaultValue)
        {
            if (!reader.IsDBNull(index))
            {
                return reader.GetString(index);
            }
            return defaultValue;
        }

        public static string GetString(IDataRecord reader, string fieldName, string defaultValue)
        {
            object val = reader[fieldName];
            if (val is DBNull)
            {
                return defaultValue;
            }
            return (string)val;
        }

        public static decimal GetDecimal(IDataRecord reader, int index, decimal defaultValue)
        {
            if (!reader.IsDBNull(index))
            {
                return reader.GetDecimal(index);
            }
            return defaultValue;
        }

        public static decimal GetDecimal(IDataRecord reader, string fieldName, decimal defaultValue)
        {
            object val = reader[fieldName];
            if (val is DBNull)
            {
                return defaultValue;
            }
            return (decimal)val;
        }

        public static int GetInt(IDataRecord reader, int index, int defaultValue)
        {
            if (!reader.IsDBNull(index))
            {
                return reader.GetInt32(index);
            }
            return defaultValue;
        }

        public static int GetInt(IDataRecord reader, string fieldName, int defaultValue)
        {
            object val = reader[fieldName];
            if (val is DBNull)
            {
                return defaultValue;
            }
            if (val is long)
            {
                long longVal = (long)val;
                return (int)longVal;
            }
            return (int)val;
        }

        public static long GetLong(IDataRecord reader, string fieldName, int defaultValue)
        {
            object val = reader[fieldName];
            if (val is DBNull)
            {
                return defaultValue;
            }
            return (long)val;
        }

        public static bool GetBool(IDataRecord reader, int index, bool defaultValue)
        {
            if (!reader.IsDBNull(index))
            {
                return reader.GetBoolean(index);
            }
            return defaultValue;
        }

        public static bool GetBool(IDataRecord reader, string fieldName, bool defaultValue)
        {
            object val = reader[fieldName];
            if (val is DBNull)
            {
                return defaultValue;
            }
            if (val is bool)
            {
                return (bool)val;
            }
            else
            {
                return val.ToString() != "0";
            }
        }

        public static DateTime GetDateTime(IDataRecord reader, int index, DateTime defaultValue)
        {
            if (!reader.IsDBNull(index))
            {
                return reader.GetDateTime(index);
            }
            return defaultValue;
        }

        public static DateTime GetDateTime(IDataRecord reader, string fieldName, DateTime defaultValue)
        {
            object val = reader[fieldName];
            if (val is DBNull)
            {
                return defaultValue;
            }
            return (DateTime)val;
        }
    }
}
