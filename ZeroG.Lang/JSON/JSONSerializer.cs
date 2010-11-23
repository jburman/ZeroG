using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.IO;

namespace WS.Lang.JSON
{
    public class JSONSerializer
    {
        /// <summary>
        /// Creates a simple object with a single field / value pair.
        /// This method does NOT encode the supplied value to be JavaScript safe.   
        /// Raw strings that may be tampered with should NOT be passed this method.  Instead, utilized the 
        /// JSONSerializer.GetValue method.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static string SimpleObjectUnsafe(string key, object value)
        {
            var valStr = "null";
            if (value is bool)
            {
                valStr = value.ToString().ToLower();
            }
            else if (value is string)
            {
                valStr = (string)value;
            }
            else if (value is DateTime)
            {
                valStr = ((DateTime)value).ToString("f");
            }
            else if (value != null)
            {
                valStr = value.ToString();
            }

            return "{ \"" + JSONObject.SerializeString(key) + "\" : " + valStr + " }";
        }

        public static string GetArray<T>(T[] array)
        {
            StringBuilder sb = new StringBuilder();

            sb.Append("[");

            if (null != array)
            {
                foreach (T o in array)
                {
                    sb.Append(GetValue(o));
                    sb.Append(",");
                }

                if (sb[sb.Length - 1] == ',')
                {
                    sb.Length -= 1;
                }
            }

            sb.Append("]");

            return sb.ToString();
        }

        public static string Serialize(Dictionary<string, object> dict)
        {
            StringBuilder sb = new StringBuilder();

            sb.Append("{");

            if (null != dict)
            {
                foreach (var key in dict.Keys)
                {
                    if (sb.Length > 1)
                    {
                        sb.Append(",");
                    }
                    sb.Append(GetField(key, dict[key]));
                }
            }

            sb.Append("}");

            return sb.ToString();
        }

        /// <summary>
        /// Serializes a Data Table as an array of JSON objects.
        /// </summary>
        /// <param name="data"></param>
        /// <param name="outStream"></param>
        public static void Serialize(DataTable data, Stream outStream)
        {
            StreamWriter sw = new StreamWriter(outStream);
            Serialize(data, sw);
        }

        /// <summary>
        /// Serializes a Data Table as an array of JSON objects.
        /// </summary>
        /// <param name="data"></param>
        /// <param name="outStream"></param>
        public static void Serialize(DataTable data, StreamWriter outStream)
        {
            if (null != data)
            {
                outStream.Write("[");
                DataColumnCollection cols = data.Columns;
                DataRowCollection rows = data.Rows;

                for (int i = 0; rows.Count > i; i++)
                {
                    DataRow row = rows[i];
                    outStream.Write("{");
                    for (int j = 0; cols.Count > j; j++)
                    {
                        DataColumn col = cols[j];
                        outStream.Write(GetField(col.ColumnName, row[j]));
                        if (j < cols.Count - 1)
                        {
                            outStream.Write(',');
                        }
                    }
                    outStream.Write("}");
                    if (i < rows.Count - 1)
                    {
                        outStream.WriteLine(',');
                    }
                }
                outStream.Write("]");
            }
        }

        public static string GetField(string name, object o)
        {
            return string.Format("\"{0}\":{1}", name, GetValue(o));
        }

        public static string GetValue(object o)
        {
            if (o is Array)
            {
                return JSONObject.SerializeArray((object[])o);
            }
            else
            {
                return JSONObject.Serialize(o, true);
            }
        }
    }

}
