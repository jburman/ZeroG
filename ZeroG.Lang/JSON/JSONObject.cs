using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace ZeroG.Lang.JSON
{
    /// <summary>
    /// Provides a generic structure for storing deserialized JSON objects and arrays, and it also 
    /// provides methods for serializing .NET types into JSON.
    /// </summary>
    /// <authors>
    ///     <author>Jeremy Burman</author>
    /// </authors>
    /// <seealso cref="ZeroG.Lang.JSON.JSONParser"/>
    /// <example>
    ///     // JSON object example
    ///     JSONObject o = JSONParser.Parse("{ \"firstName\" : \"Jeremy\", \"lastName\" : \"Burman\" }");
    ///     Dictionary<string, object> jsonObj = o.Object;
    ///     string firstName = jsonObj["firstName"];
    ///     
    ///     // JSON array example
    ///     JSONObject o = JSONParser.Parse("[1, 2, 3]");
    ///     object[] arr = o.Array;
    ///     int val = (int)arr[1];
    ///     
    ///     // To determine if an object or array was parsed, check for null
    ///     JSONObject o = JSONParser.Parse("... some JSON string...");
    ///     if(null == o.Object) {
    ///         // then an array was parsed
    ///     } else {
    ///         // an object was parsed
    ///     }
    /// </example>
    public class JSONObject
    {
        private static Regex _WhiteSpaceMatch = new Regex("[ ]+", RegexOptions.Compiled);

        public readonly object[] Array;
        public readonly Dictionary<string, object> Object;

        public JSONObject(object[] array)
        {
            Array = array;
            Object = null;
        }

        public JSONObject(Dictionary<string, object> obj)
        {
            Array = null;
            Object = obj;
        }

        public static string Serialize(object val)
        {
            return Serialize(val, false);
        }

        public static string Serialize(object val, bool addQuotes)
        {
            return Serialize(val, addQuotes, false);
        }

        public static string Serialize(object val, bool addQuotes, bool compressWhiteSpace)
        {
            if (val == null)
            {
                return "\"\"";
            }
            else if (val is string)
            {
                string strVal = SerializeString((string)val);
                if (compressWhiteSpace)
                {
                    MatchCollection matches = _WhiteSpaceMatch.Matches(strVal);
                    foreach (Match m in matches)
                    {
                        if (m.Length > 1)
                        {
                            strVal = strVal.Replace(m.Value, " ");
                        }
                    }
                }

                if (addQuotes)
                {
                    return "\"" + strVal + "\"";
                }
                else
                {
                    return strVal;
                }
            }
            else if (val is bool)
            {
                return SerializeBool((bool)val);
            }
            else if (val is DateTime)
            {
                string strVal = ((DateTime)val).ToString("f");
                if (addQuotes)
                {
                    strVal = "\"" + strVal + "\"";
                }
                return strVal;
            }
            else if (val is Enum)
            {
                string strVal = val.ToString();
                if (addQuotes)
                {
                    strVal = "\"" + strVal + "\"";
                }
                return strVal;
            }
            else
            {
                return val.ToString();
            }

        }

        public static string SerializeString(string text)
        {
            if (null == text)
            {
                return string.Empty;
            }

            StringBuilder sb = new StringBuilder(text);
            sb.Replace("\\", "\\u005C");
            sb.Replace("'", "\\u0027");
            sb.Replace("\"", "\\u0022");
            sb.Replace("\n", "\\n");
            sb.Replace("\r", "\\r");
            sb.Replace("\t", "\\u0009");
            sb.Replace("\f", "\\f");
            sb.Replace("\v", "\\v");

            return sb.ToString();
        }

        public static string SerializeBool(bool value)
        {
            return value.ToString().ToLower();
        }

        public static string SerializeArray(object[] values)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("[");
            if (null != values)
            {
                for (int i = 0; values.Length > i; i++)
                {
                    sb.Append(Serialize(values[i], true));
                    sb.Append(',');
                }
                if (sb.Length > 1)
                {
                    sb.Length = sb.Length - 1;
                }
            }
            sb.Append("]");
            return sb.ToString();
        }

        public static string SerializeScalarArray<T>(T[] values)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("[");
            if (null != values)
            {
                for (int i = 0; values.Length > i; i++)
                {
                    sb.Append(Serialize(values[i], true));
                    sb.Append(',');
                }
                if (sb.Length > 1)
                {
                    sb.Length = sb.Length - 1;
                }
            }
            sb.Append("]");
            return sb.ToString();
        }

        public object this[string memberName]
        {
            get
            {
                return Object[memberName];
            }
        }
    }

    public sealed class JSONObjectWalkingBuilder
    {
        private class ListHolder
        {
            public string ObjKey;
            public List<object> List;
        }
        
        private object _rootObj;

        private Stack<object> _data;
        private string _objKey;
        private string _parentObjKey;

        public JSONObjectWalkingBuilder(JSONWalkingEvents events)
        {
            _data               = new Stack<object>();
            _objKey             = null;
            _parentObjKey       = Guid.NewGuid().ToString("N");
            
            events.ObjectStart  += new JSONEventHandler(events_ObjectStart);
            events.ObjectEnd    += new JSONEventHandler(events_ObjectEnd);
            events.ObjectKey    += new JSONEventHandler<string>(events_ObjectKey);

            events.ArrayStart   += new JSONEventHandler(events_ArrayStart);
            events.ArrayEnd     += new JSONEventHandler(events_ArrayEnd);

            events.String       += new JSONEventHandler<string>(events_String);
            events.Number       += new JSONEventHandler<double>(events_Number);
            events.Boolean      += new JSONEventHandler<bool>(events_Boolean);

            events.Null += new JSONEventHandler(events_Null);
        }

        private bool _AddObjectValue(object value)
        {
            if (null != _objKey)
            {
                (_data.Peek() as Dictionary<string, object>)[_objKey] = value;
                _objKey = null;
                return true;
            }
            return false;
        }

        private void _AddArrayValue(object value)
        {
            (_data.Peek() as ListHolder).List.Add(value);
        }

        private void events_Null()
        {
            if (!_AddObjectValue(null))
            {
                _AddArrayValue(null);
            }
        }

        private void events_Number(double value)
        {
            if (!_AddObjectValue(value))
            {
                _AddArrayValue(value);
            }
        }

        private void events_String(string value)
        {
            if (!_AddObjectValue(value))
            {
                _AddArrayValue(value);
            }
        }

        private void events_Boolean(bool value)
        {
            if (!_AddObjectValue(value))
            {
                _AddArrayValue(value);
            }
        }

        private void events_ArrayStart()
        {
            _data.Push(new ListHolder()
            {
                ObjKey = _objKey,
                List = new List<Object>()
            });
            _objKey = null;
        }
        
        private void events_ArrayEnd()
        {
            ListHolder list = _data.Pop() as ListHolder;
            if (null != list.ObjKey)
            {
                (_data.Peek() as Dictionary<string, object>)[list.ObjKey] = list.List.ToArray();
            }
            else if (0 == _data.Count)
            {
                _rootObj = list.List.ToArray();
            }
            else
            {
                _AddArrayValue(list.List.ToArray());
            }
        }

        private void events_ObjectStart()
        {
            Dictionary<string, object> newObj = new Dictionary<string, object>();
            if(null != _objKey) 
            {
                newObj[_parentObjKey] = _objKey;
            }
            _data.Push(newObj);
            _objKey = null;
        }

        private void events_ObjectEnd()
        {
            Dictionary<string, object> obj = _data.Pop() as Dictionary<string, object>;
            if (0 == _data.Count)
            {
                _rootObj = obj;
            }
            else if (obj.ContainsKey(_parentObjKey))
            {
                (_data.Peek() as Dictionary<string, object>)[obj[_parentObjKey].ToString()] = obj;
                obj.Remove(_parentObjKey);
            }
            else
            {
                ((ListHolder)_data.Peek()).List.Add(obj);
            }

        }

        private void events_ObjectKey(string value)
        {
            _objKey = value;
        }

        public JSONObject Object
        {
            get
            {
                if (_rootObj is Array)
                {
                    return new JSONObject((object[])_rootObj);
                }
                else
                {
                    return new JSONObject((Dictionary<string, object>)_rootObj);
                }
            }
        }
    }
}
