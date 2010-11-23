using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Diagnostics;
using System.Text.RegularExpressions;

namespace WS.Lang.JSON
{
    public interface IJSONParseListener
    {
        void ObjectCreated(Dictionary<string, object> obj);
        void ArrayCreated(object[] arr);
    }

    public class JSONParseException : Exception
    {
        public readonly long Position;

        public JSONParseException(string message, long position)
            : base(message)
        {
            Position = position;
        }
    }

    public class JSONParser
    {
        public static JSONObject Parse(string input)
        {
            return Parse(input, null);
        }

        public static JSONObject Parse(string input, IJSONParseListener listener)
        {
            MemoryStream ms = new MemoryStream(Encoding.UTF8.GetBytes(input));
            StreamReader sr = new StreamReader(ms);
            JSONObject returnObj = null;

            // JSON strings are either objects or arrays
            if (!sr.EndOfStream)
            {
                _ConsumeWhiteSpace(sr);
                char start = (char)sr.Read();
                if (start == '{')
                {
                    Dictionary<string, object> obj = _ParseObject(sr);
                    if (null != listener)
                    {
                        listener.ObjectCreated(obj);
                    }
                    returnObj = new JSONObject(obj);
                }
                else if (start == '[')
                {
                    object[] arr = _ParseArray(sr);
                    if (null != listener)
                    {
                        listener.ArrayCreated(arr);
                    }
                    returnObj = new JSONObject(arr);
                }
                else
                {
                    _ThrowParseEx(sr, "Invalid JSON input.  No object or array structure found.");
                }
            }
            else
            {
                throw new ArgumentNullException("input");
            }
            return returnObj;
        }

        private static Dictionary<string, object> _ParseObject(StreamReader sr)
        {
            Dictionary<string, object> returnObj = new Dictionary<string, object>();

            if ((char)sr.Peek() == '{') { sr.Read(); };
            if ((char)sr.Peek() != '}')
            {
                bool moreValues = false;
                char nextChar = ' ';
                do
                {
                    _ConsumeWhiteSpace(sr);
                    string name = _ParseString(sr);
                    _ConsumeWhiteSpace(sr);

                    _ReadAndExpectChar(sr, ':');

                    returnObj[name] = _ParseValue(sr);

                    _ConsumeWhiteSpace(sr);
                    nextChar = (char)sr.Read();
                    moreValues = (',' == nextChar);
                } while (moreValues);
                _ExpectChar(sr, '}', nextChar);
            }
            else
            {
                _ReadAndExpectChar(sr, '}');
            }

            return returnObj;
        }

        private static object _ParseValue(StreamReader sr)
        {
            object returnVal = null;

            _ConsumeWhiteSpace(sr);
            char nextChar = (char)sr.Peek();

            if (nextChar == '{')
            {
                returnVal = _ParseObject(sr);
            }
            else if (nextChar == '[')
            {
                returnVal = _ParseArray(sr);
            }
            else if (nextChar == '"')
            {
                returnVal = _ParseString(sr);
            }
            else
            {
                if (nextChar == _TrueChars[0] || nextChar == _FalseChars[0])
                {
                    returnVal = _ParseBool(sr);
                }
                else if (nextChar == 'n')
                {
                    _ParseNull(sr);
                    returnVal = null;
                }
                else // parse number
                {
                    returnVal = _ParseNumber(sr);
                }
            }

            return returnVal;
        }

        private static object[] _ParseArray(StreamReader sr)
        {
            List<object> values = new List<object>();

            if ((char)sr.Peek() == '[') { sr.Read(); };
            bool moreValues = false;
            char nextChar = ' ';
            do
            {
                if (!sr.EndOfStream)
                {
                    _ConsumeWhiteSpace(sr);
                    if (']' != (char)sr.Peek()) // allows empty arrays
                    {
                        values.Add(_ParseValue(sr));
                        _ConsumeWhiteSpace(sr);
                    }

                    nextChar = (char)sr.Read();
                    if (nextChar == ',')
                    {
                        moreValues = true;
                    }
                    else if (']' == nextChar)
                    {
                        moreValues = false;
                    }
                    else
                    {
                        _ThrowParseEx(sr, "Error parsing array.  Expected ',' or ']' but found " + nextChar);
                    }
                }
                else
                {
                    _ThrowParseEx(sr, "Expected array element but was EOF");
                }
            } while (moreValues);

            return values.ToArray();
        }

        private static char _ReadNotEOF(StreamReader sr)
        {
            if (!sr.EndOfStream)
            {
                return (char)sr.Read();
            }
            else
            {
                _ThrowParseEx(sr, "Unexpected EOF.");
                return ' ';
            }
        }

        private static void _ReadAndExpectChar(StreamReader sr, char c)
        {
            if (!sr.EndOfStream)
            {
                char nextChar = (char)sr.Read();
                _ExpectChar(sr, c, nextChar);
            }
            else
            {
                _ThrowParseEx(sr, "Expected char \"" + c + "\" but was EOF.");
            }
        }

        private static void _ExpectChar(StreamReader sr, char expected, char actual)
        {
            if (expected != actual)
            {
                _ThrowParseEx(sr, "Expected char \"" + expected + "\" but found: " + actual);
            }
        }

        private static Regex _WhiteSpace = new Regex("\\s", RegexOptions.Compiled);

        private static void _ConsumeWhiteSpace(StreamReader sr)
        {
            while (!sr.EndOfStream && _WhiteSpace.IsMatch(((char)sr.Peek()).ToString()))
            {
                sr.Read();
            }
        }

        private static readonly char[] _TrueChars = new char[] {
             't', 'r', 'u', 'e'
         };

        private static readonly char[] _FalseChars = new char[] {
             'f', 'a', 'l', 's', 'e'
         };

        private static readonly char[] _NullChars = new char[] {
             'n', 'u', 'l', 'l'
         };

        private static void _ParseNull(StreamReader sr)
        {
            char[] buf = new char[_NullChars.Length];
            int read = sr.ReadBlock(buf, 0, buf.Length);
            if (read == buf.Length)
            {
                for (int i = 0; buf.Length > i; i++)
                {
                    if ((char)buf[i] != _NullChars[i])
                    {
                        _ThrowParseEx(sr, "Expected " + _NullChars[i] + " but got " + buf[i]);
                    }
                }
            }
            else
            {
                _ThrowParseEx(sr, "Expected \"null\"");
            }
        }

        private static double _ParseNumber(StreamReader sr)
        {
            StringBuilder num = new StringBuilder();
            char nextChar = _ReadNotEOF(sr);
            if ('-' == nextChar || ('0' <= nextChar && '9' >= nextChar))
            {
                num.Append(nextChar);
                nextChar = (char)sr.Peek();
                while ('.' == nextChar || ('0' <= nextChar && '9' >= nextChar))
                {
                    num.Append(_ReadNotEOF(sr));
                    nextChar = (char)sr.Peek();
                }

                try
                {
                    double d = double.Parse(num.ToString());
                    return d;
                }
                catch (FormatException)
                {
                    _ThrowParseEx(sr, "Invalid number " + num.ToString());
                }
            }
            else
            {
                _ThrowParseEx(sr, "Invalid number.");
            }
            return 0;
        }

        private static bool _ParseBool(StreamReader sr)
        {
            if (_TrueChars[0] == ((char)sr.Peek()))
            {
                char[] buf = new char[_TrueChars.Length];
                int read = sr.ReadBlock(buf, 0, buf.Length);
                if (read == buf.Length)
                {
                    for (int i = 0; buf.Length > i; i++)
                    {
                        if ((char)buf[i] != _TrueChars[i])
                        {
                            _ThrowParseEx(sr, "Expected " + _TrueChars[i] + " but got " + buf[i]);
                        }
                    }
                    return true;
                }
                else
                {

                    _ThrowParseEx(sr, "Expected \"true\".");
                }
            }
            else
            {
                char[] buf = new char[_FalseChars.Length];
                int read = sr.ReadBlock(buf, 0, buf.Length);
                if (read == buf.Length)
                {
                    for (int i = 0; buf.Length > i; i++)
                    {
                        if ((char)buf[i] != _FalseChars[i])
                        {
                            _ThrowParseEx(sr, "Expected " + _FalseChars[i] + " but got " + buf[i]);
                        }
                    }
                }
                else
                {
                    _ThrowParseEx(sr, "Expected \"false\".");
                }
            }
            return false;
        }

        private static string _ParseString(StreamReader sr)
        {
            _ReadAndExpectChar(sr, '"');

            StringBuilder sb = new StringBuilder();
            char nextChar = ' ';
            do
            {
                if (!sr.EndOfStream)
                {
                    nextChar = (char)sr.Read();
                    if ('"' != nextChar)
                    {
                        if (nextChar == '\\')
                        {
                            _HandleJSONEscape(sr, sb);
                        }
                        else
                        {
                            sb.Append(nextChar);
                        }
                    }
                }
                else
                {
                    _ThrowParseEx(sr, "Unexpected EOF while reading string literal");
                }
            } while (nextChar != '"' && !sr.EndOfStream);

            string returnString = sb.ToString();
            return returnString;
        }

        private static Regex _JSONHexLit = new Regex("[0-9a-hA-H][0-9a-hA-H][0-9a-hA-H][0-9a-hA-H]");
        private static void _HandleJSONEscape(StreamReader sr, StringBuilder sb)
        {
            if (!sr.EndOfStream)
            {
                char nextChar = (char)sr.Read();
                switch (nextChar)
                {
                    case 'u':
                        StringBuilder hex = new StringBuilder();
                        hex.Append((char)sr.Read());
                        hex.Append((char)sr.Read());
                        hex.Append((char)sr.Read());
                        hex.Append((char)sr.Read());
                        Match m = _JSONHexLit.Match(hex.ToString());
                        if (m.Success)
                        {
                            sb.Append(char.ConvertFromUtf32(Convert.ToInt32(hex.ToString(), 16)));
                        }
                        else
                        {
                            _ThrowParseEx(sr, "Invalid hexadecimal " + hex.ToString());
                        }
                        break;
                    case '"':
                    case '\\':
                    case '/':
                        sb.Append(nextChar);
                        break;
                    case 'b':
                        sb.Append('\b');
                        break;
                    case 'f':
                        sb.Append('\f');
                        break;
                    case 'n':
                        sb.Append('\n');
                        break;
                    case 'r':
                        sb.Append('\r');
                        break;
                    case 't':
                        sb.Append('\t');
                        break;
                    default:
                        _ThrowParseEx(sr, "Unexpected string character found.");
                        break;
                }
            }
            else
            {
                _ThrowParseEx(sr, "Incomplete JSON sentence.");
            }
        }

        private static void _ThrowParseEx(StreamReader sr, string message)
        {
            throw new JSONParseException(message, sr.BaseStream.Position);
        }
    }

}
