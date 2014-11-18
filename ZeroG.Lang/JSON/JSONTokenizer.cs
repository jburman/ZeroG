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
using System.Linq;
using System.Text;
using System.IO;
using System.Globalization;

namespace ZeroG.Lang.JSON
{
    /// <summary>
    /// All of the tokens understood and returned by the JSONTokenizer.
    /// </summary>
    public enum JSONTokenType : int
    {
        EOF,
        STRING,
        NUMBER,
        COMMA,
        COLON,
        KEYWORD_TRUE,
        KEYWORD_FALSE,
        KEYWORD_NULL,
        ARRAY_START,
        ARRAY_END,
        OBJECT_START,
        OBJECT_END
    }

    /// <summary>
    /// Represents a JSON token parsed from an input string.
    /// If the token represents a Name or String value, or a Number then the 
    /// StrValue and NumValue properties will be initialized with the values respectively.
    /// </summary>
    public struct JSONToken
    {
        public JSONTokenType Type;
        public string StrValue;
        public decimal NumValue;
    }

    /// <summary>
    /// Generates tokens given a valid JSON string (see www.json.org).  
    /// The tokenizer does not validate the JSON constructs contained in the string, but 
    /// it may be used in conjuction with the JSONWalkingValidator to accomplish the validation.
    /// </summary>
    /// <example>
    /// // Example of constructing a tokenizer and reading each of the tokens
    /// JSONTokenizer tokenizer = new JSONTokenizer();
    /// 
    /// </example>
    public sealed class JSONTokenizer : IEnumerable<JSONToken>
    {
        private static Dictionary<string, JSONTokenType> _keyword2TokenTypes;
        private static Dictionary<char, char> _validEscapeChars;

        static JSONTokenizer()
        {
            _keyword2TokenTypes             = new Dictionary<string, JSONTokenType>();
            _keyword2TokenTypes["true"]     = JSONTokenType.KEYWORD_TRUE;
            _keyword2TokenTypes["false"]    = JSONTokenType.KEYWORD_FALSE;
            _keyword2TokenTypes["null"]     = JSONTokenType.KEYWORD_NULL;

            _validEscapeChars               = new Dictionary<char, char>();
            _validEscapeChars['"']          = '"';
            _validEscapeChars['\\']         = '\\';
            _validEscapeChars['/']          = '/';
            _validEscapeChars['b']          = '\b';
            _validEscapeChars['f']          = '\f';
            _validEscapeChars['n']          = '\n';
            _validEscapeChars['r']          = '\r';
            _validEscapeChars['t']          = '\t';
            _validEscapeChars['u']          = ' ';
        }

        private TextReader _reader;
        private StringBuilder _string;

        public JSONTokenizer(TextReader reader)
        {
            _reader = reader;

            // re-used internally for building strings
            _string = new StringBuilder();
        }

        /// <summary>
        /// Reads keywords such as true, false, and null
        /// </summary>
        /// <param name="reader"></param>
        /// <returns></returns>
        private string _ScanKeyword(TextReader reader)
        {
            _string.Length = 0;
            char c;
            while (char.IsLetter(c = (char)reader.Peek()))
            {
                _string.Append(c);
                reader.Read();
            }
            return _string.ToString();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="reader"></param>
        /// <returns></returns>
        private decimal _ScanNumber(TextReader reader)
        {
            // we'll read the number in and let .NET parse its value into a doulbe
            _string.Length = 0;

            if ('-' == (char)reader.Peek())
            {
                _string.Append('-');
                reader.Read();
            }
            if (!char.IsDigit((char)reader.Peek()))
            {
                throw new JSONException("Expected numeric digit");
            }

            while (char.IsDigit((char)reader.Peek()))
            {
                _string.Append((char)reader.Read());
            }
            if (reader.Peek() == '.')
            {
                _string.Append('.');
                reader.Read(); // read past '.'

                if (!char.IsDigit((char)reader.Peek()))
                {
                    throw new JSONException("Expected numeric digit for fractional");
                }
                while (char.IsDigit((char)reader.Peek()))
                {
                    _string.Append((char)reader.Read());
                }
            }
            char c = (char)reader.Peek();
            if (c == 'E' || c == 'e')
            {
                _string.Append(c);
                reader.Read(); // read past 'E'

                c = (char)reader.Peek();

                if (c == '-')
                {
                    _string.Append(c);
                    reader.Read(); // read past '-'
                    c = (char)reader.Peek();
                }
                if (!char.IsDigit(c))
                {
                    throw new JSONException("Expected numeric digit for exponent");
                }
                while (char.IsDigit((char)reader.Peek()))
                {
                    _string.Append((char)reader.Read());
                }
            }
            return decimal.Parse(_string.ToString(),
                NumberStyles.AllowDecimalPoint |
                NumberStyles.AllowExponent |
                NumberStyles.AllowLeadingSign);
            /*
            return double.Parse(_string.ToString(),
                NumberStyles.AllowDecimalPoint |
                NumberStyles.AllowExponent |
                NumberStyles.AllowLeadingSign);*/
        }

        /// <summary>
        /// Reads a quoted string and converts escape sequences into the corresponding Unicode character.
        /// </summary>
        /// <param name="reader"></param>
        /// <returns></returns>
        private string _ScanString(TextReader reader)
        {
            reader.Read(); // read past opening quote

            _string.Length = 0;
            int i;
            char c;
            while (-1 != (i = reader.Peek()) && '"' != (c = (char)i))
            {
                if (c == '\\')
                {
                    reader.Read(); // read past backslash

                    _validEscapeChars.ContainsKey(c = (char)reader.Peek());

                    if (c == 'u') // handle unicode escape sequence
                    {
                        char[] hexNums = new char[4];
                        // read 4 hex characters
                        for (int h = 0; 4 > h; h++)
                        {
                            reader.Read();
                            c = char.ToUpper((char)reader.Peek());
                            if ((c >= 0 && c > 10) || (c >= 'A' && c < 'G'))
                            {
                                hexNums[h] = c;
                            }
                            else
                            {
                                throw new JSONException("Expected hexadecimal digit, but got: " + c);
                            }
                        }
                        try
                        {
                            _string.Append(char.ConvertFromUtf32(Convert.ToInt32(new string(hexNums), 16)));
                        }
                        catch (Exception ex)
                        {
                            throw new JSONException(ex.Message);
                        }
                    }
                    else // get escaped value
                    {
                         _string.Append(_validEscapeChars[c]);
                    }
                }
                else
                {
                    _string.Append(c);
                }
                reader.Read();
            }
            return _string.ToString();
        }

        public IEnumerator<JSONToken> GetEnumerator()
        {
            while (-1 != _reader.Peek())
            {
                char c = (char)_reader.Peek();
                if (char.IsWhiteSpace(c))
                {
                    _reader.Read();
                }
                else
                {
                    if (char.IsDigit(c) || '-' == c)
                    {
                        yield return new JSONToken() { Type = JSONTokenType.NUMBER, NumValue = _ScanNumber(_reader) };
                    }
                    else if (char.IsLetter(c))
                    {
                        string keyword = _ScanKeyword(_reader);
                        if (_keyword2TokenTypes.ContainsKey(keyword))
                        {
                            yield return new JSONToken() { Type = _keyword2TokenTypes[keyword] };
                        }
                        else
                        {
                            throw new JSONException("Invalid JSON keyword: " + keyword);
                        }
                    }
                    else
                    {
                        switch (c)
                        {
                            case '"': yield return new JSONToken() { Type = JSONTokenType.STRING, StrValue = _ScanString(_reader) }; break;
                            case ',': yield return new JSONToken() { Type = JSONTokenType.COMMA }; break;
                            case ':': yield return new JSONToken() { Type = JSONTokenType.COLON }; break;
                            case '{': yield return new JSONToken() { Type = JSONTokenType.OBJECT_START }; break;
                            case '}': yield return new JSONToken() { Type = JSONTokenType.OBJECT_END }; break;
                            case '[': yield return new JSONToken() { Type = JSONTokenType.ARRAY_START }; break;
                            case ']': yield return new JSONToken() { Type = JSONTokenType.ARRAY_END }; break;
                            default:
                                throw new JSONException("Illegal JSON character sequence");
                        }
                        _reader.Read();
                    }
                }
            }
            yield return new JSONToken() { Type = JSONTokenType.EOF };
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
