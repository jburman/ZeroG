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

namespace ZeroG.Lang.BSON
{
    public enum BSONTypes : byte
    {
        ZERO        = 0x00,
        DOUBLE      = 0x01,
        STRING      = 0x02,
        DOCUMENT    = 0x03,
        ARRAY       = 0x04,
        BINARY      = 0x05,
        UNDEFINED   = 0x06, // ignored
        OBJECTID    = 0x07,
        BOOLEAN     = 0x08,
        DATETIME    = 0x09,
        NULL        = 0x0A,
        REGEX       = 0x0B,
        REFERENCE   = 0x0C,
        CODE        = 0x0D,
        SYMBOL      = 0x0E,
        SCOPEDCODE  = 0x0F,
        INT32       = 0x10,
        TIMESTAMP   = 0x11, // ignored
        INT64       = 0x12
    }

    /// <summary>
    /// All of the tokens understood and returned by the BSONTokenizer.
    /// </summary>
    public enum BSONTokenType : int
    {
        EOF,
        STRING,
        CSTRING,
        INT32,
        INT64,
        DOUBLE,
        OBJECT_ID,
        DATETIME,
        REGEX,
        CODE,
        SYMBOL,
        CODE_WITH_SCOPE,
        KEYWORD_TRUE,
        KEYWORD_FALSE,
        KEYWORD_NULL,
        ARRAY_START,
        DOCUMENT_START,
        BINARY,
        END
    }

    /// <summary>
    /// Sub-types for binary data stored in BSON documents.
    /// </summary>
    public enum BSONBinaryType : byte
    {
        Generic     = 0x00,
        Function    = 0x01,
        BinaryOld   = 0x02,
        UUID        = 0x03,
        MD5         = 0x05,
        User        = 0x80
    }

    /// <summary>
    /// Represents a BSON token parsed from an input string.
    /// If the token represents a Name or String value, or a Number then the 
    /// StrValue and NumValue properties will be initialized with the values respectively.
    /// </summary>
    public class BSONToken
    {
        public BSONTokenType Type;
        public string Name;
    }

    public class BSONBinaryToken : BSONToken
    {
        public BSONBinaryType BinType;
        public byte[] Value;
    }

    public class BSONCodeWithScope : BSONToken
    {
        public int Length;
        public string Code;
    }

    public class BSONDocumentToken : BSONToken
    {
        public int Length;
    }

    public class BSONRegexToken : BSONToken
    {
        public string Pattern;
        public string Options;
    }

    public class BSONValueToken<T> : BSONToken
    {
        public T Value;
    }

    /// <summary>
    /// Generates tokens given a valid BSON document (see bsonspec.org).  
    /// The tokenizer does not validate the BSON constructs contained in the document, but 
    /// it may be used in conjuction with the BSONWalkingValidator to accomplish the validation.
    /// </summary>
    /// <example>
    /// // Example of constructing a tokenizer and reading each of the tokens
    /// BSONTokenizer tokenizer = new BSONTokenizer(binaryReader);
    /// foreach(BSONToken token in tokenizer) 
    /// {
    /// }
    /// </example>
    public sealed class BSONTokenizer : IEnumerable<BSONToken>
    {
        static BSONTokenizer()
        {
        }

        private const int _MaxCStringSize = 4192;

        private BinaryReader _reader;
        private byte[] _bin;

        public BSONTokenizer(BinaryReader reader)
        {
            _reader = reader;

            // this is used for reading CStrings.  It is increased in size as needed
            _bin = new byte[256];
        }

        private byte[] _ScanCString(BinaryReader reader)
        {
            int i = 0;
            for (; 0 != reader.PeekChar(); i++)
            {
                if (i == _bin.Length)
                {
                    if (i >= _MaxCStringSize)
                    {
                        throw new BSONException("Maximum CString size reached.");
                    }
                    Array.Resize<byte>(ref _bin, _bin.Length * 2);
                }

                _bin[i] = reader.ReadByte();
            }

            reader.ReadByte(); // read past Zero

            byte[] returnVal = new byte[i];
            Array.Copy(_bin, returnVal, i);
            return returnVal;
        }

        private string _ScanCStringToString(BinaryReader reader)
        {
            return Encoding.UTF8.GetString(_ScanCString(reader));
        }

        private string _ScanString(BinaryReader reader)
        {
            var strLen = reader.ReadInt32();
            string returnVal = Encoding.UTF8.GetString(_reader.ReadBytes(strLen - 1));
            reader.ReadByte();
            return returnVal;
        }

        private BSONTypes _ScanType(BinaryReader reader)
        {
            return (BSONTypes)reader.ReadByte();
        }

        public IEnumerator<BSONToken> GetEnumerator()
        {
            DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            // read length of BSON document
            int docLength = _reader.ReadInt32();
            string name = null;

            while (-1 != _reader.PeekChar())
            {
                BSONTypes type = _ScanType(_reader);

                switch (type)
                {
                    case BSONTypes.DOUBLE:
                        yield return new BSONValueToken<double>()
                        {
                            Type = BSONTokenType.DOUBLE,
                            Name = _ScanCStringToString(_reader),
                            Value = _reader.ReadDouble()
                        };
                        break;
                    case BSONTypes.STRING:
                        yield return new BSONValueToken<string>()
                        {
                            Type = BSONTokenType.STRING,
                            Name = _ScanCStringToString(_reader),
                            Value = _ScanString(_reader)
                        };
                        break;
                    case BSONTypes.INT32:
                        yield return new BSONValueToken<Int32>()
                        {
                            Type = BSONTokenType.INT32,
                            Name = _ScanCStringToString(_reader),
                            Value = _reader.ReadInt32()
                        };
                        break;
                    case BSONTypes.INT64:
                        yield return new BSONValueToken<Int64>()
                        {
                            Type = BSONTokenType.INT64,
                            Name = _ScanCStringToString(_reader),
                            Value = _reader.ReadInt64()
                        };
                        break;
                    case BSONTypes.DOCUMENT:
                        yield return new BSONDocumentToken()
                        {
                            Type = BSONTokenType.DOCUMENT_START,
                            Name = _ScanCStringToString(_reader),
                            Length = _reader.ReadInt32()
                        };
                        break;
                    case BSONTypes.ARRAY:
                        yield return new BSONDocumentToken()
                        {
                            Type = BSONTokenType.ARRAY_START,
                            Name = _ScanCStringToString(_reader),
                            Length = _reader.ReadInt32()
                        };
                        break;
                    case BSONTypes.BINARY:
                        name = _ScanCStringToString(_reader);
                        int binLength = _reader.ReadInt32();

                        yield return new BSONBinaryToken()
                        {
                            Name = name,
                            Type = BSONTokenType.BINARY,
                            BinType = (BSONBinaryType)_reader.ReadByte(),
                            Value = _reader.ReadBytes(binLength)
                        };
                        break;
                    case BSONTypes.OBJECTID:
                        yield return new BSONValueToken<byte[]>()
                        {
                            Type = BSONTokenType.OBJECT_ID,
                            Name = _ScanCStringToString(_reader),
                            Value = _reader.ReadBytes(12)
                        };
                        break;
                    case BSONTypes.BOOLEAN:
                        name = _ScanCStringToString(_reader);
                        byte boolByte = _reader.ReadByte();
                        if (0x00 == boolByte)
                        {
                            yield return new BSONToken() { Type = BSONTokenType.KEYWORD_FALSE, Name = name };
                        }
                        else if (0x01 == boolByte)
                        {
                            yield return new BSONToken() { Type = BSONTokenType.KEYWORD_TRUE, Name = name };
                        }
                        else
                        {
                            throw new BSONException("Unvalid boolean value found.  Must be \"0x00\" or \"0x01\".");
                        }
                        break;
                    case BSONTypes.DATETIME:
                        yield return new BSONValueToken<DateTime>
                        {
                            Type = BSONTokenType.DATETIME,
                            Name = _ScanCStringToString(_reader),
                            Value = UnixEpoch.AddMilliseconds(_reader.ReadInt64())
                        };
                        break;
                    case BSONTypes.NULL:
                        yield return new BSONToken()
                        {
                            Type = BSONTokenType.KEYWORD_NULL,
                            Name = _ScanCStringToString(_reader)
                        };
                        break;
                    case BSONTypes.REGEX:
                        yield return new BSONRegexToken()
                        {
                            Type = BSONTokenType.REGEX,
                            Name = _ScanCStringToString(_reader),
                            Pattern = _ScanCStringToString(_reader),
                            Options = _ScanCStringToString(_reader)
                        };
                        break;
                    case BSONTypes.CODE:
                        yield return new BSONValueToken<string>
                        {
                            Type = BSONTokenType.CODE,
                            Name = _ScanCStringToString(_reader),
                            Value = _ScanString(_reader)
                        };
                        break;
                    case BSONTypes.SYMBOL:
                        yield return new BSONValueToken<string>
                        {
                            Type = BSONTokenType.SYMBOL,
                            Name = _ScanCStringToString(_reader),
                            Value = _ScanString(_reader)
                        };
                        break;
                    case BSONTypes.SCOPEDCODE:
                        yield return new BSONCodeWithScope()
                        {
                            Type = BSONTokenType.CODE_WITH_SCOPE,
                            Name = _ScanCStringToString(_reader),
                            Length = _reader.ReadInt32(),
                            Code = _reader.ReadString()
                        };
                        yield return new BSONDocumentToken() { Type = BSONTokenType.DOCUMENT_START, Length = _reader.ReadInt32() };
                        break;
                    case BSONTypes.ZERO:
                        yield return new BSONToken() { Type = BSONTokenType.END };
                        break;
                    default:
                        throw new BSONException("Illegal BSON character sequence");
                }
            }
            yield return new BSONToken() { Type = BSONTokenType.EOF };
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
