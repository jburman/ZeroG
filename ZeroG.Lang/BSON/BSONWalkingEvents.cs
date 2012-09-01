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

namespace ZeroG.Lang.BSON
{
    public delegate void BSONEventHandler();
    public delegate void BSONElementEventHandler(string name, BSONTokenType type);
    public delegate void BSONValueEventHandler(BSONToken token);
    public delegate void BSONValueEventHandler<T>(T value);

    /// <summary>
    /// Provides a set of Events that may be listened to as a BSON construct is being parsed.
    /// </summary>
    /// <seealso cref="ZeroG.Lang.BSON.BSONWalkingValidator"/>
    public sealed class BSONWalkingEvents
    {
        public event BSONEventHandler DocumentStart;
        public event BSONEventHandler DocumentEnd;
        public event BSONEventHandler ArrayStart;
        public event BSONEventHandler ArrayEnd;

        /// <summary>
        /// Fires when an element type terminal and e_name is encountered 
        /// </summary>
        public event BSONElementEventHandler Element;
        /// <summary>
        /// Fires with the value of the element that follows the e_name (unless a document or array follows)
        /// </summary>
        public event BSONValueEventHandler<string> String;
        public event BSONValueEventHandler<double> Double;
        public event BSONValueEventHandler<Int32> Int32;
        public event BSONValueEventHandler<Int64> Int64;
        public event BSONValueEventHandler<bool> Boolean;
        public event BSONValueEventHandler<DateTime> DateTime;
        public event BSONEventHandler Null;
        /// <summary>
        /// Fired for any value that does not have a specific event defined for it (e.g. String).
        /// </summary>
        public event BSONValueEventHandler Value;

        internal void RaiseDocumentStart(BSONTypes types)
        {
            if (BSONTypes.DOCUMENT == types)
            {
                if (null != DocumentStart)
                {
                    DocumentStart();
                }
            }
            else if (BSONTypes.ARRAY == types)
            {
                if (null != ArrayStart)
                {
                    ArrayStart();
                }
            }
        }

        internal void RaiseDocumentEnd(BSONTypes types)
        {
            if (BSONTypes.DOCUMENT == types)
            {
                if (null != DocumentEnd)
                {
                    DocumentEnd();
                }
            }
            else if (BSONTypes.ARRAY == types)
            {
                if (null != ArrayEnd)
                {
                    ArrayEnd();
                }
            }
        }

        internal void RaiseElement(string name, BSONTokenType type)
        {
            if (null != Element)
            {
                Element(name, type);
            }
        }

        internal void RaiseValue(BSONToken value)
        {
            switch (value.Type)
            {
                case BSONTokenType.STRING:
                    if (null != String)
                    {
                        String(((BSONValueToken<string>)value).Value);
                    }
                    break;
                case BSONTokenType.DOUBLE:
                    if (null != Double)
                    {
                        Double(((BSONValueToken<double>)value).Value);
                    }
                    break;
                case BSONTokenType.BOOLEAN:
                    if (null != Boolean)
                    {
                        Boolean(((BSONValueToken<bool>)value).Value);
                    }
                    break;
                case BSONTokenType.DATETIME:
                    if (null != DateTime)
                    {
                        DateTime(((BSONValueToken<DateTime>)value).Value);
                    }
                    break;
                case BSONTokenType.INT32:
                    if (null != Int32)
                    {
                        Int32(((BSONValueToken<Int32>)value).Value);
                    }
                    break;
                case BSONTokenType.INT64:
                    if (null != Int64)
                    {
                        Int64(((BSONValueToken<Int64>)value).Value);
                    }
                    break;
                case BSONTokenType.NULL:
                    if (null != Null)
                    {
                        Null();
                    }
                    break;
                default:
                    if (null != Value)
                    {
                        Value(value);
                    }
                    break;
            }
        }
    }
}
