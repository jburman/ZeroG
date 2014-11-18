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

namespace ZeroG.Lang.JSON
{
    public delegate void JSONEventHandler();
    public delegate void JSONEventHandler<T>(T value);

    /// <summary>
    /// Provides a set of Events that may be listened to as a JSON construct is being parsed.
    /// </summary>
    /// <seealso cref="ZeroG.Lang.JSON.JSONWalkingValidator"/>
    public sealed class JSONWalkingEvents
    {
        public event JSONEventHandler ArrayStart;
        public event JSONEventHandler ArrayEnd;
        /// <summary>
        /// Fired when a comma is encountered in an Array, signifying that a new element will be parsed.
        /// </summary>
        public event JSONEventHandler ArrayNext;

        public event JSONEventHandler ObjectStart;
        public event JSONEventHandler ObjectEnd;
        /// <summary>
        /// Fired when a comma is encountered inside of an Object construct, signifying that another field will be parsed.
        /// </summary>
        public event JSONEventHandler ObjectNext;

        /// <summary>
        /// Fired when a field name is parsed inside of an Object construct.
        /// For example, given the JSON { "foo" : "bar" }, then ObjectKey will be 
        /// fired when "foo" is parsed.
        /// </summary>
        public event JSONEventHandler<string> ObjectKey;

        public event JSONEventHandler<string> String;
        public event JSONEventHandler<decimal> Number;
        public event JSONEventHandler<bool> Boolean;
        public event JSONEventHandler Null;

        internal void RaiseArrayStart()
        {
            if (null != ArrayStart)
            {
                ArrayStart();
            }
        }

        internal void RaiseArrayEnd()
        {
            if (null != ArrayEnd)
            {
                ArrayEnd();
            }
        }

        internal void RaiseArrayNext()
        {
            if (null != ArrayNext)
            {
                ArrayNext();
            }
        }

        internal void RaiseObjectStart()
        {
            if (null != ObjectStart)
            {
                ObjectStart();
            }
        }

        internal void RaiseObjectEnd()
        {
            if (null != ObjectEnd)
            {
                ObjectEnd();
            }
        }

        internal void RaiseObjectNext()
        {
            if (null != ObjectNext)
            {
                ObjectNext();
            }
        }

        internal void RaiseObjectKey(string name)
        {
            if (null != ObjectKey)
            {
                ObjectKey(name);
            }
        }

        internal void RaiseString(string value)
        {
            if (null != String)
            {
                String(value);
            }
        }

        internal void RaiseNumber(decimal value)
        {
            if (null != Number)
            {
                Number(value);
            }
        }

        internal void RaiseBoolean(bool value)
        {
            if (null != Boolean)
            {
                Boolean(value);
            }
        }

        internal void RaiseNull()
        {
            if (null != Null)
            {
                Null();
            }
        }
    }
}
