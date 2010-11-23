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
    using JSONTokenStream = System.Collections.Generic.IEnumerator<JSONToken>;

    /// <summary>
    /// Walks the tokens returned from a JSONTokenizer and validates that they form a valid 
    /// JSON construct.
    /// Additionally, if a JSONWalkingEvents instance is supplied, then its events will be 
    /// raised as the JSON tokens are consumed by the validator.
    /// </summary>
    /// <seealso cref="ZeroG.Lang.JSON.JSONWalkingEvents"/>
    public class JSONWalkingValidator
    {
        #region Private helpers

        /// <summary>
        /// Start walking the JSON structure.
        /// </summary>
        /// <param name="tokens"></param>
        /// <param name="events"></param>
        private void _Start(JSONTokenStream tokens, JSONWalkingEvents events) 
        {
            tokens.MoveNext();
            JSONToken next = tokens.Current;

            if (JSONTokenType.OBJECT_START == next.Type)
            {
                _Object(tokens, events);
            }
            else if (JSONTokenType.ARRAY_START == next.Type)
            {
                _Array(tokens, events);
            }
            else
            {
                throw new JSONValidationException("Expected OBJECT or ARRAY but got: " + next.Type);
            }
        }

        /// <summary>
        /// Walk an Object.  Fires ObjectStart, ObjectKey, ObjectNext, and ObjectEnd
        /// </summary>
        /// <param name="tokens"></param>
        /// <param name="events"></param>
        private void _Object(JSONTokenStream tokens, JSONWalkingEvents events)
        {
            if (null != events)
            {
                events.RaiseObjectStart();
            }

            tokens.MoveNext();
            JSONToken next = tokens.Current;

            while (JSONTokenType.OBJECT_END != next.Type)
            {
                _ObjectField(tokens, events);

                tokens.MoveNext();
                next = tokens.Current;

                if (JSONTokenType.COMMA == next.Type)
                {
                    tokens.MoveNext();
                    next = tokens.Current;

                    if (null != events)
                    {
                        events.RaiseObjectNext();
                    }
                }
            }

            if (JSONTokenType.OBJECT_END == next.Type)
            {
                if (null != events)
                {
                    events.RaiseObjectEnd();
                }
                return;
            }
            else
            {
                throw new JSONValidationException("Expected VALUE or end of OBJECT but got: " + next.Type);
            }
        }

        private void _ObjectField(JSONTokenStream tokens, JSONWalkingEvents events)
        {
            JSONToken next = tokens.Current;

            if (JSONTokenType.STRING == next.Type)
            {
                if (null != events)
                {
                    events.RaiseObjectKey(next.StrValue);
                }

                tokens.MoveNext();
                next = tokens.Current;
                if (JSONTokenType.COLON == next.Type)
                {
                    tokens.MoveNext();
                    next = tokens.Current;
                    switch (next.Type)
                    {
                        case JSONTokenType.STRING:
                            if (null != events)
                            {
                                events.RaiseString(next.StrValue);
                            }
                            break;
                        case JSONTokenType.NUMBER:
                            if (null != events)
                            {
                                events.RaiseNumber(next.NumValue);
                            }
                            break;
                        case JSONTokenType.KEYWORD_TRUE:
                            if (null != events)
                            {
                                events.RaiseBoolean(true);
                            }
                            break;
                        case JSONTokenType.KEYWORD_FALSE:
                            if (null != events)
                            {
                                events.RaiseBoolean(false);
                            }
                            break;
                        case JSONTokenType.KEYWORD_NULL:
                            if (null != events)
                            {
                                events.RaiseNull();
                            }
                            break;
                        case JSONTokenType.OBJECT_START:
                            _Object(tokens, events);
                            break;
                        case JSONTokenType.ARRAY_START:
                            _Array(tokens, events);
                            break;
                        default:
                            throw new JSONValidationException("Expected value but got: " + next.Type);
                    }
                }
                else
                {
                    throw new JSONValidationException("Expected COLON but got: " + next.Type);
                }
            }
            else
            {
                throw new JSONValidationException("Expected STRING but got: " + next.Type);
            }
        }

        /// <summary>
        /// Reads an Array.  Fires ArrayStart, ArrayNext, and ArrayEnd.
        /// </summary>
        /// <param name="tokens"></param>
        /// <param name="events"></param>
        private void _Array(JSONTokenStream tokens, JSONWalkingEvents events)
        {
            if (null != events)
            {
                events.RaiseArrayStart();
            }

            tokens.MoveNext();
            JSONToken next = tokens.Current;

            while (JSONTokenType.ARRAY_END != next.Type)
            {
                _ArrayElement(tokens, events);

                tokens.MoveNext();
                next = tokens.Current;

                if (JSONTokenType.COMMA == next.Type)
                {
                    tokens.MoveNext();
                    next = tokens.Current;

                    if (null != events)
                    {
                        events.RaiseArrayNext();
                    }
                }
            }

            if (JSONTokenType.ARRAY_END == next.Type)
            {
                if (null != events)
                {
                    events.RaiseArrayEnd();
                }
                return;
            }
            else
            {
                throw new JSONValidationException("Expected VALUE or end of ARRAY but got: " + next.Type);
            }
        }

        /// <summary>
        /// Reads a String, Number, keyword, Object, or Array element within an Array.
        /// </summary>
        /// <param name="tokens"></param>
        /// <param name="events"></param>
        private void _ArrayElement(JSONTokenStream tokens, JSONWalkingEvents events)
        {
            JSONToken next = tokens.Current;
                
            switch (next.Type)
            {
                case JSONTokenType.STRING:
                    if (null != events)
                    {
                        events.RaiseString(next.StrValue);
                    }
                    break;
                case JSONTokenType.NUMBER:
                    if (null != events)
                    {
                        events.RaiseNumber(next.NumValue);
                    }
                    break;
                case JSONTokenType.KEYWORD_TRUE:
                    if (null != events)
                    {
                        events.RaiseBoolean(true);
                    }
                    break;
                case JSONTokenType.KEYWORD_FALSE:
                    if (null != events)
                    {
                        events.RaiseBoolean(false);
                    }
                    break;
                case JSONTokenType.KEYWORD_NULL:
                    if (null != events)
                    {
                        events.RaiseNull();
                    }
                    break;
                case JSONTokenType.OBJECT_START:
                    _Object(tokens, events);
                    break;
                case JSONTokenType.ARRAY_START:
                    _Array(tokens, events);
                    break;
                default:
                    throw new JSONValidationException("Expected value but got: " + next.Type);
            }
        }
        #endregion

        /// <summary>
        /// Walks JSON tokens and validates but does not fire any events.
        /// </summary>
        /// <param name="tokens"></param>
        public void Walk(JSONTokenStream tokens)
        {
            Walk(tokens, null);
        }

        /// <summary>
        /// Walks JSON tokens and validates and fires events on the supplied events instance.
        /// </summary>
        /// <param name="tokens"></param>
        /// <param name="events"></param>
        public void Walk(JSONTokenStream tokens, JSONWalkingEvents events)
        {
            _Start(tokens, events);

            tokens.MoveNext();

            if (JSONTokenType.EOF != tokens.Current.Type)
            {
                throw new JSONValidationException("Expected EOF but got: " + tokens.Current.Type);
            }
        }
    }
}
