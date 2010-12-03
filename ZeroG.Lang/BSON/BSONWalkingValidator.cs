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
    using BSONTokenStream = System.Collections.Generic.IEnumerator<BSONToken>;

    /// <summary>
    /// Walks the tokens returned from a BSONTokenizer and validates that they form a valid 
    /// BSON construct.
    /// Additionally, if a BSONWalkingEvents instance is supplied, then its events will be 
    /// raised as the BSON tokens are consumed by the validator.
    /// </summary>
    /// <seealso cref="ZeroG.Lang.BSON.BSONWalkingEvents"/>
    public class BSONWalkingValidator
    {
        #region Private helpers

        /// <summary>
        /// Start walking the BSON structure.
        /// </summary>
        /// <param name="tokens"></param>
        /// <param name="events"></param>
        private void _Start(BSONTokenStream tokens, BSONWalkingEvents events) 
        {
            tokens.MoveNext();
            BSONToken next = tokens.Current;

            if (BSONTokenType.DOCUMENT_START == next.Type)
            {
                _Document(tokens, events, BSONTypes.DOCUMENT);
            }
            else if (BSONTokenType.ARRAY_START == next.Type)
            {
                _Document(tokens, events, BSONTypes.ARRAY);
            }
            else
            {
                throw new BSONValidationException("Expected DOCUMENT or ARRAY but got: " + next.Type);
            }
        }

        /// <summary>
        /// Walk a Document.  Fires DocumentStart and DocumentEnd
        /// </summary>
        /// <param name="tokens"></param>
        /// <param name="events"></param>
        /// <param name="docType"></param>
        private void _Document(BSONTokenStream tokens, BSONWalkingEvents events, BSONTypes docType)
        {
            if (null != events)
            {
                events.RaiseDocumentStart(docType);
            }

            BSONToken next = tokens.Current;

            while (true)
            {
                tokens.MoveNext();
                next = tokens.Current;
                if (BSONTokenType.END != next.Type && BSONTokenType.EOF != next.Type)
                {
                    _DocumentElement(next, tokens, events);
                }
                else
                {
                    break;
                }
            }

            if (BSONTokenType.END == next.Type)
            {
                if (null != events)
                {
                    events.RaiseDocumentEnd(docType);
                }
                return;
            }
            else
            {
                throw new BSONValidationException("Expected value or end of document but got: " + next.Type);
            }
        }

        private void _DocumentElement(BSONToken token, BSONTokenStream tokens, BSONWalkingEvents events)
        {
            if (BSONTokenType.END == token.Type
                || BSONTokenType.EOF == token.Type)
            {
                throw new BSONValidationException("Unexpected BSON token type encountered: " + token.Type);
            }

            if (BSONTokenType.DOCUMENT_START == token.Type)
            {
                events.RaiseElement(token.Name, token.Type);
                _Document(tokens, events, BSONTypes.DOCUMENT);
            }
            else if (BSONTokenType.ARRAY_START == token.Type)
            {
                events.RaiseElement(token.Name, token.Type);
                _Document(tokens, events, BSONTypes.ARRAY);
            }
            else if (BSONTokenType.CODE_WITH_SCOPE == token.Type)
            {
                events.RaiseElement(token.Name, token.Type);
                events.RaiseValue(token);
                
                tokens.MoveNext();
                token = tokens.Current;

                if (BSONTokenType.END == token.Type
                    || BSONTokenType.EOF == token.Type)
                {
                    throw new BSONValidationException("Expected DOCUMENT but got: " + token.Type);
                }

                if (BSONTokenType.DOCUMENT_START == token.Type)
                {
                    _Document(tokens, events, BSONTypes.DOCUMENT);
                }
                else if (BSONTokenType.ARRAY_START == token.Type)
                {
                    _Document(tokens, events, BSONTypes.ARRAY);
                }
            }
            else
            {
                events.RaiseElement(token.Name, token.Type);
                events.RaiseValue(token);
            }
        }
        #endregion

        /// <summary>
        /// Walks BSON tokens and validates but does not fire any events.
        /// </summary>
        /// <param name="tokens"></param>
        public void Walk(BSONTokenStream tokens)
        {
            Walk(tokens, null);
        }

        /// <summary>
        /// Walks BSON tokens and validates and fires events on the supplied events instance.
        /// </summary>
        /// <param name="tokens"></param>
        /// <param name="events"></param>
        public void Walk(BSONTokenStream tokens, BSONWalkingEvents events)
        {
            _Start(tokens, events);

            tokens.MoveNext();

            if (BSONTokenType.EOF != tokens.Current.Type)
            {
                throw new BSONValidationException("Expected EOF but got: " + tokens.Current.Type);
            }
        }
    }
}
