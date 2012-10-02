﻿#region License, Terms and Conditions
// Copyright (c) 2012 Jeremy Burman
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

namespace ZeroG.Data.Object
{
    public struct ObjectFindOptions
    {
        /// <summary>
        /// Logical operator to use
        /// </summary>
        public ObjectFindLogic Logic;
        /// <summary>
        /// Comparison operator to use
        /// </summary>
        public ObjectFindOperator Operator;
        /// <summary>
        /// Limit to the number of objects to return
        /// </summary>
        public uint Limit;

        public override string ToString()
        {
            return Logic + ":" + Operator + ":" + Limit;
        }

        public override bool Equals(object obj)
        {
            if (null == obj || !(obj is ObjectFindOptions))
            {
                return false;
            }
            return ToString().Equals(((ObjectFindOptions)obj).ToString());
        }

        public override int GetHashCode()
        {
            return ToString().GetHashCode();
        }
    }
}
