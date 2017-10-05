#region License, Terms and Conditions
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

using System.Runtime.Serialization;
namespace ZeroG.Data.Object
{
    [DataContract]
    public struct ObjectFindOptions
    {
        /// <summary>
        /// Logical operator to use
        /// </summary>
        [DataMember(Order=1)]
        public ObjectFindLogic Logic;
        /// <summary>
        /// Comparison operator to use
        /// </summary>
        [DataMember(Order = 2)]
        public ObjectFindOperator Operator;
        /// <summary>
        /// Limit to the number of objects to return
        /// </summary>
        [DataMember(Order = 3)]
        public uint Limit;
        /// <summary>
        /// Specify an order to the results.
        /// </summary>
        [DataMember(Order = 4)]
        public OrderOptions Order;

        public override string ToString()
        {
            return Logic + ":" + Operator + ":" + Limit + ((null != Order)? ":" + Order.ToString() : string.Empty);
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
