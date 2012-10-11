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

using System.Collections.Generic;
namespace ZeroG.Data.Object
{
    /// <summary>
    /// The IObjectServiceClient interface provides a convenience interface for interacting with the ObjectService.
    /// </summary>
    public interface IObjectServiceClient
    {
        ObjectID Store(byte[] value);
        ObjectID Store(byte[] secondaryKey, byte[] value);
        ObjectID Store(byte[] value, ObjectIndex[] indexes);
        ObjectID Store(byte[] secondaryKey, byte[] value, ObjectIndex[] indexes);

        byte[] Get(int objectId);
        byte[] GetBySecondaryKey(byte[] secondaryKey);

        void Remove(int id);
        void Remove(int[] ids);

        /// <summary>
        /// Simplistic interface for finding objects. 
        /// Allows a logical operator, comparison operator, object limit, and 
        /// ordering to be specified along with a list of index names and constraint values.
        /// It is very limited however, as only one logical and comparison operator may be used and not 
        /// all comparison operators are supported.
        /// </summary>
        /// <param name="options"></param>
        /// <param name="indexes"></param>
        /// <returns></returns>
        byte[][] Find(ObjectFindOptions options, ObjectIndex[] indexes);

        /// <summary>
        /// Allows objects to be found using a constraint string defined in JSON.
        /// This allows for all logical and comparison operators to be used together to build more powerful
        /// search capability.
        /// </summary>
        /// <param name="constraint"></param>
        /// <returns></returns>
        byte[][] Find(string constraint);
        /// <summary>
        /// Allows objects to be found using a constraint string defined in JSON.
        /// Also allows an object limit and ordering to be specified.
        /// This allows for all logical and comparison operators to be used together to build more powerful
        /// search capability.
        /// </summary>
        /// <param name="constraint"></param>
        /// <param name="limit"></param>
        /// <param name="order"></param>
        /// <returns></returns>
        byte[][] Find(string constraint, uint limit, OrderOptions order);
    }
}
