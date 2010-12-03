using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ZeroG.Lang.BSON
{
    public class BSONException : Exception
    {
        public BSONException(string message)
            : base(message)
        {
        }
    }

    public class BSONValidationException : BSONException
    {
        public BSONValidationException(string message)
            : base(message)
        {
        }
    }
}
