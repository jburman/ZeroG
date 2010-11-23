using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ZeroG.Lang.JSON
{
    public class JSONException : Exception
    {
        public JSONException(string message) : base(message)
        {
        }
    }

    public class JSONValidationException : JSONException
    {
        public JSONValidationException(string message)
            : base(message)
        {
        }
    }
}
