using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ZeroG.Data.Database.Drivers
{
    public class SqliteValues
    {
        private static readonly Type NumType = typeof(Int64);
        private static readonly Type StringType = typeof(string);
        private static readonly Type BlobType = typeof(byte[]);

        //public static T GetValue<T>(object rawValue)
        //{
        //    Type toType = typeof(T);

        //}

        internal static T ReadValue<T>(IDataRecord reader, int valueIndex, Type rawValueType, Type toType)
        {
            object nextVal = null;

            if (rawValueType == null)
                rawValueType = reader.GetFieldType(valueIndex);

            if (rawValueType == NumType)
            {
                nextVal = reader.GetInt64(0);
            }
            else if (rawValueType == StringType)
            {
                nextVal = reader.GetString(0);
            }
            else if (rawValueType == BlobType)
            {
                nextVal = Convert.ChangeType(reader.GetValue(0), BlobType);
            }

            return (T)Convert.ChangeType(nextVal, toType);
        }
    }
}
