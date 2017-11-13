using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ZeroG.Data.Object;

namespace ZeroG.Data.Database.Drivers
{
    public class SqliteIndexValues
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

            if (reader.IsDBNull(valueIndex))
            {
                return default(T);
            }
            else
            { 
                if (rawValueType == NumType)
                {
                    nextVal = reader.GetInt64(valueIndex);
                }
                else if (rawValueType == StringType)
                {
                    nextVal = reader.GetString(valueIndex);
                }
                else if (rawValueType == BlobType)
                {
                    nextVal = Convert.ChangeType(reader.GetValue(valueIndex), BlobType);
                }

                return (T)Convert.ChangeType(nextVal, toType);
            }
        }

        internal static object ReadValue(IDataRecord reader, int valueIndex, Type rawValueType)
        {
            object nextVal = null;

            if (rawValueType == null)
                rawValueType = reader.GetFieldType(valueIndex);

            Type valType = reader.GetFieldType(valueIndex);

            if (reader.IsDBNull(valueIndex))
                return null;
            else
            {
                if (rawValueType == NumType)
                    nextVal = reader.GetInt64(0);
                else if (rawValueType == StringType)
                    nextVal = reader.GetString(0);
                else if (rawValueType == BlobType)
                    nextVal = Convert.ChangeType(reader.GetValue(0), BlobType);

                ObjectIndexType indexType = ObjectIndex.DefaultDataType.GetDataType(nextVal);
                if (indexType == ObjectIndexType.Unknown)
                    throw new ArgumentOutOfRangeException(nameof(valueIndex), "Unsupported value type " + nextVal?.GetType().Name);
                return Convert.ChangeType(nextVal, indexType.GetSystemType());
            }
        }
    }
}
