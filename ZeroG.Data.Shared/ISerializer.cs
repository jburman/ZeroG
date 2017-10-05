using System;

namespace ZeroG.Data
{
    public interface ISerializer
    {
        byte[] Serialize(string value);
        byte[] Serialize(uint id);
        byte[] Serialize(int id);
        byte[] Serialize(Guid id);
        byte[] Serialize<T>(T value);
        byte[] Deserialize(string value);
        string DeserializeString(byte[] val);
        int DeserializeInt32(byte[] val);
        uint DeserializeUInt32(byte[] val);
        Guid DeserializeGuid(byte[] val);
        T Deserialize<T>(byte[] value);

        byte[] CreateFullObjectKey(string nameSpace, string objectName);
        byte[] CreateFullObjectKey(string objectFullName);
    }
}
