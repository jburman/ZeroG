using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ZeroG.Data.Object;
using System.Text;
using System.IO;

namespace ZeroG.Tests.Object
{
    [TestClass]
    public class ObjectIndexTest
    {
        [TestMethod]
        public void Create()
        {
            var idx = ObjectIndex.Create("Index_Name1", 5);

            Assert.AreEqual("Index_Name1", idx.Name);
            Assert.AreEqual(5, idx.GetObjectValue());
            Assert.AreEqual(ObjectIndexType.Integer, idx.DataType);
            Assert.AreEqual(5, BitConverter.ToInt32(idx.Value, 0));
        }

        [TestMethod]
        public void CreateEachDataType()
        {
            var idx = ObjectIndex.Create("Index_Name1", 5);
            Assert.AreEqual("Index_Name1", idx.Name);
            Assert.AreEqual(5, idx.GetObjectValue());
            Assert.AreEqual(ObjectIndexType.Integer, idx.DataType);
            Assert.AreEqual(5, BitConverter.ToInt32(idx.Value, 0));

            idx = ObjectIndex.Create("Index_Name1", "test val");
            Assert.AreEqual("Index_Name1", idx.Name);
            Assert.AreEqual("test val", idx.GetObjectValue());
            Assert.AreEqual(ObjectIndexType.String, idx.DataType);
            Assert.AreEqual("test val", Encoding.UTF8.GetString(idx.Value));

            idx = ObjectIndex.Create("Index_Name1", new Decimal(5));
            Assert.AreEqual("Index_Name1", idx.Name);
            Assert.AreEqual(new Decimal(5), idx.GetObjectValue());
            Assert.AreEqual(ObjectIndexType.Decimal, idx.DataType);
            var reader = new BinaryReader(new MemoryStream(idx.Value));
            Assert.AreEqual(new Decimal(5), reader.ReadDecimal());

            var binValue = Encoding.UTF8.GetBytes("test bin");
            idx = ObjectIndex.Create("Index_Name1", binValue);
            Assert.AreEqual("Index_Name1", idx.Name);
            Assert.AreEqual(binValue, idx.GetObjectValue());
            Assert.AreEqual(ObjectIndexType.Binary, idx.DataType);
            Assert.AreEqual(binValue, idx.Value);

            var dtVal = DateTime.Now;
            var val = dtVal.ToBinary();
            idx = ObjectIndex.Create("Index_Name1", dtVal);
            Assert.AreEqual("Index_Name1", idx.Name);
            Assert.AreEqual(dtVal, idx.GetObjectValue());
            Assert.AreEqual(ObjectIndexType.DateTime, idx.DataType);
            Assert.AreEqual(dtVal, DateTime.FromBinary(BitConverter.ToInt64(idx.Value, 0)));
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void InvalidCharacterIndexName()
        {
            ObjectIndex.Create("Test % Value", 5);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void IndexNameTooShort()
        {
            ObjectIndex.Create("A", 5);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void IndexNameTooLong()
        {
            ObjectIndex.Create(new string('A', 31), 5);
        }
    }
}
