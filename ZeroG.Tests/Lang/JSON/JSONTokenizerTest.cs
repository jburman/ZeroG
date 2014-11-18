using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ZeroG.Lang.JSON;

namespace ZeroG.Tests.Lang.JSON
{
    [TestClass]
    public class JSONTokenizerTest
    {
        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext { get; set; }

        [TestMethod]
        [TestCategory("Core")]
        public void ObjectLiteral()
        {
            var json = @"{ ""Age"" : 5 }";

            var tok = new JSONTokenizer(new StringReader(json));
            var tokenList = new List<JSONToken>();
            foreach (var t in tok)
            {
                tokenList.Add(t);
            }

            Assert.AreEqual(6, tokenList.Count);

            Assert.AreEqual(JSONTokenType.OBJECT_START, tokenList[0].Type);
            Assert.AreEqual(JSONTokenType.STRING, tokenList[1].Type);
            Assert.AreEqual("Age", tokenList[1].StrValue);
            Assert.AreEqual(JSONTokenType.COLON, tokenList[2].Type);
            Assert.AreEqual(JSONTokenType.NUMBER, tokenList[3].Type);
            Assert.AreEqual(5m, tokenList[3].NumValue);
            Assert.AreEqual(JSONTokenType.OBJECT_END, tokenList[4].Type);
            Assert.AreEqual(JSONTokenType.EOF, tokenList[5].Type);
        }

        [TestMethod]
        [TestCategory("Core")]
        public void NumberArray()
        {
            var json = @"{ ""Ages"" : [5,7,8] }";
            var tok = new JSONTokenizer(new StringReader(json));
            var tokenList = new List<JSONToken>();
            foreach (var t in tok)
            {
                tokenList.Add(t);
            }

            Assert.AreEqual(12, tokenList.Count);

            Assert.AreEqual(JSONTokenType.OBJECT_START, tokenList[0].Type);
            Assert.AreEqual(JSONTokenType.STRING, tokenList[1].Type);
            Assert.AreEqual("Ages", tokenList[1].StrValue);
            Assert.AreEqual(JSONTokenType.COLON, tokenList[2].Type);
            Assert.AreEqual(JSONTokenType.ARRAY_START, tokenList[3].Type);
            Assert.AreEqual(JSONTokenType.NUMBER, tokenList[4].Type);
            Assert.AreEqual(5m, tokenList[4].NumValue);
            Assert.AreEqual(JSONTokenType.COMMA, tokenList[5].Type);

            Assert.AreEqual(JSONTokenType.NUMBER, tokenList[6].Type);
            Assert.AreEqual(7m, tokenList[6].NumValue);
            Assert.AreEqual(JSONTokenType.COMMA, tokenList[7].Type);

            Assert.AreEqual(JSONTokenType.NUMBER, tokenList[8].Type);
            Assert.AreEqual(8m, tokenList[8].NumValue);

            Assert.AreEqual(JSONTokenType.ARRAY_END, tokenList[9].Type);

            Assert.AreEqual(JSONTokenType.OBJECT_END, tokenList[10].Type);
            Assert.AreEqual(JSONTokenType.EOF, tokenList[11].Type);
        }

        [TestMethod]
        [TestCategory("Core")]
        public void ValuesArray()
        {
            var json = @"{ ""Vals"" : [1.453,null,""str val"",true,false] }";
            var tok = new JSONTokenizer(new StringReader(json));
            var tokenList = new List<JSONToken>();
            foreach (var t in tok)
            {
                tokenList.Add(t);
            }

            Assert.AreEqual(16, tokenList.Count);

            Assert.AreEqual(JSONTokenType.OBJECT_START, tokenList[0].Type);
            Assert.AreEqual(JSONTokenType.STRING, tokenList[1].Type);
            Assert.AreEqual("Vals", tokenList[1].StrValue);
            Assert.AreEqual(JSONTokenType.COLON, tokenList[2].Type);
            Assert.AreEqual(JSONTokenType.ARRAY_START, tokenList[3].Type);
            Assert.AreEqual(JSONTokenType.NUMBER, tokenList[4].Type);
            Assert.AreEqual(1.453m, tokenList[4].NumValue);
            Assert.AreEqual(JSONTokenType.COMMA, tokenList[5].Type);

            Assert.AreEqual(JSONTokenType.KEYWORD_NULL, tokenList[6].Type);
            Assert.AreEqual(JSONTokenType.COMMA, tokenList[7].Type);

            Assert.AreEqual(JSONTokenType.STRING, tokenList[8].Type);
            Assert.AreEqual("str val", tokenList[8].StrValue);
            Assert.AreEqual(JSONTokenType.COMMA, tokenList[9].Type);

            Assert.AreEqual(JSONTokenType.KEYWORD_TRUE, tokenList[10].Type);
            Assert.AreEqual(JSONTokenType.COMMA, tokenList[11].Type);

            Assert.AreEqual(JSONTokenType.KEYWORD_FALSE, tokenList[12].Type);

            Assert.AreEqual(JSONTokenType.ARRAY_END, tokenList[13].Type);

            Assert.AreEqual(JSONTokenType.OBJECT_END, tokenList[14].Type);
            Assert.AreEqual(JSONTokenType.EOF, tokenList[15].Type);
        }

        [TestMethod]
        [TestCategory("Core")]
        public void NestedObjectLiteral()
        {
            var json = @"{ ""Action"" : { ""Name"" : ""Parse"" } }";
            var tok = new JSONTokenizer(new StringReader(json));
            var tokenList = new List<JSONToken>();
            foreach (var t in tok)
            {
                tokenList.Add(t);
            }

            Assert.AreEqual(10, tokenList.Count);

            Assert.AreEqual(JSONTokenType.OBJECT_START, tokenList[0].Type);
            Assert.AreEqual(JSONTokenType.STRING, tokenList[1].Type);
            Assert.AreEqual("Action", tokenList[1].StrValue);
            Assert.AreEqual(JSONTokenType.COLON, tokenList[2].Type);

            Assert.AreEqual(JSONTokenType.OBJECT_START, tokenList[3].Type);
            Assert.AreEqual(JSONTokenType.STRING, tokenList[4].Type);
            Assert.AreEqual("Name", tokenList[4].StrValue);
            Assert.AreEqual(JSONTokenType.COLON, tokenList[5].Type);
            Assert.AreEqual(JSONTokenType.STRING, tokenList[6].Type);
            Assert.AreEqual("Parse", tokenList[6].StrValue);

            Assert.AreEqual(JSONTokenType.OBJECT_END, tokenList[7].Type);
            Assert.AreEqual(JSONTokenType.OBJECT_END, tokenList[8].Type);
            Assert.AreEqual(JSONTokenType.EOF, tokenList[9].Type);
        }

        [TestMethod]
        [TestCategory("Core")]
        public void MalformedKey()
        {
            var json = @"{ ""Age : 5 }";

            var tok = new JSONTokenizer(new StringReader(json));
            var tokenList = new List<JSONToken>();
            foreach (var t in tok)
            {
                tokenList.Add(t);
            }

            Assert.AreEqual(3, tokenList.Count);
            Assert.AreEqual(JSONTokenType.OBJECT_START, tokenList[0].Type);
            Assert.AreEqual(JSONTokenType.STRING, tokenList[1].Type);
            Assert.AreEqual("Age : 5 }", tokenList[1].StrValue);
            Assert.AreEqual(JSONTokenType.EOF, tokenList[2].Type);
        }

        [TestMethod]
        [TestCategory("Core")]
        public void LargeRecurse()
        {
            var tempFile = Path.GetTempFileName();
            try
            {
                using (var fs = new FileStream(tempFile, FileMode.Append))
                {
                    for (int i = 0; 500000 > i; i++)
                    {
                        byte[] buf = Encoding.UTF8.GetBytes("{\"k" + i + "\" : ");
                        fs.Write(buf, 0, buf.Length);
                    }
                    for (int i = 0; 500000 > i; i++)
                    {
                        byte[] buf = Encoding.UTF8.GetBytes("}");
                        fs.Write(buf, 0, buf.Length);
                    }
                    fs.Flush();
                }

                using (var sr = new StreamReader(tempFile))
                {
                    var tok = new JSONTokenizer(sr);
                    var tokenList = new List<JSONToken>();
                    int count = 0;
                    foreach (var t in tok)
                    {
                        if (JSONTokenType.OBJECT_START == t.Type)
                        {
                            ++count;
                        }
                    }
                    Assert.AreEqual(500000, count);
                }
            }
            finally
            {
                File.Delete(tempFile);
            }
        }
    }
}
