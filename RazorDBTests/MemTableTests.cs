using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using RazorDB;

namespace RazorDBTests {

    [TestFixture]
    public class MemTableTests {

        [Test]
        public void AddAndLookupItems() {

            MemTable mt = new MemTable();

            List<KeyValuePair<ByteArray, ByteArray>> values = new List<KeyValuePair<ByteArray,ByteArray>>();

            for (int i = 0; i < 10000; i++) {
                var randomKey = ByteArray.Random(40);
                var randomValue = ByteArray.Random(256);

                values.Add(new KeyValuePair<ByteArray, ByteArray>(randomKey, randomValue));
                mt.Add(randomKey, randomValue);
            }

            ByteArray value;
            foreach (var pair in values) {
                Assert.True(mt.Lookup(pair.Key, out value));
                Assert.AreEqual(pair.Value, value);
            }
            Assert.False(mt.Lookup(ByteArray.Random(40), out value));

            Assert.AreEqual(10000 * (40 + 256), mt.Size);
        }
    }
}
