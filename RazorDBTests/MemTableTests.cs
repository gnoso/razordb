using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using NUnit.Framework;
using RazorDB;
using System.Diagnostics;

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
                Assert.IsTrue(mt.Lookup(pair.Key, out value));
                Assert.AreEqual(pair.Value, value);
            }
            Assert.IsFalse(mt.Lookup(ByteArray.Random(40), out value));

            Assert.AreEqual(10000 * (40 + 256), mt.Size);
            Assert.IsTrue(mt.Full);
        }

        [Test]
        public void WriteMemTableToSsTable() {

            MemTable mt = new MemTable();

            for (int i = 0; i < 10000; i++) {
                var randomKey = ByteArray.Random(40);
                var randomValue = ByteArray.Random(256);

                mt.Add(randomKey, randomValue);
            }

            var timer = new Stopwatch();
            string path = Path.GetFullPath("WriteMemTableToSsTable.mt");
            timer.Start();
            mt.WriteToSortedBlockTable(path);
            timer.Stop();
            
            Console.WriteLine("Wrote sorted table at a throughput of {0} MB/s", (double) mt.Size / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0) );
        }
    }

}
