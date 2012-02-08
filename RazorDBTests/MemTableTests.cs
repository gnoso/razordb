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
        public void SetItemsMultipleTimes() {

            MemTable mt = new MemTable();

            Dictionary<ByteArray, ByteArray> values = new Dictionary<ByteArray, ByteArray>();

            for (int i = 0; i < 10000; i++) {
                var randomKey = new ByteArray(BitConverter.GetBytes(i % 10));
                var randomValue = ByteArray.Random(256);

                values[randomKey] = randomValue;
                mt.Add(randomKey, randomValue);
            }

            ByteArray value;
            foreach (var pair in values) {
                Assert.IsTrue(mt.Lookup(pair.Key, out value));
                Assert.AreEqual(pair.Value, value);
            }
            Assert.IsFalse(mt.Lookup(ByteArray.Random(4), out value));

            Assert.AreEqual(10000 * (4 + 256), mt.Size);
        }

        [Test]
        public void WriteMemTableToSsTable() {

            string path = Path.GetFullPath("TestData\\WriteMemTableToSsTable");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            MemTable mt = new MemTable();

            for (int i = 0; i < 10000; i++) {
                var randomKey = ByteArray.Random(40);
                var randomValue = ByteArray.Random(256);

                mt.Add(randomKey, randomValue);
            }

            var timer = new Stopwatch();
            timer.Start();
            mt.WriteToSortedBlockTable("TestData\\WriteMemTableToSsTable", 0, 1);
            timer.Stop();
            
            Console.WriteLine("Wrote sorted table at a throughput of {0} MB/s", (double) mt.Size / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0) );
        }

        [Test]
        public void AddAndLookupItemsPersisted() {

            string path = Path.GetFullPath("TestData\\AddAndLookupItemsPersisted");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            JournalWriter jw = new JournalWriter("TestData\\AddAndLookupItemsPersisted", 523, false);

            List<KeyValuePair<ByteArray, ByteArray>> values = new List<KeyValuePair<ByteArray, ByteArray>>();

            for (int i = 0; i < 10000; i++) {
                var randomKey = ByteArray.Random(40);
                var randomValue = ByteArray.Random(256);

                values.Add(new KeyValuePair<ByteArray, ByteArray>(randomKey, randomValue));
                jw.Add(randomKey, randomValue);
            }
            jw.Close();

            MemTable mtl = new MemTable();
            mtl.ReadFromJournal("TestData\\AddAndLookupItemsPersisted", 523);

            ByteArray value;
            foreach (var pair in values) {
                Assert.IsTrue(mtl.Lookup(pair.Key, out value));
                Assert.AreEqual(pair.Value, value);
            }
            Assert.IsFalse(mtl.Lookup(ByteArray.Random(40), out value));

            Assert.AreEqual(10000 * (40 + 256), mtl.Size);
            Assert.IsTrue(mtl.Full);
        }

    }

}
