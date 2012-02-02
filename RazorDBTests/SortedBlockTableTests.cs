using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using RazorDB;
using System.IO;
using System.Diagnostics;

namespace RazorDBTests {

    [TestFixture]
    public class SortedBlockTableTests {


        [Test]
        public void ReadKeys() {

            var mt = new MemTable();
            for (int i = 0; i < 10000; i++) {
                var k0 = ByteArray.Random(40);
                var v0 = ByteArray.Random(200);
                mt.Add(k0, v0);
            }

            mt.WriteToSortedBlockTable("ReadKeys", 0, 10);

            var sbt = new SortedBlockTable("ReadKeys", 0, 10);

            var timer = new Stopwatch();
            timer.Start();
            Assert.AreEqual(10000, sbt.Enumerate().Count());
            timer.Stop();
            Console.WriteLine("Counted sorted table at a throughput of {0} MB/s", (double)mt.Size / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

            // Confirm that the items are sorted.
            ByteArray lastKey = new ByteArray(new byte[0]);
            timer.Reset();
            timer.Start();
            foreach (var pair in sbt.Enumerate()) {
                Assert.IsTrue(lastKey.CompareTo(pair.Key) < 0);
                lastKey = pair.Key;
            }
            timer.Stop();
            Console.WriteLine("Read & verify sorted table at a throughput of {0} MB/s", (double)mt.Size / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

            sbt.Close();

        }

        [Test]
        public void RandomizedLookups() {

            List<KeyValuePair<ByteArray, ByteArray>> items = new List<KeyValuePair<ByteArray, ByteArray>>();

            int num_items = 10000;
            var mt = new MemTable();
            for (int i = 0; i < num_items; i++) {
                var k0 = ByteArray.Random(40);
                var v0 = ByteArray.Random(200);
                mt.Add(k0, v0);

                items.Add( new KeyValuePair<ByteArray,ByteArray>(k0, v0) );
            }

            mt.WriteToSortedBlockTable("ReadKeys", 10, 10);

            var sbt = new SortedBlockTable("ReadKeys", 10, 10);

            var indexCache = new Cache();

            var timer = new Stopwatch();
            timer.Start();
            foreach (var pair in items) {
                ByteArray value;
                Assert.IsTrue(SortedBlockTable.Lookup("ReadKeys", 10, 10, indexCache, pair.Key, out value));
                Assert.AreEqual(pair.Value, value);
            }
            timer.Stop();

            ByteArray randomValue;
            Assert.IsFalse(SortedBlockTable.Lookup("ReadKeys", 10, 10, indexCache, ByteArray.Random(40), out randomValue));

            Console.WriteLine("Randomized read sbt table at a throughput of {0} MB/s (avg {1} ms per lookup)", (double)mt.Size / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0), (double)timer.Elapsed.TotalSeconds / (double) num_items);

            sbt.Close();

        }

    }
}
