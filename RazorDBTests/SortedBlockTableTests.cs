using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using RazorDB;
using System.IO;
using System.Diagnostics;
using System.Threading;

namespace RazorDBTests {

    [TestFixture]
    public class SortedBlockTableTests {

        [Test]
        public void ReadKeys() {

            string path = Path.GetFullPath("TestData\\ReadKeys");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            var mt = new MemTable();
            for (int i = 0; i < 10000; i++) {
                var k0 = ByteArray.Random(40);
                var v0 = ByteArray.Random(200);
                mt.Add(k0, v0);
            }

            mt.WriteToSortedBlockTable("TestData\\ReadKeys", 0, 10);

            var sbt = new SortedBlockTable("TestData\\ReadKeys", 0, 10);

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
        public void EnumerateFromKeys() {

            string path = Path.GetFullPath("TestData\\EnumerateFromKeys");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);
            
            List<KeyValuePair<ByteArray, ByteArray>> items = new List<KeyValuePair<ByteArray, ByteArray>>();

            int num_items = 10000;
            var mt = new MemTable();
            for (int i = 0; i < num_items; i++) {
                var k0 = ByteArray.Random(40);
                var v0 = ByteArray.Random(200);
                mt.Add(k0, v0);

                items.Add(new KeyValuePair<ByteArray, ByteArray>(k0, v0));
            }

            mt.WriteToSortedBlockTable("TestData\\EnumerateFromKeys", 10, 10);

            var sbt = new SortedBlockTable("TestData\\EnumerateFromKeys", 10, 10);

            try {
                var indexCache = new Cache();

                var timer = new Stopwatch();
                timer.Start();
                Assert.AreEqual(10000, sbt.EnumerateFromKey(indexCache, new ByteArray(new byte[] { 0 })).Count());
                timer.Stop();
                Console.WriteLine("Counted from beginning at a throughput of {0} MB/s", (double)mt.Size / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

                items = items.OrderBy( (a) => a.Key ).ToList();

                timer.Reset();
                timer.Start();
                Assert.AreEqual(5000, sbt.EnumerateFromKey(indexCache, items[5000].Key).Count());
                timer.Stop();
                Console.WriteLine("Counted from halfway at a throughput of {0} MB/s", (double)mt.Size / 2 / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

                Assert.AreEqual(0, sbt.EnumerateFromKey(indexCache, new ByteArray(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF })).Count());

            } finally {
                sbt.Close();
            }

        }

        [Test]
        public void RandomizedLookups() {


            string path = Path.GetFullPath("TestData\\RandomizedKeys");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            List<KeyValuePair<ByteArray, ByteArray>> items = new List<KeyValuePair<ByteArray, ByteArray>>();

            int num_items = 10000;
            var mt = new MemTable();
            for (int i = 0; i < num_items; i++) {
                var k0 = ByteArray.Random(40);
                var v0 = ByteArray.Random(200);
                mt.Add(k0, v0);

                items.Add( new KeyValuePair<ByteArray,ByteArray>(k0, v0) );
            }

            mt.WriteToSortedBlockTable("TestData\\RandomizedKeys", 10, 10);

            var sbt = new SortedBlockTable("TestData\\RandomizedKeys", 10, 10);

            var indexCache = new Cache();

            var timer = new Stopwatch();
            timer.Start();
            foreach (var pair in items) {
                ByteArray value;
                Assert.IsTrue(SortedBlockTable.Lookup("TestData\\RandomizedKeys", 10, 10, indexCache, pair.Key, out value));
                Assert.AreEqual(pair.Value, value);
            }
            timer.Stop();

            ByteArray randomValue;
            Assert.IsFalse(SortedBlockTable.Lookup("TestData\\RandomizedKeys", 10, 10, indexCache, ByteArray.Random(40), out randomValue));

            Console.WriteLine("Randomized read sbt table at a throughput of {0} MB/s (avg {1} ms per lookup)", (double)mt.Size / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0), (double)timer.Elapsed.TotalSeconds / (double) num_items);

            sbt.Close();

        }

        [Test]
        public void RandomizedThreadedLookups() {

            string path = Path.GetFullPath("TestData\\RandomizedThreadedLookups");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            List<KeyValuePair<ByteArray, ByteArray>> items = new List<KeyValuePair<ByteArray, ByteArray>>();

            int num_items = 10000;
            var mt = new MemTable();
            for (int i = 0; i < num_items; i++) {
                var k0 = ByteArray.Random(40);
                var v0 = ByteArray.Random(200);
                mt.Add(k0, v0);

                items.Add(new KeyValuePair<ByteArray, ByteArray>(k0, v0));
            }

            mt.WriteToSortedBlockTable("TestData\\RandomizedThreadedLookups", 10, 10);

            var sbt = new SortedBlockTable("TestData\\RandomizedThreadedLookups", 10, 10);

            var indexCache = new Cache();

            List<Thread> threads = new List<Thread>();
            for (int t = 0; t < 10; t++) {
                threads.Add(new Thread( (num) => {
                    for (int k=0; k < num_items / 10; k++) {
                        var pair = items[k * (int)num];
                        ByteArray value;
                        Assert.IsTrue(SortedBlockTable.Lookup("TestData\\RandomizedThreadedLookups", 10, 10, indexCache, pair.Key, out value));
                        Assert.AreEqual(pair.Value, value);
                    }
                }));
            }

            var timer = new Stopwatch();
            timer.Start();
            int threadNum = 0;
            threads.ForEach( (t) => t.Start(threadNum++) );
            threads.ForEach((t) => t.Join());
            timer.Stop();

            Console.WriteLine("Randomized (threaded) read sbt table at a throughput of {0} MB/s (avg {1} ms per lookup)", (double)mt.Size / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0), (double)timer.Elapsed.TotalSeconds / (double)num_items);

            sbt.Close();
        }
    }
}
