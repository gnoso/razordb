<<<<<<< HEAD
﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using RazorDB;
using System.IO;
using System.Diagnostics;
using System.Threading;

namespace RazorDBTests {
	[TestFixture] public class SortedBlockTableTests {
		[Test] public void TestFileOpenSpeed() {
            string path = Path.GetFullPath("TestData\\TestFileOpenSpeed");
            if (!Directory.Exists(path)) Directory.CreateDirectory(path);

            var mt = new MemTable();
            for (int i = 0; i < 10000; i++) {
                var k0 = Key.Random(40);
                var v0 = Value.Random(200);
                mt.Add(k0, v0);
            }

            mt.WriteToSortedBlockTable("TestData\\TestFileOpenSpeed", 0, 10);

            var openTables = new List<SortedBlockTable>();
            var cache = new RazorCache();
            var timer = new Stopwatch();
            timer.Start();
            for (int j = 0; j < 10000; j++) {
                var sbt = new SortedBlockTable(cache, "TestData\\TestFileOpenSpeed", 0, 10);
                openTables.Add(sbt);
            }
            timer.Stop();

            Console.WriteLine("Open block table {0} ms", timer.Elapsed.TotalMilliseconds / 10000);

            timer.Reset();
            timer.Start();
            for (int k = 0; k < 10000; k++) {
                openTables[k].Close();
            }
            timer.Stop();
            Console.WriteLine("Close block table {0} ms", timer.Elapsed.TotalMilliseconds / 10000);
        }

        [Test] public void ReadKeys() {
            string path = Path.GetFullPath("TestData\\ReadKeys");
            if (!Directory.Exists(path)) Directory.CreateDirectory(path);

            var mt = new MemTable();
            for (int i = 0; i < 10000; i++) {
                var k0 = Key.Random(40);
                var v0 = Value.Random(200);
                mt.Add(k0, v0);
            }

            mt.WriteToSortedBlockTable("TestData\\ReadKeys", 0, 10);

            var cache = new RazorCache();
            var sbt = new SortedBlockTable(cache, "TestData\\ReadKeys", 0, 10);

            var timer = new Stopwatch();
            timer.Start();
            Assert.AreEqual(10000, sbt.Enumerate().Count());
            timer.Stop();
            Console.WriteLine("Counted sorted table at a throughput of {0} MB/s", (double)mt.Size / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

            // Confirm that the items are sorted.
            Key lastKey = Key.Empty;
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

        [Test] public void EnumerateFromKeys() {
            string path = Path.GetFullPath("TestData\\EnumerateFromKeys");
            if (!Directory.Exists(path)) Directory.CreateDirectory(path);

            List<KeyValuePair<Key, Value>> items = new List<KeyValuePair<Key, Value>>();

            int num_items = 10000;
            var mt = new MemTable();
            for (int i = 0; i < num_items; i++) {
                var k0 = Key.Random(40);
                var v0 = Value.Random(200);
                mt.Add(k0, v0);

                items.Add(new KeyValuePair<Key, Value>(k0, v0));
            }

            mt.WriteToSortedBlockTable("TestData\\EnumerateFromKeys", 10, 10);

            var cache = new RazorCache();
            var sbt = new SortedBlockTable(cache, "TestData\\EnumerateFromKeys", 10, 10);

            try {
                var indexCache = new RazorCache();

                var timer = new Stopwatch();
                timer.Start();
                Assert.AreEqual(10000, sbt.EnumerateFromKey(indexCache, new Key(new byte[] { 0 },0)).Count());
                timer.Stop();
                Console.WriteLine("Counted from beginning at a throughput of {0} MB/s", (double)mt.Size / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

                items = items.OrderBy( (a) => a.Key ).ToList();

                timer.Reset();
                timer.Start();
                Assert.AreEqual(5000, sbt.EnumerateFromKey(indexCache, items[5000].Key).Count());
                timer.Stop();
                Console.WriteLine("Counted from halfway at a throughput of {0} MB/s", (double)mt.Size / 2 / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

                Assert.AreEqual(0, sbt.EnumerateFromKey(indexCache, new Key(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF }, 0xFF)).Count());
            } finally {
                sbt.Close();
            }
        }

        [Test] public void RandomizedLookups() {
            string path = Path.GetFullPath("TestData\\RandomizedKeys");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            List<KeyValuePair<Key, Value>> items = new List<KeyValuePair<Key, Value>>();

            int num_items = 10000;
            var mt = new MemTable();
            for (int i = 0; i < num_items; i++) {
                var k0 = Key.Random(40);
                var v0 = Value.Random(200);
                mt.Add(k0, v0);

                items.Add(new KeyValuePair<Key, Value>(k0, v0));
            }

            mt.WriteToSortedBlockTable("TestData\\RandomizedKeys", 10, 10);

            var cache = new RazorCache();
			var sbt = new SortedBlockTable(cache, "TestData\\RandomizedKeys", 10, 10);
            var indexCache = new RazorCache();
            var timer = new Stopwatch();
            timer.Start();
            foreach (var pair in items) {
                Value value;
                Assert.IsTrue(SortedBlockTable.Lookup("TestData\\RandomizedKeys", 10, 10, indexCache, pair.Key, out value));
                Assert.AreEqual(pair.Value, value);
            }
            timer.Stop();

            Value randomValue;
            Assert.IsFalse(SortedBlockTable.Lookup("TestData\\RandomizedKeys", 10, 10, indexCache, Key.Random(40), out randomValue));

            Console.WriteLine("Randomized read sbt table at a throughput of {0} MB/s (avg {1} ms per lookup)", (double)mt.Size / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0), (double)timer.Elapsed.TotalSeconds / (double) num_items);

            sbt.Close();
        }

        [Test] public void RandomizedThreadedLookups() {
            string path = Path.GetFullPath("TestData\\RandomizedThreadedLookups");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            List<KeyValuePair<Key, Value>> items = new List<KeyValuePair<Key, Value>>();

            int num_items = 10000;
            var mt = new MemTable();
            for (int i = 0; i < num_items; i++) {
                var k0 = Key.Random(40);
                var v0 = Value.Random(200);
                mt.Add(k0, v0);

                items.Add(new KeyValuePair<Key, Value>(k0, v0));
            }

            mt.WriteToSortedBlockTable("TestData\\RandomizedThreadedLookups", 10, 10);

            var cache = new RazorCache();
            var sbt = new SortedBlockTable(cache,"TestData\\RandomizedThreadedLookups", 10, 10);
            var indexCache = new RazorCache();

            List<Thread> threads = new List<Thread>();
            for (int t = 0; t < 10; t++) {
                threads.Add(new Thread( (num) => {
                    for (int k=0; k < num_items / 10; k++) {
                        var pair = items[k * (int)num];
                        Value value;
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
=======
﻿/* 
Copyright 2012 Gnoso Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
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
        public void TestFileOpenSpeed() {
            
            string path = Path.GetFullPath("TestData\\TestFileOpenSpeed");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            var mt = new MemTable();
            for (int i = 0; i < 10000; i++) {
                var k0 = KeyEx.Random(40);
                var v0 = Value.Random(200);
                mt.Add(k0, v0);
            }

            mt.WriteToSortedBlockTable("TestData\\TestFileOpenSpeed", 0, 10);

            var openTables = new List<SortedBlockTable>();

            var cache = new RazorCache();
            var timer = new Stopwatch();
            timer.Start();
            for (int j = 0; j < 10000; j++) {
                var sbt = new SortedBlockTable(cache, "TestData\\TestFileOpenSpeed", 0, 10);
                openTables.Add(sbt);
            }
            timer.Stop();

            Console.WriteLine("Open block table {0} ms", timer.Elapsed.TotalMilliseconds / 10000);

            timer.Reset();
            timer.Start();
            for (int k = 0; k < 10000; k++) {
                openTables[k].Close();
            }
            timer.Stop();

            Console.WriteLine("Close block table {0} ms", timer.Elapsed.TotalMilliseconds / 10000);

        }

        [Test]
        public void V1ReadKeys() {

            string path = Path.GetFullPath("TestData\\V1ReadKeys");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            var mt = new MemTable();
            for (int i = 0; i < 10000; i++) {
                var k0 = KeyEx.Random(40);
                var v0 = Value.Random(200);
                mt.Add(k0, v0);
            }

            mt.WriteToSortedBlockTable("TestData\\V1ReadKeys", 0, 10, SortedBlockTableFormat.Razor01);

            var cache = new RazorCache();
            var sbt = new SortedBlockTable(cache, "TestData\\V1ReadKeys", 0, 10);

            var timer = new Stopwatch();
            timer.Start();
            Assert.AreEqual(10000, sbt.Enumerate().Count());
            timer.Stop();
            Console.WriteLine("Counted sorted table at a throughput of {0} MB/s", (double)mt.Size / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

            // Confirm that the items are sorted.
            KeyEx lastKey = KeyEx.Empty;
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
        public void V1EnumerateFromKeys() {

            string path = Path.GetFullPath("TestData\\V1EnumerateFromKeys");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            List<KeyValuePair<KeyEx, Value>> items = new List<KeyValuePair<KeyEx, Value>>();

            int num_items = 10000;
            var mt = new MemTable();
            for (int i = 0; i < num_items; i++) {
                var k0 = KeyEx.Random(40);
                var v0 = Value.Random(200);
                mt.Add(k0, v0);

                items.Add(new KeyValuePair<KeyEx, Value>(k0, v0));
            }

            mt.WriteToSortedBlockTable("TestData\\V1EnumerateFromKeys", 10, 10, SortedBlockTableFormat.Razor01);

            var cache = new RazorCache();
            var sbt = new SortedBlockTable(cache, "TestData\\V1EnumerateFromKeys", 10, 10);

            try {
                var indexCache = new RazorCache();

                var timer = new Stopwatch();
                timer.Start();
                Assert.AreEqual(10000, sbt.EnumerateFromKey(indexCache, new KeyEx(new byte[] { 0 }, 0)).Count());
                timer.Stop();
                Console.WriteLine("Counted from beginning at a throughput of {0} MB/s", (double)mt.Size / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

                items = items.OrderBy((a) => a.Key).ToList();

                timer.Reset();
                timer.Start();
                Assert.AreEqual(5000, sbt.EnumerateFromKey(indexCache, items[5000].Key).Count());
                timer.Stop();
                Console.WriteLine("Counted from halfway at a throughput of {0} MB/s", (double)mt.Size / 2 / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

                Assert.AreEqual(0, sbt.EnumerateFromKey(indexCache, new KeyEx(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF }, 0xFF)).Count());

            } finally {
                sbt.Close();
            }

        }

        [Test]
        public void DefaultReadKeys() {

            string path = Path.GetFullPath("TestData\\DefaultReadKeys");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            var mt = new MemTable();
            for (int i = 0; i < 10000; i++) {
                var k0 = KeyEx.Random(40);
                var v0 = Value.Random(200);
                mt.Add(k0, v0);
            }

            mt.WriteToSortedBlockTable("TestData\\DefaultReadKeys", 0, 10);

            var cache = new RazorCache();
            var sbt = new SortedBlockTable(cache, "TestData\\DefaultReadKeys", 0, 10);

            var timer = new Stopwatch();
            timer.Start();
            Assert.AreEqual(10000, sbt.Enumerate().Count());
            timer.Stop();
            Console.WriteLine("Counted sorted table at a throughput of {0} MB/s", (double)mt.Size / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

            // Confirm that the items are sorted.
            KeyEx lastKey = KeyEx.Empty;
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
        public void DefaultCompressibleEnumerateFromKeys() {

            string path = Path.GetFullPath("TestData\\DefaultCompressibleEnumerateFromKeys");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            List<KeyValuePair<KeyEx, Value>> items = new List<KeyValuePair<KeyEx, Value>>();
            var v0 = Value.Random(200);

            int num_items = 10000;
            var mt = new MemTable();
            for (int i = 0; i < num_items; i++) {
                var k0 = KeyEx.Random(40);
                mt.Add(k0, v0);

                items.Add(new KeyValuePair<KeyEx, Value>(k0, v0));
            }

            mt.WriteToSortedBlockTable("TestData\\DefaultCompressibleEnumerateFromKeys", 10, 10);

            var cache = new RazorCache();
            var sbt = new SortedBlockTable(cache, "TestData\\DefaultCompressibleEnumerateFromKeys", 10, 10);

            try {
                var indexCache = new RazorCache();

                var timer = new Stopwatch();
                timer.Start();
                Assert.AreEqual(10000, sbt.EnumerateFromKey(indexCache, new KeyEx(new byte[] { 0 }, 0)).Count());
                timer.Stop();
                Console.WriteLine("Counted from beginning at a throughput of {0} MB/s", (double)mt.Size / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

                timer.Reset();
                timer.Start();
                Assert.AreEqual(10000, sbt.EnumerateFromKey(indexCache, new KeyEx(new byte[] { 0 }, 0)).Count());
                timer.Stop();
                Console.WriteLine("Counted from beginning again at a throughput of {0} MB/s", (double)mt.Size / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

                items = items.OrderBy((a) => a.Key).ToList();

                timer.Reset();
                timer.Start();
                Assert.AreEqual(5000, sbt.EnumerateFromKey(indexCache, items[5000].Key).Count());
                timer.Stop();
                Console.WriteLine("Counted from halfway at a throughput of {0} MB/s", (double)mt.Size / 2 / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

                Assert.AreEqual(0, sbt.EnumerateFromKey(indexCache, new KeyEx(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF }, 0xFF)).Count());

            } finally {
                sbt.Close();
            }

        }

        [Test]
        public void DefaultCompressibleNoCacheEnumerateFromKeys() {

            string path = Path.GetFullPath("TestData\\DefaultCompressibleNoCacheEnumerateFromKeys");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            List<KeyValuePair<KeyEx, Value>> items = new List<KeyValuePair<KeyEx, Value>>();
            var v0 = Value.Random(100);

            int num_items = 15000;
            var mt = new MemTable();
            for (int i = 0; i < num_items; i++) {
                var k0 = KeyEx.Random(40);
                mt.Add(k0, v0);

                items.Add(new KeyValuePair<KeyEx, Value>(k0, v0));
            }

            double scanThroughput = 0;
            double writeThroughput = 0;
            int ct = 20;

            for (int i = 0; i < ct; i++) {
                var timer = new Stopwatch();
                timer.Start();
                mt.WriteToSortedBlockTable("TestData\\DefaultCompressibleNoCacheEnumerateFromKeys", 2, i);
                timer.Stop();
                double wtp = (double)mt.Size / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0);
                timer.Reset();

                timer.Start();
                SortedBlockTable sbt = null;
                try {
                    sbt = new SortedBlockTable(null, "TestData\\DefaultCompressibleNoCacheEnumerateFromKeys", 2, i);
                    Assert.AreEqual(15000, sbt.EnumerateFromKey(new RazorCache(), new KeyEx(new byte[] { 0 }, 0)).Count());
                } finally {
                    sbt.Close();
                }
                timer.Stop();
                double tp = (double)mt.Size / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0);
                scanThroughput += tp;
                writeThroughput += wtp;
                Console.WriteLine("{0}: Write throughput: {1} MB/s Scan throughput: {2} MB/s", i, wtp, tp);
            }

            Console.WriteLine("Average write throughput: {0} MB/s Scan throughput: {1} MB/s", writeThroughput / ct, scanThroughput / ct);
        }

        [Test]
        public void DefaultCompressibleLongerEnumerateFromKeys() {

            string path = Path.GetFullPath("TestData\\DefaultCompressibleEnumerateFromKeys");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            List<KeyValuePair<KeyEx, Value>> items = new List<KeyValuePair<KeyEx, Value>>();
            var v0 = Value.Random(200);

            int num_items = 100000;
            var mt = new MemTable();
            for (int i = 0; i < num_items; i++) {
                var k0 = KeyEx.Random(40);
                mt.Add(k0, v0);

                items.Add(new KeyValuePair<KeyEx, Value>(k0, v0));
            }

            mt.WriteToSortedBlockTable("TestData\\DefaultCompressibleEnumerateFromKeys", 10, 10);

            var cache = new RazorCache();
            var sbt = new SortedBlockTable(cache, "TestData\\DefaultCompressibleEnumerateFromKeys", 10, 10);

            try {
                var indexCache = new RazorCache();

                var timer = new Stopwatch();
                timer.Start();
                Assert.AreEqual(100000, sbt.EnumerateFromKey(indexCache, new KeyEx(new byte[] { 0 }, 0)).Count());
                timer.Stop();
                Console.WriteLine("Counted from beginning at a throughput of {0} MB/s", (double)mt.Size / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

                items = items.OrderBy((a) => a.Key).ToList();

                timer.Reset();
                timer.Start();
                Assert.AreEqual(50000, sbt.EnumerateFromKey(indexCache, items[50000].Key).Count());
                timer.Stop();
                Console.WriteLine("Counted from halfway at a throughput of {0} MB/s", (double)mt.Size / 2 / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

                Assert.AreEqual(0, sbt.EnumerateFromKey(indexCache, new KeyEx(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF }, 0xFF)).Count());

            } finally {
                sbt.Close();
            }

        }

        [Test]
        public void DefaultEnumerateFromKeys() {

            string path = Path.GetFullPath("TestData\\DefaultEnumerateFromKeys");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            List<KeyValuePair<KeyEx, Value>> items = new List<KeyValuePair<KeyEx, Value>>();

            int num_items = 10000;
            var mt = new MemTable();
            for (int i = 0; i < num_items; i++) {
                var k0 = KeyEx.Random(40);
                var v0 = Value.Random(200);
                mt.Add(k0, v0);

                items.Add(new KeyValuePair<KeyEx, Value>(k0, v0));
            }

            mt.WriteToSortedBlockTable("TestData\\DefaultEnumerateFromKeys", 10, 10);

            var cache = new RazorCache();
            var sbt = new SortedBlockTable(cache, "TestData\\DefaultEnumerateFromKeys", 10, 10);

            try {
                var indexCache = new RazorCache();

                var timer = new Stopwatch();
                timer.Start();
                Assert.AreEqual(10000, sbt.EnumerateFromKey(indexCache, new KeyEx(new byte[] { 0 }, 0)).Count());
                timer.Stop();
                Console.WriteLine("Counted from beginning at a throughput of {0} MB/s", (double)mt.Size / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

                items = items.OrderBy( (a) => a.Key ).ToList();

                timer.Reset();
                timer.Start();
                Assert.AreEqual(5000, sbt.EnumerateFromKey(indexCache, items[5000].Key).Count());
                timer.Stop();
                Console.WriteLine("Counted from halfway at a throughput of {0} MB/s", (double)mt.Size / 2 / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

                Assert.AreEqual(0, sbt.EnumerateFromKey(indexCache, new KeyEx(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF }, 0xFF)).Count());

            } finally {
                sbt.Close();
            }

        }

        [Test]
        public void DefaultRandomizedLookups() {


            string path = Path.GetFullPath("TestData\\DefaultRandomizedLookups");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            List<KeyValuePair<KeyEx, Value>> items = new List<KeyValuePair<KeyEx, Value>>();

            int num_items = 10000;
            var mt = new MemTable();
            for (int i = 0; i < num_items; i++) {
                var k0 = KeyEx.Random(40);
                var v0 = Value.Random(200);
                mt.Add(k0, v0);

                items.Add(new KeyValuePair<KeyEx, Value>(k0, v0));
            }

            mt.WriteToSortedBlockTable("TestData\\DefaultRandomizedLookups", 10, 10);

            var cache = new RazorCache();
            var sbt = new SortedBlockTable(cache, "TestData\\DefaultRandomizedLookups", 10, 10);

            var indexCache = new RazorCache();

            var timer = new Stopwatch();
            timer.Start();
            foreach (var pair in items) {
                Value value;
                Assert.IsTrue(SortedBlockTable.Lookup("TestData\\DefaultRandomizedLookups", 10, 10, indexCache, pair.Key, out value));
                Assert.AreEqual(pair.Value, value);
            }
            timer.Stop();

            Value randomValue;
            Assert.IsFalse(SortedBlockTable.Lookup("TestData\\DefaultRandomizedLookups", 10, 10, indexCache, KeyEx.Random(40), out randomValue));

            Console.WriteLine("Randomized read sbt table at a throughput of {0} MB/s (avg {1} ms per lookup)", (double)mt.Size / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0), (double)timer.Elapsed.TotalSeconds / (double) num_items);

            sbt.Close();

        }

        [Test]
        public void DefaultRandomizedThreadedLookups() {

            string path = Path.GetFullPath("TestData\\DefaultRandomizedThreadedLookups");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            List<KeyValuePair<KeyEx, Value>> items = new List<KeyValuePair<KeyEx, Value>>();

            int num_items = 10000;
            var mt = new MemTable();
            for (int i = 0; i < num_items; i++) {
                var k0 = KeyEx.Random(40);
                var v0 = Value.Random(200);
                mt.Add(k0, v0);

                items.Add(new KeyValuePair<KeyEx, Value>(k0, v0));
            }

            mt.WriteToSortedBlockTable("TestData\\DefaultRandomizedThreadedLookups", 10, 10);

            var cache = new RazorCache();
            var sbt = new SortedBlockTable(cache, "TestData\\DefaultRandomizedThreadedLookups", 10, 10);

            var indexCache = new RazorCache();

            List<Thread> threads = new List<Thread>();
            for (int t = 0; t < 10; t++) {
                threads.Add(new Thread( (num) => {
                    for (int k=0; k < num_items / 10; k++) {
                        var pair = items[k * (int)num];
                        Value value;
                        Assert.IsTrue(SortedBlockTable.Lookup("TestData\\DefaultRandomizedThreadedLookups", 10, 10, indexCache, pair.Key, out value));
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

        [Test]
        public void V1RandomizedLookups() {


            string path = Path.GetFullPath("TestData\\V1RandomizedLookups");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            List<KeyValuePair<KeyEx, Value>> items = new List<KeyValuePair<KeyEx, Value>>();

            int num_items = 10000;
            var mt = new MemTable();
            for (int i = 0; i < num_items; i++) {
                var k0 = KeyEx.Random(40);
                var v0 = Value.Random(200);
                mt.Add(k0, v0);

                items.Add(new KeyValuePair<KeyEx, Value>(k0, v0));
            }

            mt.WriteToSortedBlockTable("TestData\\V1RandomizedLookups", 10, 10, SortedBlockTableFormat.Razor01);

            var cache = new RazorCache();
            var sbt = new SortedBlockTable(cache, "TestData\\V1RandomizedLookups", 10, 10);

            var indexCache = new RazorCache();

            var timer = new Stopwatch();
            timer.Start();
            foreach (var pair in items) {
                Value value;
                Assert.IsTrue(SortedBlockTable.Lookup("TestData\\V1RandomizedLookups", 10, 10, indexCache, pair.Key, out value));
                Assert.AreEqual(pair.Value, value);
            }
            timer.Stop();

            Value randomValue;
            Assert.IsFalse(SortedBlockTable.Lookup("TestData\\V1RandomizedLookups", 10, 10, indexCache, KeyEx.Random(40), out randomValue));

            Console.WriteLine("Randomized read sbt table at a throughput of {0} MB/s (avg {1} ms per lookup)", (double)mt.Size / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0), (double)timer.Elapsed.TotalSeconds / (double)num_items);

            sbt.Close();

        }

        [Test]
        public void V1RandomizedThreadedLookups() {

            string path = Path.GetFullPath("TestData\\V1RandomizedThreadedLookups");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            List<KeyValuePair<KeyEx, Value>> items = new List<KeyValuePair<KeyEx, Value>>();

            int num_items = 10000;
            var mt = new MemTable();
            for (int i = 0; i < num_items; i++) {
                var k0 = KeyEx.Random(40);
                var v0 = Value.Random(200);
                mt.Add(k0, v0);

                items.Add(new KeyValuePair<KeyEx, Value>(k0, v0));
            }

            mt.WriteToSortedBlockTable("TestData\\V1RandomizedThreadedLookups", 10, 10, SortedBlockTableFormat.Razor01);

            var cache = new RazorCache();
            var sbt = new SortedBlockTable(cache, "TestData\\V1RandomizedThreadedLookups", 10, 10);

            var indexCache = new RazorCache();

            List<Thread> threads = new List<Thread>();
            for (int t = 0; t < 10; t++) {
                threads.Add(new Thread((num) => {
                    for (int k = 0; k < num_items / 10; k++) {
                        var pair = items[k * (int)num];
                        Value value;
                        Assert.IsTrue(SortedBlockTable.Lookup("TestData\\V1RandomizedThreadedLookups", 10, 10, indexCache, pair.Key, out value));
                        Assert.AreEqual(pair.Value, value);
                    }
                }));
            }

            var timer = new Stopwatch();
            timer.Start();
            int threadNum = 0;
            threads.ForEach((t) => t.Start(threadNum++));
            threads.ForEach((t) => t.Join());
            timer.Stop();

            Console.WriteLine("Randomized (threaded) read sbt table at a throughput of {0} MB/s (avg {1} ms per lookup)", (double)mt.Size / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0), (double)timer.Elapsed.TotalSeconds / (double)num_items);

            sbt.Close();
        }
    }
}
>>>>>>> 2113174cc3c1eb5faf9c5d41ff794fede514e9ac
