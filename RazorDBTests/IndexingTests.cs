/*
Copyright 2012, 2013 Gnoso Inc.

This software is licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except for what is in compliance with the License.

You may obtain a copy of this license at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either expressed or implied.

See the License for the specific language governing permissions and limitations.
*/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using RazorDB;
using System.Diagnostics;
using System.IO;

namespace RazorDBTests {

    [TestFixture]
    public class IndexingTests {

        [Test]
        public void TruncateTest() {

            string path = Path.GetFullPath("TestData\\TruncateTest");
            using (var db = new KeyValueStore(path)) {
                var indexed = new SortedDictionary<string, byte[]>();
                for (int i = 0; i < 15000; i++) {
                    indexed["RandomIndex"] = ByteArray.Random(20).InternalBytes;
                    var randKey = ByteArray.Random(40);
                    var randValue = ByteArray.Random(256);
                    db.Set(randKey.InternalBytes, randValue.InternalBytes, indexed);
                }
            }
            using (var db = new KeyValueStore(path)) {
                db.Truncate();
            }
            var files = Directory.GetFiles(path, "*.*", SearchOption.AllDirectories);
            Assert.AreEqual(new string[] { Path.GetFullPath(Path.Combine(path, "0.jf")) }, files);
            var dirs = Directory.GetDirectories(path, "*.*", SearchOption.AllDirectories);
            Assert.AreEqual(new string[0], dirs);
        }

        [Test]
        public void AddObjectsAndLookup() {

            string path = Path.GetFullPath("TestData\\AddObjectsAndLookup");

            using (var db = new KeyValueStore(path)) {
                db.Truncate();

                var indexed = new SortedDictionary<string, byte[]>();
                indexed["NumberType"] = Encoding.UTF8.GetBytes("Fib");
                db.Set(BitConverter.GetBytes(112), Encoding.UTF8.GetBytes("112"), indexed);
                db.Set(BitConverter.GetBytes(1123), Encoding.UTF8.GetBytes("1123"), indexed);
                db.Set(BitConverter.GetBytes(11235), Encoding.UTF8.GetBytes("11235"), indexed);
                db.Set(BitConverter.GetBytes(112358), Encoding.UTF8.GetBytes("112358"), indexed);

                indexed["NumberType"] = Encoding.UTF8.GetBytes("Seq");
                db.Set(BitConverter.GetBytes(1), Encoding.UTF8.GetBytes("1"), indexed);
                db.Set(BitConverter.GetBytes(2), Encoding.UTF8.GetBytes("2"), indexed);
                db.Set(BitConverter.GetBytes(3), Encoding.UTF8.GetBytes("3"), indexed);
                db.Set(BitConverter.GetBytes(4), Encoding.UTF8.GetBytes("4"), indexed);

                indexed["NumberType"] = Encoding.UTF8.GetBytes("Zero");
                db.Set(BitConverter.GetBytes(0), Encoding.UTF8.GetBytes("0"), indexed);
            }
            using (var db = new KeyValueStore(path)) {
                var zeros = db.Find("NumberType", Encoding.UTF8.GetBytes("Zero")).ToList();
                Assert.AreEqual(1, zeros.Count());
                Assert.AreEqual("0", Encoding.UTF8.GetString(zeros[0].Value));

                var seqs = db.Find("NumberType", Encoding.UTF8.GetBytes("Seq")).ToList();
                Assert.AreEqual(4, seqs.Count());
                Assert.AreEqual("1", Encoding.UTF8.GetString(seqs[0].Value));
                Assert.AreEqual("2", Encoding.UTF8.GetString(seqs[1].Value));
                Assert.AreEqual("3", Encoding.UTF8.GetString(seqs[2].Value));
                Assert.AreEqual("4", Encoding.UTF8.GetString(seqs[3].Value));

                var fib = db.Find("NumberType", Encoding.UTF8.GetBytes("Fib")).ToList();
                Assert.AreEqual(4, seqs.Count());
                Assert.AreEqual("1123", Encoding.UTF8.GetString(fib[0].Value));
                Assert.AreEqual("112", Encoding.UTF8.GetString(fib[1].Value));
                Assert.AreEqual("11235", Encoding.UTF8.GetString(fib[2].Value));
                Assert.AreEqual("112358", Encoding.UTF8.GetString(fib[3].Value));

                var non = db.Find("NoIndex", new byte[] { 23 }).ToList();
                Assert.AreEqual(0, non.Count());
                non = db.Find("NumberType", Encoding.UTF8.GetBytes("Unfound")).ToList();
                Assert.AreEqual(0, non.Count());
            }
        }

        [Test]
        public void FindStartsWith() {

            string path = Path.GetFullPath("TestData\\FindStartsWith");

            using (var db = new KeyValueStore(path)) {
                db.Truncate();

                var indexed = new SortedDictionary<string, byte[]>();
                indexed["Bytes"] = Encoding.UTF8.GetBytes("112");
                db.Set(BitConverter.GetBytes(112), Encoding.UTF8.GetBytes("112"), indexed);
                indexed["Bytes"] = Encoding.UTF8.GetBytes("1123");
                db.Set(BitConverter.GetBytes(1123), Encoding.UTF8.GetBytes("1123"), indexed);
                indexed["Bytes"] = Encoding.UTF8.GetBytes("11235");
                db.Set(BitConverter.GetBytes(11235), Encoding.UTF8.GetBytes("11235"), indexed);
                indexed["Bytes"] = Encoding.UTF8.GetBytes("112358");
                db.Set(BitConverter.GetBytes(112358), Encoding.UTF8.GetBytes("112358"), indexed);

            }
            using (var db = new KeyValueStore(path)) {
                var exact = db.Find("Bytes", Encoding.UTF8.GetBytes("1123")).ToList();
                Assert.AreEqual(1, exact.Count());
                Assert.AreEqual("1123", Encoding.UTF8.GetString(exact[0].Value));

                var startsWith = db.FindStartsWith("Bytes", Encoding.UTF8.GetBytes("1123")).ToList();
                Assert.AreEqual(3, startsWith.Count());
                Assert.AreEqual("112358", Encoding.UTF8.GetString(startsWith[0].Value));
                Assert.AreEqual("11235", Encoding.UTF8.GetString(startsWith[1].Value));
                Assert.AreEqual("1123", Encoding.UTF8.GetString(startsWith[2].Value));
            }
        }


        [Test]
        public void AddObjectsAndLookupWhileMerging() {

            string path = Path.GetFullPath("TestData\\AddObjectsAndLookupWhileMerging");
            var timer = new Stopwatch();

            using (var db = new KeyValueStore(path)) {
                db.Truncate();
                int totalSize = 0;
                db.Manifest.Logger = msg => Console.WriteLine(msg);

                var indexed = new SortedDictionary<string, byte[]>();
                int num_items = 1000000;
                timer.Start();
                for (int i = 0; i < num_items; i++) {
                    indexed["Mod"] = BitConverter.GetBytes(i % 100);
                    db.Set(BitConverter.GetBytes(i), BitConverter.GetBytes(i * 1000), indexed);
                    totalSize += 8 + 4;
                }
                timer.Stop();

                Console.WriteLine("Wrote data (with indexing) at a throughput of {0} MB/s", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

                timer.Reset();
                timer.Start();
                var ctModZeros = db.Find("Mod", BitConverter.GetBytes((int)0)).Count();
                timer.Stop();
                Assert.AreEqual(10000, ctModZeros);
                Console.WriteLine("Scanned index at a throughput of {0} items/s", (double)ctModZeros / timer.Elapsed.TotalSeconds);
            }
        }

        [Test]
        public void AddObjectsAndLookupWithMixedCase() {

            string path = Path.GetFullPath("TestData\\AddObjectsAndLookupWithMixedCase");
            if (Directory.Exists(path))
                Directory.Delete(path, true);
            Directory.CreateDirectory(path);

            var timer = new Stopwatch();

            using (var db = new KeyValueStore(path)) {
                db.Truncate();
                int totalSize = 0;
                db.Manifest.Logger = msg => Console.WriteLine(msg);

                var indexed = new SortedDictionary<string, byte[]>();
                int num_items = 1000000;
                timer.Start();
                for (int i = 0; i < num_items; i++) {
                    indexed["Mod"] = BitConverter.GetBytes(i % 100);
                    db.Set(BitConverter.GetBytes(i), BitConverter.GetBytes(i * 1000), indexed);
                    totalSize += 8 + 4;
                }
                timer.Stop();

                Console.WriteLine("Wrote data (with indexing) at a throughput of {0} MB/s", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

                timer.Reset();
                timer.Start();
                var ctModZeros = db.Find("mod", BitConverter.GetBytes((int)0)).Count();
                timer.Stop();
                Assert.AreEqual(10000, ctModZeros);
                Console.WriteLine("Scanned index at a throughput of {0} items/s", (double)ctModZeros / timer.Elapsed.TotalSeconds);
            }
        }

        [Test]
        public void RemoveDeletedValuesFromIndex() {

            string path = Path.GetFullPath("TestData\\RemoveDeletedValuesFromIndex");
            var timer = new Stopwatch();

            using (var db = new KeyValueStore(path)) {
                db.Truncate();
                int totalSize = 0;
                db.Manifest.Logger = msg => Console.WriteLine(msg);

                var indexed = new SortedDictionary<string, byte[]>();
                int num_items = 1000;
                timer.Start();
                for (int i = 0; i < num_items; i++) {
                    indexed["Mod"] = BitConverter.GetBytes(i % 100);
                    db.Set(BitConverter.GetBytes(i), BitConverter.GetBytes(i), indexed);
                    totalSize += 8 + 4;
                }
                timer.Stop();

                Console.WriteLine("Wrote data (with indexing) at a throughput of {0} MB/s", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

                timer.Reset();
                timer.Start();
                var ctModZeros = db.Find("Mod", BitConverter.GetBytes((int)0)).Count();
                timer.Stop();
                Assert.AreEqual(10, ctModZeros);
                Console.WriteLine("Scanned index at a throughput of {0} items/s", (double)ctModZeros / timer.Elapsed.TotalSeconds);

            }

            // Open the index directly and see if the data is there
            using (var db = new KeyValueStore(Path.Combine(path, "Mod"))) {
                int num_vals = db.EnumerateFromKey(BitConverter.GetBytes((int)0)).Count(pair => pair.Key.Take(4).All(b => b == 0));

                Assert.AreEqual(10, num_vals);
            }

            // Re-open the main key-value store and delete the value at 30
            using (var db = new KeyValueStore(path)) {
                db.Delete(BitConverter.GetBytes(200));

                // Clean the data from the index
                db.RemoveFromIndex(BitConverter.GetBytes(200), new Dictionary<string, byte[]> { { "Mod", BitConverter.GetBytes(200 % 100) } });
            }

            // Open the index again directly and confirm that the lookup key is gone now as well
            using (var db = new KeyValueStore(Path.Combine(path, "Mod"))) {
                int num_vals = db.EnumerateFromKey(BitConverter.GetBytes((int)0)).Count(pair => pair.Key.Take(4).All(b => b == 0));

                Assert.AreEqual(9, num_vals);
            }

        }

        [Test]
        public void RemoveUpdatedValuesFromIndex() {

            string path = Path.GetFullPath("TestData\\RemoveUpdatedValuesFromIndex");
            var timer = new Stopwatch();

            using (var db = new KeyValueStore(path)) {
                db.Truncate();
                int totalSize = 0;
                db.Manifest.Logger = msg => Console.WriteLine(msg);

                var indexed = new SortedDictionary<string, byte[]>();
                int num_items = 1000;
                timer.Start();
                for (int i = 0; i < num_items; i++) {
                    indexed["Mod"] = BitConverter.GetBytes(i % 100);
                    db.Set(BitConverter.GetBytes(i), BitConverter.GetBytes(i), indexed);
                    totalSize += 8 + 4;
                }
                timer.Stop();

                Console.WriteLine("Wrote data (with indexing) at a throughput of {0} MB/s", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

                timer.Reset();
                timer.Start();
                var ctModZeros = db.Find("Mod", BitConverter.GetBytes((int)0)).Count();
                timer.Stop();
                Assert.AreEqual(10, ctModZeros);
                Console.WriteLine("Scanned index at a throughput of {0} items/s", (double)ctModZeros / timer.Elapsed.TotalSeconds);

            }

            // Open the index directly and see if the data is there
            using (var db = new KeyValueStore(Path.Combine(path, "Mod"))) {
                int num_vals = db.EnumerateFromKey(BitConverter.GetBytes((int)0)).Count(pair => pair.Key.Take(4).All(b => b == 0));

                Assert.AreEqual(10, num_vals);
            }

            // Re-open the main key-value store and delete the value at 30
            using (var db = new KeyValueStore(path)) {
                db.Set(BitConverter.GetBytes(200), BitConverter.GetBytes(20));

                // Clean the data from the index
                db.RemoveFromIndex(BitConverter.GetBytes(200), new Dictionary<string, byte[]> { { "Mod", BitConverter.GetBytes(200 % 100) } });
            }

            // Open the index again directly and confirm that the lookup key is gone now as well
            using (var db = new KeyValueStore(Path.Combine(path, "Mod"))) {
                int num_vals = db.EnumerateFromKey(BitConverter.GetBytes((int)0)).Count(pair => pair.Key.Take(4).All(b => b == 0));

                Assert.AreEqual(9, num_vals);
            }

        }

        [Test]
        public void RemoveUpdatedValuesFromIndex2() {

            string path = Path.GetFullPath("TestData\\RemoveUpdatedValuesFromIndex2");
            var timer = new Stopwatch();

            using (var db = new KeyValueStore(path)) {
                db.Truncate();
                int totalSize = 0;
                db.Manifest.Logger = msg => Console.WriteLine(msg);

                var indexed = new SortedDictionary<string, byte[]>();
                int num_items = 1000;
                timer.Start();
                for (int i = 0; i < num_items; i++) {
                    indexed["Mod"] = BitConverter.GetBytes(i % 100);
                    db.Set(BitConverter.GetBytes(i), BitConverter.GetBytes(i), indexed);
                    totalSize += 8 + 4;
                }
                timer.Stop();

                Console.WriteLine("Wrote data (with indexing) at a throughput of {0} MB/s", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

                timer.Reset();
                timer.Start();
                var ctModZeros = db.Find("Mod", BitConverter.GetBytes((int)0)).Count();
                timer.Stop();
                Assert.AreEqual(10, ctModZeros);
                Console.WriteLine("Scanned index at a throughput of {0} items/s", (double)ctModZeros / timer.Elapsed.TotalSeconds);

            }

            // Open the index directly and see if the data is there
            using (var db = new KeyValueStore(Path.Combine(path, "Mod"))) {
                int num_vals = db.EnumerateFromKey(BitConverter.GetBytes((int)0)).Count(pair => pair.Key.Take(4).All(b => b == 0));

                Assert.AreEqual(10, num_vals);
            }

            // Re-open the main key-value store and update the value at 30
            using (var db = new KeyValueStore(path)) {
                var indexed = new SortedDictionary<string, byte[]>();
                indexed["Mod"] = BitConverter.GetBytes(201 % 100);
                db.Set(BitConverter.GetBytes(200), BitConverter.GetBytes(200), indexed);
                // Clean the data from the index
                db.RemoveFromIndex(BitConverter.GetBytes(200), new Dictionary<string, byte[]> { { "Mod", BitConverter.GetBytes(200 % 100) } });
            }

            // Open the index again directly and confirm that the lookup key is gone now as well
            using (var db = new KeyValueStore(Path.Combine(path, "Mod"))) {
                int num_vals = db.EnumerateFromKey(BitConverter.GetBytes((int)0)).Count(pair => pair.Key.Take(4).All(b => b == 0));

                Assert.AreEqual(9, num_vals);
            }

        }

        [Test]
        public void LookupOldDataFromIndex() {

            string path = Path.GetFullPath("TestData\\LookupOldDataFromIndex");

            using (var db = new KeyValueStore(path)) {
                db.Truncate();
                db.Manifest.Logger = msg => Console.WriteLine(msg);

                db.Set(Encoding.UTF8.GetBytes("KeyA"), Encoding.UTF8.GetBytes("ValueA:1"), new Dictionary<string, byte[]> { { "Idx", Encoding.UTF8.GetBytes("1") } });
                db.Set(Encoding.UTF8.GetBytes("KeyB"), Encoding.UTF8.GetBytes("ValueB:2"), new Dictionary<string, byte[]> { { "Idx", Encoding.UTF8.GetBytes("2") } });
                db.Set(Encoding.UTF8.GetBytes("KeyC"), Encoding.UTF8.GetBytes("ValueC:3"), new Dictionary<string, byte[]> { { "Idx", Encoding.UTF8.GetBytes("3") } });

                var lookupValue = db.Find("Idx", Encoding.UTF8.GetBytes("3")).Single();
                Assert.AreEqual("ValueC:3", Encoding.UTF8.GetString(lookupValue.Value));
                Assert.AreEqual("KeyC", Encoding.UTF8.GetString(lookupValue.Key));

                db.Set(Encoding.UTF8.GetBytes("KeyC"), Encoding.UTF8.GetBytes("ValueC:4"), new Dictionary<string, byte[]> { { "Idx", Encoding.UTF8.GetBytes("4") } });

                lookupValue = db.Find("Idx", Encoding.UTF8.GetBytes("4")).Single();
                Assert.AreEqual("ValueC:4", Encoding.UTF8.GetString(lookupValue.Value));
                Assert.AreEqual("KeyC", Encoding.UTF8.GetString(lookupValue.Key));

                Assert.True(db.Find("Idx", Encoding.UTF8.GetBytes("3")).Any());

                db.RemoveFromIndex(Encoding.UTF8.GetBytes("KeyC"), new Dictionary<string, byte[]> { { "Idx", Encoding.UTF8.GetBytes("3") } });

                Assert.False(db.Find("Idx", Encoding.UTF8.GetBytes("3")).Any());
            }


        }

        [Test]
        public void IndexClean() {

            string path = Path.GetFullPath("TestData\\IndexClean");

            using (var db = new KeyValueStore(path)) {
                db.Truncate();
                db.Manifest.Logger = msg => Console.WriteLine(msg);

                db.Set(Encoding.UTF8.GetBytes("KeyA"), Encoding.UTF8.GetBytes("ValueA:1"), new Dictionary<string, byte[]> { { "Idx", Encoding.UTF8.GetBytes("1") } });
                db.Set(Encoding.UTF8.GetBytes("KeyB"), Encoding.UTF8.GetBytes("ValueB:2"), new Dictionary<string, byte[]> { { "Idx", Encoding.UTF8.GetBytes("2") } });
                db.Set(Encoding.UTF8.GetBytes("KeyC"), Encoding.UTF8.GetBytes("ValueC:3"), new Dictionary<string, byte[]> { { "Idx", Encoding.UTF8.GetBytes("3") } });

                var lookupValue = db.Find("Idx", Encoding.UTF8.GetBytes("3")).Single();
                Assert.AreEqual("ValueC:3", Encoding.UTF8.GetString(lookupValue.Value));
                Assert.AreEqual("KeyC", Encoding.UTF8.GetString(lookupValue.Key));

                db.Delete(Encoding.UTF8.GetBytes("KeyC"));
            }

            // Open the index directly and confirm that the lookup key is still there
            using (var db = new KeyValueStore(Path.Combine(path, "Idx"))) {
                Assert.AreEqual(3, db.Enumerate().Count());
            }

            using (var db = new KeyValueStore(path)) {
                db.CleanIndex("Idx");
            }

            // Open the index directly and confirm that the lookup key is now gone
            using (var db = new KeyValueStore(Path.Combine(path, "Idx"))) {
                Assert.AreEqual(2, db.Enumerate().Count());
            }

        }




        [TestFixture]
        public class TableMerge {

            const int recordMax = 100000;
            [TestFixtureSetUp]
            public void SetupData() {
                for (int r = 0; r < recordMax; r++) {
                    dataset[r] = new byte[200];
                    rand.NextBytes(dataset[r]);
                }
            }

            Random rand = new Random((int)DateTime.Now.Ticks);
            private byte[][] dataset = new byte[recordMax][];


            //[TestCase(9999, 1000000)]
            //[TestCase(8, 1000000)]
            //[TestCase(4, 1000000)]
            [TestCase(9999, recordMax)]
            [TestCase(8, recordMax)]
            [TestCase(4, recordMax)]
            public void TestMergeTableTiming(int mergeMax, int size) {
                PerformanceCounter PC = new PerformanceCounter();
                PC.CategoryName = "Process";
                PC.CounterName = "Working Set - Private";
                PC.InstanceName = Process.GetCurrentProcess().ProcessName;

                Console.WriteLine("TESTING:  page max({0}) record count({1})", mergeMax, size);
                var basename = "RazorDbTests.IndexingTests";
                var rand = new Random((int)DateTime.Now.Ticks);
                var indexHash = new Dictionary<ByteArray, byte[]>();
                var itemKeyLen = 35;

                var kvsName = string.Format("MergeTableTiming_{0}_{1}", mergeMax, DateTime.Now.Ticks);

                var sw = new Stopwatch();
                sw.Start();
                using (var testKVS = new KeyValueStore(Path.Combine(basename, kvsName))) {
                    // add a bunch of values that look like indexes
                    for (int r = 0; r < size; r++) {
                        var indexLen = (int)(DateTime.Now.Ticks % 60) + 50;
                        var indexKeyBytes = dataset[r];
                        var valuekeyBytes = indexKeyBytes.Skip(indexKeyBytes.Length - itemKeyLen).ToArray();
                        testKVS.Set(indexKeyBytes, valuekeyBytes); // old style index
                        indexHash.Add(new ByteArray(valuekeyBytes), indexKeyBytes);
                    }
                    TableManager.RunTableMergePass(testKVS);
                }
                sw.Stop();
                var memsize = Convert.ToInt32(PC.NextValue()) / (int)(1024);
                Console.WriteLine("Total processing time: {0} entries    {1} mergeSz    {2}  MEMORY: {3}", size, mergeMax, sw.Elapsed.ToString(), memsize);
                Console.WriteLine();
                PC.Close();
                PC.Dispose();
            }
        }
    }

}
