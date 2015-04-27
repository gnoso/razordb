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
using NUnit.Framework;
using System.Text;
using RazorDB;
using System.IO;
using System.Diagnostics;
using System.Threading;
using System.Collections.Generic;
using System.Linq;

namespace RazorDBTests {

    [TestFixture]
    public class KeyValueStoreTests {

        [TestFixtureSetUp]
        public void Setup() {
            string path = Path.GetFullPath("TestData");
            if (!Directory.Exists(path)) 
                Directory.CreateDirectory(path);
        }

        [Test]
        public void BasicGetAndSet() {

            using (var db = new KeyValueStore("TestData\\GetAndSet")) {
                db.Truncate();

                for (int i = 0; i < 10; i++) {
                    byte[] key = BitConverter.GetBytes(i);
                    byte[] value = Encoding.UTF8.GetBytes("Number " + i.ToString());
                    db.Set(key, value);
                }

                for (int j = 0; j < 15; j++) {
                    byte[] key = BitConverter.GetBytes(j);

                    byte[] value = db.Get(key);
                    if (j < 10) {
                        Assert.AreEqual(Encoding.UTF8.GetBytes("Number " + j.ToString()), value);
                    } else {
                        Assert.IsNull(value);
                    }
                }
            }
        }

        [Test]
        public void BasicPersistentGetAndSet() {

            string path = Path.GetFullPath("TestData\\BasicPersistentGetAndSet");
            using (var db = new KeyValueStore(path)) {
                db.Truncate();

                for (int i = 0; i < 10; i++) {
                    byte[] key = BitConverter.GetBytes(i);
                    byte[] value = Encoding.UTF8.GetBytes("Number " + i.ToString());
                    db.Set(key, value);
                }
            }

            using (var db = new KeyValueStore(path)) {
                for (int j = 0; j < 15; j++) {
                    byte[] key = BitConverter.GetBytes(j);

                    byte[] value = db.Get(key);
                    if (j < 10) {
                        Assert.AreEqual(Encoding.UTF8.GetBytes("Number " + j.ToString()), value);
                    } else {
                        Assert.IsNull(value);
                    }
                }
            }
        }

        [Test]
        public void GetAndSetWithDelete() {

            string path = Path.GetFullPath("TestData\\GetAndSetWithDelete");
            using (var db = new KeyValueStore(path)) {
                db.Truncate();

                for (int i = 0; i < 10; i++) {
                    byte[] key = BitConverter.GetBytes(i);
                    byte[] value = Encoding.UTF8.GetBytes("Number " + i.ToString());
                    db.Set(key, value);
                }

                db.Delete(BitConverter.GetBytes(3));
                db.Delete(BitConverter.GetBytes(30));
                db.Delete(BitConverter.GetBytes(7));
                db.Delete(BitConverter.GetBytes(1));
                db.Delete(BitConverter.GetBytes(3));
            }

            using (var db = new KeyValueStore(path)) {
                for (int j = 0; j < 15; j++) {
                    byte[] key = BitConverter.GetBytes(j);

                    byte[] value = db.Get(key);
                    if (j == 3 || j == 1 || j == 7) {
                        Assert.IsNull(value);
                    } else if (j < 10) {
                        Assert.AreEqual(Encoding.UTF8.GetBytes("Number " + j.ToString()), value);
                    } else {
                        Assert.IsNull(value);
                    }
                }
            }
        }

        [Test]
        public void WriteSpeedTests() {
            var sw = new Stopwatch();
            var timingDict = new Dictionary<string, TimeSpan>();

            Config.SortedBlockTableFileOptions = FileOptions.SequentialScan;
            sw.Reset();
            sw.Start();
            DoWriteSpeedTests("TestData\\WriteSpeedTestsSequential1");
            sw.Stop();
            timingDict.Add("Sequential #1", sw.Elapsed);

            //Config.SortedBlockTableFileOptions = FileOptions.Asynchronous;
            //sw.Reset();
            //sw.Start();
            //DoWriteSpeedTests("TestData\\WriteSpeedTestsAsynchronous");
            //sw.Stop();
            //timingDict.Add("Asynchronous #1", sw.Elapsed);

            //Config.SortedBlockTableFileOptions = FileOptions.SequentialScan | FileOptions.Asynchronous;
            //sw.Reset();
            //sw.Start();
            //DoWriteSpeedTests("TestData\\WriteSpeedTestsSequentialORAsync1");
            //sw.Stop();
            //timingDict.Add("Sequential or Async #1", sw.Elapsed);


            //Config.SortedBlockTableFileOptions = FileOptions.Asynchronous;
            //sw.Reset();
            //sw.Start();
            //DoWriteSpeedTests("TestData\\WriteSpeedTestsAsynchronous2");
            //sw.Stop();
            //timingDict.Add("Asynchronous #2", sw.Elapsed);

            //Config.SortedBlockTableFileOptions = FileOptions.SequentialScan;
            //sw.Reset();
            //sw.Start();
            //DoWriteSpeedTests("TestData\\WriteSpeedTestsSequential2");
            //sw.Stop();
            //timingDict.Add("Sequential #2", sw.Elapsed);

            //Config.SortedBlockTableFileOptions = FileOptions.SequentialScan | FileOptions.Asynchronous;
            //sw.Reset();
            //sw.Start();
            //DoWriteSpeedTests("TestData\\WriteSpeedTestsSequentialORAsync2");
            //sw.Stop();
            //timingDict.Add("Sequential or Async #2", sw.Elapsed);

            foreach (var key in timingDict.Keys)
                Console.WriteLine("{0} elapsed time: {1}", key, timingDict[key]);
        }

        private static void DoWriteSpeedTests(string basepath) {
            Action<KeyValueStore, int, int, int> InsertDenseBlock = (KeyValueStore db, int key, int density, int count) => {
                byte[] value = ByteArray.Random(Config.MaxSmallValueSize - 12).InternalBytes;
                for (int i = 0; i < count; i++) {
                    byte[] keyBytes = BitConverter.GetBytes(key + density * i);
                    Array.Reverse(keyBytes); // make sure they are in lexicographical order so they sort closely together.

                    db.Set(keyBytes, value);
                }
            };

            // Make sure that when we have high key density, pages don't start to overlap with more than 10 pages at the level higher than the current one.
            string path = Path.GetFullPath(basepath);
            using (var db = new KeyValueStore(path)) {
                db.Truncate();
                db.Manifest.Logger = (msg) => Console.WriteLine(msg);

                InsertDenseBlock(db, 0, 1, 10000);
                InsertDenseBlock(db, 20000, 1, 10000);
                InsertDenseBlock(db, 15000, 1, 10000);
            }
        }

        [Test]
        public void KeyDensityMaximumPageOverlapTest() {

            Action<KeyValueStore, int,int, int> InsertDenseBlock = (KeyValueStore db, int key, int density, int count) => {
                byte[] value = ByteArray.Random(Config.MaxSmallValueSize - 12).InternalBytes;
                for (int i = 0; i < count; i++) {
                    byte[] keyBytes = BitConverter.GetBytes(key + density * i);
                    Array.Reverse(keyBytes); // make sure they are in lexicographical order so they sort closely together.

                    db.Set(keyBytes, value);
                }
            };

            // Make sure that when we have high key density, pages don't start to overlap with more than 10 pages at the level higher than the current one.
            string path = Path.GetFullPath("TestData\\KeyDensityMaximumPageOverlapTest");
            using (var db = new KeyValueStore(path)) {
                db.Truncate();
                db.Manifest.Logger = (msg) => Console.WriteLine(msg);

                InsertDenseBlock(db, 100, 1, 10000);
            }
            Console.WriteLine("Database is densely seeded.");
            // Close out the db to sync up all pending merge operations
            using (var db = new KeyValueStore(path)) {
                db.Manifest.Logger = (msg) => Console.WriteLine(msg);

                // Insert a spanning block that will cover all of the area already covered
                InsertDenseBlock(db, 0, 10000, 2);
            }
            Console.WriteLine("Spanning block inserted.");
            // Close out the db to sync up all pending merge operations
            using (var db = new KeyValueStore(path)) {
                db.Manifest.Logger = (msg) => Console.WriteLine(msg);
                db.MergeCallback = (level, input, output) => {
                    // We should not have more than 12 pages on the input side or else our page overlap throttle isn't working properly.
                    Assert.LessOrEqual(input.Count(), 12);
                };

                // Now insert a bunch of data into a non-overlapping portion of the space in order to force the spanning block to rise through the levels.
                InsertDenseBlock(db, 100000, 1, 1000);
            }
        }

        [Test, Explicit("Success depends on a race condition happening. Too unreliable for regular use.")]
        public void RotationShutdownRaceTest() {

            // Test to be sure that the rotation page has definitely been written by the time we exit the dispose region (the db must wait for that to occur).
            string path = Path.GetFullPath("TestData\\BulkSetWithDelete");
            using (var db = new KeyValueStore(path)) {
                db.Truncate();

                for (int i = 0; i < 50000; i++) {
                    byte[] key = BitConverter.GetBytes(i);
                    byte[] value = Encoding.UTF8.GetBytes("Number " + i.ToString());
                    db.Set(key, value);
                }
                // There is a chance that this could happen fast enough to make this assertion fail on some machines, but it should be unlikely.
                // The goal is to reproduce the race condition. If this assert succeeds then we have reproduced it.
                using (var mf = db.Manifest.GetLatestManifest()) {
                    Assert.IsFalse(mf.GetPagesAtLevel(0).Length > 0);
                }
            }
            using (var db = new KeyValueStore(path)) {
                using (var mf = db.Manifest.GetLatestManifest()) {
                    Assert.Greater(mf.GetPagesAtLevel(0).Length, 0);
                }
            }
        }

        [Test, Explicit("Success depends on a race condition happening. Too unreliable for regular use.")]
        public void RotationReadRaceTest() {

            string path = Path.GetFullPath("TestData\\RotationReadRaceTest");
            using (var db = new KeyValueStore(path)) {
                db.Truncate();

                int num_items = 58900;
                Console.WriteLine("Writing {0} items.", num_items);
                for (int i = 0; i < num_items; i++) {
                    byte[] key = BitConverter.GetBytes(i);
                    byte[] value = Encoding.UTF8.GetBytes("Number " + i.ToString());
                    db.Set(key, value);
                }
                Console.WriteLine("Read 1 item.");
                // Even though the page is rotated, but not written to disk yet, we should be able to query for the data anyway.
                {
                    byte[] key = BitConverter.GetBytes(0);
                    byte[] value = db.Get(key);
                    Assert.AreEqual(Encoding.UTF8.GetBytes("Number 0"), value);
                }
                Console.WriteLine("Check Manifest.");
                // There is a chance that this could happen fast enough to make this assertion fail on some machines, but it should be unlikely.
                // The goal is to reproduce the race condition. If this assert succeeds then we have reproduced it.
                using (var mf = db.Manifest.GetLatestManifest()) {
                    Assert.IsFalse(mf.GetPagesAtLevel(0).Length > 0);
                }
                Console.WriteLine("Done Checking Manifest.");
            }
            Console.WriteLine("Closed.");
            using (var db = new KeyValueStore(path)) {
                using (var mf = db.Manifest.GetLatestManifest()) {
                    Assert.IsTrue(mf.GetPagesAtLevel(0).Length > 0);
                }
            }
            Console.WriteLine("Done.");
        }

        [Test]
        public void RotationBulkRead() {

            string path = Path.GetFullPath("TestData\\RotationBulkReadRace");
            using (var db = new KeyValueStore(path)) {
                db.Truncate();

                int num_items = 30000;

                byte[] split = BitConverter.GetBytes(num_items >> 2);
                int number_we_should_scan = 0;
                for (int i = 0; i < num_items; i++) {
                    byte[] key = BitConverter.GetBytes(i);
                    if (ByteArray.CompareMemCmp(split, key) <= 0)
                        number_we_should_scan++;
                }
                Console.WriteLine("Number to Scan: {0}", number_we_should_scan);

                Console.WriteLine("Writing {0} items.", num_items);
                for (int i = 0; i < num_items; i++) {
                    byte[] key = BitConverter.GetBytes(i);
                    byte[] value = Encoding.UTF8.GetBytes("Number " + i.ToString());
                    db.Set(key, value);
                }
                Assert.AreEqual(number_we_should_scan, db.EnumerateFromKey(split).Count());
            }
        }

        [Test]
        public void BulkSetWithDelete() {

            int numItems = 100000;
            string path = Path.GetFullPath("TestData\\BulkSetWithDelete");
            if (Directory.Exists(path))
                Directory.Delete(path, true);
            Directory.CreateDirectory(path);

            using (var db = new KeyValueStore(path)) {
                db.Manifest.Logger = msg => Console.WriteLine(msg);
                db.Truncate();

                Stopwatch timer = new Stopwatch();
                timer.Start();
                for (int i = 0; i < numItems; i++) {
                    byte[] key = BitConverter.GetBytes(i);
                    byte[] value = Encoding.UTF8.GetBytes("Number " + i.ToString());
                    db.Set(key, value);
                }
                timer.Stop();
                Console.WriteLine("Wrote {0} items in {1}s", numItems, timer.Elapsed.TotalSeconds);

                int skip = 1000;
                timer.Reset();
                timer.Start();
                // Delete every skip-th item in reverse order,
                for (int j = numItems; j >= 0; j--) {
                    if (j % skip == 0) {
                        byte[] key = BitConverter.GetBytes(j);
                        db.Delete(key);
                    }
                }
                timer.Stop();
                Console.WriteLine("Deleted every {0}-th item in {1}s", skip, timer.Elapsed.TotalSeconds);

                // Now check all the results
                timer.Reset();
                timer.Start();
                for (int k = 0; k < numItems; k++) {
                    byte[] key = BitConverter.GetBytes(k);
                    byte[] value = db.Get(key);
                    if (k % skip == 0) {
                        Assert.IsNull(value);
                    } else {
                        Assert.AreEqual(Encoding.UTF8.GetBytes("Number " + k.ToString()), value, string.Format("{0}", k));
                    }
                }
                timer.Stop();
                Console.WriteLine("Read and check every item in {0}s", timer.Elapsed.TotalSeconds);
            }
        }


        [Test]
        public void BulkSet() {

            string path = Path.GetFullPath("TestData\\BulkSet");
            var timer = new Stopwatch();
            int totalSize = 0;

            using (var db = new KeyValueStore(path)) {
                db.Truncate();

                db.Manifest.Logger = (msg) => { Console.WriteLine(msg); };

                timer.Start();
                for (int i = 0; i < 105000; i++) {
                    var randomKey = ByteArray.Random(40);
                    var randomValue = ByteArray.Random(256);
                    db.Set(randomKey.InternalBytes, randomValue.InternalBytes);

                    totalSize += randomKey.Length + randomValue.Length;
                }
                timer.Stop();
                Console.WriteLine("Wrote sorted table at a throughput of {0} MB/s", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));
            }

       }

        [Test]
        public void BulkThreadedSet() {

            int numThreads = 10;
            int totalItems = 100000;
            int totalSize = 0;

            string path = Path.GetFullPath("TestData\\BulkThreadedSet");

            List<Thread> threads = new List<Thread>();
            using (var db = new KeyValueStore(path)) {
                db.Truncate();

                for (int j = 0; j < numThreads; j++) {
                    threads.Add(new Thread( (num) => {
                        int itemsPerThread = totalItems / numThreads;
                        for (int i = 0; i < itemsPerThread; i++) {
                            var randomKey = new ByteArray( BitConverter.GetBytes( ((int)num * itemsPerThread) + i ) );
                            var randomValue = ByteArray.Random(256);
                            db.Set(randomKey.InternalBytes, randomValue.InternalBytes);

                            Interlocked.Add(ref totalSize, randomKey.Length + randomValue.Length);
                        }
                    }));
                }

                var timer = new Stopwatch();
                timer.Start();

                // Start all the threads
                int tnum = 0;
                threads.ForEach((t) => t.Start(tnum++));

                // Wait on all the threads to complete
                threads.ForEach((t) => t.Join(300000));

                timer.Stop();
                Console.WriteLine("Wrote sorted table at a throughput of {0} MB/s", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));
            }

        }

        [Test]
        public void BulkSetBulkGet() {

            string path = Path.GetFullPath("TestData\\BulkSetBulkGet");
            var timer = new Stopwatch();
            int totalSize = 0;
            int readSize = 0;

            var items = new Dictionary<ByteArray, ByteArray>();

            using (var db = new KeyValueStore(path)) {
                db.Truncate();

                db.Manifest.Logger = (msg) => Console.WriteLine(msg);

                timer.Start();
                for (int i = 0; i < 105000; i++) {
                    var randomKey = ByteArray.Random(40);
                    var randomValue = ByteArray.Random(256);
                    db.Set(randomKey.InternalBytes, randomValue.InternalBytes);

                    items[randomKey] = randomValue;
                    readSize += randomKey.Length + randomValue.Length;
                    totalSize += randomKey.Length + randomValue.Length;
                }
                timer.Stop();
                Console.WriteLine("Wrote sorted table at a throughput of {0} MB/s", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));
            }
            // Close and re-open the database to force all the sstable merging to complete.
            using (var db = new KeyValueStore(path)) {
                
                db.Manifest.Logger = (msg) => Console.WriteLine(msg);

                timer.Reset();
                Console.WriteLine("Begin randomized read back.");
                timer.Start();
                foreach ( var insertedItem in items) {
                    try {
                        byte[] value = db.Get(insertedItem.Key.InternalBytes);
                        Assert.AreEqual(insertedItem.Value, new ByteArray(value));
                    } catch (Exception /*e*/) {
                        //Console.WriteLine("Key: {0}\n{1}",insertedItem.Key,e);
                        //Debugger.Launch();
                        //db.Get(insertedItem.Key.InternalBytes);
                        //db.Manifest.LogContents();
                        throw;
                    }
                }
                timer.Stop();
                Console.WriteLine("Randomized read throughput of {0} MB/s (avg {1} ms per lookup)", (double)readSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0), (double)timer.Elapsed.TotalSeconds / (double)items.Count);

            }

        }

        [Test]
        public void BulkSetGetWhileReMerging() {

            string path = Path.GetFullPath("TestData\\BulkSetGetWhileReMerging");
            var timer = new Stopwatch();
            int totalSize = 0;

            var items = new Dictionary<ByteArray, ByteArray>();

            using (var db = new KeyValueStore(path)) {
                db.Truncate();

                db.Manifest.Logger = (msg) => Console.WriteLine(msg);

                timer.Start();
                for (int i = 0; i < 105000; i++) {
                    var randomKey = ByteArray.Random(40);
                    var randomValue = ByteArray.Random(256);
                    db.Set(randomKey.InternalBytes, randomValue.InternalBytes);

                    items[randomKey] = randomValue;
                    totalSize += randomKey.Length + randomValue.Length;
                }
                timer.Stop();
                Console.WriteLine("Wrote sorted table at a throughput of {0} MB/s", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

                timer.Reset();
                Console.WriteLine("Begin randomized read back.");
                timer.Start();
                foreach (var insertedItem in items) {
                    try {
                        byte[] value = db.Get(insertedItem.Key.InternalBytes);
                        Assert.AreEqual(insertedItem.Value, new ByteArray(value));
                    } catch (Exception /*e*/) {
                        //Console.WriteLine("Key: {0}\n{1}", insertedItem.Key, e);
                        //Debugger.Launch();
                        //db.Get(insertedItem.Key.InternalBytes);
                        //db.Manifest.LogContents();
                        throw;
                    }
                }
                timer.Stop();
                Console.WriteLine("Randomized read throughput of {0} MB/s (avg {1} ms per lookup)", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0), (double)timer.Elapsed.TotalSeconds / (double)items.Count);

            }
        }

        [Test]
        public void BulkSetBulkEnumerate() {

            string path = Path.GetFullPath("TestData\\BulkSetBulkEnumerate");
            var timer = new Stopwatch();
            int totalSize = 0;
            int readSize = 0;
            int num_items = 100000;

            using (var db = new KeyValueStore(path)) {
                db.Truncate();

                db.Manifest.Logger = (msg) => Console.WriteLine(msg);

                timer.Start();
                for (int i = 0; i < num_items; i++) {
                    var randomKey = ByteArray.Random(40);
                    var randomValue = ByteArray.Random(256);
                    db.Set(randomKey.InternalBytes, randomValue.InternalBytes);

                    readSize += randomKey.Length + randomValue.Length;
                    totalSize += randomKey.Length + randomValue.Length;
                }
                timer.Stop();
                Console.WriteLine("Wrote sorted table at a throughput of {0} MB/s", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));
            }
            // Close and re-open the database to force all the sstable merging to complete.
            using (var db = new KeyValueStore(path)) {

                db.Manifest.Logger = (msg) => Console.WriteLine(msg);

                timer.Reset();
                Console.WriteLine("Begin enumeration.");
                timer.Start();
                ByteArray lastKey = new ByteArray();
                int ct = 0;
                foreach (var pair in db.Enumerate()) {
                    try {
                        ByteArray k = new ByteArray(pair.Key);
                        Assert.True(lastKey.CompareTo(k) < 0);
                        lastKey = k;
                        ct++;
                    } catch (Exception /*e*/) {
                        //Console.WriteLine("Key: {0}\n{1}",insertedItem.Key,e);
                        //Debugger.Launch();
                        //db.Get(insertedItem.Key.InternalBytes);
                        //db.Manifest.LogContents();
                        throw;
                    }
                }
                timer.Stop();
                Assert.AreEqual(num_items, ct, num_items.ToString() + " items should be enumerated.");

                Console.WriteLine("Enumerated read throughput of {0} MB/s", (double)readSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));
            }

        }

        [Test]
        public void BulkSetBulkEnumerateWhileMerging() {

            string path = Path.GetFullPath("TestData\\BulkSetBulkEnumerateWhileMerging");
            var timer = new Stopwatch();
            int totalSize = 0;
            int readSize = 0;
            int num_items = 100000;

            using (var db = new KeyValueStore(path)) {
                db.Truncate();

                db.Manifest.Logger = (msg) => Console.WriteLine(msg);

                timer.Start();
                for (int i = 0; i < num_items; i++) {
                    var randomKey = ByteArray.Random(40);
                    var randomValue = ByteArray.Random(256);
                    db.Set(randomKey.InternalBytes, randomValue.InternalBytes);

                    readSize += randomKey.Length + randomValue.Length;
                    totalSize += randomKey.Length + randomValue.Length;
                }
                timer.Stop();
                Console.WriteLine("Wrote sorted table at a throughput of {0} MB/s", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

                timer.Reset();
                Console.WriteLine("Begin enumeration.");
                timer.Start();
                ByteArray lastKey = new ByteArray();
                int ct = 0;
                foreach (var pair in db.Enumerate()) {
                    try {
                        ByteArray k = new ByteArray(pair.Key);
                        Assert.True(lastKey.CompareTo(k) < 0);
                        lastKey = k;
                        ct++;
                    } catch (Exception e) {
                        Console.WriteLine("Key: {0}\n{1}",pair.Key,e);
                        Debugger.Launch();
                        throw;
                    }
                }
                timer.Stop();
                Assert.AreEqual(num_items, ct, num_items.ToString() + " items should be enumerated.");

                Console.WriteLine("Enumerated read throughput of {0} MB/s", (double)readSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));
            }

        }

        [Test]
        public void BulkSetBulkEnumerateWithCache() {

            string path = Path.GetFullPath("TestData\\BulkSetBulkEnumerateWithCache");
            var timer = new Stopwatch();
            int totalSize = 0;
            int readSize = 0;
            int num_items = 100000;

            using (var db = new KeyValueStore(path)) {
                db.Truncate();

                db.Manifest.Logger = (msg) => Console.WriteLine(msg);

                timer.Start();
                for (int i = 0; i < num_items; i++) {
                    var randomKey = ByteArray.Random(40);
                    var randomValue = ByteArray.Random(256);
                    db.Set(randomKey.InternalBytes, randomValue.InternalBytes);

                    readSize += randomKey.Length + randomValue.Length;
                    totalSize += randomKey.Length + randomValue.Length;
                }
                timer.Stop();
                Console.WriteLine("Wrote sorted table at a throughput of {0} MB/s", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

                timer.Reset();
                timer.Start();
                Assert.AreEqual(num_items, db.Enumerate().Count());
                timer.Stop();
                Console.WriteLine("Enumerated read throughput of {0} MB/s", (double)readSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

                timer.Reset();
                timer.Start();
                Assert.AreEqual(num_items, db.Enumerate().Count());
                timer.Stop();
                Console.WriteLine("Enumerated (second pass) read throughput of {0} MB/s", (double)readSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));
            }

        }

        [Test]
        public void BulkSetEnumerateAll() {

            string path = Path.GetFullPath("TestData\\BulkSetEnumerateAll");
            var timer = new Stopwatch();
            int totalSize = 0;
            int readSize = 0;

            using (var db = new KeyValueStore(path)) {
                db.Truncate();

                db.Manifest.Logger = (msg) => Console.WriteLine(msg);

                timer.Start();
                for (int i = 0; i < 105000; i++) {
                    var randomKey = BitConverter.GetBytes(i);
                    var randomValue = BitConverter.GetBytes(i);
                    db.Set(randomKey, randomValue);

                    readSize += randomKey.Length + randomValue.Length;
                    totalSize += randomKey.Length + randomValue.Length;
                }
                timer.Stop();
                Console.WriteLine("Wrote sorted table at a throughput of {0} MB/s", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));
            }
            // Close and re-open the database to force all the sstable merging to complete.
            using (var db = new KeyValueStore(path)) {

                db.Manifest.Logger = (msg) => Console.WriteLine(msg);

                timer.Reset();
                Console.WriteLine("Begin enumeration.");
                timer.Start();
                ByteArray lastKey = ByteArray.Empty;
                int ct = 0;
                foreach (var pair in db.Enumerate()) {
                    try {
                        ByteArray k = new ByteArray(pair.Key);
                        ByteArray v = new ByteArray(pair.Value);
                        Assert.AreEqual(k, v);
                        Assert.True(lastKey.CompareTo(k) < 0);
                        lastKey = k;
                        ct++;
                    } catch (Exception /*e*/) {
                        //Console.WriteLine("Key: {0}\n{1}",insertedItem.Key,e);
                        //Debugger.Launch();
                        //db.Get(insertedItem.Key.InternalBytes);
                        //db.Manifest.LogContents();
                        throw;
                    }
                }
                timer.Stop();
                Assert.AreEqual(105000, ct, "105000 items should be enumerated.");

                Console.WriteLine("Enumerated read throughput of {0} MB/s (avg {1} ms per 1000 items)", (double)readSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0), (double)timer.Elapsed.TotalSeconds / (double)105);
            }

        }

        [Test]
        public void BulkSetEnumerateAll2() {

            string path = Path.GetFullPath("TestData\\BulkSetEnumerateAll2");
            var timer = new Stopwatch();
            int totalSize = 0;
            int readSize = 0;

            using (var db = new KeyValueStore(path)) {
                db.Truncate();

                db.Manifest.Logger = (msg) => Console.WriteLine(msg);

                timer.Start();
                for (int i = 0; i < 105000; i++) {
                    var randomKey = BitConverter.GetBytes(i);
                    var randomValue = BitConverter.GetBytes(i);
                    db.Set(randomKey, randomValue);

                    readSize += randomKey.Length + randomValue.Length;
                    totalSize += randomKey.Length + randomValue.Length;
                }
                timer.Stop();
                Console.WriteLine("Wrote sorted table at a throughput of {0} MB/s", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

                timer.Reset();
                Console.WriteLine("Begin enumeration.");
                timer.Start();
                ByteArray lastKey = ByteArray.Empty;
                int ct = 0;
                foreach (var pair in db.Enumerate()) {
                    try {
                        ByteArray k = new ByteArray(pair.Key);
                        ByteArray v = new ByteArray(pair.Value);
                        Assert.AreEqual(k, v);
                        Assert.True(lastKey.CompareTo(k) < 0);
                        lastKey = k;
                        ct++;
                    } catch (Exception /*e*/) {
                        //Console.WriteLine("Key: {0}\n{1}",insertedItem.Key,e);
                        //Debugger.Launch();
                        //db.Get(insertedItem.Key.InternalBytes);
                        //db.Manifest.LogContents();
                        throw;
                    }
                }
                timer.Stop();
                Assert.AreEqual(105000, ct, "105000 items should be enumerated.");

                Console.WriteLine("Enumerated read throughput of {0} MB/s (avg {1} ms per 1000 items)", (double)readSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0), (double)timer.Elapsed.TotalSeconds / (double)105);
            }

        }

        [Test]
        public void BulkSetEnumerateAll3() {

            string path = Path.GetFullPath("TestData\\BulkSetEnumerateAll3");
            var timer = new Stopwatch();

            using (var db = new KeyValueStore(path)) {
                db.Truncate();
                int totalSize = 0;
                db.Manifest.Logger = msg => Console.WriteLine(msg);

                int num_items = 1000000;
                timer.Start();
                for (int i = 0; i < num_items; i++) {
                    byte[] key = new byte[8];
                    BitConverter.GetBytes(i % 100).CopyTo(key,0);
                    BitConverter.GetBytes(i).CopyTo(key,4);
                    byte[] value = BitConverter.GetBytes(i);
                    db.Set(key, value);
                    totalSize += 8 + 4;
                }
                timer.Stop();

                Console.WriteLine("Wrote data (with indexing) at a throughput of {0} MB/s", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

                timer.Reset();
                timer.Start();
                var ctModZeros = db.EnumerateFromKey(BitConverter.GetBytes(0)).Count();
                timer.Stop();
                
                Console.WriteLine("Scanned index at a throughput of {0} items/s", (double) ctModZeros / timer.Elapsed.TotalSeconds);
            }
        }

        [Test]
        public void BulkSetEnumerateAllWithMissingSBT_ThrowAll() {

            string path = Path.GetFullPath("TestData\\BulkSetEnumerateAllWithMissingSBT_ThrowAll"+DateTime.Now.Ticks.ToString());
            var timer = new Stopwatch();
            int totalSize = 0;
            int readSize = 0;
            Action<string> logger = (msg) => { Console.WriteLine(msg); };
            using (var db = new KeyValueStore(path)) {
                db.Truncate();
                timer.Start();
                for (int i = 0; i < 500000; i++) {
                    var randomKey = BitConverter.GetBytes(i);
                    var randomValue = BitConverter.GetBytes(i);
                    db.Set(randomKey, randomValue);

                    readSize += randomKey.Length + randomValue.Length;
                    totalSize += randomKey.Length + randomValue.Length;
                }
                timer.Stop();
                Console.WriteLine("Wrote sorted table at a throughput of {0} MB/s", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));
            }

            // delete the sbt files
            var files = Directory.GetFiles(path, "*.sbt");
            foreach(var fname in files)
                File.Delete(fname);

            // Close and re-open the database to force all the sstable merging to complete.
            Console.WriteLine("Begin enumeration.");
            RazorDB.Config.ExceptionHandling = ExceptionHandling.ThrowAll;
            Assert.Throws(typeof(FileNotFoundException), () => {
                using (var db = new KeyValueStore(path)) {
                    foreach (var pair in db.Enumerate());
                }
            });
        }

        [Test]
        public void BulkSetEnumerateAllWithMissingSBT_AttemptRecovery() {
            try {
                RazorDB.Config.ExceptionHandling = ExceptionHandling.AttemptRecovery;

                string path = Path.GetFullPath("TestData\\BulkSetEnumerateAllWithMissingSBT_AttemptRecovery");
                var timer = new Stopwatch();
                int totalSize = 0;
                int readSize = 0;
                Action<string> logger = (msg) => { Console.WriteLine(msg); };
                using (var db = new KeyValueStore(path)) {
                    db.Truncate();
                    timer.Start();
                    for (int i = 0; i < 500000; i++) {
                        var randomKey = BitConverter.GetBytes(i);
                        var randomValue = BitConverter.GetBytes(i);
                        db.Set(randomKey, randomValue);

                        readSize += randomKey.Length + randomValue.Length;
                        totalSize += randomKey.Length + randomValue.Length;
                    }
                    timer.Stop();
                    Console.WriteLine("Wrote sorted table at a throughput of {0} MB/s", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));
                }

                // delete the sbt files
                var files = Directory.GetFiles(path, "*.sbt");
                foreach (var fname in files)
                    File.Delete(fname);

                // Close and re-open the database to force all the sstable merging to complete.
                Console.WriteLine("Begin enumeration.");
                using (var db = new KeyValueStore(path)) {
                    timer.Reset();
                    timer.Start();
                    ByteArray lastKey = ByteArray.Empty;
                    int ct = 0;
                    foreach (var pair in db.Enumerate()) {
                        try {
                            ByteArray k = new ByteArray(pair.Key);
                            ByteArray v = new ByteArray(pair.Value);
                            Assert.AreEqual(k, v);
                            Assert.True(lastKey.CompareTo(k) < 0);
                            lastKey = k;
                            ct++;
                        } catch (Exception /*e*/) {
                            //Console.WriteLine("Key: {0}\n{1}",insertedItem.Key,e);
                            //Debugger.Launch();
                            //db.Get(insertedItem.Key.InternalBytes);
                            //db.Manifest.LogContents();
                            throw;
                        }
                    }
                    timer.Stop();
                    Assert.AreEqual(80568, ct);
                    Console.WriteLine("Enumerated read throughput of {0} MB/s (avg {1} ms per 1000 items)", (double)readSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0), (double)timer.Elapsed.TotalSeconds / (double)105);
                }

                // add some more records after deleting files
                using (var db = new KeyValueStore(path)) {
                    timer.Start();
                    // add 1,000,000 new keys
                    for (int i = 1000000; i < 3000000; i++) {
                        var randomKey = BitConverter.GetBytes(i);
                        var randomValue = BitConverter.GetBytes(i);
                        db.Set(randomKey, randomValue);

                        readSize += randomKey.Length + randomValue.Length;
                        totalSize += randomKey.Length + randomValue.Length;
                    }
                    timer.Stop();
                    Console.WriteLine("Wrote sorted table at a throughput of {0} MB/s", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));
                }

                // Close and re-open the database to force all the sstable merging to complete.
                Console.WriteLine("Begin enumeration.");
                using (var db = new KeyValueStore(path)) {
                    timer.Reset();
                    timer.Start();
                    ByteArray lastKey = ByteArray.Empty;
                    int ct = 0;
                    foreach (var pair in db.Enumerate()) {
                        try {
                            ByteArray k = new ByteArray(pair.Key);
                            ByteArray v = new ByteArray(pair.Value);
                            Assert.AreEqual(k, v);
                            Assert.True(lastKey.CompareTo(k) < 0);
                            lastKey = k;
                            ct++;
                        } catch (Exception /*e*/) {
                            //Console.WriteLine("Key: {0}\n{1}",insertedItem.Key,e);
                            //Debugger.Launch();
                            //db.Get(insertedItem.Key.InternalBytes);
                            //db.Manifest.LogContents();
                            throw;
                        }
                    }
                    timer.Stop();
                    Assert.AreEqual(2080568, ct);
                    Console.WriteLine("Enumerated read throughput of {0} MB/s (avg {1} ms per 1000 items)", (double)readSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0), (double)timer.Elapsed.TotalSeconds / (double)105);
                }
            } finally {
                RazorDB.Config.ExceptionHandling = ExceptionHandling.ThrowAll;
            }

        }


        [Test]
        public void BulkSetEnumerateFromKey() {

            string path = Path.GetFullPath("TestData\\BulkSetEnumerateFromKey");
            var timer = new Stopwatch();
            int totalSize = 0;
            int readSize = 0;

            using (var db = new KeyValueStore(path)) {
                db.Truncate();

                db.Manifest.Logger = (msg) => Console.WriteLine(msg);

                timer.Start();
                for (int i = 0; i < 105000; i++) {
                    var randomKey = BitConverter.GetBytes(i).Reverse().ToArray();
                    var randomValue = BitConverter.GetBytes(i);
                    db.Set(randomKey, randomValue);

                    readSize += randomKey.Length + randomValue.Length;
                    totalSize += randomKey.Length + randomValue.Length;
                }
                timer.Stop();
                Console.WriteLine("Wrote sorted table at a throughput of {0} MB/s", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

                timer.Reset();
                Console.WriteLine("Begin enumeration.");
                timer.Start();
                int lastKeyNum = 0;
                int ct = 0;
                int sum = 0;
                var searchKey = BitConverter.GetBytes(50000).Reverse().ToArray();
                foreach (var pair in db.EnumerateFromKey( searchKey )) {
                    try {
                        int num = BitConverter.ToInt32(pair.Key.Reverse().ToArray(),0);

                        Assert.GreaterOrEqual(num, 50000);
                        sum += num;
                        Assert.Less(lastKeyNum, num);
                        lastKeyNum = num;
                        ct++;
                    } catch (Exception /*e*/) {
                        //Console.WriteLine("Key: {0}\n{1}",insertedItem.Key,e);
                        //Debugger.Launch();
                        //db.Get(insertedItem.Key.InternalBytes);
                        //db.Manifest.LogContents();
                        throw;
                    }
                }
                timer.Stop();
                Assert.AreEqual(55000, ct, "55000 items should be enumerated.");

                Console.WriteLine("Enumerated read throughput of {0} MB/s (avg {1} ms per 1000 items)", (double)readSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0), (double)timer.Elapsed.TotalSeconds / (double)105);
            }

        }

        [Test]
        public void BulkSetThreadedGetWhileReMerging() {

            string path = Path.GetFullPath("TestData\\BulkSetThreadedGetWhileReMerging");
            var timer = new Stopwatch();
            int totalSize = 0;

            var items = new Dictionary<ByteArray, ByteArray>();

            using (var db = new KeyValueStore(path)) {
                db.Truncate();

                db.Manifest.Logger = (msg) => Console.WriteLine(msg);

                timer.Start();
                int totalItems = 105000;
                for (int i = 0; i < totalItems; i++) {
                    var randomKey = ByteArray.Random(40);
                    var randomValue = ByteArray.Random(256);
                    db.Set(randomKey.InternalBytes, randomValue.InternalBytes);

                    items[randomKey] = randomValue;
                    totalSize += randomKey.Length + randomValue.Length;
                }
                timer.Stop();
                Console.WriteLine("Wrote sorted table at a throughput of {0} MB/s", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

                List<KeyValuePair<ByteArray,ByteArray>> itemsList = items.ToList();
                int numThreads = 10;
                List<Thread> threads = new List<Thread>();

                for (int j = 0; j < numThreads; j++) {
                    threads.Add(new Thread((num) => {
                        int itemsPerThread = totalItems / numThreads;
                        for (int i = 0; i < itemsPerThread; i++) {
                            try {
                                int idx = i * (int)num;
                                byte[] value = db.Get(itemsList[idx].Key.InternalBytes);
                                Assert.AreEqual(itemsList[idx].Value, new ByteArray(value));
                            } catch (Exception /*e*/) {
                                //Console.WriteLine("Key: {0}\n{1}", insertedItem.Key, e);
                                //Debugger.Launch();
                                //db.Get(insertedItem.Key.InternalBytes);
                                //db.Manifest.LogContents();
                                throw;
                            }
                        }
                    }));
                }


                timer.Reset();
                Console.WriteLine("Begin randomized read back.");
                timer.Start();
                for (int k=0; k < numThreads; k++) {
                    threads[k].Start(k);
                }
                threads.ForEach(t => t.Join());
                timer.Stop();
                Console.WriteLine("Randomized read throughput of {0} MB/s (avg {1} ms per lookup)", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0), (double)timer.Elapsed.TotalSeconds / (double)items.Count);

            }
        }

        [Test]
        public void TestJournalFileGrowth() {

            string path = Path.GetFullPath("TestData\\TestJournalFileGrowth");
            // Open database and store enough data to cause a page split
            using (var db = new KeyValueStore(path)) {
                db.Truncate();

                ByteArray key = ByteArray.Random(40);
                for (int i = 0; i < 100000; i++) {
                    ByteArray value = ByteArray.Random(400);

                    db.Set(key.InternalBytes, value.InternalBytes);
                }

                var journalfileName = Config.JournalFile(db.Manifest.BaseFileName, db.Manifest.CurrentVersion(0));
                var journalLength = new FileInfo(journalfileName).Length;
                // Make sure the journal is smaller than the max memtable size.
                Assert.LessOrEqual(journalLength, Config.MaxMemTableSize);

                // Double check to be sure that the contents of the database are correct in this case.
                int count = 0;
                foreach (var value in db.Enumerate()) {
                    Assert.AreEqual(key.InternalBytes, value.Key);
                    count++;
                }
                Assert.AreEqual(1, count);
            }
            
        }

        [Test]
        public void TestOverwritingAndDeleting() {

            var keys = new List<ByteArray>();

            string path = Path.GetFullPath("TestData\\TestOverwriting");
            // Open database and store enough data to cause a page split
            using (var db = new KeyValueStore(path)) {
                db.Truncate();

                for (int i = 0; i < 12000; i++) {
                    ByteArray key = ByteArray.Random(40);
                    ByteArray value = ByteArray.Random(400);

                    keys.Add(key);
                    db.Set(key.InternalBytes, value.InternalBytes);
                }
                // Dispose KVS to make sure the datastore is closed out and all merging threads are shut down.
            }

            // how much space are we using?
            Func<string, long> GetSpaceUsed = (string dirPath) => Directory.GetFiles(dirPath).Sum(fileName => new FileInfo(fileName).Length);

            var spaceUsedNow = GetSpaceUsed(path);

            using (var db = new KeyValueStore(path)) {
                // Reset the same exact keys to different data
                for (int k = 0; k < 4; k++) {
                    for (int i = 0; i < 12000; i++) {
                        ByteArray key = keys[i];
                        ByteArray value = ByteArray.Random(400);

                        db.Set(key.InternalBytes, value.InternalBytes);
                    }
                }
            }

            var spaceUsedAfterAdditions = GetSpaceUsed(path);
            var spaceRatio = (double)spaceUsedAfterAdditions / (double)spaceUsedNow;

            Assert.Less(spaceRatio, 1.4);
        }


        [Test, Explicit]
        public void DumpKeySpaceUsed() {
            double valueBytes=0L;
            double keyBytes = 0L;
            double dupBytes = 0L;
            double totalRecords = 0L;

            Action<string> dumpFolderBytes = (folder) => {
                double tableValBytes = 0L;
                double tableKeyBytes = 0L;
                double tableDupBytes = 0L;
                double tableRecords = 0L;
                byte[] lastkey = null;
                using (var kvs = new KeyValueStore(folder)) {
                    foreach (var pair in kvs.Enumerate()) {
                        tableRecords++;
                        tableValBytes += pair.Value.Length;
                        tableKeyBytes += pair.Key.Length;
                        if (lastkey != null) {
                            int i = 0;
                            for (i = 0; i < lastkey.Length && i < pair.Key.Length; i++)
                                if (lastkey[i] != pair.Key[i])
                                    continue;
                            tableDupBytes += i;
                        }
                        lastkey = pair.Key;
                    }
                }
                valueBytes += tableValBytes;
                keyBytes += tableKeyBytes;
                dupBytes += tableDupBytes;
                totalRecords += tableRecords;
                Console.WriteLine("{0} Total Bytes: {1}", folder, tableValBytes + tableKeyBytes);
                Console.WriteLine("         #Records: {0}", tableRecords);
                Console.WriteLine("      Key   Bytes: {0}", tableKeyBytes);
                Console.WriteLine("      Value Bytes: {0}", tableValBytes);
                Console.WriteLine("      Dupl. Bytes: {0}", tableDupBytes);
                Console.WriteLine(" %Savings in keys: {0}%", tableDupBytes / tableKeyBytes * 100);
                Console.WriteLine(" %Savings overall: {0}%", tableDupBytes / (tableValBytes + tableKeyBytes) * 100);
                Console.WriteLine();
            };

            var baseFolder = @"d:\ncoverdata\ncover";
            foreach (var folder in Directory.GetDirectories(baseFolder, "*", SearchOption.AllDirectories)) 
                dumpFolderBytes(folder);

            Console.WriteLine("Total KeyValueStore Bytes: {0}", valueBytes + keyBytes);
            Console.WriteLine("         #Records: {0}", totalRecords);
            Console.WriteLine("      Key   Bytes: {0}", keyBytes);
            Console.WriteLine("      Value Bytes: {0}", valueBytes);
            Console.WriteLine("      Dupl. Bytes: {0}", dupBytes);
            Console.WriteLine(" %Savings in keys: {0}%", dupBytes / keyBytes * 100);
            Console.WriteLine(" %Savings overall: {0}%", dupBytes / (valueBytes + keyBytes) * 100);

        }
    }

}