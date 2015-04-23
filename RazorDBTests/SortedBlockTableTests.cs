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
using System.IO;
using System.Diagnostics;
using System.Threading;

namespace RazorDBTests {

    [TestFixture]
    public class SortedBlockTableTests {

        [Test]
        public void TestRecordTypeAnding() {
            byte testbyte = (byte)RecordHeaderFlag.PrefixedRecord;
            Assert.AreNotEqual(testbyte & (byte)RecordHeaderFlag.Record, testbyte);
            Assert.AreEqual(testbyte & (byte)RecordHeaderFlag.PrefixedRecord, testbyte);
            Assert.AreEqual(testbyte & (byte)(RecordHeaderFlag.PrefixedRecord | RecordHeaderFlag.Record), testbyte);

            testbyte = (byte)RecordHeaderFlag.Record;
            Assert.AreNotEqual(testbyte & (byte)RecordHeaderFlag.PrefixedRecord, testbyte);
            Assert.AreEqual(testbyte & (byte)RecordHeaderFlag.Record, testbyte);
            Assert.AreEqual(testbyte & (byte)(RecordHeaderFlag.PrefixedRecord | RecordHeaderFlag.Record), testbyte);

            testbyte = (byte)RecordHeaderFlag.EndOfBlock;
            Assert.AreEqual(testbyte & (byte)RecordHeaderFlag.EndOfBlock, testbyte);
            Assert.AreNotEqual(testbyte & (byte)RecordHeaderFlag.PrefixedRecord, testbyte);
            Assert.AreNotEqual(testbyte & (byte)RecordHeaderFlag.Record, testbyte);
            Assert.AreNotEqual(testbyte & (byte)(RecordHeaderFlag.Record | RecordHeaderFlag.PrefixedRecord), testbyte);
        }


        [Test]
        public void TestShortToBytesRoundTrip() {
            Func<short, byte[]> twoBytes = (num) => {
                var bytes = new byte[2];
                bytes[0] = (byte)(num >> 8);
                bytes[1] = (byte)(num & 255);
                return bytes;
            };

            Func<byte[], short> toShort = (bytes) => {
                return (short)(bytes[0] << 8 | bytes[1]);
            };

            for (short i = short.MaxValue * -1; i < short.MaxValue; i++) {
                Assert.AreEqual(i, toShort(twoBytes(i)));
            }
        }

        [Test,Explicit]
        public void DumpPrefixedSBT() {

            string path = Path.GetFullPath("TestData\\DumpPrefixedSBT");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            var mt = new MemTable();
            for (int i = 0; i < 10000; i++) {
                var k0 = Key.Random(40);
                var v0 = Value.Random(200);
                mt.Add(k0, v0);
            }

            mt.WriteToSortedBlockTable("TestData\\DumpPrefixedSBT", 0, 10);
            var cache = new RazorCache();
            var sbt = new SortedBlockTable(cache, "TestData\\DumpPrefixedSBT", 0, 10);
            
            foreach (var pair in sbt.EnumerateRaw()) {
                Console.WriteLine("Key: {0}   Value: {1}", pair.Key.ToString(), pair.Value.ToString());
            }
        }

        [Test,Explicit]
        public void WriteAndDumpSBT() {

            string path = Path.GetFullPath("TestData\\DumpPrefixedSBT");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            var mt = new MemTable();
            for (int i = 0; i < 10000; i++) {
                var k0 = Key.Random(40);
                var v0 = Value.Random(200);
                mt.Add(k0, v0);
            }

            mt.WriteToSortedBlockTable("TestData\\DumpPrefixedSBT", 0, 10);
            var cache = new RazorCache();
            var sbt = new SortedBlockTable(cache, "TestData\\DumpPrefixedSBT", 0, 10);
            sbt.DumpContents((msg) => Console.WriteLine(msg));
        }




        [Test]
        public void TestFileOpenSpeed() {

            string path = Path.GetFullPath("TestData\\TestFileOpenSpeed");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

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

        [Test]
        public void ReadKeys() {

            string path = Path.GetFullPath("TestData\\ReadKeys");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

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

        [Test]
        public void EnumerateFromKeys() {

            string path = Path.GetFullPath("TestData\\EnumerateFromKeys");
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

            mt.WriteToSortedBlockTable("TestData\\EnumerateFromKeys", 10, 10);

            var cache = new RazorCache();
            var sbt = new SortedBlockTable(cache, "TestData\\EnumerateFromKeys", 10, 10);

            try {
                var indexCache = new RazorCache();

                var timer = new Stopwatch();
                timer.Start();
                Assert.AreEqual(10000, sbt.EnumerateFromKey(indexCache, new Key(new byte[] { 0 }, 0)).Count());
                timer.Stop();
                Console.WriteLine("Counted from beginning at a throughput of {0} MB/s", (double)mt.Size / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

                items = items.OrderBy((a) => a.Key).ToList();

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

        [Test]
        public void RandomizedLookups() {


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
                Assert.IsTrue(SortedBlockTable.Lookup("TestData\\RandomizedKeys", 10, 10, indexCache, pair.Key, out value, ExceptionHandling.ThrowAll, null));
                Assert.AreEqual(pair.Value, value);
            }
            timer.Stop();

            Value randomValue;
            Assert.IsFalse(SortedBlockTable.Lookup("TestData\\RandomizedKeys", 10, 10, indexCache, Key.Random(40), out randomValue, ExceptionHandling.ThrowAll, null));

            Console.WriteLine("Randomized read sbt table at a throughput of {0} MB/s (avg {1} ms per lookup)", (double)mt.Size / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0), (double)timer.Elapsed.TotalSeconds / (double)num_items);

            sbt.Close();

        }

        [Test]
        public void RandomizedThreadedLookups() {

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
            var sbt = new SortedBlockTable(cache, "TestData\\RandomizedThreadedLookups", 10, 10);
            var indexCache = new RazorCache();

            List<Thread> threads = new List<Thread>();
            for (int t = 0; t < 10; t++) {
                threads.Add(new Thread((num) => {
                    for (int k = 0; k < num_items / 10; k++) {
                        var pair = items[k * (int)num];
                        Value value;
                        Assert.IsTrue(SortedBlockTable.Lookup("TestData\\RandomizedThreadedLookups", 10, 10, indexCache, pair.Key, out value, ExceptionHandling.ThrowAll, null));
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
