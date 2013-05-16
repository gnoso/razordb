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
using System.IO;
using NUnit.Framework;
using RazorDB;
using System.Diagnostics;
using System.Threading;

namespace RazorDBTests {

    [TestFixture]
    public class MemTableTests {

        [Test]
        public void AddAndLookupItems() {

            MemTable mt = new MemTable();

            List<KeyValuePair<Key, Value>> values = new List<KeyValuePair<Key, Value>>();

            for (int i = 0; i < 10000; i++) {
                var randomKey = Key.Random(40);
                var randomValue = Value.Random(256);

                values.Add(new KeyValuePair<Key, Value>(randomKey, randomValue));
                mt.Add(randomKey, randomValue);
            }

            Value value;
            foreach (var pair in values) {
                Assert.IsTrue(mt.Lookup(pair.Key, out value));
                Assert.AreEqual(pair.Value, value);
            }
            Assert.IsFalse(mt.Lookup(Key.Random(40), out value));

            Assert.AreEqual(10000 * (40 + 256), mt.Size);
            Assert.IsTrue(mt.Full);
        }

        [Test]
        public void SetItemsMultipleTimes() {

            MemTable mt = new MemTable();

            Dictionary<Key, Value> values = new Dictionary<Key, Value>();

            for (int i = 0; i < 10000; i++) {
                var randomKey = new Key(new ByteArray(BitConverter.GetBytes(i % 10)));
                var randomValue = Value.Random(256);

                values[randomKey] = randomValue;
                mt.Add(randomKey, randomValue);
            }

            Value value;
            foreach (var pair in values) {
                Assert.IsTrue(mt.Lookup(pair.Key, out value));
                Assert.AreEqual(pair.Value, value);
            }
            Assert.IsFalse(mt.Lookup(Key.Random(4), out value));
            Assert.AreEqual(10, mt.Enumerate().Count());
            Assert.AreEqual(10, values.Count);
        }

        [Test]
        public void WriteMemTableToSsTable() {

            string path = Path.GetFullPath("TestData\\WriteMemTableToSsTable");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            MemTable mt = new MemTable();

            for (int i = 0; i < 10000; i++) {
                var randomKey = Key.Random(40);
                var randomValue = Value.Random(256);

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

            List<KeyValuePair<Key, Value>> values = new List<KeyValuePair<Key, Value>>();

            for (int i = 0; i < 10000; i++) {
                var randomKey = Key.Random(40);
                var randomValue = Value.Random(256);

                values.Add(new KeyValuePair<Key, Value>(randomKey, randomValue));
                jw.Add(randomKey, randomValue);
            }
            jw.Close();

            MemTable mtl = new MemTable();
            mtl.ReadFromJournal("TestData\\AddAndLookupItemsPersisted", 523);

            Value value;
            foreach (var pair in values) {
                Assert.IsTrue(mtl.Lookup(pair.Key, out value));
                Assert.AreEqual(pair.Value, value);
            }
            Assert.IsFalse(mtl.Lookup(Key.Random(40), out value));

            Assert.AreEqual(10000 * (40 + 256), mtl.Size);
            Assert.IsTrue(mtl.Full);
        }

        [Test]
        public void SnapshotEnumerator() {

            // This test is designed to highlight inefficiencies in the memtable snapshotting mechanism (fixed now with snapshot-able tree)

            MemTable mt = new MemTable();

            for (int i = 0; i < 10000; i++) {
                var randomKey = new Key(new ByteArray(BitConverter.GetBytes(i)));
                var randomValue = Value.Random(256);

                mt.Add(randomKey, randomValue);
            }

            Stopwatch timer = new Stopwatch();
            timer.Start();
            for (int k = 0; k < 100; k++) {
                Assert.AreEqual(10000, mt.GetEnumerableSnapshot().Count());
            }
            timer.Stop();

            Console.WriteLine("Elapsed Time: {0}ms", timer.ElapsedMilliseconds);
        }
    }

}
