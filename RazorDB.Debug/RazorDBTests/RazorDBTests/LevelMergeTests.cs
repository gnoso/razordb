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
    public class LevelMergeTests {

        [Test]
        public void TestMergeIterator() {

            Random r = new Random();
            int totalElements = 0;
            // Create 10 randomly sized lists of random numbers
            List<IEnumerable<int>> collections = new List<IEnumerable<int>>();
            for (int i = 0; i < 10; i++) {
                List<int> randomData = new List<int>();
                for (int j = 0; j < r.Next(100); j++) {
                    randomData.Add(r.Next());
                    totalElements++;
                }
                collections.Add(randomData);
            }
            // Sort all the individual lists
            var sortedCollections = collections.Select(list => list.OrderBy(i => i).AsEnumerable());

            // Now scan through the merged list and make sure the result is ordered
            int lastNum = int.MinValue;
            int numElements = 0;
            foreach (var num in MergeEnumerator.Merge(sortedCollections)) {
                Assert.LessOrEqual(lastNum, num);
                lastNum = num;
                numElements++;
            }
            Assert.AreEqual(totalElements, numElements);
        }

        [Test]
        public void TestEmptyMergeIterator() {

            var enumerators = new List<IEnumerable<int>>();
            Assert.AreEqual(0, MergeEnumerator.Merge(enumerators).Count());
            enumerators.Add(new List<int>().OrderBy(e => e));
            enumerators.Add(new List<int>().OrderBy(e => e));
            enumerators.Add(new List<int>().OrderBy(e => e));
            Assert.AreEqual(0, MergeEnumerator.Merge(enumerators).Count());
        }

        [Test]
        public void TestDuplicateMergeIterator() {
            var enumerators = new List<IEnumerable<int>>();
            enumerators.Add(Enumerable.Range(1, 10));
            Assert.AreEqual(55, MergeEnumerator.Merge(enumerators).Sum());

            enumerators = new List<IEnumerable<int>>();
            enumerators.Add(Enumerable.Range(1, 10));
            enumerators.Add(Enumerable.Range(2, 9));
            enumerators.Add(Enumerable.Range(1, 10));
            Assert.AreEqual(55, MergeEnumerator.Merge(enumerators).Sum());

            enumerators = new List<IEnumerable<int>>();
            enumerators.Add(new List<int>());
            enumerators.Add(Enumerable.Range(2, 9));
            enumerators.Add(Enumerable.Range(1, 10));
            Assert.AreEqual(55, MergeEnumerator.Merge(enumerators).Sum());
        }

        [Test]
        public void LevelMergeReadTest() {

            string path = Path.GetFullPath("TestData\\LevelMergeReadTest");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            int num_tables_to_merge = 4;
            int items_per_table = 2500;
            int totalData = 0;
            for (int i = 0; i < num_tables_to_merge; i++) {
                var mt = new MemTable();
                for (int j = 0; j < items_per_table; j++) {
                    var randKey = Key.Random(40);
                    var randVal = Value.Random(512);
                    mt.Add(randKey, randVal);
                }
                mt.WriteToSortedBlockTable("TestData\\LevelMergeReadTest", 0, i);
                totalData += mt.Size;
            }
            var tables = new List<IEnumerable<KeyValuePair<Key, Value>>>();
            var sbts = new List<SortedBlockTable>();
            var cache = new RazorCache();
            for (int j = 0; j < num_tables_to_merge; j++) {
                var sbt = new SortedBlockTable(cache, "TestData\\LevelMergeReadTest", 0, j);
                tables.Add(sbt.Enumerate());
                sbts.Add(sbt);
            }

            int ct = 0;
            Key key = Key.FromBytes(new byte[] { 0, 0 });
            var timer = new Stopwatch();
            timer.Start();
            foreach (var pair in MergeEnumerator.Merge(tables, p => p.Key)) {
                Assert.True(key.CompareTo(pair.Key) < 0);
                key = pair.Key;
                ct++;
            }
            timer.Stop();

            sbts.ForEach(s => s.Close());

            Console.WriteLine("Scanned through a multilevel merge at a throughput of {0} MB/s", (double)totalData / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));
        }

        [Test]
        public void LevelMergeReadTest2() {

            string path = Path.GetFullPath("TestData\\LevelMergeReadTest2");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            int num_tables_to_merge = 4;
            int items_per_table = 2500;
            int totalData = 0;
            for (int i = 0; i < num_tables_to_merge; i++) {
                var mt = new MemTable();
                for (int j = 0; j < items_per_table; j++) {
                    var randKey = Key.Random(40);
                    var randVal = Value.Random(512);
                    mt.Add(randKey, randVal);
                }
                mt.WriteToSortedBlockTable("TestData\\LevelMergeReadTest2", 0, i);
                totalData += mt.Size;
            }

            var cache = new RazorCache();
            int ct = 0;
            Key key = new Key(new ByteArray(new byte[] { 0 }));
            var timer = new Stopwatch();
            timer.Start();
            foreach (var pair in SortedBlockTable.EnumerateMergedTablesPreCached(cache, "TestData\\LevelMergeReadTest2",
                new List<PageRef>{
                                                              new PageRef { Level = 0, Version = 0},
                                                              new PageRef { Level = 0, Version = 1},
                                                              new PageRef { Level = 0, Version = 2},
                                                              new PageRef { Level = 0, Version = 3}
                }, ExceptionHandling.ThrowAll, null)) {
                Assert.True(key.CompareTo(pair.Key) < 0);
                key = pair.Key;
                ct++;
            }
            timer.Stop();

            Console.WriteLine("Scanned through a multilevel merge at a throughput of {0} MB/s", (double)totalData / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));
        }

        [Test]
        public void LevelMergeOutputTest() {

            string path = Path.GetFullPath("TestData\\LevelMergeOutputTest");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            int num_tables_to_merge = 4;
            int items_per_table = 2500;
            int totalData = 0;
            for (int i = 0; i < num_tables_to_merge; i++) {
                var mt = new MemTable();
                for (int j = 0; j < items_per_table; j++) {
                    var randKey = Key.Random(40);
                    var randVal = Value.Random(512);
                    mt.Add(randKey, randVal);
                }
                mt.WriteToSortedBlockTable("TestData\\LevelMergeOutputTest", 0, i);
                totalData += mt.Size;
            }

            var cache = new RazorCache();
            var timer = new Stopwatch();
            timer.Start();

            Manifest mf = new Manifest("TestData\\LevelMergeOutputTest");
            SortedBlockTable.MergeTables(cache, mf, 1, new List<PageRef>{
                                                                            new PageRef { Level = 0, Version = 0},
                                                                            new PageRef { Level = 0, Version = 1},
                                                                            new PageRef { Level = 0, Version = 2},
                                                                            new PageRef { Level = 0, Version = 3}
                                                                        }, ExceptionHandling.ThrowAll, null);
            timer.Stop();

            Console.WriteLine("Wrote a multilevel merge at a throughput of {0} MB/s", (double)totalData / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));
        }

        [Test]
        public void LevelMergeDuplicateValuesTest() {

            string path = Path.GetFullPath("TestData\\LevelMergeDuplicateValuesTest");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);
            foreach (string file in Directory.GetFiles(path)) {
                File.Delete(file);
            }

            int num_tables_to_merge = 4;
            int items_per_table = 2500;
            int totalData = 0;
            for (int i = 0; i < num_tables_to_merge; i++) {
                var mt = new MemTable();
                for (int j = 0; j < items_per_table; j++) {
                    int numToStore = j % 100;
                    var key = new Key(new ByteArray(BitConverter.GetBytes(numToStore)));
                    var value = new Value(BitConverter.GetBytes(j));
                    mt.Add(key, value);
                }
                mt.WriteToSortedBlockTable("TestData\\LevelMergeDuplicateValuesTest", 0, i);
                totalData += mt.Size;
            }

            var cache = new RazorCache();
            var timer = new Stopwatch();
            timer.Start();

            Manifest mf = new Manifest("TestData\\LevelMergeDuplicateValuesTest");
            SortedBlockTable.MergeTables(cache, mf, 1, new List<PageRef>{
                                                                            new PageRef { Level = 0, Version = 0},
                                                                            new PageRef { Level = 0, Version = 1},
                                                                            new PageRef { Level = 0, Version = 2},
                                                                            new PageRef { Level = 0, Version = 3}
                                                                        }, ExceptionHandling.ThrowAll, null);
            timer.Stop();

            // Open the block table and scan it to check the stored values
            var sbt = new SortedBlockTable(cache, mf.BaseFileName, 1, 1);
            try {
                var pairs = sbt.Enumerate().ToList();
                Assert.AreEqual(100, pairs.Count());
                Assert.AreEqual(2400, BitConverter.ToInt32(pairs.First().Value.ValueBytes, 0));
            } finally {
                sbt.Close();
            }

            Console.WriteLine("Wrote a multilevel merge with duplicates at a throughput of {0} MB/s", (double)totalData / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));
        }

    }
}
