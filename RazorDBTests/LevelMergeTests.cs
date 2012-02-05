using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using RazorDB;
using System.Diagnostics;

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
                for (int j=0; j < r.Next(100); j++) {
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
            enumerators.Add(new List<int>().OrderBy( e => e) );
            enumerators.Add(new List<int>().OrderBy(e => e));
            enumerators.Add(new List<int>().OrderBy(e => e));
            Assert.AreEqual(0, MergeEnumerator.Merge(enumerators).Count());
        }

        [Test]
        public void LevelMergeReadTest() {

            int num_tables_to_merge = 4;
            int items_per_table = 2500;
            int totalData = 0;
            for (int i = 0; i < num_tables_to_merge; i++) {
                var mt = new MemTable();
                for (int j=0; j < items_per_table; j++) {
                    var randKey = ByteArray.Random(40);
                    var randVal = ByteArray.Random(512);
                    mt.Add(randKey, randVal);
                }
                mt.WriteToSortedBlockTable("LevelMergeReadTest", 0, i);
                totalData += mt.Size;
            }
            var tables = new List<IEnumerable<KeyValuePair<ByteArray,ByteArray>>>();
            var sbts = new List<SortedBlockTable>();
            for (int j=0; j < num_tables_to_merge; j++) {
                var sbt = new SortedBlockTable("LevelMergeReadTest", 0, j);
                tables.Add(sbt.Enumerate());
                sbts.Add(sbt);
            }

            int ct = 0;
            ByteArray key = new ByteArray(new byte[]{0});
            var timer = new Stopwatch();
            timer.Start();
            foreach (var pair in MergeEnumerator.Merge(tables, pair => pair.Key )) {
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

            int num_tables_to_merge = 4;
            int items_per_table = 2500;
            int totalData = 0;
            for (int i = 0; i < num_tables_to_merge; i++) {
                var mt = new MemTable();
                for (int j = 0; j < items_per_table; j++) {
                    var randKey = ByteArray.Random(40);
                    var randVal = ByteArray.Random(512);
                    mt.Add(randKey, randVal);
                }
                mt.WriteToSortedBlockTable("LevelMergeReadTest2", 0, i);
                totalData += mt.Size;
            }

            int ct = 0;
            ByteArray key = new ByteArray(new byte[] { 0 });
            var timer = new Stopwatch();
            timer.Start();
            foreach (var pair in SortedBlockTable.EnumerateMergedTables("LevelMergeReadTest2", 
                new List<PageRef>{
                                                              new PageRef { Level = 0, Version = 0},
                                                              new PageRef { Level = 0, Version = 1},
                                                              new PageRef { Level = 0, Version = 2},
                                                              new PageRef { Level = 0, Version = 3}
                })) {
                Assert.True(key.CompareTo(pair.Key) < 0);
                key = pair.Key;
                ct++;
            }
            timer.Stop();

            Console.WriteLine("Scanned through a multilevel merge at a throughput of {0} MB/s", (double)totalData / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));
        }

        [Test]
        public void LevelMergeOutputTest() {

            int num_tables_to_merge = 4;
            int items_per_table = 2500;
            int totalData = 0;
            for (int i = 0; i < num_tables_to_merge; i++) {
                var mt = new MemTable();
                for (int j = 0; j < items_per_table; j++) {
                    var randKey = ByteArray.Random(40);
                    var randVal = ByteArray.Random(512);
                    mt.Add(randKey, randVal);
                }
                mt.WriteToSortedBlockTable("LevelMergeOutputTest", 0, i);
                totalData += mt.Size;
            }

            ByteArray key = new ByteArray(new byte[] { 0 });
            var timer = new Stopwatch();
            timer.Start();
            
            Manifest mf = new Manifest("LevelMergeOutputTest");
            var outputTables = SortedBlockTable.MergeTables(mf, 1, new List<PageRef>{
                                                                                                new PageRef { Level = 0, Version = 0},
                                                                                                new PageRef { Level = 0, Version = 1},
                                                                                                new PageRef { Level = 0, Version = 2},
                                                                                                new PageRef { Level = 0, Version = 3}
                                                                                            });
            timer.Stop();

            Console.WriteLine("Wrote a multilevel merge at a throughput of {0} MB/s", (double)totalData / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));
        }

    }
}
