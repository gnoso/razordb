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
            var sortedCollections = collections.Select(list => list.OrderBy(i => i));

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

            var enumerators = new List<IOrderedEnumerable<int>>();
            Assert.AreEqual(0, MergeEnumerator.Merge(enumerators).Count());
            enumerators.Add(new List<int>().OrderBy( e => e) );
            enumerators.Add(new List<int>().OrderBy(e => e));
            enumerators.Add(new List<int>().OrderBy(e => e));
            Assert.AreEqual(0, MergeEnumerator.Merge(enumerators).Count());
        }

        [Test]
        public void LevelMergeEmpty() {

            int num_tables_to_merge = 4;
            int items_per_table = 2500;
            for (int i = 0; i < num_tables_to_merge; i++) {
                var mt = new MemTable();
                for (int j=0; j < items_per_table; j++) {
                    var randKey = ByteArray.Random(40);
                    var randVal = ByteArray.Random(512);
                    mt.Add(randKey, randVal);
                }
                mt.WriteToSortedBlockTable("LevelMergeEmpty", 0, i);
            }
        }
    }
}
