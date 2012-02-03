using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using RazorDB;

namespace RazorDBTests {

    [TestFixture]
    public class LevelMergeTests {

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
