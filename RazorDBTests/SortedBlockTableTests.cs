using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using RazorDB;
using System.IO;
using System.Diagnostics;

namespace RazorDBTests {

    [TestFixture]
    public class SortedBlockTableTests {

        [Test]
        public void ReadBlock() {

            var k0 = ByteArray.Random(40);
            var v0 = ByteArray.Random(200);
            var k1 = ByteArray.Random(40);
            var v1 = ByteArray.Random(200);

            var mt = new MemTable();
            mt.Add(k0, v0);
            mt.Add(k1, v1);

            string path = Path.GetFullPath("ReadBlockTest.mt");
            mt.WriteToSortedBlockTable(path);

            var sbt = new SortedBlockTable(path);
            byte[] block = sbt.ReadBlock(0);
            sbt.Close();

            Assert.AreEqual(Config.SortedBlockSize, block.Length);
            Assert.AreEqual(block[0], 40);
        }

        [Test]
        public void ReadKeys() {

            var mt = new MemTable();
            for (int i = 0; i < 10000; i++) {
                var k0 = ByteArray.Random(40);
                var v0 = ByteArray.Random(200);
                mt.Add(k0, v0);
            }

            string path = Path.GetFullPath("ReadKeys.mt");
            mt.WriteToSortedBlockTable(path);

            var sbt = new SortedBlockTable(path);

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
                Assert.True(lastKey.CompareTo(pair.Key) < 0);
                lastKey = pair.Key;
            }
            timer.Stop();
            Console.WriteLine("Read & verify sorted table at a throughput of {0} MB/s", (double)mt.Size / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

            sbt.Close();

        }

    }
}
