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
using System.Security.Cryptography;

namespace RazorDBTests {

    [TestFixture]
    public class LargeValueTests {

        [TestFixtureSetUp]
        public void Setup() {
            string path = Path.GetFullPath("TestData");
            if (!Directory.Exists(path)) 
                Directory.CreateDirectory(path);
        }

        [Test]
        public void LargeDataSetGetTest() {

            string path = Path.GetFullPath("TestData\\LargeDataSetGetTest");
            int totalSize = 0;
            int num_items = 500;
            var timer = new Stopwatch();

            using (var db = new KeyValueStore(path)) {
                db.Truncate();

                // Generate a data value that is larger than the block size.
                var value = ByteArray.Random(Config.SortedBlockSize + 256);

                // Do it enough times to ensure a roll-over
                for (int i = 0; i < num_items; i++) {
                    var key = BitConverter.GetBytes(i);
                    db.Set(key, value.InternalBytes);
                    totalSize += value.InternalBytes.Length;
                }

                timer.Start();
                for (int i = 0; i < num_items; i++) {
                    var key = BitConverter.GetBytes(i);
                    Assert.AreEqual(value.InternalBytes, db.Get(key));
                }
                timer.Stop();

                Console.WriteLine("Randomized read throughput of {0} MB/s (avg {1} ms per lookup)", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0), (double)timer.Elapsed.TotalSeconds / (double)num_items);
            }
        }

        [Test]
        public void LargeDataSetGetWithIndexTest() {

            string path = Path.GetFullPath("TestData\\LargeDataSetGetWithIndexTest");
            int totalSize = 0;
            int num_items = 500;
            var timer = new Stopwatch();

            using (var db = new KeyValueStore(path)) {
                db.Truncate();

                // Generate a data value that is larger than the block size.
                var value = ByteArray.Random(Config.SortedBlockSize + 256);
                var indexed = new SortedDictionary<string, byte[]>();

                // Do it enough times to ensure a roll-over
                for (int i = 0; i < num_items; i++) {
                    var key = BitConverter.GetBytes(i);
                    indexed["Index"] = BitConverter.GetBytes(i+10);
                    db.Set(key, value.InternalBytes, indexed);
                    totalSize += value.InternalBytes.Length;
                }

                timer.Start();
                for (int i = 0; i < num_items; i++) {
                    var items = db.Find("Index", BitConverter.GetBytes(i + 10));
                    var val = items.First();
                    Assert.AreEqual(value.InternalBytes, val.Value);
                }
                timer.Stop();

                Console.WriteLine("Randomized read throughput of {0} MB/s (avg {1} ms per lookup)", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0), (double)timer.Elapsed.TotalSeconds / (double)num_items);
            }
        }

        [Test]
        public void LargeDataEnumerateTest() {

            string path = Path.GetFullPath("TestData\\LargeDataEnumerateTest");
            int totalSize = 0;
            int num_items = 500;
            var timer = new Stopwatch();

            using (var db = new KeyValueStore(path)) {
                db.Truncate();

                // Generate a data value that is larger than the block size.
                var value = ByteArray.Random(Config.SortedBlockSize + 256);

                // Do it enough times to ensure a roll-over
                for (int i = 0; i < num_items; i++) {
                    var key = BitConverter.GetBytes(i).Reverse().ToArray(); // this has to be little endian to sort in an obvious way
                    db.Set(key, value.InternalBytes);
                    totalSize += value.InternalBytes.Length;
                }

                int j=0;
                timer.Start();
                foreach (var pair in db.Enumerate()) {
                    var key = BitConverter.GetBytes(j).Reverse().ToArray();
                    Assert.AreEqual(key, pair.Key);
                    Assert.AreEqual(value.InternalBytes, pair.Value);
                    j++;
                }
                timer.Stop();

                Console.WriteLine("Randomized read throughput of {0} MB/s (avg {1} ms per lookup)", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0), (double)timer.Elapsed.TotalSeconds / (double)num_items);

            }
        }

        [Test]
        public void TestTooLargeData() {
            Assert.Throws<InvalidDataException>(() => {
                string path = Path.GetFullPath("TestData\\TestTooLargeData");
                using (var db = new KeyValueStore(path)) {
                    db.Set(Key.Random(10).KeyBytes, ByteArray.Random(Config.MaxLargeValueSize).InternalBytes);
                }
            });
        }

        byte[] GenerateBlock(int num) {
            byte[] block = new byte[num + 20];
            for (int i = 0; i < num; i++) {
                block[i] = (byte)(i & 0xFF);
            }

            // Calculate checksum and copy to the block
            SHA1Managed sha = new SHA1Managed();
            byte[] checksum = sha.ComputeHash(block, 0, num);
            checksum.CopyTo(block, num);

            return block;
        }

        private void CheckBlock(byte[] bytes) {
            int num = bytes.Length - 20;
            SHA1Managed sha = new SHA1Managed();
            byte[] checksum = sha.ComputeHash(bytes, 0, num);
            for (int i = 0; i < 20; i++) {
                Assert.AreEqual(checksum[i], bytes[i + num], "Checksum[{0}] = {1} differs from bytes[{2}] = {3}", i, checksum[i], i + num, bytes[i + num]);
            }
        }


        [Test]
        public void TestLargeAndSmallEvenWrites() {

            string path = Path.GetFullPath("TestData\\TestLargeAndSmallInterlacedWrites");
            using (var db = new KeyValueStore(path)) {

                db.Truncate();

                // Create a random set of keybytes
                List<byte[]> keys = new List<byte[]>();
                for (int i = 0; i < 10; i++) {
                    keys.Add(Key.Random(10).KeyBytes);
                }

                // Set Evens to large
                for (int i = 0; i < keys.Count; i++) {
                    var k = keys[i];
                    var v = ((i & 1) == 0) ? GenerateBlock(Config.MaxLargeValueSize - 100) : GenerateBlock(10);
                    db.Set(k, v);
                }

                // Now check the results
                for (int i = 0; i < keys.Count; i++) {
                    var k = keys[i];
                    var v = db.Get(k);
                    CheckBlock(v);
                    if ((i & 1) == 1) {
                        Assert.Less(v.Length, 100, " i = {0} should be small, but size={1}", i,  v.Length);
                    } else {
                        Assert.Greater(v.Length, 100, " i = {0} should be large, but size={1}", i, v.Length);
                    }
                }
            }
        }

        [Test]
        public void TestLargeAndSmallOddWrites() {

            string path = Path.GetFullPath("TestData\\TestLargeAndSmallInterlacedWrites");
            using (var db = new KeyValueStore(path)) {

                db.Truncate();

                // Create a random set of keybytes
                List<byte[]> keys = new List<byte[]>();
                for (int i = 0; i < 10; i++) {
                    keys.Add(Key.Random(10).KeyBytes);
                }

                // Set Odds to large
                for (int i = 0; i < keys.Count; i++) {
                    var k = keys[i];
                    var v = ((i & 1) == 1) ? GenerateBlock(Config.MaxLargeValueSize - 100) : GenerateBlock(10);
                    db.Set(k, v);
                }

                // Now check the results
                for (int i = 0; i < keys.Count; i++) {
                    var k = keys[i];
                    var v = db.Get(k);
                    CheckBlock(v);
                    if ((i & 1) == 0) {
                        Assert.Less(v.Length, 100, " i = {0} should be small, but size={1}", i, v.Length);
                    } else {
                        Assert.Greater(v.Length, 100, " i = {0} should be large, but size={1}", i, v.Length);
                    }
                }
            }
        }

        [Test]
        public void TestLargeAndSmallInterlacedWrites() {

            string path = Path.GetFullPath("TestData\\TestLargeAndSmallInterlacedWrites");

            // Create a random set of keybytes
            List<byte[]> keys = new List<byte[]>();
            for (int i = 0; i < 10; i++) {
                keys.Add( new Key(new[] { (byte)i, (byte)i }, 0).KeyBytes);
            }
           
            using (var db = new KeyValueStore(path)) {

                db.Truncate();
                db.Manifest.Logger = msg => Console.WriteLine(msg);

                // Set Evens to large
                for (int i = 0; i < keys.Count; i++) {
                    var k = keys[i];
                    if (((i & 1) == 0)) {
                        db.Set(k, GenerateBlock(Config.MaxLargeValueSize - 100));
                    } else {
                        db.Set(k, GenerateBlock(10));
                    }
                }

                // Set Odds to large
                for (int i = 0; i < keys.Count; i++) {
                    var k = keys[i];
                    if (((i & 1) == 1)) {
                        db.Set(k, GenerateBlock(Config.MaxLargeValueSize - 100));
                    } else {
                        db.Set(k, GenerateBlock(10));
                    }
                }

                // Now check the results
                for (int i = 0; i < keys.Count; i++) {
                    var k = keys[i];
                    var v = db.Get(k);
                    CheckBlock(v);
                    if ((i & 1) == 0) {
                        Assert.Less(v.Length, 100, " i = {0} should be small, but size={1}", i, v.Length);
                    } else {
                        Assert.Greater(v.Length, 100, " i = {0} should be large, but size={1}", i, v.Length);
                    }
                }
            }
        }
    }

}