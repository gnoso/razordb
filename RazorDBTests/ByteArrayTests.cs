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
using RazorDB;
using System.Diagnostics;
using System.Collections.Generic;

namespace RazorDBTests {

    [TestFixture]
    public class ByteArrayTests {

        [Test]
        public void Comparison() {

            var a0 = new ByteArray(new byte[] { 0 });
            var a2 = new ByteArray(new byte[] { 0, 1, 2, 4 });
            var a2B = new ByteArray(new byte[] { 0, 1, 2, 4 });
            var a3 = new ByteArray(new byte[] { 0, 1, 2, 5 });
            var a4 = new ByteArray(new byte[] { 0, 1, 2, 5, 6 });

            // Simple direct comparisons
            Assert.IsTrue(a2.CompareTo(a3) < 0);
            Assert.IsTrue(a3.CompareTo(a2) > 0);
            Assert.IsTrue(a2.CompareTo(a2B) == 0);

            // Length comparisons
            Assert.IsTrue(a4.CompareTo(a0) > 0);
            Assert.IsTrue(a3.CompareTo(a4) < 0);
        }

        //[Test]
        //public void TestComparisonSpeed() {

        //    var a = new ByteArray(new byte[] { 0 });
        //    var b = new ByteArray(new byte[] { 1 });

        //    Stopwatch timer = new Stopwatch();
        //    timer.Start();
        //    for (int i = 0; i < 1000000; i++) {
        //        Assert.True(a.CompareTo(b) < 0);
        //    }
        //    timer.Stop();
        //    Console.WriteLine("Elapsed Time (1 byte): {0} ms", timer.ElapsedMilliseconds);

        //    a = new ByteArray(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19 });
        //    b = new ByteArray(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 20 });

        //    timer = new Stopwatch();
        //    timer.Start();
        //    for (int i = 0; i < 1000000; i++) {
        //        Assert.True(a.CompareTo(b) < 0);
        //    }
        //    timer.Stop();
        //    Console.WriteLine("Elapsed Time (20 byte): {0} ms", timer.ElapsedMilliseconds);
        //}
    }

    [TestFixture]
    public class KeyTests {

        [Test]
        public void TestKey() {

            ByteArray keyBytes = ByteArray.Random(10);
            byte[] allBytes = new byte[keyBytes.Length +1 ];
            Array.Copy(keyBytes.InternalBytes, allBytes, keyBytes.Length);

            var keys = new List<Key>();
            for (int i = 99; i >= 0 ; i--) {
                keys.Add(new Key(keyBytes.InternalBytes, (byte)i));
            }
            keys.Sort();
            int j = 0;
            foreach (var k in keys) {
                Assert.AreEqual(11, k.Length);

                allBytes[allBytes.Length-1] = (byte)j;
                Assert.AreEqual(allBytes, k.InternalBytes);

                Assert.AreEqual(j, k.SequenceNum);
                j++;
            }

            var keyA = new Key(keyBytes.InternalBytes, 23);
            var keyB = Key.FromBytes(keyA.InternalBytes);
            Assert.AreEqual(keyA, keyB);
        }
    }

}
