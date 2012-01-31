using System;
using NUnit.Framework;
using RazorDB;
using System.Diagnostics;

namespace RazorDBTests {

    [TestFixture]
    public class ByteArrayTests {

        [Test]
        public void Comparison() {

            var a0 = new ByteArray(new byte[] { 0 });
            var a1 = new ByteArray(new byte[] { 0, 1, 2, 3 });
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

}
