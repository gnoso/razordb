using System;
using NUnit.Framework;
using RazorDB;

namespace RazorDBTests {

    [TestFixture]
    public class ByteArrayTests {

        [Test]
        public void Comparison() {

            var a0 = new ByteArray(new byte[] { 0 });
            var a1 = new ByteArray(new byte[] { 0, 1, 2, 3 });
            var a2 = new ByteArray(new byte[] { 0, 1, 2, 4 });
            var a3 = new ByteArray(new byte[] { 0, 1, 2, 5 });
            var a4 = new ByteArray(new byte[] { 0, 1, 2, 5, 6 });

            // Simple direct comparisons
            Assert.IsTrue(a2.CompareTo(a3) < 0);
            Assert.IsTrue(a3.CompareTo(a2) > 0);
        }
    }

}
