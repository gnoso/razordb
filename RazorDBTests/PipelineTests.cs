using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using RazorDB;
using System.Threading;

namespace RazorDBTests {
    [TestFixture]
    public class PipelineTests {

        [Test]
        public void BufferPool() {

            BufferPool bp = new BufferPool(10000);
            int ct = 1000;
            for (int i = 0; i < ct; i++) {
                byte[] b = bp.GetBuffer();
                Assert.AreEqual(10000, b.Length);
                bp.ReturnBuffer(b);
            }
            Console.WriteLine("{0} allocations for {1} uses of buffer pool.", bp.NumAllocations, ct);
        }
    }
}
