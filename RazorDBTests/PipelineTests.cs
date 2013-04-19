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

        [Test]
        public void PipelineTest() {

            Pipeline<int> pipeline = new Pipeline<int>((i) => {
                Console.WriteLine("Processing {0}.", i);
            });

            Console.WriteLine("Starting.");
            pipeline.Push(0);
            Console.WriteLine("Pushed 0.");
            pipeline.Push(1);
            Console.WriteLine("Pushed 1.");
            pipeline.Push(2);
            Console.WriteLine("Pushed 2.");
            pipeline.Push(3);
            Console.WriteLine("Pushed 3.");
            pipeline.WaitForDrain();
            Console.WriteLine("Done Waiting.");
        }

        [Test]
        public void PipelineTest2() {

            Pipeline<int> Waiting2Pipeline = new Pipeline<int>((i) => {
                Thread.Sleep(50);
                Console.WriteLine("Waited 50ms. => {0}", i);
            });
            Pipeline<int> DoublePipeline = new Pipeline<int>((i) => {
                Console.WriteLine("Double {0} => {1}", i, i*2);
                Waiting2Pipeline.Push(i * 2);
            });
            Pipeline<int> HalfPipeline = new Pipeline<int>((i) => {
                Console.WriteLine("Half {0} => {1}", i, i/2);
                DoublePipeline.Push(i/2);
            });

            HalfPipeline.Push(10);
            Console.WriteLine("Pushed 10.");
            HalfPipeline.Push(500);
            Console.WriteLine("Pushed 500.");
            HalfPipeline.Push(1000);
            Console.WriteLine("Pushed 1000.");

            HalfPipeline.WaitForDrain();
            DoublePipeline.WaitForDrain();
            Waiting2Pipeline.WaitForDrain();
            Console.WriteLine("Done.");
        }

        [Test]
        public void PipelineBackpressureTest() {

            int finalTotal = 0;

            Pipeline<int> Waiting2Pipeline = new Pipeline<int>((i) => {
                Thread.Sleep(1);
                finalTotal += i;
            },10);
            Pipeline<int> DoublePipeline = new Pipeline<int>((i) => {
                Waiting2Pipeline.Push(i / 2);
            },20);
            Pipeline<int> HalfPipeline = new Pipeline<int>((i) => {
                DoublePipeline.Push(i * 2);
            },5);

            for (int i = 0; i < 200; i++) {
                HalfPipeline.Push(i);
            }

            Console.WriteLine("Done pushing.");
            HalfPipeline.WaitForDrain();
            DoublePipeline.WaitForDrain();
            Waiting2Pipeline.WaitForDrain();
            Console.WriteLine("Max 0: {0} Max 1: {1} Max 2: {2}", HalfPipeline.MaxQueueSize, DoublePipeline.MaxQueueSize, Waiting2Pipeline.MaxQueueSize);
            Console.WriteLine("Done.");
            Assert.AreEqual(19900, finalTotal);
        }

        [Test,ExpectedException(typeof(InvalidOperationException))]
        public void StopTest() {
            Pipeline<int> pipeline = new Pipeline<int>((i) => {
                Console.WriteLine("Processing {0}.", i);
            });

            for (int i = 0; i < 5; i++) {
                pipeline.Push(i);
            }

            pipeline.WaitForDrain();
            pipeline.Push(0);
        }

        [Test]
        public void DisposeTest() {
            bool failed = false;
            Pipeline<int> pipeline = new Pipeline<int>((i) => {
                Thread.Sleep(50);
                failed = true; // this should not run because the dispose will kill the thread before it gets scheduled
            });

            pipeline.Push(1);
            pipeline.Dispose();
            Assert.True(!failed);
        }

        [Test, ExpectedException(typeof(InvalidOperationException))]
        public void DisposeTest2() {
            bool failed = false;
            Pipeline<int> pipeline = new Pipeline<int>((i) => {
                Thread.Sleep(50);
                failed = true; // this should not run because the dispose will kill the thread before it gets scheduled
            });

            pipeline.Push(1);
            pipeline.Dispose();
            pipeline.Push(0);
            Assert.True(!failed);
        }

        [Test]
        public void OnErrorTest2() {
            Pipeline<int> pipeline = new Pipeline<int>((i) => {
                throw new NotSupportedException();
            });
            Exception error = null;
            pipeline.OnError = delegate(Exception e) {
                error = e;
            };

            pipeline.Push(1);
            pipeline.WaitForDrain();
            Assert.True(error != null && error is NotSupportedException);
        }

        [Test]
        public void MultiThreadedPipeline() {

            int finalTotal = 0;
            int lastNumber = -1;
            bool pipelineInOrder = true;

            Pipeline<int> FinishPipeline = new Pipeline<int>((i) => {
                if (Interlocked.Increment(ref lastNumber) != i) {
                    pipelineInOrder = false;
                }
                finalTotal += i;
            });
            Pipeline<int> WaitPipeline = new Pipeline<int>((i) => {
                if ((i & 1) == 0) {
                    Thread.Sleep(5); // force items to come in out of order
                } else {
                    Thread.Sleep(1);
                }
                FinishPipeline.OrderedPush(i,i);
            },10,2);
            Pipeline<int> FirstPipeline = new Pipeline<int>((i) => {
                WaitPipeline.Push(i);
            });

            for (int i = 0; i < 200; i++) {
                FirstPipeline.Push(i);
            }

            Console.WriteLine("Done pushing.");
            FirstPipeline.WaitForDrain();
            WaitPipeline.WaitForDrain();
            FinishPipeline.WaitForDrain();
            Console.WriteLine("Max 0: {0} Max 1: {1} Max 2: {2}", FirstPipeline.MaxQueueSize, WaitPipeline.MaxQueueSize, FinishPipeline.MaxQueueSize);
            Console.WriteLine("Done.");
            Assert.AreEqual(19900, finalTotal);
            Assert. True(pipelineInOrder);
        }

        [Test]
        public void OrderedQueueTest() {

            var q = new OrderingQueue<string>();

            q.Enqueue(0, "Zero");
            Assert.True(q.CanDequeue);
            Assert.AreEqual("Zero",q.Dequeue());

            q.Enqueue(2, "Two");
            Assert.False(q.CanDequeue);

            q.Enqueue(3, "Three");
            Assert.False(q.CanDequeue);

            q.Enqueue(1, "One");
            Assert.True(q.CanDequeue);
            Assert.AreEqual("One",q.Dequeue());
            Assert.True(q.CanDequeue);
            Assert.AreEqual("Two",q.Dequeue());
            Assert.True(q.CanDequeue);
            Assert.AreEqual("Three",q.Dequeue());
            Assert.False(q.CanDequeue);

        }

    }
}
