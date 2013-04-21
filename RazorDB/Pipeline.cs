using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace RazorDB {

    // Manage a buffer pool to save allocation costs by reusing buffer blocks
    // For simplicity we assume that all buffers are the same size
    public class BufferPool {

        public BufferPool(int bufferSize) {
            size = bufferSize;
            buffers = new LinkedList<byte[]>();
        }
        int size;
        LinkedList<byte[]> buffers;
        int numAllocations = 0;

        public int NumAllocations {
            get { return numAllocations; }
        }

        public byte[] GetBuffer() {
            lock (buffers) {
                if (buffers.First == null) {
                    numAllocations++;
                    return new byte[size];
                } else {
                    var val = buffers.First.Value;
                    buffers.RemoveFirst();
                    return val;
                }
            }
        }

        public void ReturnBuffer(byte[] buffer) {
            // memset(0) the buffer
            Array.Clear(buffer, 0, buffer.Length);
            // add buffer back to the buffer pool
            lock (buffers) {
                buffers.AddLast(buffer);
            }
        }
    }

    public class OrderingQueue <T> {

        SortedList<int, T> list = new SortedList<int, T>();
        int seqNum = 0;

        public void Enqueue(int sequence, T item) {
            lock (list) {
                list.Add(sequence, item);
            }
        }

        public bool CanDequeue {
            get {
                lock (list) { return list.Count > 0 && list.First().Key == seqNum; }
            }
        }

        public T Dequeue() {
            lock (list) {
                if (list.Count > 0) {
                    var v = list.First().Value;
                    list.Remove(seqNum);
                    seqNum++;
                    return v;
                } else {
                    throw new InvalidOperationException("Queue is empty.");
                }
            }
        }

    }

    public class Pipeline<T> : IDisposable {

        public Pipeline(Action<T> work, int queueLimit = 10, int numThreads = 1) {
            if (work == null) {
                throw new ArgumentNullException();
            }
            QueueLimit = queueLimit;
            this.work = work;
            this.queue = new Queue<T>();
            this.orderingQueue = new OrderingQueue<T>();
            this.queueCapacity = new Semaphore(queueLimit, queueLimit);

            this.workerThreads = new Thread[numThreads];
            for (int i = 0; i < numThreads; i++) {
                this.workerThreads[i] = new Thread(ThreadMain);
                this.workerThreads[i].Start();
            }
        }
        ~Pipeline() {
            Dispose();
        }
        public void Dispose() {
            running = false;
            foreach (var t in workerThreads) {
                t.Abort();
            }
        }
        volatile bool running = true;

        Action<T> work;
        public Action<Exception> OnError { get; set; }
        Queue<T> queue;
        Thread[] workerThreads;
        public int MaxQueueSize { get; set; }
        public int QueueLimit { get; set; }
        Semaphore queueCapacity;
        OrderingQueue<T> orderingQueue;

        public void OrderedPush(int seqNum, T value) {
            lock (orderingQueue) {
                // Push a "packet" on the ordered queue
                orderingQueue.Enqueue(seqNum, value);
                // Pull off as many as we can (as many as can be done in order)
                while (orderingQueue.CanDequeue) {
                    var v = orderingQueue.Dequeue();
                    Push(v);
                }
            }
        }

        public void Push(T value) {
            if (!running) {
                throw new InvalidOperationException("Pipeline is not running.");
            }
            // Apply backpressure through use of a semaphore if the queue gets too backed up
            queueCapacity.WaitOne();

            // Go ahead and put our value on the queue
            lock (queue) {
                queue.Enqueue(value);
                queuePopulatedEvent.Set();
                MaxQueueSize = Math.Max(MaxQueueSize, queue.Count);
            }
        }

        public void WaitForDrain() {
            running = false;
            queuePopulatedEvent.Set();
            foreach (var t in workerThreads) {
                t.Join();
            }
        }

        ManualResetEvent queuePopulatedEvent = new ManualResetEvent(true);

        void ThreadMain() {
            while (true) {
                // If the queue ran empty, then wait on the event
                if (running)
                    queuePopulatedEvent.WaitOne();

                // Grab the next item from the queue
                T element = default(T);
                lock (queue) {
                    if (queue.Count > 0) {
                        // Pull a work package off the queue
                        element = queue.Dequeue();

                        // Add more capacity back, so we can accept more pushes
                        queueCapacity.Release();

                        // If the queue has run empty, then set an event so we don't spin waiting for more work
                        if (queue.Count == 0 && running) {
                            queuePopulatedEvent.Reset();
                        }
                    } else if (!running) {
                        // we've signalled done and the queue is empty, so shut down the thread
                        break;
                    } else {
                        // The queue was actually empty, so reset and jump back to the top of the loop
                        if (running)
                            queuePopulatedEvent.Reset();
                        continue;
                    }
                }

                // Do the work
                try {
                    work(element);
                } catch (Exception e) {
                    if (OnError != null)
                        OnError(e);
                }
            }
        }

    }

}
