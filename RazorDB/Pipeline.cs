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
        private int size;
        private LinkedList<byte[]> buffers;
        private int numAllocations = 0;

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

    public interface ProcessingSegment {

    }

    public class Pipeline {


    }
}
