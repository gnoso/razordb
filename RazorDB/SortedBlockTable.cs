using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;

namespace RazorDB {

    // With this implementation, the maximum sized data that can be stored is ... block size >= keylen + valuelen + sizecounter (no more than 8 bytes)
    public class SortedBlockTableWriter {

        public SortedBlockTableWriter(string fileName) {
            _fileStream = new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.None, Config.SortedBlockSize, FileOptions.Asynchronous | FileOptions.SequentialScan);
            _buffer = new byte[Config.SortedBlockSize];
            _bufferPos = 0;
        }

        private FileStream _fileStream;
        private byte[] _buffer;
        private int _bufferPos;

        // Write a new key value pair to the output file. This method assumes that the data is being fed in key-sorted order.
        public void WritePair(ByteArray key, ByteArray value) {

            byte[] keySize = new byte[8];
            int keySizeLen = Helper.Get7BitEncodedInt(keySize, key.Length);
            byte[] valueSize = new byte[8];
            int valueSizeLen = Helper.Get7BitEncodedInt(valueSize, value.Length);
            
            int bytesNeeded = keySizeLen + key.Length + valueSizeLen + value.Length;

            // Do we need to write out a block before adding this key value pair?
            if ( (_bufferPos + bytesNeeded) > Config.SortedBlockSize ) {
                WriteBlock();
            }

            // write the data out to the buffer
            Array.Copy(keySize, 0, _buffer, _bufferPos, keySizeLen);
            _bufferPos += keySizeLen;
            Array.Copy(key.InternalBytes, 0, _buffer, _bufferPos, key.Length);
            _bufferPos += key.Length;
            Array.Copy(valueSize, 0, _buffer, _bufferPos, valueSizeLen);
            _bufferPos += valueSizeLen;
            Array.Copy(value.InternalBytes, 0, _buffer, _bufferPos, value.Length);
            _bufferPos += value.Length;

        }

        private int _outstandingWrites = 0;

        private void WriteBlock() {
            Interlocked.Increment(ref _outstandingWrites);
            _fileStream.BeginWrite(_buffer, 0, Config.SortedBlockSize, (asyncResult) => {
                _fileStream.EndWrite(asyncResult); 
                Interlocked.Decrement(ref _outstandingWrites);
            }, null);
            _buffer = new byte[Config.SortedBlockSize];
            _bufferPos = 0;
        }

        public void Close() {
            WriteBlock();

            // Spin and wait for all pending writes to complete
            while (_outstandingWrites != 0) Thread.Sleep(0);

            _fileStream.Close();
            _fileStream = null;
        }
    }

    public class SortedBlockTable {

    }
}
