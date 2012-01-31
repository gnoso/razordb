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
        private IAsyncResult _async;

        // Write a new key value pair to the output file. This method assumes that the data is being fed in key-sorted order.
        public void WritePair(ByteArray key, ByteArray value) {

            byte[] keySize = new byte[8];
            int keySizeLen = Helper.Encode7BitInt(keySize, key.Length);
            byte[] valueSize = new byte[8];
            int valueSizeLen = Helper.Encode7BitInt(valueSize, value.Length);
            
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

        private void WriteBlock() {
            // make sure any outstanding writes are completed
            if (_async != null) {
                _fileStream.EndWrite(_async);
            }
            _async = _fileStream.BeginWrite(_buffer, 0, Config.SortedBlockSize, null, null);
            _buffer = new byte[Config.SortedBlockSize];
            _bufferPos = 0;
        }

        public void Close() {
            WriteBlock();
            _fileStream.EndWrite(_async);
            _fileStream.Close();
            _fileStream = null;
        }
    }

    public class SortedBlockTable {

        public SortedBlockTable(string fileName) {
            _fileStream = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read, Config.SortedBlockSize, FileOptions.Asynchronous);
        }
        private FileStream _fileStream;

        public byte[] ReadBlock(int blockNum) {
            _fileStream.Seek(blockNum * Config.SortedBlockSize, SeekOrigin.Begin);
            byte[] block = new byte[Config.SortedBlockSize];
            _fileStream.Read(block, 0, Config.SortedBlockSize);
            return block;
        }

        public IAsyncResult BeginReadBlock(int blockNum) {
            _fileStream.Seek(blockNum * Config.SortedBlockSize, SeekOrigin.Begin);
            byte[] block = new byte[Config.SortedBlockSize];
            return _fileStream.BeginRead(block, 0, Config.SortedBlockSize, null, block);
        }

        public byte[] EndReadBlock(IAsyncResult async) {
            _fileStream.EndRead(async);
            return (byte[]) async.AsyncState;
        }

        public IEnumerable<KeyValuePair<ByteArray, ByteArray>> Enumerate() {

            int numBlocks = (int) _fileStream.Length / Config.SortedBlockSize;
            var asyncResult = BeginReadBlock(0);

            for (int i = 0; i < numBlocks; i++) {
                
                // wait on last block read to complete so we can start processing the data
                byte[] block = EndReadBlock(asyncResult);

                // Go ahead and kick off the next block read asynchronously while we parse the last one
                if (i < numBlocks)
                    asyncResult = BeginReadBlock(i + 1);

                int offset = 0;
                while (offset >= 0) {
                    yield return ReadPair(block, ref offset);
                }
            }
        }

        private KeyValuePair<ByteArray, ByteArray> ReadPair(byte[] block, ref int offset) {
            int keySize = Helper.Decode7BitInt(block, ref offset);
            var key = ByteArray.From(block, offset, keySize);
            offset += keySize;
            int valueSize = Helper.Decode7BitInt(block, ref offset);
            var val = ByteArray.From(block, offset, valueSize);
            offset += valueSize;

            // if the next keySize bit is zero then we have exhausted this block. Set to -1 to terminate enumeration
            if (block[offset] == 0)
                offset = -1;

            return new KeyValuePair<ByteArray,ByteArray>(key,val);
        }

        public void Close() {
            _fileStream.Close();
            _fileStream = null;
        }
    }
}
