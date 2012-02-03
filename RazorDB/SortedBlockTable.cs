using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;

namespace RazorDB {

    // With this implementation, the maximum sized data that can be stored is ... block size >= keylen + valuelen + sizecounter (no more than 8 bytes)
    public class SortedBlockTableWriter {

        public SortedBlockTableWriter(string baseFileName, int level, int version) {
            string fileName = Config.SortedBlockTableFile(baseFileName, level, version);
            _fileStream = new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.None, Config.SortedBlockSize, FileOptions.Asynchronous | FileOptions.SequentialScan);
            _buffer = new byte[Config.SortedBlockSize];
            _bufferPos = 0;
            _pageIndex = new List<ByteArray>();
        }

        private FileStream _fileStream;
        private byte[] _buffer;
        private int _bufferPos;
        private IAsyncResult _async;
        private List<ByteArray> _pageIndex;
        private int dataBlocks = 0;
        private int indexBlocks = 0;
        private int totalBlocks = 0;

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

            // If we are at the beginning of the buffer, then add this key to the index.
            if (_bufferPos == 0)
                _pageIndex.Add(key);

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
            totalBlocks++;
        }

        private void WriteIndexKey(ByteArray key) {
            byte[] keySize = new byte[8];
            int keySizeLen = Helper.Encode7BitInt(keySize, key.Length);

            int bytesNeeded = keySizeLen + key.Length;

            // Do we need to write out a block before adding this key value pair?
            if ((_bufferPos + bytesNeeded) > Config.SortedBlockSize) {
                WriteBlock();
            }

            // write the data out to the buffer
            Array.Copy(keySize, 0, _buffer, _bufferPos, keySizeLen);
            _bufferPos += keySizeLen;
            Array.Copy(key.InternalBytes, 0, _buffer, _bufferPos, key.Length);
            _bufferPos += key.Length;
        }

        private void WriteIndex() {
            foreach (var key in _pageIndex) {
                WriteIndexKey(key);
            }
        }

        private void WriteMetadata() {
            MemoryStream ms = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(ms);

            writer.Write(Encoding.ASCII.GetBytes("@RAZORDB"));
            writer.Write7BitEncodedInt(totalBlocks + 1);
            writer.Write7BitEncodedInt(dataBlocks);
            writer.Write7BitEncodedInt(indexBlocks);

            byte[] metadata = ms.ToArray();
            Array.Copy(metadata, _buffer, metadata.Length);

            // Commit the block to disk and wait for the operation to complete
            WriteBlock();
            _fileStream.EndWrite(_async);
        }

        public void Close() {
            if (_fileStream != null) {
                // Write the last block of the data
                WriteBlock();
                dataBlocks = totalBlocks;
                // Write the Index entries
                WriteIndex();
                // Write the last block of the index
                WriteBlock();
                indexBlocks = totalBlocks - dataBlocks;
                // Write metadata block at the end
                WriteMetadata();
                _fileStream.Close();
            }
            _fileStream = null;
        }
    }

    public class SortedBlockTable {

        public SortedBlockTable(string baseFileName, int level, int version) {
            _baseFileName = baseFileName;
            _level = level;
            _version = version;
            string path = Config.SortedBlockTableFile(baseFileName, level, version);
            _fileStream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, Config.SortedBlockSize, FileOptions.Asynchronous);
            ReadMetadata();
        }
        private FileStream _fileStream;
        private string _baseFileName;
        private int _level;
        private int _version;
        private int _dataBlocks;
        private int _indexBlocks;
        private int _totalBlocks;

        private IAsyncResult BeginReadBlock(int blockNum) {
            _fileStream.Seek(blockNum * Config.SortedBlockSize, SeekOrigin.Begin);
            byte[] block = new byte[Config.SortedBlockSize];
            return _fileStream.BeginRead(block, 0, Config.SortedBlockSize, null, block);
        }

        private byte[] EndReadBlock(IAsyncResult async) {
            _fileStream.EndRead(async);
            return (byte[]) async.AsyncState;
        }

        private byte[] ReadBlock(int blockNum) {
            _fileStream.Seek(blockNum * Config.SortedBlockSize, SeekOrigin.Begin);
            byte[] block = new byte[Config.SortedBlockSize];
            _fileStream.Read(block, 0, Config.SortedBlockSize);
            return block;
        }

        private void ReadMetadata() {
            int numBlocks = (int)_fileStream.Length / Config.SortedBlockSize;
            MemoryStream ms = new MemoryStream(ReadBlock(numBlocks - 1));
            BinaryReader reader = new BinaryReader(ms);
            string checkString = Encoding.ASCII.GetString(reader.ReadBytes(8));
            if (checkString != "@RAZORDB") {
                throw new InvalidDataException("This does not appear to be a valid table file.");
            }
            _totalBlocks = reader.Read7BitEncodedInt();
            _dataBlocks = reader.Read7BitEncodedInt();
            _indexBlocks = reader.Read7BitEncodedInt();
            if (_totalBlocks != numBlocks) {
                throw new InvalidDataException("The file size does not match the metadata size.");
            }
            if (_totalBlocks != (_dataBlocks + _indexBlocks + 1)) {
                throw new InvalidDataException("Corrupted metadata.");
            }
        }

        public static bool Lookup(string baseFileName, int level, int version, Cache indexCache, ByteArray key, out ByteArray value) {
            SortedBlockTable sbt = new SortedBlockTable(baseFileName, level, version);
            try {
                int dataBlockNum = FindBlockForKey(baseFileName, level, version, indexCache, key);

                if (dataBlockNum >= 0 && dataBlockNum < sbt._dataBlocks) {
                    byte[] block = sbt.ReadBlock(dataBlockNum);
                    int offset = 0;
                    while (offset >= 0) {
                        var pair = ReadPair(block, ref offset);
                        if (pair.Key.Equals(key)) {
                            value = pair.Value;
                            return true;
                        }
                    }
                } 
            } finally {
                sbt.Close();
            }
            value = new ByteArray();
            return false;
        }

        private static int FindBlockForKey(string baseFileName, int level, int version, Cache indexCache, ByteArray key) {
            ByteArray[] index = indexCache.GetBlockTableIndex(baseFileName, level, version);
            int dataBlockNum = Array.BinarySearch(index, key);
            if (dataBlockNum < 0) {
                dataBlockNum = ~dataBlockNum - 1;
            }
            return dataBlockNum;
        }

        public IEnumerable<KeyValuePair<ByteArray, ByteArray>> Enumerate() {

            var asyncResult = BeginReadBlock(0);

            for (int i = 0; i < _dataBlocks; i++) {
                
                // wait on last block read to complete so we can start processing the data
                byte[] block = EndReadBlock(asyncResult);

                // Go ahead and kick off the next block read asynchronously while we parse the last one
                if (i < _dataBlocks)
                    asyncResult = BeginReadBlock(i + 1);

                int offset = 0;
                while (offset >= 0) {
                    yield return ReadPair(block, ref offset);
                }
            }
        }

        public IEnumerable<KeyValuePair<ByteArray, ByteArray>> EnumerateFromKey(Cache indexCache, ByteArray key) {

            int startingBlock = FindBlockForKey(_baseFileName, _level, _version, indexCache, key);
            if (startingBlock < 0)
                startingBlock = 0;
            if (startingBlock < _dataBlocks) {

                var asyncResult = BeginReadBlock(startingBlock);

                for (int i = startingBlock; i < _dataBlocks; i++) {

                    // wait on last block read to complete so we can start processing the data
                    byte[] block = EndReadBlock(asyncResult);

                    // Go ahead and kick off the next block read asynchronously while we parse the last one
                    if (i < _dataBlocks)
                        asyncResult = BeginReadBlock(i + 1);

                    int offset = 0;

                    // On the first block, we need to seek to the key first
                    if ( i == startingBlock) {
                        while (offset >= 0) {
                            var pair = ReadPair(block, ref offset);
                            if (pair.Key.CompareTo(key) >= 0) {
                                yield return pair;
                                break;
                            }
                        }
                    }

                    // Now loop through the rest of the block
                    while (offset >= 0) {
                        yield return ReadPair(block, ref offset);
                    }
                }
            }

        }

        private IEnumerable<ByteArray> EnumerateIndex() {

            var asyncResult = BeginReadBlock(_dataBlocks);
            var endIndexBlocks = (_dataBlocks + _indexBlocks);

            for (int i = _dataBlocks; i < endIndexBlocks; i++) {

                // wait on last block read to complete so we can start processing the data
                byte[] block = EndReadBlock(asyncResult);

                // Go ahead and kick off the next block read asynchronously while we parse the last one
                if (i < endIndexBlocks)
                    asyncResult = BeginReadBlock(i + 1);

                int offset = 0;
                while (offset >= 0) {
                    yield return ReadKey(block, ref offset);
                }
            }
        }

        public ByteArray[] GetIndex() {
            return EnumerateIndex().ToArray();
        }

        private static ByteArray ReadKey(byte[] block, ref int offset) {
            int keySize = Helper.Decode7BitInt(block, ref offset);
            var key = ByteArray.From(block, offset, keySize);
            offset += keySize;

            // if the next keySize bit is zero then we have exhausted this block. Set to -1 to terminate enumeration
            if (block[offset] == 0)
                offset = -1;

            return key;
        }

        private static KeyValuePair<ByteArray, ByteArray> ReadPair(byte[] block, ref int offset) {
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

        public struct MergeTablePair {
            int Level;
            int Version;
        }
        public static IEnumerable<MergeTablePair> MergeTables(Manifest mf, string baseFileName, IEnumerable<MergeTablePair> tables) {
            return null;
        }

        public void Close() {
            _fileStream.Close();
            _fileStream = null;
        }
    }
}
