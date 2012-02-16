using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;

namespace RazorDB {

    // With this implementation, the maximum sized data that can be stored is ... block size >= keylen + valuelen + (sizecounter - no more than 8 bytes)
    public class SortedBlockTableWriter {

        public SortedBlockTableWriter(string baseFileName, int level, int version) {
            string fileName = Config.SortedBlockTableFile(baseFileName, level, version);
            _fileStream = new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.None, Config.SortedBlockSize, FileOptions.Asynchronous | FileOptions.SequentialScan);
            _bufferA = new byte[Config.SortedBlockSize];
            _bufferB = new byte[Config.SortedBlockSize];
            _buffer = _bufferA;
            _bufferPos = 0;
            _pageIndex = new List<ByteArray>();
            Version = version;
            WrittenSize = 0;
        }

        private FileStream _fileStream;
        private byte[] _bufferA;     // pre-allocated bufferB
        private byte[] _bufferB;     // pre-allocated bufferA
        private byte[] _buffer;      // current buffer that is being loaded
        private int _bufferPos;
        private IAsyncResult _async;
        private List<ByteArray> _pageIndex;
        private int dataBlocks = 0;
        private int indexBlocks = 0;
        private int totalBlocks = 0;

        public int Version { get; private set; }
        public int WrittenSize { get; private set; }

        private void SwapBuffers() {
            _buffer = Object.ReferenceEquals(_buffer, _bufferA) ? _bufferB : _bufferA;
            Array.Clear(_buffer, 0, _buffer.Length);
        }

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

            WrittenSize += bytesNeeded;
        }

        private void WriteBlock() {
            // make sure any outstanding writes are completed
            if (_async != null) {
                _fileStream.EndWrite(_async);
            }
            _async = _fileStream.BeginWrite(_buffer, 0, Config.SortedBlockSize, null, null);
            SwapBuffers();
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

        private void SwapBlocks(byte[] blockA, byte[] blockB, ref byte[] current) {
            current = Object.ReferenceEquals(current, blockA) ? blockB : blockA; // swap the blocks so we can issue another disk i/o
            Array.Clear(current, 0, current.Length);
        }
        
        private IAsyncResult BeginReadBlock(byte[] block, int blockNum) {
            _fileStream.Seek(blockNum * Config.SortedBlockSize, SeekOrigin.Begin);
            return _fileStream.BeginRead(block, 0, Config.SortedBlockSize, null, block);
        }

        private byte[] EndReadBlock(IAsyncResult async) {
            _fileStream.EndRead(async);
            return (byte[]) async.AsyncState;
        }

        private byte[] ReadBlock(byte[] block, int blockNum) {
            _fileStream.Seek(blockNum * Config.SortedBlockSize, SeekOrigin.Begin);
            _fileStream.Read(block, 0, Config.SortedBlockSize);
            return block;
        }

        [ThreadStatic]
        private static byte[] threadAllocBlock = null;

        private static byte[] LocalThreadAllocatedBlock() {
            if (threadAllocBlock == null) {
                threadAllocBlock = new byte[Config.SortedBlockSize];
            } else {
                Array.Clear(threadAllocBlock, 0, threadAllocBlock.Length);
            }
            return threadAllocBlock;
        }

        private void ReadMetadata() {
            int numBlocks = (int)_fileStream.Length / Config.SortedBlockSize;
            MemoryStream ms = new MemoryStream(ReadBlock(LocalThreadAllocatedBlock(), numBlocks - 1));
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
                    byte[] block = sbt.ReadBlock(LocalThreadAllocatedBlock(), dataBlockNum);
                    return ScanBlockForKey(block, key, out value);
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
            return EnumerateFromKey(null, ByteArray.Empty);
        }

        public IEnumerable<KeyValuePair<ByteArray, ByteArray>> EnumerateFromKey(Cache indexCache, ByteArray key) {

            int startingBlock;
            if (key.Length == 0) {
                startingBlock = 0;
            } else {
                startingBlock = FindBlockForKey(_baseFileName, _level, _version, indexCache, key);
                if (startingBlock < 0)
                    startingBlock = 0;
            }
            if (startingBlock < _dataBlocks) {

                byte[] allocBlockA = new byte[Config.SortedBlockSize];
                byte[] allocBlockB = new byte[Config.SortedBlockSize];
                byte[] currentBlock = allocBlockA;

                var asyncResult = BeginReadBlock(currentBlock, startingBlock);

                for (int i = startingBlock; i < _dataBlocks; i++) {

                    // wait on last block read to complete so we can start processing the data
                    byte[] block = EndReadBlock(asyncResult);

                    // Go ahead and kick off the next block read asynchronously while we parse the last one
                    if (i < _dataBlocks) {
                        SwapBlocks(allocBlockA, allocBlockB, ref currentBlock); // swap the blocks so we can issue another disk i/o
                        asyncResult = BeginReadBlock(currentBlock, i + 1);
                    }

                    int offset = 0;

                    // On the first block, we need to seek to the key first (if we don't have an empty key)
                    if (i == startingBlock && key.Length != 0) {
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

            byte[] allocBlockA = new byte[Config.SortedBlockSize];
            byte[] allocBlockB = new byte[Config.SortedBlockSize];
            byte[] currentBlock = allocBlockA;

            var asyncResult = BeginReadBlock(currentBlock, _dataBlocks);
            var endIndexBlocks = (_dataBlocks + _indexBlocks);

            for (int i = _dataBlocks; i < endIndexBlocks; i++) {

                // wait on last block read to complete so we can start processing the data
                byte[] block = EndReadBlock(asyncResult);

                // Go ahead and kick off the next block read asynchronously while we parse the last one
                if (i < endIndexBlocks) {
                    SwapBlocks(allocBlockA, allocBlockB, ref currentBlock); // swap the blocks so we can issue another disk i/o
                    asyncResult = BeginReadBlock(currentBlock, i + 1);
                }

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

        private static bool ScanBlockForKey(byte[] block, ByteArray key, out ByteArray value) {
            int offset = 0;
            value = new ByteArray();

            while (block[offset] != 0 && offset < Config.SortedBlockSize) {
                int startingOffset = offset;
                int keySize = Helper.Decode7BitInt(block, ref offset);
                int cmp = key.CompareTo(block, offset, keySize);
                if (cmp == 0) {
                    // Found it
                    var pair = ReadPair(block, ref startingOffset);
                    value = pair.Value;
                    return true;
                } else if (cmp < 0) {
                    return false;
                }
                offset += keySize;
                // Skip past the value
                int valueSize = Helper.Decode7BitInt(block, ref offset);
                offset += valueSize;
            }
            return false;
        }

        private static KeyValuePair<ByteArray, ByteArray> ReadPair(byte[] block, ref int offset) {
            int keySize = Helper.Decode7BitInt(block, ref offset);
            var key = ByteArray.From(block, offset, keySize);
            offset += keySize;
            int valueSize = Helper.Decode7BitInt(block, ref offset);
            var val = ByteArray.From(block, offset, valueSize);
            offset += valueSize;

            // if the next keySize bit is zero then we have exhausted this block. Set to -1 to terminate enumeration
            if (offset >= Config.SortedBlockSize || block[offset] == 0)
                offset = -1;

            return new KeyValuePair<ByteArray,ByteArray>(key,val);
        }

        public static IEnumerable<KeyValuePair<ByteArray, ByteArray>> EnumerateMergedTables(string baseFileName, IEnumerable<PageRef> tableSpecs) {
             var tables = tableSpecs
               .Select(pageRef => new SortedBlockTable(baseFileName, pageRef.Level, pageRef.Version))
               .ToList();
             try {
                 foreach (var pair in MergeEnumerator.Merge(tables.Select(t => t.Enumerate()), t => t.Key)) {
                     yield return pair;
                 }
             } finally {
                 tables.ForEach(t => t.Close());
             }
        }

        public static IEnumerable<PageRecord> MergeTables(Manifest mf, int destinationLevel, IEnumerable<PageRef> tableSpecs) {

            var orderedTableSpecs = tableSpecs.OrderByPagePriority();
            var outputTables = new List<PageRecord>();
            SortedBlockTableWriter writer = null;

            ByteArray firstKey = new ByteArray();
            ByteArray lastKey = new ByteArray();

            foreach (var pair in EnumerateMergedTables(mf.BaseFileName, orderedTableSpecs)) {
                if (writer == null) {
                    writer = new SortedBlockTableWriter(mf.BaseFileName, destinationLevel, mf.NextVersion(destinationLevel));
                    firstKey = pair.Key;
                }
                writer.WritePair(pair.Key, pair.Value);
                lastKey = pair.Key;
                if (writer.WrittenSize >= Config.MaxSortedBlockTableSize) {
                    writer.Close();
                    outputTables.Add(new PageRecord(destinationLevel, writer.Version, firstKey, lastKey));
                    writer = null;
                }
            }
            if (writer != null) {
                writer.Close();
                outputTables.Add(new PageRecord(destinationLevel, writer.Version, firstKey, lastKey));
            }

            return outputTables;
        }
        
        public void Close() {
            _fileStream.Close();
            _fileStream = null;
        }
    }
}
