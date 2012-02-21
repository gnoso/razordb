using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;

namespace RazorDB {

    public enum RecordHeaderFlag { Record = 0xE0, EndOfBlock = 0xFF };

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

        private List<ushort> _keyOffsets = new List<ushort>();

        // Write a new key value pair to the output file. This method assumes that the data is being fed in key-sorted order.
        public void WritePair(ByteArray key, ByteArray value) {

            byte[] keySize = new byte[8];
            int keySizeLen = Helper.Encode7BitInt(keySize, key.Length);
            byte[] valueSize = new byte[8];
            int valueSizeLen = Helper.Encode7BitInt(valueSize, value.Length);
            
            int bytesNeeded = keySizeLen + key.Length + valueSizeLen + value.Length + 4 + 1;

            // Do we need to write out a block before adding this key value pair?
            if ( (_bufferPos + bytesNeeded) > Config.SortedBlockSize ) {
                WriteDataBlock();
            }

            // If we are at the beginning of the buffer, then add this key to the index.
            if (_bufferPos == 0) {
                _pageIndex.Add(key);
                // Add a place for the root node offset
                _bufferPos += 2;
            }

            // store the pair in preparation for writing
            _keyOffsets.Add((ushort)_bufferPos);

            // This is a record header
            _buffer[_bufferPos++] = (byte)RecordHeaderFlag.Record;

            // Add space for left and right node pointers
            _bufferPos += 4;

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

        private ushort BuildBlockTree(byte[] block, int startIndex, int endIndex, List<ushort> keyOffsets) {
            int middleIndex = (startIndex + endIndex) >> 1;
            ushort nodeOffset = keyOffsets[middleIndex];
            
            // Build left side
            if (startIndex < middleIndex) {
                ushort leftOffset = BuildBlockTree(block, startIndex, middleIndex - 1, keyOffsets);
                byte[] left = BitConverter.GetBytes(leftOffset);
                Array.Copy(left, 0, block, nodeOffset + 1, 2);
            }
            // Build right side
            if (middleIndex < endIndex) {
                ushort rightOffset = BuildBlockTree(block, middleIndex + 1, endIndex, keyOffsets);
                byte[] right = BitConverter.GetBytes(rightOffset);
                Array.Copy(right, 0, block, nodeOffset + 3, 2);
            }
            return nodeOffset;
        }

        private void WriteDataBlock() {

            // Write the end of buffer flag if we have room
            if (_bufferPos < Config.SortedBlockSize) {
                _buffer[_bufferPos++] = (byte)RecordHeaderFlag.EndOfBlock;
            }

            // Build the tree structure and fill in the starting pointer
            ushort middleTreePtr = BuildBlockTree(_buffer, 0, _keyOffsets.Count - 1, _keyOffsets);
            byte[] middleTree = BitConverter.GetBytes(middleTreePtr);
            Array.Copy(middleTree, _buffer, 2);
            _keyOffsets.Clear();

            WriteBlock();
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
                WriteDataBlock();
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

        public SortedBlockTable(RazorCache cache, string baseFileName, int level, int version) {
            _baseFileName = baseFileName;
            _level = level;
            _version = version;
            _cache = cache;
            _path = Config.SortedBlockTableFile(baseFileName, level, version);
            ReadMetadata();
        }
        private string _path;

        // Lazy open the filestream
        private FileStream _fileStream;
        private FileStream internalFileStream {
            get {
                if (_fileStream == null)
                    _fileStream = new FileStream(_path, FileMode.Open, FileAccess.Read, FileShare.Read, Config.SortedBlockSize, FileOptions.Asynchronous);
                return _fileStream; 
            }
        }

        private string _baseFileName;
        private int _level;
        private int _version;
        private int _dataBlocks;
        private int _indexBlocks;
        private int _totalBlocks;
        private RazorCache _cache;

        private static Dictionary<string, FileStream> _blockTables = new Dictionary<string, FileStream>();

        private void SwapBlocks(byte[] blockA, byte[] blockB, ref byte[] current) {
            current = Object.ReferenceEquals(current, blockA) ? blockB : blockA; // swap the blocks so we can issue another disk i/o
            Array.Clear(current, 0, current.Length);
        }

        internal class AsyncBlock : IAsyncResult {
            internal byte[] Buffer;
            internal int BlockNum;

            public object AsyncState { get { return this; } }
            public WaitHandle AsyncWaitHandle { get { throw new NotImplementedException(); } }
            public bool CompletedSynchronously { get { return true; } }
            public bool IsCompleted { get { return true; } }
        }

        private IAsyncResult BeginReadBlock(byte[] block, int blockNum) {
            if (_cache != null) {
                byte[] cachedBlock = _cache.GetBlock(_baseFileName, _level, _version, blockNum);
                if (cachedBlock != null) {
                    return new AsyncBlock { Buffer = cachedBlock, BlockNum = blockNum };
                }
            }
            internalFileStream.Seek(blockNum * Config.SortedBlockSize, SeekOrigin.Begin);
            return internalFileStream.BeginRead(block, 0, Config.SortedBlockSize, null, new AsyncBlock { Buffer = block, BlockNum = blockNum });
        }

        private byte[] EndReadBlock(IAsyncResult async) {
            AsyncBlock ablock = async as AsyncBlock;
            if (ablock != null) {
                return ablock.Buffer;
            } else {
                internalFileStream.EndRead(async);
                ablock = (AsyncBlock)async.AsyncState;
                if (_cache != null) {
                    var blockCopy = (byte[])ablock.Buffer.Clone();
                    _cache.SetBlock(_baseFileName, _level, _version, ablock.BlockNum, blockCopy);
                }
                return ablock.Buffer;
            }
        }

        private byte[] ReadBlock(byte[] block, int blockNum) {
            byte[] cachedBlock = _cache.GetBlock(_baseFileName, _level, _version, blockNum);
            if (cachedBlock == null) {
                internalFileStream.Seek(blockNum * Config.SortedBlockSize, SeekOrigin.Begin);
                internalFileStream.Read(block, 0, Config.SortedBlockSize);
                var blockCopy = (byte[])block.Clone();
                _cache.SetBlock(_baseFileName, _level, _version, blockNum, blockCopy);
                return block;
            } else {
                return cachedBlock;
            }
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

        public class Metadata {
            public int dataBlocks;
            public int indexBlocks;
            public int totalBlocks;
        }
        private static Dictionary<string, Metadata> _cachedMetadata = new Dictionary<string, Metadata>();

        private void ReadMetadata() {
            lock (_cachedMetadata) {
                Metadata md;
                if (!_cachedMetadata.TryGetValue(_path, out md)) {
                    md = ReadMetadataFromDisk();
                    _cachedMetadata.Add(_path, md);
                }
                _dataBlocks = md.dataBlocks;
                _indexBlocks = md.indexBlocks;
                _totalBlocks = md.totalBlocks;
            }
        }

        private Metadata ReadMetadataFromDisk() {
            int numBlocks = (int)internalFileStream.Length / Config.SortedBlockSize;
            MemoryStream ms = new MemoryStream(ReadBlock(LocalThreadAllocatedBlock(), numBlocks - 1));
            BinaryReader reader = new BinaryReader(ms);
            string checkString = Encoding.ASCII.GetString(reader.ReadBytes(8));
            if (checkString != "@RAZORDB") {
                throw new InvalidDataException("This does not appear to be a valid table file.");
            }
            var metadata = new Metadata();
            metadata.totalBlocks = reader.Read7BitEncodedInt();
            metadata.dataBlocks = reader.Read7BitEncodedInt();
            metadata.indexBlocks = reader.Read7BitEncodedInt();
            if (metadata.totalBlocks != numBlocks) {
                throw new InvalidDataException("The file size does not match the metadata size.");
            }
            if (metadata.totalBlocks != (metadata.dataBlocks + metadata.indexBlocks + 1)) {
                throw new InvalidDataException("Corrupted metadata.");
            }
            return metadata;
        }

        public static bool Lookup(string baseFileName, int level, int version, RazorCache cache, ByteArray key, out ByteArray value) {
            SortedBlockTable sbt = new SortedBlockTable(cache, baseFileName, level, version);
            try {
                int dataBlockNum = FindBlockForKey(baseFileName, level, version, cache, key);

                if (dataBlockNum >= 0 && dataBlockNum < sbt._dataBlocks) {
                    byte[] block = sbt.ReadBlock(LocalThreadAllocatedBlock(), dataBlockNum);
                    return SearchBlockForKey(block, key, out value);
                } 
            } finally {
                sbt.Close();
            }
            value = new ByteArray();
            return false;
        }

        private static int FindBlockForKey(string baseFileName, int level, int version, RazorCache indexCache, ByteArray key) {
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

        public IEnumerable<KeyValuePair<ByteArray, ByteArray>> EnumerateFromKey(RazorCache indexCache, ByteArray key) {

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

                    int offset = 2; // reset offset, start after tree root pointer

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
            int offset = 2; // skip over the tree root pointer
            value = new ByteArray();

            while (block[offset] == (byte) RecordHeaderFlag.Record && offset < Config.SortedBlockSize) {
                int startingOffset = offset;
                offset++; // skip past the header flag
                offset += 4; // skip past the tree pointers
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

        private static bool SearchBlockForKey(byte[] block, ByteArray key, out ByteArray value) {
            int offset = BitConverter.ToUInt16(block, 0); // grab the tree root
            value = new ByteArray();

            while (block[offset] == (byte) RecordHeaderFlag.Record && offset < Config.SortedBlockSize) {
                int startingOffset = offset;
                offset += 1; // skip header
                offset += 4; // skip tree pointers
                int keySize = Helper.Decode7BitInt(block, ref offset);
                int cmp = key.CompareTo(block, offset, keySize);
                if (cmp == 0) {
                    // Found it
                    var pair = ReadPair(block, ref startingOffset);
                    value = pair.Value;
                    return true;
                } else if (cmp < 0) {
                    // key < node => explore left side
                    offset = BitConverter.ToUInt16(block, startingOffset + 1);
                } else if (cmp > 0) {
                    // key > node => explore right side
                    offset = BitConverter.ToUInt16(block, startingOffset + 3);
                }
            }
            return false;
        }

        private static KeyValuePair<ByteArray, ByteArray> ReadPair(byte[] block, ref int offset) {
            offset += 1; // skip over header flag
            offset += 4; // skip over the tree pointers
            int keySize = Helper.Decode7BitInt(block, ref offset);
            var key = ByteArray.From(block, offset, keySize);
            offset += keySize;
            int valueSize = Helper.Decode7BitInt(block, ref offset);
            var val = ByteArray.From(block, offset, valueSize);
            offset += valueSize;

            // if the next keySize bit is zero then we have exhausted this block. Set to -1 to terminate enumeration
            if (offset >= Config.SortedBlockSize || block[offset] == (byte)RecordHeaderFlag.EndOfBlock )
                offset = -1;

            return new KeyValuePair<ByteArray,ByteArray>(key,val);
        }

        public static IEnumerable<KeyValuePair<ByteArray, ByteArray>> EnumerateMergedTables(RazorCache cache, string baseFileName, IEnumerable<PageRef> tableSpecs) {
             var tables = tableSpecs
               .Select(pageRef => new SortedBlockTable(cache, baseFileName, pageRef.Level, pageRef.Version))
               .ToList();
             try {
                 foreach (var pair in MergeEnumerator.Merge(tables.Select(t => t.Enumerate()), t => t.Key)) {
                     yield return pair;
                 }
             } finally {
                 tables.ForEach(t => t.Close());
             }
        }

        public static IEnumerable<PageRecord> MergeTables(RazorCache cache, Manifest mf, int destinationLevel, IEnumerable<PageRef> tableSpecs) {

            var orderedTableSpecs = tableSpecs.OrderByPagePriority();
            var outputTables = new List<PageRecord>();
            SortedBlockTableWriter writer = null;

            ByteArray firstKey = new ByteArray();
            ByteArray lastKey = new ByteArray();

            foreach (var pair in EnumerateMergedTables(cache, mf.BaseFileName, orderedTableSpecs)) {
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

        private string BytesToString(byte[] block, int start, int length) {
            return string.Concat(block.Skip(start).Take(length).Select((b) => b.ToString("X2")).ToArray());
        }

        public void DumpContents(Action<string> msg) {
            msg(string.Format("Path: {0}", _path));
            msg(string.Format("BaseFileName: {0} Level: {1} Version: {2}", _baseFileName, _level, _version));
            msg(string.Format("Data Blocks: {0}\nIndex Blocks: {1}\nTotal Blocks: {2}", _dataBlocks, _indexBlocks, _totalBlocks));
            msg("");
            for (int i = 0; i < _dataBlocks; i++) {
                msg(string.Format("Data Block {0}",i));
                byte[] block = ReadBlock( new byte[Config.SortedBlockSize], i);

                int treePtr = BitConverter.ToUInt16(block, 0);
                msg(string.Format("{0:X4} \"{1}\" Tree Offset: {2}", 0, BytesToString(block,0,2), treePtr));
            }
        }

        public void Close() {
            if (_fileStream != null) {
                _fileStream.Close();
            }
            _fileStream = null;
        }
    }
}
