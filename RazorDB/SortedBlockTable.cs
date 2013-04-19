<<<<<<< HEAD
﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;

namespace RazorDB {
    public enum RecordHeaderFlag {
		Record = 0xE0, EndOfBlock = 0xFF
	};

    // With this implementation, the maximum sized data that can be stored is ... block size >= keylen + valuelen + (sizecounter - no more than 8 bytes)
    public class SortedBlockTableWriter {

        public SortedBlockTableWriter(string baseFileName, int level, int version) {
            string fileName = Config.SortedBlockTableFile(baseFileName, level, version);
            _fileStream = new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.None, Config.SortedBlockSize, FileOptions.Asynchronous | FileOptions.SequentialScan);
            _bufferA = new byte[Config.SortedBlockSize];
            _bufferB = new byte[Config.SortedBlockSize];
            _buffer = _bufferA;
            _bufferPos = 0;
            _pageIndex = new List<Key>();
            Version = version;
            WrittenSize = 0;
        }

        FileStream _fileStream;
        byte[] _bufferA;     // pre-allocated bufferB
        byte[] _bufferB;     // pre-allocated bufferA
        byte[] _buffer;      // current buffer that is being loaded
        int _bufferPos;
        IAsyncResult _async;
        List<Key> _pageIndex;
        int dataBlocks = 0;
        int indexBlocks = 0;
        int totalBlocks = 0;

        public int Version { get; set; }
        public int WrittenSize { get; set; }

        void SwapBuffers() {
            _buffer = Object.ReferenceEquals(_buffer, _bufferA) ? _bufferB : _bufferA;
            Array.Clear(_buffer, 0, _buffer.Length);
        }

        List<ushort> _keyOffsets = new List<ushort>();

        // Write a new key value pair to the output file. This method assumes that the data is being fed in key-sorted order.
        public void WritePair(Key key, Value value) {

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

        ushort BuildBlockTree(byte[] block, int startIndex, int endIndex, List<ushort> keyOffsets) {
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

        void WriteDataBlock() {

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

        void WriteBlock() {

            // make sure any outstanding writes are completed
            if (_async != null) {
                _fileStream.EndWrite(_async);
            }
            _async = _fileStream.BeginWrite(_buffer, 0, Config.SortedBlockSize, null, null);
            SwapBuffers();
            _bufferPos = 0;
            totalBlocks++;
        }


        void WriteIndexKey(Key key) {
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

        void WriteIndex() {
            foreach (var key in _pageIndex) {
                WriteIndexKey(key);
            }
        }

        void WriteMetadata() {
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
        string _path;

        // Lazy open the filestream
        FileStream _fileStream;
        FileStream internalFileStream {
            get {
                if (_fileStream == null)
                    _fileStream = new FileStream(_path, FileMode.Open, FileAccess.Read, FileShare.Read, Config.SortedBlockSize, FileOptions.Asynchronous);
                return _fileStream; 
            }
        }

        string _baseFileName;
        int _level;
        int _version;
        int _dataBlocks;
        int _indexBlocks;
        int _totalBlocks;
        RazorCache _cache;

        void SwapBlocks(byte[] blockA, byte[] blockB, ref byte[] current) {
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

        IAsyncResult BeginReadBlock(byte[] block, int blockNum) {
            if (_cache != null) {
                byte[] cachedBlock = _cache.GetBlock(_baseFileName, _level, _version, blockNum);
                if (cachedBlock != null) {
                    return new AsyncBlock { Buffer = cachedBlock, BlockNum = blockNum };
                }
            }
            internalFileStream.Seek(blockNum * Config.SortedBlockSize, SeekOrigin.Begin);
            return internalFileStream.BeginRead(block, 0, Config.SortedBlockSize, null, new AsyncBlock { Buffer = block, BlockNum = blockNum });
        }

        byte[] EndReadBlock(IAsyncResult async) {
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

        byte[] ReadBlock(byte[] block, int blockNum) {
            if (_cache != null) {
                byte[] cachedBlock = _cache.GetBlock(_baseFileName, _level, _version, blockNum);
                if (cachedBlock != null) {
                    return cachedBlock;
                }
            }
            internalFileStream.Seek(blockNum * Config.SortedBlockSize, SeekOrigin.Begin);
            internalFileStream.Read(block, 0, Config.SortedBlockSize);
            if (_cache != null) {
                var blockCopy = (byte[])block.Clone();
                _cache.SetBlock(_baseFileName, _level, _version, blockNum, blockCopy);
            }
            return block;
        }

        [ThreadStatic] static byte[] threadAllocBlock = null;

        static byte[] LocalThreadAllocatedBlock() {
            if (threadAllocBlock == null) {
                threadAllocBlock = new byte[Config.SortedBlockSize];
            } else {
                Array.Clear(threadAllocBlock, 0, threadAllocBlock.Length);
            }
            return threadAllocBlock;
        }

        void ReadMetadata() {
            byte[] mdBlock = null;
            int numBlocks = -1;
            if (_cache != null) {
                mdBlock = _cache.GetBlock(_baseFileName, _level, _version, int.MaxValue);
                if (mdBlock == null) {
                    numBlocks = (int)internalFileStream.Length / Config.SortedBlockSize;
                    mdBlock = ReadBlock(LocalThreadAllocatedBlock(), numBlocks - 1);
                    byte[] blockCopy = (byte[])mdBlock.Clone();
                    _cache.SetBlock(_baseFileName, _level, _version, int.MaxValue, blockCopy);
                }
            } else {
                numBlocks = (int)internalFileStream.Length / Config.SortedBlockSize;
                mdBlock = ReadBlock(LocalThreadAllocatedBlock(), numBlocks - 1);
            }
            MemoryStream ms = new MemoryStream(mdBlock);
            BinaryReader reader = new BinaryReader(ms);
            string checkString = Encoding.ASCII.GetString(reader.ReadBytes(8));
            if (checkString != "@RAZORDB") {
                throw new InvalidDataException("This does not appear to be a valid table file.");
            }
            _totalBlocks = reader.Read7BitEncodedInt();
            _dataBlocks = reader.Read7BitEncodedInt();
            _indexBlocks = reader.Read7BitEncodedInt();
            if (_totalBlocks != numBlocks && numBlocks != -1) {
                throw new InvalidDataException("The file size does not match the metadata size.");
            }
            if (_totalBlocks != (_dataBlocks + _indexBlocks + 1)) {
                throw new InvalidDataException("Corrupted metadata.");
            }
        }

        public static bool Lookup(string baseFileName, int level, int version, RazorCache cache, Key key, out Value value) {
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
            value = Value.Empty;
            return false;
        }

        static int FindBlockForKey(string baseFileName, int level, int version, RazorCache indexCache, Key key) {
            Key[] index = indexCache.GetBlockTableIndex(baseFileName, level, version);
            int dataBlockNum = Array.BinarySearch(index, key);
            if (dataBlockNum < 0) {
                dataBlockNum = ~dataBlockNum - 1;
            }
            return dataBlockNum;
        }

        public IEnumerable<KeyValuePair<Key, Value>> Enumerate() {
            return EnumerateFromKey(_cache, Key.Empty);
        }

        public IEnumerable<KeyValuePair<Key, Value>> EnumerateFromKey(RazorCache indexCache, Key key) {

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

                try {
                    for (int i = startingBlock; i < _dataBlocks; i++) {
                        // wait on last block read to complete so we can start processing the data
                        byte[] block = EndReadBlock(asyncResult);
                        asyncResult = null;

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
                } finally {
                    if (asyncResult != null)
                        EndReadBlock(asyncResult);
                }
            }
        }

        IEnumerable<Key> EnumerateIndex() {
            byte[] allocBlockA = new byte[Config.SortedBlockSize];
            byte[] allocBlockB = new byte[Config.SortedBlockSize];
            byte[] currentBlock = allocBlockA;

            var endIndexBlocks = (_dataBlocks + _indexBlocks);
            var asyncResult = BeginReadBlock(currentBlock, _dataBlocks);

            try {
                for (int i = _dataBlocks; i < endIndexBlocks; i++) {

                    // wait on last block read to complete so we can start processing the data
                    byte[] block = EndReadBlock(asyncResult);
                    asyncResult = null;

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
            } finally {
                if (asyncResult != null)
                    EndReadBlock(asyncResult);
            }
        }

        public Key[] GetIndex() {
            return EnumerateIndex().ToArray();
        }

        static Key ReadKey(byte[] block, ref int offset) {
            int keySize = Helper.Decode7BitInt(block, ref offset);
            var key = ByteArray.From(block, offset, keySize);
            offset += keySize;

            // if the next keySize bit is zero then we have exhausted this block. Set to -1 to terminate enumeration
            if (block[offset] == 0) offset = -1;
            return new Key(key);
        }

        static bool ScanBlockForKey(byte[] block, Key key, out Value value) {
            int offset = 2; // skip over the tree root pointer
            value = Value.Empty;

            while (offset >= 2 && offset < Config.SortedBlockSize && block[offset] == (byte)RecordHeaderFlag.Record) {
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

        static bool SearchBlockForKey(byte[] block, Key key, out Value value) {
            int offset = BitConverter.ToUInt16(block, 0); // grab the tree root
            value = Value.Empty;

            while (offset >= 2 && offset < Config.SortedBlockSize && block[offset] == (byte) RecordHeaderFlag.Record) {
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

        static KeyValuePair<Key, Value> ReadPair(byte[] block, ref int offset) {
            offset += 1; // skip over header flag
            offset += 4; // skip over the tree pointers
            int keySize = Helper.Decode7BitInt(block, ref offset);
            var key = new Key(ByteArray.From(block, offset, keySize));
            offset += keySize;
            int valueSize = Helper.Decode7BitInt(block, ref offset);
            var val = Value.From(block, offset, valueSize);
            offset += valueSize;

            // if the next keySize bit is zero then we have exhausted this block. Set to -1 to terminate enumeration
            if (offset >= Config.SortedBlockSize || block[offset] == (byte)RecordHeaderFlag.EndOfBlock ) offset = -1;
            return new KeyValuePair<Key, Value>(key, val);
        }

        public static IEnumerable<KeyValuePair<Key, Value>> EnumerateMergedTablesPreCached(RazorCache cache, string baseFileName, IEnumerable<PageRef> tableSpecs) {
             var tables = tableSpecs
               .Select(pageRef => new SortedBlockTable(cache, baseFileName, pageRef.Level, pageRef.Version))
               .ToList();
             try {
                foreach (var pair in MergeEnumerator.Merge(tables.Select(t => t.Enumerate().ToList().AsEnumerable()), t => t.Key)) {
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

            Key firstKey = new Key();
            Key lastKey = new Key();
            Key maxKey = new Key(); // Maximum key we can span with this table to avoid covering more than 10 pages in the destination

            Action<KeyValuePair<Key, Value>> OpenPage = (pair) => {
                writer = new SortedBlockTableWriter(mf.BaseFileName, destinationLevel, mf.NextVersion(destinationLevel));
                firstKey = pair.Key;
                using (var m = mf.GetLatestManifest()) {
                    maxKey = m.FindSpanningLimit(destinationLevel + 1, firstKey);
                }
            };

            Action ClosePage = () => {
                writer.Close();
                outputTables.Add(new PageRecord(destinationLevel, writer.Version, firstKey, lastKey));
                writer = null;
            };

            foreach (var pair in EnumerateMergedTablesPreCached(cache, mf.BaseFileName, orderedTableSpecs)) {
                if (writer == null) {
                    OpenPage(pair);
                }
                if (writer.WrittenSize >= Config.MaxSortedBlockTableSize || (!maxKey.IsEmpty && pair.Key.CompareTo(maxKey) >= 0)) {
                    ClosePage();
                }
                if (writer == null) {
                    OpenPage(pair);
                }
                writer.WritePair(pair.Key, pair.Value);
                lastKey = pair.Key;
            }
            if (writer != null) {
                ClosePage();
            }
            return outputTables;
        }

        string BytesToString(byte[] block, int start, int length) {
            return string.Concat(block.Skip(start).Take(length).Select((b) => b.ToString("X2")).ToArray());
        }

        public void DumpContents(Action<string> msg) {
            msg(string.Format("Path: {0}", _path));
            msg(string.Format("BaseFileName: {0} Level: {1} Version: {2}", _baseFileName, _level, _version));
            msg(string.Format("Data Blocks: {0}\nIndex Blocks: {1}\nTotal Blocks: {2}", _dataBlocks, _indexBlocks, _totalBlocks));
            msg("");
            for (int i = 0; i < _dataBlocks; i++) {
                msg(string.Format("\n*** Data Block {0} ***",i));
                byte[] block = ReadBlock( new byte[Config.SortedBlockSize], i);

                int treePtr = BitConverter.ToUInt16(block, 0);
                msg(string.Format("{0:X4} \"{1}\" Tree Offset: {2:X4}", 0, BytesToString(block,0,2), treePtr));

                int offset = 2;
                while ( offset < Config.SortedBlockSize && block[offset] != (byte)RecordHeaderFlag.EndOfBlock) {

                    // Record
                    msg(string.Format("{0:X4} \"{1}\" {2}", offset, BytesToString(block, offset, 1), ((RecordHeaderFlag)block[offset]).ToString()));

                    // Node Pointers
                    msg(string.Format("{0:X4} \"{1}\" Left:  {2:X4}", offset + 1, BytesToString(block, offset + 1, 2), BitConverter.ToUInt16(block, offset + 1)));
                    msg(string.Format("{0:X4} \"{1}\" Right: {2:X4}", offset + 3, BytesToString(block, offset + 3, 2), BitConverter.ToUInt16(block, offset + 3)));
                    offset += 5;

                    // Key
                    int keyOffset = offset;
                    int keySize = Helper.Decode7BitInt(block, ref offset);
                    msg(string.Format("{0:X4} \"{1}\" KeySize: {2}", keyOffset, BytesToString(block, keyOffset, offset-keyOffset), keySize));
                    msg(string.Format("{0:X4} \"{1}\"", offset, BytesToString(block, offset, keySize)));
                    offset += keySize;

                    // Key
                    int dataOffset = offset;
                    int dataSize = Helper.Decode7BitInt(block, ref offset);
                    msg(string.Format("{0:X4} \"{1}\" DataSize: {2}", dataOffset, BytesToString(block, dataOffset, offset - dataOffset), dataSize));
                    msg(string.Format("{0:X4} \"{1}\"", offset, BytesToString(block, offset, dataSize)));
                    offset += dataSize;

                }
            }
        }

        public void ScanCheck() {
            long totalKeyBytes = 0;
            long totalValueBytes = 0;
            int totalRecords = 0;
            int deletedRecords = 0;
            int nullRecords = 0;
            int smallRecords = 0;
            int largeDescRecords = 0;
            int largeChunkRecords = 0;
            try {
                foreach (var pair in Enumerate()) {
                    try {
                        Key k = pair.Key;
                        Value v = pair.Value;

                        totalKeyBytes += k.KeyBytes.Length;
                        totalValueBytes += v.ValueBytes.Length;
                        totalRecords += 1;
                        switch (v.Type) {
                            case ValueFlag.Null:
                                nullRecords += 1;
                                break;
                            case ValueFlag.Deleted:
                                deletedRecords += 1;
                                break;
                            case ValueFlag.SmallValue:
                                smallRecords += 1;
                                break;
                            case ValueFlag.LargeValueDescriptor:
                                largeDescRecords += 1;
                                break;
                            case ValueFlag.LargeValueChunk:
                                largeChunkRecords += 1;
                                break;
                            default:
                                throw new ApplicationException("Unknown Value Type");
                        }
                    } catch (Exception ex) {
                        Console.WriteLine("**Error Reading Record: {0}", ex);
                    }
                }
            } catch (Exception ex) {
                Console.WriteLine("**Error Enumerating File: {0}", ex);
            } finally {
                Console.WriteLine("  KeyBytes: {0}, ValueBytes: {1}\n  Records: {2} Deleted: {3} Null: {4} Small: {5} LargeDesc: {6} LargeChunk: {7}",
                    totalKeyBytes, totalValueBytes, totalRecords, deletedRecords, nullRecords, smallRecords, largeDescRecords, largeChunkRecords);
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
=======
﻿/* 
Copyright 2012 Gnoso Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;

namespace RazorDB {

    public enum RecordHeaderFlag { Record = 0xE0, RecordV2 = 0xE1, EndOfBlock = 0xFF };

    public enum SortedBlockTableFormat { Razor01 = 1, Razor02 = 2, Default = Razor02 };

    // With this implementation, the maximum sized data that can be stored is ... block size >= keylen + valuelen + (sizecounter - no more than 8 bytes)
    public class SortedBlockTableWriter {

        public SortedBlockTableWriter(string baseFileName, int level, int version, SortedBlockTableFormat format = SortedBlockTableFormat.Default) {
            string fileName = Config.SortedBlockTableFile(baseFileName, level, version);
            _fileStream = new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.None, Config.SortedBlockSize, FileOptions.Asynchronous | FileOptions.SequentialScan);
            _bufferPool = new BufferPool(Config.SortedBlockSize * 3 / 2);
            _buffer = _bufferPool.GetBuffer();
            _bufferPos = 0;
            _pageIndex = new List<KeyEx>();
            _fileFormat = format;
            Version = version;
            WrittenSize = 0;
            InitializeWritePipeline();
        }

        private FileStream _fileStream;
        private BufferPool _bufferPool;
        private byte[] _buffer;      // current buffer that is being loaded

        private int _bufferPos;
        private List<KeyEx> _pageIndex;
        private int dataBlocks = 0;
        private int indexBlocks = 0;
        private int totalBlocks = 0;
        private SortedBlockTableFormat _fileFormat;
        private List<int> onDiskBlockSizes = new List<int>();

        public int Version { get; private set; }
        public int WrittenSize { get; private set; }

        private List<ushort> _keyOffsets = new List<ushort>();

        public struct Block {
            public byte[] Buffer;
            public int Offset;
            public int Length;
            public int SequenceNum;
        }
        public struct TransformedBlock {
            public Block Input;
            public Block Output;
        }
        private Pipeline<Block> plCompression;
        private Pipeline<TransformedBlock> plVerify;
        private Pipeline<Block> plBlockWriter;

        private void InitializeWritePipeline() {

            // Final stage in the pipeline == Write data blocks to disk
            plBlockWriter = new Pipeline<Block>( (block) => {
                // Account for the block 
                onDiskBlockSizes.Add(block.Length);

                // Synchronously Write the block to disk
                _fileStream.Write(block.Buffer, block.Offset, block.Length);

                // Return buffer to pool for re-use since we no longer need it
                _bufferPool.ReturnBuffer(block.Buffer);
            },4,1);

            // Second to last stage - Verify the compression stage as a failsafe
            plVerify = new Pipeline<TransformedBlock>((transform) => {

                var verifyBuffer = _bufferPool.GetBuffer();
                
                // Decompress the block and verify the contents to be sure it matches (fail-safe)
                bool errorThrown = false;
                int decompSize = 0;
                try {
                    decompSize = Compression.Decompress(transform.Output.Buffer, 2, transform.Output.Length - 2, verifyBuffer, 0);
                } catch (Exception e) {
                    System.Diagnostics.Trace.WriteLine(string.Format("Decompression Error Detected: {0}", e.Message));
                    errorThrown = true;
                }

                // Only proceed with compression if no anomalies were detected in the compression process
                if (!errorThrown &&                             // No exceptions thrown
                    Config.SortedBlockSize == decompSize &&     // decompressed size is the same as the source data size
                    Crc32.Compute(transform.Input.Buffer, 0, transform.Input.Length) == Crc32.Compute(verifyBuffer, 0, decompSize)) // CRC32 of the source data is same as the CRC32 of the decompressed data
                {
                    transform.Output.Buffer[0] = 0xFF; // Signal compressed buffer (first two bytes FFEE)
                    transform.Output.Buffer[1] = 0xEE; // These two bytes cannot occur naturally as the first two bytes of a block
                    // (index block starts with F0 and data block is the tree root pointer which won't be that large)

                    // Go ahead and push the compressed output to disk stage
                    plBlockWriter.OrderedPush(transform.Output.SequenceNum, transform.Output);
                    _bufferPool.ReturnBuffer(transform.Input.Buffer);
                } else {
                    plBlockWriter.OrderedPush(transform.Input.SequenceNum, transform.Input);
                    _bufferPool.ReturnBuffer(transform.Output.Buffer);
                }

                _bufferPool.ReturnBuffer(verifyBuffer);
            },4,1);

            // First stage - Compress the block
            plCompression = new Pipeline<Block>((input) => {

                var compressionBuffer = _bufferPool.GetBuffer();
                int compressedSize = Compression.Compress(input.Buffer, input.Length, compressionBuffer, 2);

                // Make sure compressed block is actually smaller
                if (compressedSize < Config.SortedBlockSize) {
                    plVerify.Push(new TransformedBlock {
                        Input = input, 
                        Output = new Block {
                            Buffer = compressionBuffer, 
                            Offset = 0, 
                            Length = compressedSize,
                            SequenceNum = input.SequenceNum,
                        } 
                    });
                }
            },4,2);

        }

        private void FlushPipeline() {
            plCompression.WaitForDrain();
            plVerify.WaitForDrain();
            plBlockWriter.WaitForDrain();
        }

        // Write a new key value pair to the output file. This method assumes that the data is being fed in key-sorted order.
        public void WritePair(KeyEx key, Value value) {

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
            _buffer[_bufferPos++] = (byte)RecordHeaderFlag.RecordV2;

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

            var block = new Block {
                Buffer = _buffer,
                Offset = 0,
                Length = Config.SortedBlockSize,
                SequenceNum = totalBlocks,
            };
            if (_fileFormat == SortedBlockTableFormat.Razor02) {
                // If we are doing the Razor 2 format, then push data into the compression pipeline
                plCompression.Push(block);
            } else {
                // Old version, so push data directly into the write pipeline
                plBlockWriter.Push(block);
            }

            // Request a new buffer so we can continue preparing data for disk.
            _buffer = _bufferPool.GetBuffer();

            // Reset counters
            _bufferPos = 0;
            totalBlocks++;
        }

        private void WriteIndexKey(KeyEx key) {
            byte[] keySize = new byte[8];
            int keySizeLen = Helper.Encode7BitInt(keySize, key.Length);

            int bytesNeeded = keySizeLen + key.Length;

            // Do we need to write out a block before adding this key value pair?
            if ((_bufferPos + bytesNeeded) > Config.SortedBlockSize) {
                WriteBlock();
            }

            // write the data out to the buffer
            _buffer[_bufferPos] = 0xF0;
            _bufferPos++;
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
            byte[] metadata;

            switch (_fileFormat) {
                case SortedBlockTableFormat.Razor01:
                    writer.Write(Encoding.ASCII.GetBytes("@RAZORDB"));
                    writer.Write7BitEncodedInt(totalBlocks + 1);
                    writer.Write7BitEncodedInt(dataBlocks);
                    writer.Write7BitEncodedInt(indexBlocks);

                    metadata = ms.ToArray();
                    Array.Copy(metadata, _buffer, metadata.Length);

                    // Commit the block to disk and wait for the operation to complete
                    WriteBlock();

                    // Flush remaining blocks through the pipeline
                    FlushPipeline();
                    break;
                case SortedBlockTableFormat.Razor02:
                    // Flush out any blocks before writing directly to the filestream
                    // or reading metadata values (which could change while data is going through the pipeline)
                    FlushPipeline();

                    writer.Write(Encoding.ASCII.GetBytes("@RAZOR02"));
                    writer.Write7BitEncodedInt(totalBlocks + 1);
                    writer.Write7BitEncodedInt(dataBlocks);
                    writer.Write7BitEncodedInt(indexBlocks);

                    writer.Write7BitEncodedInt(onDiskBlockSizes.Count);
                    foreach (var blockSize in onDiskBlockSizes) {
                        writer.Write7BitEncodedInt(blockSize);
                    }
                    // Write the length of the metadata + length of the counter
                    writer.Write( (int)ms.Length + 4);

                    metadata = ms.ToArray();
                    
                    // Now write the metadata
                    _fileStream.Write(metadata, 0, metadata.Length);
                    _fileStream.Flush();
                    break;
                default:
                    throw new NotSupportedException();
            }

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
        private SortedBlockTableFormat _fileFormat;
        private RazorCache _cache;
        private List<int> onDiskBlockSizes = new List<int>();
        private List<int> onDiskCumulativeBlockSizes = new List<int>();

        private static Dictionary<string, FileStream> _blockTables = new Dictionary<string, FileStream>();

        private void SwapBlocks(byte[] blockA, byte[] blockB, ref byte[] current) {
            current = Object.ReferenceEquals(current, blockA) ? blockB : blockA; // swap the blocks so we can issue another disk i/o
            Array.Clear(current, 0, current.Length);
        }
 
        internal class AsyncBlock : IAsyncResult {
            internal byte[] Buffer;
            internal int BuffCountBytes;
            internal byte[] CompBuffer;
            internal int BlockNum;
            internal ManualResetEvent Done;

            public object AsyncState { get { return this; } }
            public WaitHandle AsyncWaitHandle { get { throw new NotImplementedException(); } }
            public bool CompletedSynchronously { get { return true; } }
            public bool IsCompleted { get { return true; } }
        }

        private IAsyncResult BeginReadBlock(byte[] block, byte[] compBlock, int blockNum) {
            if (_cache != null) {
                byte[] cachedBlock = _cache.GetBlock(_baseFileName, _level, _version, blockNum);
                if (cachedBlock != null) {
                    return new AsyncBlock { Buffer = cachedBlock, BlockNum = blockNum };
                }
            }
            switch (_fileFormat) {
                case SortedBlockTableFormat.Razor01:
                    internalFileStream.Seek(blockNum * Config.SortedBlockSize, SeekOrigin.Begin);
                    return internalFileStream.BeginRead(block, 0, Config.SortedBlockSize, null, new AsyncBlock { Buffer = block, BlockNum = blockNum });
                case SortedBlockTableFormat.Razor02:
                    var offset = onDiskCumulativeBlockSizes[blockNum];
                    var length = onDiskBlockSizes[blockNum];
                    internalFileStream.Seek(offset, SeekOrigin.Begin);
                    var asyncBlock = new AsyncBlock { Buffer = block, BuffCountBytes = length, CompBuffer = compBlock, BlockNum = blockNum, Done = new ManualResetEvent(false) };
                    return internalFileStream.BeginRead(block, 0, length, (ar) => {

                        if (asyncBlock.Buffer[0] == 0xFF && asyncBlock.Buffer[1] == 0xEE) { // Check for the signal preamble to determine whether we decompress this block or not
                            Compression.Decompress(asyncBlock.Buffer, 2, asyncBlock.BuffCountBytes - 2, asyncBlock.CompBuffer, 0);
                        } else {
                            asyncBlock.CompBuffer = asyncBlock.Buffer;
                        }
                        asyncBlock.Done.Set(); 

                    }, asyncBlock);
                default:
                    throw new NotSupportedException();
            }
        }

        private byte[] EndReadBlock(IAsyncResult async) {
            AsyncBlock ablock = async as AsyncBlock;
            if (ablock != null) {
                // This represents a block read from cache, nothing to do but return it...
                return ablock.Buffer;
            } else {
                var bytesRead = internalFileStream.EndRead(async);
                ablock = (AsyncBlock)async.AsyncState;
                byte[] returnVal = null;
                switch (_fileFormat) {
                    case SortedBlockTableFormat.Razor01:
                        returnVal = ablock.Buffer;
                        break;
                    case SortedBlockTableFormat.Razor02:
                        // Decompress the data
                        ablock.Done.WaitOne();
                        returnVal = ablock.CompBuffer;
                        break;
                    default:
                        throw new NotSupportedException();
                }
                if (_cache != null) {
                    var blockCopy = (byte[])returnVal.Clone();
                    _cache.SetBlock(_baseFileName, _level, _version, ablock.BlockNum, blockCopy);
                }
                return returnVal;
            }
        }

        private byte[] ReadBlock(byte[] block, byte[] compBlock, int blockNum) {
            if (_cache != null) {
                byte[] cachedBlock = _cache.GetBlock(_baseFileName, _level, _version, blockNum);
                if (cachedBlock != null) {
                    return cachedBlock;
                }
            }
            byte[] returnval;
            switch (_fileFormat) {
                case SortedBlockTableFormat.Razor01:
                    internalFileStream.Seek(blockNum * Config.SortedBlockSize, SeekOrigin.Begin);
                    internalFileStream.Read(block, 0, Config.SortedBlockSize);
                    returnval = block;
                    break;
                case SortedBlockTableFormat.Razor02:
                    var offset = onDiskCumulativeBlockSizes[blockNum];
                    var length = onDiskBlockSizes[blockNum];
                    internalFileStream.Seek(offset, SeekOrigin.Begin);
                    internalFileStream.Read(block, 0, length);
                    if (block[0] == 0xFF && block[1] == 0xEE) { // Check for the signal preamble to determine whether we decompress this block or not
                        Compression.Decompress(block, 2, length - 2, compBlock, 0);
                        returnval = compBlock;
                    } else {
                        returnval = block;
                    }
                    break;
                default:
                    throw new NotSupportedException();
            }
            if (_cache != null) {
                var blockCopy = (byte[])returnval.Clone();
                _cache.SetBlock(_baseFileName, _level, _version, blockNum, blockCopy);
            }
            return returnval;
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

        [ThreadStatic]
        private static byte[] threadAllocCompBlock = null;

        private static byte[] LocalThreadAllocatedCompBlock() {
            if (threadAllocCompBlock == null) {
                threadAllocCompBlock = new byte[Config.SortedBlockSize * 6 / 5];
            } else {
                Array.Clear(threadAllocCompBlock, 0, threadAllocCompBlock.Length);
            }
            return threadAllocCompBlock;
        }
        
        private void ReadMetadata() {
            byte[] mdBlock = null;
            int numBlocks = -1;
            int lastBlockSize = Math.Min(Config.SortedBlockSize, (int)internalFileStream.Length); // should the block of size SortedBlockSize unless the file is smaller than that, in which case we read the entire file
            int offset = Config.SortedBlockSize - lastBlockSize; // Make sure the block ends at the end of the buffer, even if it's shorter than the full buffer.

             if (_cache != null) {
                mdBlock = _cache.GetBlock(_baseFileName, _level, _version, int.MaxValue);
                if (mdBlock == null) {
                    // Start by reading the last block-sized chunk of data from the file
                    mdBlock = LocalThreadAllocatedBlock();
                    internalFileStream.Seek(-lastBlockSize, SeekOrigin.End);
                    internalFileStream.Read(mdBlock, offset, lastBlockSize);
                    byte[] blockCopy = (byte[])mdBlock.Clone();
                    _cache.SetBlock(_baseFileName, _level, _version, int.MaxValue, blockCopy);
                }
            } else {
                mdBlock = LocalThreadAllocatedBlock();
                internalFileStream.Seek(-lastBlockSize, SeekOrigin.End);
                internalFileStream.Read(mdBlock, offset, lastBlockSize);
            }

            MemoryStream ms;
            string checkString = Encoding.ASCII.GetString(mdBlock,0,8);
            if (checkString == "@RAZORDB") {
                _fileFormat = SortedBlockTableFormat.Razor01;
                numBlocks = (int)internalFileStream.Length / Config.SortedBlockSize;
                ms = new MemoryStream(mdBlock, 8, Config.SortedBlockSize - 8); // Create the memory stream skipping the initial 8 character marker.
            } else {
                int mdSize = BitConverter.ToInt32(mdBlock, mdBlock.Length - 4);
                int v2Offset = mdBlock.Length - mdSize;
                if (v2Offset < 0 || v2Offset > mdBlock.Length - 8) {
                    throw new InvalidDataException("The metadata size marker is not valid.");
                }
                checkString = Encoding.ASCII.GetString(mdBlock, v2Offset, 8);
                if (checkString == "@RAZOR02") {
                    _fileFormat = SortedBlockTableFormat.Razor02;
                    ms = new MemoryStream(mdBlock, mdBlock.Length - mdSize + 8, mdSize - 8); // Create a memory stream from the rest of the metadata block (skip the 8 character marker string)
                } else {
                    throw new NotSupportedException();
                }
            }
            BinaryReader reader = new BinaryReader(ms);
            
            _totalBlocks = reader.Read7BitEncodedInt();
            _dataBlocks = reader.Read7BitEncodedInt();
            _indexBlocks = reader.Read7BitEncodedInt();
            if (_totalBlocks != numBlocks && numBlocks != -1) {
                throw new InvalidDataException("The file size does not match the metadata size.");
            }
            if (_totalBlocks != (_dataBlocks + _indexBlocks + 1)) {
                throw new InvalidDataException("Corrupted metadata.");
            }

            if (_fileFormat == SortedBlockTableFormat.Razor02) {
                int numBlockSizes = reader.Read7BitEncodedInt();
                int cumSize = 0;
                for (int i = 0; i < numBlockSizes; i++) {
                    int size = reader.Read7BitEncodedInt();
                    onDiskBlockSizes.Add(size);
                    onDiskCumulativeBlockSizes.Add(cumSize);
                    cumSize += size;
                }
            }
        }

        public static bool Lookup(string baseFileName, int level, int version, RazorCache cache, KeyEx key, out Value value) {
            SortedBlockTable sbt = new SortedBlockTable(cache, baseFileName, level, version);
            try {
                int dataBlockNum = FindBlockForKey(baseFileName, level, version, cache, key);

                if (dataBlockNum >= 0 && dataBlockNum < sbt._dataBlocks) {
                    byte[] block = sbt.ReadBlock(LocalThreadAllocatedBlock(), LocalThreadAllocatedCompBlock(), dataBlockNum);
                    return SearchBlockForKey(block, key, out value);
                } 
            } finally {
                sbt.Close();
            }
            value = Value.Empty;
            return false;
        }

        private static int FindBlockForKey(string baseFileName, int level, int version, RazorCache indexCache, KeyEx key) {
            KeyEx[] index = indexCache.GetBlockTableIndex(baseFileName, level, version);
            int dataBlockNum = Array.BinarySearch(index, key);
            if (dataBlockNum < 0) {
                dataBlockNum = ~dataBlockNum - 1;
            }
            return dataBlockNum;
        }

        public IEnumerable<KeyValuePair<KeyEx, Value>> Enumerate() {
            return EnumerateFromKey(_cache, KeyEx.Empty);
        }

        public IEnumerable<KeyValuePair<KeyEx, Value>> EnumerateFromKey(RazorCache indexCache, KeyEx key) {

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
                byte[] compressionBlockA = new byte[Config.SortedBlockSize * 6 / 5];
                byte[] compressionBlockB = new byte[Config.SortedBlockSize * 6 / 5];
                byte[] compressionBlock = compressionBlockA;

                var asyncResult = BeginReadBlock(currentBlock, compressionBlock, startingBlock);

                try {

                    for (int i = startingBlock; i < _dataBlocks; i++) {

                        // wait on last block read to complete so we can start processing the data
                        byte[] block = EndReadBlock(asyncResult);
                        asyncResult = null;

                        // Go ahead and kick off the next block read asynchronously while we parse the last one
                        if (i < _dataBlocks - 1) {
                            SwapBlocks(allocBlockA, allocBlockB, ref currentBlock); // swap the blocks so we can issue another disk i/o
                            SwapBlocks(compressionBlockA, compressionBlockB, ref compressionBlock); // swap the blocks so we can issue another disk i/o
                            asyncResult = BeginReadBlock(currentBlock, compressionBlock, i + 1);
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

                } finally {
                    if (asyncResult != null)
                        EndReadBlock(asyncResult);
                }
            }

        }

        private IEnumerable<KeyEx> EnumerateIndex() {

            byte[] allocBlockA = new byte[Config.SortedBlockSize];
            byte[] allocBlockB = new byte[Config.SortedBlockSize];
            byte[] currentBlock = allocBlockA;
            byte[] compressionBlockA = new byte[Config.SortedBlockSize];
            byte[] compressionBlockB = new byte[Config.SortedBlockSize];
            byte[] compressionBlock = compressionBlockA;

            var endIndexBlocks = (_dataBlocks + _indexBlocks);
            var asyncResult = BeginReadBlock(currentBlock, compressionBlock, _dataBlocks);

            try {
                for (int i = _dataBlocks; i < endIndexBlocks; i++) {

                    // wait on last block read to complete so we can start processing the data
                    byte[] block = EndReadBlock(asyncResult);
                    asyncResult = null;

                    // Go ahead and kick off the next block read asynchronously while we parse the last one
                    if (i < endIndexBlocks - 1) {
                        SwapBlocks(allocBlockA, allocBlockB, ref currentBlock); // swap the blocks so we can issue another disk i/o
                        SwapBlocks(compressionBlockA, compressionBlockB, ref compressionBlock); // swap the blocks so we can issue another disk i/o
                        asyncResult = BeginReadBlock(currentBlock, compressionBlock, i + 1);
                    }

                    int offset = 0;
                    while (offset >= 0) {
                        yield return ReadKey(block, ref offset);
                    }
                }
            } finally {
                if (asyncResult != null)
                    EndReadBlock(asyncResult);
            }
        }

        public KeyEx[] GetIndex() {
            return EnumerateIndex().ToArray();
        }

        private static KeyEx ReadKey(byte[] block, ref int offset) {
            bool v2Key = false;
            if (block[offset] == 0xF0) {
                v2Key = true;
                offset++;
            }
            int keySize = Helper.Decode7BitInt(block, ref offset);
            var key = ByteArray.From(block, offset, keySize);
            offset += keySize;

            // if the next keySize bit is zero then we have exhausted this block. Set to -1 to terminate enumeration
            if (block[offset] == 0)
                offset = -1;

            if (v2Key) {
                return new KeyEx(key);
            } else {
                return KeyEx.FromKey(new Key(key));
            }
        }

        private static bool ScanBlockForKey(byte[] block, KeyEx key, out Value value) {
            int offset = 2; // skip over the tree root pointer
            value = Value.Empty;

            while (offset >= 2 && offset < Config.SortedBlockSize &&
                (block[offset] == (byte)RecordHeaderFlag.Record || block[offset] == (byte)RecordHeaderFlag.RecordV2)) {
                int startingOffset = offset;
                RecordHeaderFlag headerFlag = (RecordHeaderFlag)block[startingOffset];
                offset++; // skip past the header flag
                offset += 4; // skip past the tree pointers
                int keySize = Helper.Decode7BitInt(block, ref offset);

                int cmp = 0;
                if (headerFlag == RecordHeaderFlag.Record) {
                    cmp = key.CompareToV1(block, offset, keySize);
                } else if (headerFlag == RecordHeaderFlag.RecordV2) {
                    cmp = key.CompareTo(block, offset, keySize);
                } else { throw new NotSupportedException(); }

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

        private static bool SearchBlockForKey(byte[] block, KeyEx key, out Value value) {
            int offset = BitConverter.ToUInt16(block, 0); // grab the tree root
            value = Value.Empty;

            while (offset >= 2 && offset < Config.SortedBlockSize &&
                (block[offset] == (byte)RecordHeaderFlag.Record || block[offset] == (byte)RecordHeaderFlag.RecordV2)) {
                int startingOffset = offset;
                RecordHeaderFlag headerFlag = (RecordHeaderFlag) block[startingOffset];
                offset += 1; // skip header
                offset += 4; // skip tree pointers
                int keySize = Helper.Decode7BitInt(block, ref offset);
                
                int cmp = 0;
                if (headerFlag == RecordHeaderFlag.Record) {
                    cmp = key.CompareToV1(block, offset, keySize);
                } else if (headerFlag == RecordHeaderFlag.RecordV2) {
                    cmp = key.CompareTo(block, offset, keySize);
                } else { throw new NotSupportedException(); }

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

        private static KeyValuePair<KeyEx, Value> ReadPair(byte[] block, ref int offset) {
            
            SortedBlockTableFormat fmt;
            switch ((RecordHeaderFlag)block[offset]) {
                case RecordHeaderFlag.Record:
                    fmt = SortedBlockTableFormat.Razor01;
                    break;
                case RecordHeaderFlag.RecordV2:
                    fmt = SortedBlockTableFormat.Razor02;
                    break;
                default:
                    throw new InvalidDataException("Unknown Record Header Flag.");
            }
            offset += 1; // skip over header flag

            offset += 4; // skip over the tree pointers

            // Read the key (choose Key or KeyEx depending on record header flag)
            int keySize = Helper.Decode7BitInt(block, ref offset);
            var keyBytes = ByteArray.From(block, offset, keySize);
            KeyEx key = KeyEx.Empty;
            if (fmt == SortedBlockTableFormat.Razor01) {
                key = KeyEx.FromKey(new Key(keyBytes));
            } else {
                key = new KeyEx(keyBytes);
            }
            offset += keySize;
            
            int valueSize = Helper.Decode7BitInt(block, ref offset);
            var val = Value.From(block, offset, valueSize);
            offset += valueSize;

            // if the next keySize bit is zero then we have exhausted this block. Set to -1 to terminate enumeration
            if (offset >= Config.SortedBlockSize || block[offset] == (byte)RecordHeaderFlag.EndOfBlock )
                offset = -1;

            return new KeyValuePair<KeyEx, Value>(key, val);
        }

        public static IEnumerable<KeyValuePair<KeyEx, Value>> EnumerateMergedTables(RazorCache cache, string baseFileName, IEnumerable<PageRef> tableSpecs) {
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

            KeyEx firstKey = new KeyEx();
            KeyEx lastKey = new KeyEx();

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
                msg(string.Format("\n*** Data Block {0} ***",i));
                byte[] block = ReadBlock( new byte[Config.SortedBlockSize], new byte[Config.SortedBlockSize * 6 / 5], i);

                int treePtr = BitConverter.ToUInt16(block, 0);
                msg(string.Format("{0:X4} \"{1}\" Tree Offset: {2:X4}", 0, BytesToString(block,0,2), treePtr));

                int offset = 2;
                while ( offset < Config.SortedBlockSize && block[offset] != (byte)RecordHeaderFlag.EndOfBlock) {

                    // Record
                    msg(string.Format("{0:X4} \"{1}\" {2}", offset, BytesToString(block, offset, 1), ((RecordHeaderFlag)block[offset]).ToString()));

                    // Node Pointers
                    msg(string.Format("{0:X4} \"{1}\" Left:  {2:X4}", offset + 1, BytesToString(block, offset + 1, 2), BitConverter.ToUInt16(block, offset + 1)));
                    msg(string.Format("{0:X4} \"{1}\" Right: {2:X4}", offset + 3, BytesToString(block, offset + 3, 2), BitConverter.ToUInt16(block, offset + 3)));
                    offset += 5;

                    // Key
                    int keyOffset = offset;
                    int keySize = Helper.Decode7BitInt(block, ref offset);
                    msg(string.Format("{0:X4} \"{1}\" KeySize: {2}", keyOffset, BytesToString(block, keyOffset, offset-keyOffset), keySize));
                    msg(string.Format("{0:X4} \"{1}\"", offset, BytesToString(block, offset, keySize)));
                    offset += keySize;

                    // Key
                    int dataOffset = offset;
                    int dataSize = Helper.Decode7BitInt(block, ref offset);
                    msg(string.Format("{0:X4} \"{1}\" DataSize: {2}", dataOffset, BytesToString(block, dataOffset, offset - dataOffset), dataSize));
                    msg(string.Format("{0:X4} \"{1}\"", offset, BytesToString(block, offset, dataSize)));
                    offset += dataSize;

                }
            }
        }

        public void ScanCheck() {

            long totalKeyBytes = 0;
            long totalValueBytes = 0;
            int totalRecords = 0;
            int deletedRecords = 0;
            int nullRecords = 0;
            int smallRecords = 0;
            int largeDescRecords = 0;
            int largeChunkRecords = 0;
            try {
                foreach (var pair in Enumerate()) {
                    try {
                        KeyEx k = pair.Key;
                        Value v = pair.Value;

                        totalKeyBytes += k.KeyBytes.Length;
                        totalValueBytes += v.ValueBytes.Length;
                        totalRecords += 1;
                        switch (v.Type) {
                            case ValueFlag.Null:
                                nullRecords += 1;
                                break;
                            case ValueFlag.Deleted:
                                deletedRecords += 1;
                                break;
                            case ValueFlag.SmallValue:
                                smallRecords += 1;
                                break;
                            case ValueFlag.LargeValueDescriptor:
                                largeDescRecords += 1;
                                break;
                            case ValueFlag.LargeValueChunk:
                                largeChunkRecords += 1;
                                break;
                            default:
                                throw new ApplicationException("Unknown Value Type");
                        }
                    } catch (Exception ex) {
                        Console.WriteLine("**Error Reading Record: {0}", ex);
                    }
                }
            } catch (Exception ex) {
                Console.WriteLine("**Error Enumerating File: {0}", ex);
            } finally {
                Console.WriteLine("  KeyBytes: {0}, ValueBytes: {1}\n  Records: {2} Deleted: {3} Null: {4} Small: {5} LargeDesc: {6} LargeChunk: {7}",
                    totalKeyBytes, totalValueBytes, totalRecords, deletedRecords, nullRecords, smallRecords, largeDescRecords, largeChunkRecords);
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
>>>>>>> 2113174cc3c1eb5faf9c5d41ff794fede514e9ac
