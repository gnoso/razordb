/*
Copyright 2012-2015 Gnoso Inc.

This software is licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except for what is in compliance with the License.

You may obtain a copy of this license at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either expressed or implied.

See the License for the specific language governing permissions and limitations.
*/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;

namespace RazorDB {
    [Flags]
    public enum RecordHeaderFlag : byte { Record = 0xE0, PrefixedRecord = 0xD0, EndOfBlock = 0xFF };

    // With this implementation, the maximum sized data that can be stored is ... block size >= keylen + valuelen + (sizecounter - no more than 8 bytes)
    public class SortedBlockTableWriter {

        public SortedBlockTableWriter(string baseFileName, int level, int version) {
            string fileName = Config.SortedBlockTableFile(baseFileName, level, version);
            _fileStream = new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.None, Config.SortedBlockSize, Config.SortedBlockTableFileOptions);
            _bufferA = new byte[Config.SortedBlockSize];
            _bufferB = new byte[Config.SortedBlockSize];
            _buffer = _bufferA;
            _bufferPos = 0;
            _pageIndex = new List<Key>();
            Version = version;
            WrittenSize = 0;
        }

        private FileStream _fileStream;
        private byte[] _bufferA;     // pre-allocated bufferB
        private byte[] _bufferB;     // pre-allocated bufferA
        private byte[] _buffer;      // current buffer that is being loaded
        private int _bufferPos;
        private IAsyncResult _async;
        private List<Key> _pageIndex;
        private int dataBlocks = 0;
        private int indexBlocks = 0;
        private int totalBlocks = 0;

        public int Version { get; private set; }
        public int WrittenSize { get; private set; }

        private void SwapBuffers() {
            _buffer = Object.ReferenceEquals(_buffer, _bufferA) ? _bufferB : _bufferA;
            Array.Clear(_buffer, 0, _buffer.Length);
        }

        private byte[] _lastPairKey = null;
        // Write a new key value pair to the output file. This method assumes that the data is being fed in key-sorted order.
        public void WritePair(Key key, Value value) {

            // when prefixing the key is shortened to mininum differential remainder
            // i.e. skip bytes matching the previous key
            short prefixLen = _lastPairKey == null ? (short)0 : key.PrefixLength(_lastPairKey);

            _lastPairKey = key.InternalBytes;
            int keyLength = key.Length - prefixLen;

            byte[] keySize = new byte[8];
            byte[] valueSize = new byte[8];
            var keySizeLen = Helper.Encode7BitInt(keySize, keyLength);
            var valueSizeLen = Helper.Encode7BitInt(valueSize, value.Length);


            int bytesNeeded = keySizeLen + keyLength + valueSizeLen + value.Length + 2 + 1; // +2 prefix len bytes +1 record header

            // Do we need to write out a block before adding this key value pair?
            if ((_bufferPos + bytesNeeded) > Config.SortedBlockSize) {
                WriteDataBlock();
            }

            // If we are at the beginning of the buffer, then add this key to the index.
            if (_bufferPos == 0) {
                _pageIndex.Add(key);
            }

            // This is a record header
            _buffer[_bufferPos] = (byte)RecordHeaderFlag.PrefixedRecord;
            _bufferPos += 1;

            // write the data out to the buffer
            Helper.BlockCopy(keySize, 0, _buffer, _bufferPos, keySizeLen);
            _bufferPos += keySizeLen;

            // add the length of prefix 2 bytes
            _buffer[_bufferPos] = (byte)(prefixLen >> 8);
            _buffer[_bufferPos + 1] = (byte)(prefixLen & 255);
            _bufferPos += 2;

            Helper.BlockCopy(key.InternalBytes, prefixLen, _buffer, _bufferPos, keyLength);
            _bufferPos += keyLength;
            Helper.BlockCopy(valueSize, 0, _buffer, _bufferPos, valueSizeLen);
            _bufferPos += valueSizeLen;
            Helper.BlockCopy(value.InternalBytes, 0, _buffer, _bufferPos, value.Length);
            _bufferPos += value.Length;

            WrittenSize += bytesNeeded;
        }

        private void WriteDataBlock() {

            // Write the end of buffer flag if we have room
            if (_bufferPos < Config.SortedBlockSize) {
                _buffer[_bufferPos++] = (byte)RecordHeaderFlag.EndOfBlock;
            }

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


        private void WriteIndexKey(Key key) {
            byte[] keySize = new byte[8];
            int keySizeLen = Helper.Encode7BitInt(keySize, key.Length);

            int bytesNeeded = keySizeLen + key.Length;

            // Do we need to write out a block before adding this key value pair?
            if ((_bufferPos + bytesNeeded) > Config.SortedBlockSize) {
                WriteBlock();
            }

            // write the data out to the buffer
            Helper.BlockCopy(keySize, 0, _buffer, _bufferPos, keySizeLen);
            _bufferPos += keySizeLen;
            Helper.BlockCopy(key.InternalBytes, 0, _buffer, _bufferPos, key.Length);
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
            writer.Write7BitEncodedInt(SortedBlockTable.Magic);
            writer.Write(SortedBlockTable.CurrentFormatVersion);
            writer.Write7BitEncodedInt(totalBlocks + 1);
            writer.Write7BitEncodedInt(dataBlocks);
            writer.Write7BitEncodedInt(indexBlocks);

            byte[] metadata = ms.ToArray();
            Helper.BlockCopy(metadata, 0, _buffer, 0, metadata.Length);

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
                _fileStream.Dispose();
            }
            _fileStream = null;
        }
    }

    public class TableEnumerator : IEnumerator<KeyValuePair<Key, Value>> {

        public static IEnumerable<KeyValuePair<Key, Value>> Enumerate(int level, RazorCache rzrCache, string baseFileName, ManifestImmutable mft, Key key) {
            var enumerator = new TableEnumerator(level, rzrCache, baseFileName, mft, key);
            while (enumerator.MoveNext())
                yield return enumerator.Current;
        }

        private object _enLock = new object();
        private IEnumerator<KeyValuePair<Key, Value>> ActiveEnumerator = null;
        private bool StopEnumerating = false;
        private RazorCache Cache;
        private String BaseFileName;
        private ManifestImmutable ActiveManifest;
        private Key StartKey;
        private int Level;
        private int StartPageIndex;
        private int NextPageIndex;

        public TableEnumerator(int level, RazorCache rzrCache, string baseFileName, ManifestImmutable mft, Key key) {
            Cache = rzrCache;
            BaseFileName = baseFileName;
            ActiveManifest = mft;
            StartKey = key;
            Level = level;
            StopEnumerating = false;

            var firstPage = ActiveManifest.FindPageForIndex(level, 0);
            if (firstPage != null && key.CompareTo(firstPage.FirstKey) < 0) {
                StartPageIndex = 0;
            } else {
                StartPageIndex = ActiveManifest.FindPageIndexForKey(Level, StartKey);
            }

            InitEnumerator();
        }

        object System.Collections.IEnumerator.Current { get { return Current; } }
        public KeyValuePair<Key, Value> Current {
            get {
                if (ActiveEnumerator == null)
                    throw new InvalidOperationException("Current called on initialized enumerator.");
                return ActiveEnumerator.Current;
            }
        }

        public void Dispose() { }

        public bool MoveNext() {
            if (ActiveEnumerator == null)
                return false;

            while (!ActiveEnumerator.MoveNext()) {
                lock (_enLock)
                    ActiveEnumerator = GetEnumerator();
                if (StopEnumerating)
                    return false;
            }

            return true;
        }

        private void InitEnumerator() {
            NextPageIndex = StartPageIndex;
            if (StartPageIndex < 0) {
                ActiveEnumerator = null;
            } else {
                ActiveEnumerator = GetEnumerator();
            }
        }

        private IEnumerator<KeyValuePair<Key, Value>> GetEnumerator() {
            if (NextPageIndex < 0) {
                StopEnumerating = true;
                yield break;
            }

            PageRecord page = ActiveManifest.FindPageForIndex(Level, NextPageIndex++);

            if (page == null) {
                StopEnumerating = true;
                yield break;
            }

            var sbt = new SortedBlockTable(Cache, BaseFileName, page.Level, page.Version);
            try {
                foreach (var pair in sbt.EnumerateFromKey(Cache, StartKey))
                    yield return pair;
            } finally {
                sbt.Close();
            }
        }

        public void Reset() {
            InitEnumerator();
        }
    }

    public class SortedBlockTable {

        public SortedBlockTable(RazorCache cache, string baseFileName, int level, int version) {
            PerformanceCounters.SBTConstructed.Increment();
            _baseFileName = baseFileName;
            _level = level;
            _version = version;
            _cache = cache;
            _path = Config.SortedBlockTableFile(baseFileName, level, version);
            ReadMetadata();
        }
        private string _path;

        public static readonly byte CurrentFormatVersion = 0x02;
        private byte FormatVersion;
        public static readonly int Magic = 0x1A2B3C4D;

        private bool FileExists = true;
        private FileStream _fileStream;
        private FileStream internalFileStream {
            get {
                if (!FileExists || _fileStream != null)
                    return _fileStream;
                _fileStream = new FileStream(_path, FileMode.Open, FileAccess.Read, FileShare.Read, Config.SortedBlockSize, Config.SortedBlockTableFileOptions);
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

        private void SwapBlocks(byte[] blockA, byte[] blockB, ref byte[] current) {
            if (!FileExists) return;

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
            if (!FileExists) return null;

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
                    var blockCopy = new byte[ablock.Buffer.Length];
                    Buffer.BlockCopy(ablock.Buffer, 0, blockCopy, 0, ablock.Buffer.Length);
                    _cache.SetBlock(_baseFileName, _level, _version, ablock.BlockNum, blockCopy);
                }
                return ablock.Buffer;
            }
        }

        private byte[] ReadBlock(byte[] block, int blockNum, byte[] initScanKey) {
            _lastScanKey = initScanKey;
            if (_cache != null) {
                byte[] cachedBlock = _cache.GetBlock(_baseFileName, _level, _version, blockNum);
                if (cachedBlock != null) {
                    return cachedBlock;
                }
            }
            internalFileStream.Seek(blockNum * Config.SortedBlockSize, SeekOrigin.Begin);
            internalFileStream.Read(block, 0, Config.SortedBlockSize);
            if (_cache != null) {
                var blockCopy = new byte[block.Length];
                Buffer.BlockCopy(block, 0, blockCopy, 0, block.Length);
                _cache.SetBlock(_baseFileName, _level, _version, blockNum, blockCopy);
            }
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
            byte[] mdBlock = null;
            int numBlocks = -1;

            try {
                if (_cache != null) {
                    mdBlock = _cache.GetBlock(_baseFileName, _level, _version, int.MaxValue);
                    if (mdBlock == null) {
                        numBlocks = (int)internalFileStream.Length / Config.SortedBlockSize;
                        mdBlock = ReadBlock(LocalThreadAllocatedBlock(), numBlocks - 1, null);
                        PerformanceCounters.SBTReadMetadata.Increment();
                        byte[] blockCopy = (byte[])mdBlock.Clone();
                        _cache.SetBlock(_baseFileName, _level, _version, int.MaxValue, blockCopy);
                    } else {
                        PerformanceCounters.SBTReadMetadataCached.Increment();
                    }
                } else {
                    numBlocks = (int)internalFileStream.Length / Config.SortedBlockSize;
                    mdBlock = ReadBlock(LocalThreadAllocatedBlock(), numBlocks - 1, null);
                    PerformanceCounters.SBTReadMetadata.Increment();
                }

                MemoryStream ms = new MemoryStream(mdBlock);
                BinaryReader reader = new BinaryReader(ms);
                string checkString = Encoding.ASCII.GetString(reader.ReadBytes(8));
                if (checkString != "@RAZORDB") {
                    throw new InvalidDataException("This does not appear to be a valid table file.");
                }
                var testMagic = reader.Read7BitEncodedInt();
                if (testMagic == Magic) {
                    FormatVersion = reader.ReadByte();
                    _totalBlocks = reader.Read7BitEncodedInt();
                } else {
                    _totalBlocks = testMagic;
                    FormatVersion = 0x00;
                }

                _dataBlocks = reader.Read7BitEncodedInt();
                _indexBlocks = reader.Read7BitEncodedInt();

                if (_totalBlocks != numBlocks && numBlocks != -1) {
                    throw new InvalidDataException("The file size does not match the metadata size.");
                }
                if (_totalBlocks != (_dataBlocks + _indexBlocks + 1)) {
                    throw new InvalidDataException("Corrupted metadata.");
                }
            } catch (Exception ex) {
                if (Config.ExceptionHandling == ExceptionHandling.ThrowAll)
                    throw;

                HandleEmptySortedBlockTable(ex);
            }
        }

        private void HandleEmptySortedBlockTable(Exception ex) {
            _totalBlocks = 0;
            _dataBlocks = 0;
            _indexBlocks = 0;
            FileExists = false;
            Config.LogError("ReadMetadata {0}\nException: {1}", _path, ex.Message);
        }

        public static bool Lookup(string baseFileName, int level, int version, RazorCache cache, Key key, out Value value, ExceptionHandling exceptionHandling, Action<string> logger) {
            PerformanceCounters.SBTLookup.Increment();
            SortedBlockTable sbt = new SortedBlockTable(cache, baseFileName, level, version);
            try {
                byte[] lastScanKey;
                int dataBlockNum = FindBlockForKey(baseFileName, level, version, cache, key, out lastScanKey);
                if (dataBlockNum >= 0 && dataBlockNum < sbt._dataBlocks) {
                    byte[] block = sbt.ReadBlock(LocalThreadAllocatedBlock(), dataBlockNum, lastScanKey);
                    return sbt.ScanBlockForKey(block, key, out value);
                }
            } finally {
                sbt.Close();
            }
            value = Value.Empty;
            return false;
        }

        private static int FindBlockForKey(string baseFileName, int level, int version, RazorCache indexCache, Key key, out byte[] baseKey) {
            Key[] index = indexCache.GetBlockTableIndex(baseFileName, level, version);
            int dataBlockNum = Array.BinarySearch(index, key);
            if (dataBlockNum < 0) {
                dataBlockNum = ~dataBlockNum - 1;
            }

            baseKey = (dataBlockNum > -1 && dataBlockNum < index.Length) ? index[dataBlockNum].InternalBytes : Key.Empty.InternalBytes;
            return dataBlockNum;
        }

        public IEnumerable<KeyValuePair<Key, Value>> Enumerate() {
            return EnumerateFromKey(_cache, Key.Empty);
        }

        public IEnumerable<KeyValuePair<Key, Value>> EnumerateFromKey(RazorCache indexCache, Key key) {
            if (!FileExists)
                yield break;

            byte[] _lastKey = null;
            int startingBlock;
            if (key.Length == 0) {
                startingBlock = 0;
            } else {
                startingBlock = FindBlockForKey(_baseFileName, _level, _version, indexCache, key, out _lastKey);
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

                        int offset = FormatVersion < 2 ? 2 : 0; // handle old format with 2 bytes for offset to treehead

                        // On the first block, we need to seek to the key first (if we don't have an empty key)
                        var searchKeyBytes = key.InternalBytes;
                        if (i == startingBlock && key.Length != 0) {
                            while (offset >= 0) {
                                var pair = ReadPair(ref _lastKey, ref block, ref offset);
                                var checkBytes = pair.Key.InternalBytes;
                                if (pair.Key.CompareTo(key) >= 0) {
                                    var foundKey = pair.Key.InternalBytes;
                                    yield return pair;
                                    break;
                                }
                            }
                        }

                        // Now loop through the rest of the block
                        while (offset >= 0) {
                            var newPair = ReadPair(ref _lastKey, ref block, ref offset);
                            yield return newPair;
                        }
                    }

                } finally {
                    if (asyncResult != null)
                        EndReadBlock(asyncResult);
                }
            }

        }

        public class RawRecord {
            public RawRecord(Key k, Value v, RecordHeaderFlag f) {
                Key = k;
                Value = v;
                hdr = f;
            }
            public Key Key;
            public Value Value;
            public RecordHeaderFlag hdr;
        }

        public IEnumerable<RawRecord> EnumerateRaw() {
            return EnumerateFromKeyRaw(_cache, Key.Empty);
        }

        public IEnumerable<RawRecord> EnumerateFromKeyRaw(RazorCache indexCache, Key key) {
            if (!FileExists)
                yield break;

            int startingBlock;
            if (key.Length == 0) {
                startingBlock = 0;
            } else {
                startingBlock = FindBlockForKey(_baseFileName, _level, _version, indexCache, key, out _lastScanKey);
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

                        int offset = FormatVersion < 2 ? 2 : 02; // handle old format with 2 bytes for offset to treehead

                        // On the first block, we need to seek to the key first (if we don't have an empty key)
                        if (i == startingBlock && key.Length != 0) {
                            while (offset >= 0) {
                                var rec = ReadRawRecord(ref block, ref offset);
                                if (rec.Key.CompareTo(key) >= 0) {
                                    yield return rec;
                                    break;
                                }
                            }
                        }

                        // Now loop through the rest of the block
                        while (offset >= 0) {
                            yield return ReadRawRecord(ref block, ref offset);
                        }
                    }

                } finally {
                    if (asyncResult != null)
                        EndReadBlock(asyncResult);
                }
            }

        }


        private IEnumerable<Key> EnumerateIndex() {
            if (!FileExists)
                yield break;

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

        private static Key ReadKey(byte[] block, ref int offset) {
            int keySize = Helper.Decode7BitInt(block, ref offset);
            var key = ByteArray.From(block, offset, keySize);
            offset += keySize;

            // if the next keySize bit is zero then we have exhausted this block. Set to -1 to terminate enumeration
            if (block[offset] == 0)
                offset = -1;

            return new Key(key);
        }

        private byte[] _lastScanKey = null;
        private bool ScanBlockForKey(byte[] block, Key key, out Value value) {
            int offset = FormatVersion < 2 ? 2 : 0; // handle old format with 2 bytes for offset to treehead
            value = Value.Empty;
            while (offset >= 0 && offset < Config.SortedBlockSize
                && ((block[offset] & (byte)(RecordHeaderFlag.Record | RecordHeaderFlag.PrefixedRecord)) == block[offset])) {
                // read record header
                bool isPrefixed = block[offset] == (byte)RecordHeaderFlag.PrefixedRecord;
                offset += FormatVersion < 2 ? 5 : 1; // skip bytes of old tree blocks

                int keySize = Helper.Decode7BitInt(block, ref offset);
                int cmp;
                if (isPrefixed) {
                    var prefixLen = (short)(block[offset] << 8 | block[offset + 1]); // prefix used len in two bytes
                    offset += 2;
                    cmp = key.PrefixCompareTo(_lastScanKey, prefixLen, block, offset, keySize, out _lastScanKey);
                } else {
                    cmp = key.CompareTo(block, offset, keySize);
                }
                offset += keySize;

                if (cmp == 0) {
                    // Found it
                    value = ReadValue(ref block, ref offset);
                    return true;
                } else if (cmp < 0) {
                    return false;
                }

                // Skip past the value
                int valueSize = Helper.Decode7BitInt(block, ref offset);
                offset += valueSize;
            }
            return false;
        }
        private static Value ReadValue(ref byte[] block, ref int offset) {
            int valueSize = Helper.Decode7BitInt(block, ref offset);
            var val = Value.From(block, offset, valueSize);
            offset += valueSize;

            // if the next keySize bit is zero then we have exhausted this block. Set to -1 to terminate enumeration
            if (offset >= Config.SortedBlockSize || block[offset] == (byte)RecordHeaderFlag.EndOfBlock)
                offset = -1;

            return val;
        }

        private KeyValuePair<Key, Value> ReadPair(ref byte[] lastKey, ref byte[] block, ref int offset) {
            bool isPrefixed = block[offset] == (byte)RecordHeaderFlag.PrefixedRecord;
            offset += FormatVersion < 2 ? 5 : 1; // skip bytes of old tree blocks
            int keySize = Helper.Decode7BitInt(block, ref offset);
            short prefixLen = isPrefixed ? (short)(block[offset] << 8 | block[offset + 1]) : (short)0;
            offset += isPrefixed ? 2 : 0;
            Key key = prefixLen > 0 ? Key.KeyFromPrefix(lastKey, prefixLen, block, offset, keySize) : new Key(ByteArray.From(block, offset, keySize));
            offset += keySize;
            lastKey = key.InternalBytes;

            return new KeyValuePair<Key, Value>(key, ReadValue(ref block, ref offset));
        }

        private RawRecord ReadRawRecord(ref byte[] block, ref int offset) {
            var hdrFlag = (RecordHeaderFlag)block[offset];
#if DEBUG
            if ((byte)(hdrFlag & (RecordHeaderFlag.Record | RecordHeaderFlag.PrefixedRecord)) == 0x00)
                System.Diagnostics.Debugger.Break();
#endif
            offset += FormatVersion < 2 ? 5 : 1; // skip bytes of old tree blocks
            bool isPrefixed = hdrFlag == RecordHeaderFlag.PrefixedRecord;
            int keySize = Helper.Decode7BitInt(block, ref offset);
            short prefixLen = isPrefixed ? (short)(block[offset] << 8 | block[offset + 1]) : (short)0;
            offset += isPrefixed ? 2 : 0;
            Key key = new Key(ByteArray.From(block, offset, keySize));
            offset += keySize;

            return new RawRecord(key, ReadValue(ref block, ref offset), hdrFlag);
        }


        public static IEnumerable<KeyValuePair<Key, Value>> EnumerateMergedTablesPreCached(RazorCache cache, string baseFileName, IEnumerable<PageRef> tableSpecs, ExceptionHandling exceptionHandling, Action<string> logger) {
            PerformanceCounters.SBTEnumerateMergedTablesPrecached.Increment();
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

        public static IEnumerable<PageRecord> MergeTables(RazorCache cache, Manifest mf, int destinationLevel, IEnumerable<PageRef> tableSpecs, ExceptionHandling exceptionHandling, Action<string> logger) {

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

            try {
                foreach (var pair in EnumerateMergedTablesPreCached(cache, mf.BaseFileName, orderedTableSpecs, exceptionHandling, logger)) {
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
            } finally {
                if (writer != null) {
                    ClosePage();
                }
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
                msg(string.Format("\n*** Data Block {0} ***", i));
                byte[] block = ReadBlock(new byte[Config.SortedBlockSize], i, null);

                int offset = FormatVersion < 2 ? 2 : 0; // handle old format with 2 bytes for offset to treehead
                while (offset < Config.SortedBlockSize && block[offset] != (byte)RecordHeaderFlag.EndOfBlock) {

                    // Record
                    var recHdr = (RecordHeaderFlag)block[offset];
                    msg(string.Format("{0:X4} \"{1}\" {2}", offset, BytesToString(block, offset, 1), (recHdr).ToString()));
                    offset++;

                    bool isPrefixed = recHdr == RecordHeaderFlag.PrefixedRecord;
                    // handle old tree bytes
                    offset += isPrefixed ? 0 : 4;

                    // Key
                    int keyOffset = offset;
                    int keySize = Helper.Decode7BitInt(block, ref offset);
                    msg(string.Format("{0:X4} \"{1}\" KeySize: {2}", keyOffset, BytesToString(block, keyOffset, offset - keyOffset), keySize));
                    if (isPrefixed) {
                        short prefixLen = (short)(block[offset] << 8 | block[offset + 1]);
                        msg(string.Format("{0:X4} \"{1}\" PrefixLen: {2}", keyOffset, BytesToString(block, offset, 2), prefixLen));
                        offset += 2;
                    }
                    msg(string.Format("{0:X4} \"{1}\"", offset, BytesToString(block, offset, keySize)));
                    offset += keySize;

                    // Data
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
                        Config.LogError("Error Reading Record: {0}", ex);
                    }
                }
            } catch (Exception ex) {
                Config.LogError("Error Enumerating File: {0}", ex);
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
