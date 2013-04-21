﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;
using System.Diagnostics;

namespace RazorDB {

    public class KeyValueStore : IDisposable {

        public KeyValueStore(string baseFileName) : this(baseFileName, null) {}

        public KeyValueStore(string baseFileName, RazorCache cache) {
            if (!Directory.Exists(baseFileName)) {
                Directory.CreateDirectory(baseFileName);
            }
            _manifest = new Manifest(baseFileName);


            int memTableVersion = _manifest.CurrentVersion(0);
            // Check for a previously aborted journal rotation 
            CheckForIncompleteJournalRotation(baseFileName, memTableVersion);
            // Create new journal for this run (and potentially load from disk, if there was data loaded previously)
            _currentJournaledMemTable = new JournaledMemTable(_manifest.BaseFileName, memTableVersion);
            
            _cache = cache == null ? new RazorCache() : cache;
        }

        bool finalizing = false;
        ~KeyValueStore() {
            finalizing = true;
            Dispose();
        }

        private Manifest _manifest;
        private RazorCache _cache;
        private Dictionary<string, KeyValueStore> _secondaryIndexes = new Dictionary<string, KeyValueStore>(StringComparer.OrdinalIgnoreCase);

        // For Table Manager 
        internal long ticksTillNextMerge = 0;
        internal object mergeLock = new object();

        public Manifest Manifest { get { return _manifest; } }

        internal RazorCache Cache { get { return _cache; } }

        private volatile JournaledMemTable _currentJournaledMemTable;

        public void Truncate() {
            _currentJournaledMemTable.Close();
            TableManager.Default.Close(this);
            foreach (var pair in _secondaryIndexes) {
                pair.Value.Close();
            }
            SortedBlockTable.CloseFileManager(Manifest.BaseFileName);

            string basePath = Path.GetFullPath(Manifest.BaseFileName);
            foreach (string file in Directory.GetFiles(basePath, "*.*", SearchOption.AllDirectories)) {
                File.Delete(file);
            }
            foreach (string dir in Directory.GetDirectories(basePath, "*.*", SearchOption.AllDirectories)) {
                Directory.Delete(dir,true);
            }

            _manifest = new Manifest(basePath);
            _currentJournaledMemTable = new JournaledMemTable(_manifest.BaseFileName, _manifest.CurrentVersion(0));
            _cache = new RazorCache();
            _secondaryIndexes = new Dictionary<string, KeyValueStore>(StringComparer.OrdinalIgnoreCase);

            Manifest.LogMessage("Database Truncated.");
        }

        public void Set(byte[] key, byte[] value) {
            Set(key, value, null);
        }

        public object multiPageLock = new object();

        public void Set(byte[] key, byte[] value, IDictionary<string, byte[]> indexedValues) {

            int valueSize = value.Length;
            if (valueSize <= Config.MaxSmallValueSize) {
                var k = new KeyEx(key, 0);
                var v = new Value(value, ValueFlag.SmallValue);
                InternalSet(k, v, indexedValues);
            } else {
                lock (multiPageLock) {
                    if (value.Length >= Config.MaxLargeValueSize)
                        throw new InvalidDataException(string.Format("Value is larger than the maximum size. ({0} bytes)", Config.MaxLargeValueSize));

                    int offset = 0;
                    int seqNum = 1;
                    while (offset < valueSize) {
                        var k = new KeyEx(key, seqNum);
                        int length = Math.Min(valueSize - offset, Config.MaxSmallValueSize);
                        var v = new Value(ByteArray.From(value, offset, length).InternalBytes, ValueFlag.LargeValueChunk);
                        InternalSet(k, v, null);
                        offset += length;
                        seqNum++;
                    }
                    var dk = new KeyEx(key, 0);
                    var dv = new Value(BitConverter.GetBytes(valueSize), ValueFlag.LargeValueDescriptor);
                    InternalSet(dk, dv, indexedValues);
                }
            }
        }

        public void RemoveFromIndex(byte[] key, IDictionary<string, byte[]> indexedValues) {
            foreach (var pair in indexedValues) {
                string IndexName = pair.Key;

                // Construct Index key by concatenating the indexed value and the target key
                byte[] indexValue = pair.Value;
                byte[] indexKey = new byte[key.Length + indexValue.Length];
                indexValue.CopyTo(indexKey, 0);
                key.CopyTo(indexKey, indexValue.Length);

                KeyValueStore indexStore = GetSecondaryIndex(IndexName);
                indexStore.Delete(indexKey);
            }
        }

        public void CleanIndex(string indexName) {
            KeyValueStore indexStore = GetSecondaryIndex(indexName);

            var allValueStoreItems = new HashSet<ByteArray>( this.Enumerate().Select(item => new ByteArray(item.Key) ) );
            foreach (var indexItem in indexStore.Enumerate()) {
                if (!allValueStoreItems.Contains(new ByteArray(indexItem.Value))) {
                    indexStore.Delete(indexItem.Key);
                }
            }
        }

        private void InternalSet(KeyEx k, Value v, IDictionary<string, byte[]> indexedValues) {
            int adds = 10;
            while (!_currentJournaledMemTable.Add(k, v)) {
                adds--;
                if (adds <= 0)
                    throw new InvalidOperationException("Failed too many times trying to add an item to the JournaledMemTable");
            }
            // Add secondary index values if they were provided
            if (indexedValues != null)
                AddToIndex(k.KeyBytes, indexedValues);

            if (_currentJournaledMemTable.Full) {
                RotateMemTable();
            }

            TableManager.Default.MarkKeyValueStoreAsModified(this);
        }

        public void AddToIndex(byte[] key, IDictionary<string, byte[]> indexedValues) {
            foreach (var pair in indexedValues) {
                string IndexName = pair.Key;

                // Construct Index key by concatenating the indexed value and the target key
                byte[] indexValue = pair.Value;
                byte[] indexKey = new byte[key.Length + indexValue.Length];
                indexValue.CopyTo(indexKey, 0);
                key.CopyTo(indexKey, indexValue.Length);

                KeyValueStore indexStore = GetSecondaryIndex(IndexName);
                indexStore.Set(indexKey, key);
            }
        }

        private KeyValueStore GetSecondaryIndex(string IndexName) {
            KeyValueStore indexStore = null;
            lock (_secondaryIndexes) {
                if (!_secondaryIndexes.TryGetValue(IndexName, out indexStore)) {
                    indexStore = new KeyValueStore(Config.IndexBaseName(Manifest.BaseFileName, IndexName), _cache);
                    if (Manifest.Logger != null) {
                        indexStore.Manifest.Logger = msg => Manifest.Logger(string.Format("{0}: {1}", IndexName, msg));
                    }
                    _secondaryIndexes.Add(IndexName, indexStore);
                }
            }
            return indexStore;
        }

        public byte[] Get(byte[] key) {
            KeyEx lookupKey = new KeyEx(key, 0);
            return AssembleGetResult(lookupKey, InternalGet(lookupKey));
        }

        private Value InternalGet(KeyEx lookupKey) {
            Value output = Value.Empty;
            // Capture copy of the rotated table if there is one
            var rotatedMemTable = _rotatedJournaledMemTable;
            
            // First check the current memtable
            if (_currentJournaledMemTable.Lookup(lookupKey, out output)) {
                return output;
            }
            // Check the table in rotation
            if (rotatedMemTable != null) {
                if (rotatedMemTable.Lookup(lookupKey, out output)) {
                    return output;
                }
            }
            // Now check the files on disk
            using (var manifest = _manifest.GetLatestManifest()) {
                // Must check all pages on level 0
                var zeroPages = manifest.GetPagesAtLevel(0).OrderByDescending( (page) => page.Version );
                foreach (var page in zeroPages) {
                    if (SortedBlockTable.Lookup(_manifest.BaseFileName, page.Level, page.Version, _cache, lookupKey, out output)) {
                        return output;
                    }
                }
                // If not found, must check pages on the higher levels, but we can use the page index to make the search quicker
                for (int level = 1; level < manifest.NumLevels; level++) {
                    var page = manifest.FindPageForKey(level, lookupKey);
                    if (page != null && SortedBlockTable.Lookup(_manifest.BaseFileName, page.Level, page.Version, _cache, lookupKey, out output)) {
                        return output;
                    }
                }
            }
            // OK, not found anywhere, return null
            return Value.Empty;
        }

        private byte[] AssembleGetResult(KeyEx lookupKey, Value result) {
            switch (result.Type) {
                case ValueFlag.Null:
                case ValueFlag.Deleted:
                    return null;
                case ValueFlag.SmallValue:
                    return result.ValueBytes;
                case ValueFlag.LargeValueDescriptor: {
                    lock (multiPageLock) {
                        // read the descriptor again in case it changed
                        result = InternalGet(lookupKey);

                        // make sure type is still large value descriptor and continue
                        if (result.Type == ValueFlag.LargeValueDescriptor) {
                            int valueSize = BitConverter.ToInt32(result.ValueBytes, 0);
                            byte[] bytes = new byte[valueSize];
                            int offset = 0;
                            int seqNum = 1;
                            while (offset < valueSize) {
                                var blockKey = lookupKey.WithSequence(seqNum);
                                var block = InternalGet(blockKey);
                                if (block.Type != ValueFlag.LargeValueChunk)
                                    throw new InvalidDataException(string.Format("Corrupted data: block is missing. Block Type: {0} SeqNum: {1}, Block Key: {2}", block.Type, seqNum, blockKey));
                                offset += block.CopyValueBytesTo(bytes, offset);
                                seqNum++;
                            }
                            return bytes;
                        } else {
                            return AssembleGetResult(lookupKey, result);
                        }
                    }
                }
                default:
                    throw new InvalidOperationException("Unexpected value flag for result.");
            }
        }

        public IEnumerable<KeyValuePair<byte[], byte[]>> Find(string indexName, byte[] lookupValue) {

            KeyValueStore indexStore = GetSecondaryIndex(indexName);
            // Loop over the values
            foreach (var pair in indexStore.EnumerateFromKey(lookupValue)) {
                var key = pair.Key;
                var value = pair.Value;
                // construct our index key pattern (lookupvalue | key)
                if (ByteArray.CompareMemCmp(key, 0, lookupValue, 0, lookupValue.Length) == 0) {
                    if (key.Length == (value.Length + lookupValue.Length) && ByteArray.CompareMemCmp(key, lookupValue.Length, value, 0, value.Length) == 0) {
                        // Lookup the value of the actual object using the key that was found
                        var primaryValue = Get(value);
                        if (primaryValue != null)
                            yield return new KeyValuePair<byte[], byte[]>(value, primaryValue);
                    }
                } else {
                    // if the above condition was not met then we must have enumerated past the end of the indexed value
                    yield break;
                }
            }
        }

        public void Delete(byte[] key) {
            var k = new KeyEx(key, 0);
            InternalSet(k, Value.Deleted, null);
        }

        public IEnumerable<KeyValuePair<byte[], byte[]>> Enumerate() {
            return EnumerateFromKey(new byte[0]);
        }

        private IEnumerable<KeyValuePair<KeyEx, Value>> InternalEnumerateFromKey(byte[] startingKey) {

            var enumerators = new List<IEnumerable<KeyValuePair<KeyEx, Value>>>();
            KeyEx key = new KeyEx(startingKey, 0);

            // Capture copy of the rotated table if there is one
            var rotatedMemTable = _rotatedJournaledMemTable;

            // Select main MemTable
            enumerators.Add(_currentJournaledMemTable.EnumerateSnapshotFromKey(key));

            if (rotatedMemTable != null) {
                enumerators.Add(rotatedMemTable.EnumerateSnapshotFromKey(key));
            }

            var tables = new List<SortedBlockTable>();
            try {
                // Now check the files on disk
                using (var manifestSnapshot = _manifest.GetLatestManifest()) {
                    for (int i = 0; i < manifestSnapshot.NumLevels; i++) {
                        var pages = manifestSnapshot.GetPagesAtLevel(i)
                            .OrderByDescending(page => page.Version)
                            .Select(page => new SortedBlockTable(_cache, _manifest.BaseFileName, page.Level, page.Version));
                        tables.AddRange(pages);
                        enumerators.AddRange(tables.Select(t => t.EnumerateFromKey(_cache, key)));
                    }
                    foreach (var pair in MergeEnumerator.Merge(enumerators, t => t.Key)) {
                        yield return pair;
                    }
                }
            } finally {
                foreach (var table in tables) {
                    table.Dispose();
                }
            }
        }

        public IEnumerable<KeyValuePair<byte[], byte[]>> EnumerateFromKey(byte[] startingKey) {

            foreach (var pair in InternalEnumerateFromKey(startingKey)) {
                if (pair.Key.SequenceNum == 0) { // only enumerate top-level keys (sequence zero)
                    byte[] result = AssembleGetResult(pair.Key, pair.Value);
                    if (result != null) {
                        yield return new KeyValuePair<byte[], byte[]>(pair.Key.KeyBytes, result);
                    }
                }
            }
        }

        private object memTableRotationLock = new object();
        private JournaledMemTable _rotatedJournaledMemTable;
        private Semaphore _rotationSemaphore = new Semaphore(1, 1);

#pragma warning disable 420
        public void RotateMemTable() {
            lock (memTableRotationLock) {
                // Double check the flag in case we have multiple threads that make it into this routine
                if (_currentJournaledMemTable.Full) {
                    _rotationSemaphore.WaitOne();    // Wait for the rotation gate to be open, and automatically reset once a single thread gets through.

                    _rotatedJournaledMemTable = Interlocked.Exchange<JournaledMemTable>(ref _currentJournaledMemTable, new JournaledMemTable(_manifest.BaseFileName, _manifest.NextVersion(0)));

                    ThreadPool.QueueUserWorkItem((o) => {
                        try {
                            _rotatedJournaledMemTable.WriteToSortedBlockTable(_manifest);
                            _rotatedJournaledMemTable = null;
                        } finally {
                            _rotationSemaphore.Release(); // Open the gate for the next rotation
                        }
                    });
                }
            }
        }
#pragma warning restore 420

        private void CheckForIncompleteJournalRotation(string baseFileName, int currentMemTableVersion) {
            int previousMemTableVersion = currentMemTableVersion - 1;
            // Is there a left-over journal from a previous rotation that was aborted while in rotation.
            if (File.Exists(Config.JournalFile(baseFileName, previousMemTableVersion))) {
                var memTable = new JournaledMemTable(baseFileName, previousMemTableVersion);
                memTable.WriteToSortedBlockTable(_manifest);
                memTable.Close();
            }
        }

        public void Dispose() {
            Close();
        }

        public void Close() {
            // Make sure any inflight rotations have occurred before shutting down.
            if (!_rotationSemaphore.WaitOne(30000))
                throw new TimeoutException("Timed out waiting for table rotation to complete.");
            // Release again in case another thread tries to close it again.
            _rotationSemaphore.Release();

            if (!finalizing) {
                TableManager.Default.Close(this);
            }
            if (_currentJournaledMemTable != null) {
                _currentJournaledMemTable.Close();
                _currentJournaledMemTable = null;
            }

            if (_secondaryIndexes != null) {
                foreach (var idx in _secondaryIndexes) {
                    idx.Value.Close();
                }
            }

            SortedBlockTable.CloseFileManager(_manifest.BaseFileName);

            // Don't finalize since we already closed it.
            GC.SuppressFinalize(this);
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
                foreach (var pair in InternalEnumerateFromKey(new byte[0])) {
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
                        if (v.Type == ValueFlag.LargeValueDescriptor) {
                            var lookupKey = k;
                            int valueSize = BitConverter.ToInt32(v.ValueBytes, 0);
                            int offset = 0;
                            byte seqNum = 1;
                            while (offset < valueSize) {
                                var blockKey = lookupKey.WithSequence(seqNum);
                                var block = InternalGet(blockKey);
                                if (block.Type != ValueFlag.LargeValueChunk)
                                    throw new InvalidDataException(string.Format("Corrupted data: block is missing. Block Type: {0} SeqNum: {1}, Block Key: {2}", block.Type, seqNum, blockKey));
                                offset += block.Length - 1;
                                seqNum++;
                            }
                            if (offset != valueSize) {
                                throw new InvalidDataException(string.Format("Chunk sizes ({0}) are different from the descriptor size ({1}).", offset, valueSize));
                            }
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

    }

}
