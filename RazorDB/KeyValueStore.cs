using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;
using System.Diagnostics;

namespace RazorDB {
    
    public class KeyValueStore : IDisposable {

        public KeyValueStore(string baseFileName) {
            if (!Directory.Exists(baseFileName)) {
                Directory.CreateDirectory(baseFileName);
            }
            _manifest = new Manifest(baseFileName);


            int memTableVersion = _manifest.CurrentVersion(0);
            // Check for a previously aborted journal rotation 
            CheckForIncompleteJournalRotation(baseFileName, memTableVersion);
            // Create new journal for this run (and potentially load from disk, if there was data loaded previously)
            _currentJournaledMemTable = new JournaledMemTable(_manifest.BaseFileName, memTableVersion);
            
            _cache = new RazorCache();
            _tableManager = new TableManager(_cache, _manifest);
        }

        ~KeyValueStore() {
            Dispose();
        }

        private Manifest _manifest;
        private TableManager _tableManager;
        private RazorCache _cache;
        private Dictionary<string, KeyValueStore> _secondaryIndexes = new Dictionary<string,KeyValueStore>();

        public Manifest Manifest { get { return _manifest; } }

        private volatile JournaledMemTable _currentJournaledMemTable;

        public void Truncate() {
            _currentJournaledMemTable.Close();
            _tableManager.Close();
            foreach (var pair in _secondaryIndexes) {
                pair.Value.Close();
            }

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
            _tableManager = new TableManager(_cache, _manifest);
            _secondaryIndexes = new Dictionary<string, KeyValueStore>();

            Manifest.LogMessage("Database Truncated.");
        }

        public void Set(byte[] key, byte[] value) {
            Set(key, value, null);
        }

        public void Set(byte[] key, byte[] value, IDictionary<string, byte[]> indexedValues) {

            int valueSize = value.Length;
            if (valueSize <= Config.MaxSmallValueSize) {
                var k = new Key(key, 0);
                var v = new Value(value, ValueFlag.SmallValue);
                InternalSet(k, v, indexedValues);
            } else {
                int offset = 0;
                byte seqNum = 1;
                while (offset < valueSize) {
                    var k = new Key(key, seqNum);
                    int length = Math.Min(valueSize - offset, Config.MaxSmallValueSize);
                    var v = new Value(ByteArray.From(value, offset, length).InternalBytes, ValueFlag.LargeValueChunk);
                    InternalSet(k, v, null);
                    offset += length;
                    seqNum++;
                }
                var dk = new Key(key, 0);
                var dv = new Value(BitConverter.GetBytes(valueSize), ValueFlag.LargeValueDescriptor);
                InternalSet(dk, dv, null);
            }
        }

        private void InternalSet(Key k, Value v, IDictionary<string, byte[]> indexedValues) {
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
        }

        private void AddToIndex(byte[] key, IDictionary<string, byte[]> indexedValues) {
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
                    indexStore = new KeyValueStore(Config.IndexBaseName(Manifest.BaseFileName, IndexName));
                    if (Manifest.Logger != null) {
                        indexStore.Manifest.Logger = msg => Manifest.Logger(string.Format("{0}: {1}", IndexName, msg));
                    }
                    _secondaryIndexes.Add(IndexName, indexStore);
                }
            }
            return indexStore;
        }

        public byte[] Get(byte[] key) {
            Key lookupKey = new Key(key, 0);
            return AssembleGetResult(lookupKey, InternalGet(lookupKey));
        }

        private Value InternalGet(Key lookupKey) {
            Value output;
            // First check the current memtable
            if (_currentJournaledMemTable.Lookup(lookupKey, out output)) {
                return output;
            }
            // Capture copy of the rotated table if there is one
            var rotatedMemTable = _rotatedJournaledMemTable;
            if (rotatedMemTable != null) {
                if (rotatedMemTable.Lookup(lookupKey, out output)) {
                    return output;
                }
            }
            // Now check the files on disk
            using (var manifest = _manifest.GetLatestManifest()) {
                // Must check all pages on level 0
                var zeroPages = manifest.GetPagesAtLevel(0);
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

        private byte[] AssembleGetResult(Key lookupKey, Value result) {
            switch (result.Type) {
                case ValueFlag.Null:
                case ValueFlag.Deleted:
                    return null;
                case ValueFlag.SmallValue:
                    return result.ValueBytes;
                case ValueFlag.LargeValueDescriptor: {
                    int valueSize = BitConverter.ToInt32(result.ValueBytes, 0);
                    byte[] bytes = new byte[valueSize];
                    int offset = 0;
                    byte seqNum = 1;
                    while (offset < valueSize) {
                        var blockKey = lookupKey.WithSequence(seqNum);
                        var block = InternalGet(blockKey);
                        if (block.Type != ValueFlag.LargeValueChunk)
                            throw new InvalidDataException("Corrupted data: block is missing.");
                        offset += block.CopyValueBytesTo(bytes, offset);
                        seqNum++;
                    }
                    return bytes;
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
            var k = new Key(key, 0);
            InternalSet(k, Value.Deleted, null);
        }

        public IEnumerable<KeyValuePair<byte[], byte[]>> Enumerate() {
            return EnumerateFromKey(new byte[0]);
        }

        public IEnumerable<KeyValuePair<byte[], byte[]>> EnumerateFromKey(byte[] startingKey) {

            var enumerators = new List<IEnumerable<KeyValuePair<Key, Value>>>();
            Key key = new Key(startingKey, 0);

            // Now check the files on disk
            using (var manifestSnapshot = _manifest.GetLatestManifest()) {
                // Main MemTable
                enumerators.Add(_currentJournaledMemTable.EnumerateSnapshotFromKey(key));

                // Capture copy of the rotated table if there is one
                var rotatedMemTable = _rotatedJournaledMemTable;
                if (rotatedMemTable != null) {
                    enumerators.Add(rotatedMemTable.EnumerateSnapshotFromKey(key));
                }

                List<SortedBlockTable> tables = new List<SortedBlockTable>();
                try {
                    for (int i = 0; i < manifestSnapshot.NumLevels; i++) {
                        var pages = manifestSnapshot.GetPagesAtLevel(i)
                            .OrderByDescending(page => page.Level)
                            .Select(page => new SortedBlockTable(_cache, _manifest.BaseFileName, page.Level, page.Version));
                        tables.AddRange(pages);
                    }
                    enumerators.AddRange(tables.Select(t => t.EnumerateFromKey(_cache, key)));

                    foreach (var pair in MergeEnumerator.Merge(enumerators, t => t.Key)) {
                        if (pair.Value.Type != ValueFlag.Deleted) {
                            yield return new KeyValuePair<byte[], byte[]>(pair.Key.KeyBytes, pair.Value.ValueBytes);
                        }
                    }
                } finally {
                    // make sure all the tables get closed
                    tables.ForEach(table => table.Close());
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

            if (_tableManager != null) {
                _tableManager.Close();
                _tableManager = null;
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

            // Don't finalize since we already closed it.
            GC.SuppressFinalize(this);
        }
    }

}
