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
            _currentJournaledMemTable = new JournaledMemTable(_manifest.BaseFileName, _manifest.CurrentVersion(0));
            _tableManager = new TableManager(_manifest);
            _blockIndexCache = new Cache();
        }

        ~KeyValueStore() {
            Dispose();
        }

        private Manifest _manifest;
        private TableManager _tableManager;
        private Cache _blockIndexCache;
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
            _tableManager = new TableManager(_manifest);
            _blockIndexCache = new Cache();
            _secondaryIndexes = new Dictionary<string, KeyValueStore>();

            Manifest.LogMessage("Database Truncated.");
        }

        public void Set(byte[] key, byte[] value) {
            Set(key, value, null);
        }

        public void Set(byte[] key, byte[] value, IDictionary<string, byte[]> indexedValues) {
            var k = new ByteArray(key);
            var v = new ByteArray(value);

            int adds = 10;
            while (!_currentJournaledMemTable.Add(k, v)) {
                adds--;
                if (adds <= 0)
                    throw new InvalidOperationException("Failed too many times trying to add an item to the JournaledMemTable");
            }
            // Add secondary index values if they were provided
            if (indexedValues != null)
                AddToIndex(key, indexedValues);

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
            ByteArray lookupKey = new ByteArray(key);
            ByteArray output;
            // First check the current memtable
            if (_currentJournaledMemTable.Lookup(lookupKey, out output)) {
                return output.Length == 0 ? null : output.InternalBytes;
            }
            // Capture copy of the rotated table if there is one
            var rotatedMemTable = _rotatedJournaledMemTable;
            if (rotatedMemTable != null) {
                if (rotatedMemTable.Lookup(lookupKey, out output)) {
                    return output.Length == 0 ? null : output.InternalBytes;
                }
            }
            // Now check the files on disk
            using (var manifest = _manifest.GetLatestManifest()) {
                // Must check all pages on level 0
                var zeroPages = manifest.GetPagesAtLevel(0);
                foreach (var page in zeroPages) {
                    if (SortedBlockTable.Lookup(_manifest.BaseFileName, page.Level, page.Version, _blockIndexCache, lookupKey, out output)) {
                        return output.Length == 0 ? null : output.InternalBytes;
                    }
                }
                // If not found, must check pages on the higher levels, but we can use the page index to make the search quicker
                for (int level = 1; level < manifest.NumLevels; level++) {
                    var page = manifest.FindPageForKey(level, lookupKey);
                    if (page != null && SortedBlockTable.Lookup(_manifest.BaseFileName, page.Level, page.Version, _blockIndexCache, lookupKey, out output)) {
                        return output.Length == 0 ? null : output.InternalBytes;
                    }
                }
            }
            // OK, not found anywhere, return null
            return null;
        }

        public IEnumerable<KeyValuePair<byte[], byte[]>> Find(string indexName, byte[] lookupValue) {

            KeyValueStore indexStore = GetSecondaryIndex(indexName);
            // Loop over the values
            foreach (var pair in indexStore.EnumerateFromKey(lookupValue)) {
                var key = pair.Key;
                var value = pair.Value;
                // construct our index key pattern (lookupvalue | key)
                if (ByteArray.CompareMemCmp(key, 0, lookupValue, 0, lookupValue.Length) == 0) {
                    if (ByteArray.CompareMemCmp(key, lookupValue.Length, value, 0, value.Length) != 0) {
                        throw new InvalidDataException("Key data is invalid.");
                    }

                    // Lookup the value of the actual object using the key that was found
                    var primaryValue = Get(value);
                    if (primaryValue != null)
                        yield return new KeyValuePair<byte[], byte[]>(value, primaryValue);
                } else {
                    // if the above condition was not met then we must have enumerated past the end of the indexed value
                    yield break;
                }
            }
        }

        public void Delete(byte[] key) {
            Set(key, new byte[0]);
        }

        public IEnumerable<KeyValuePair<byte[], byte[]>> Enumerate() {

            var enumerators = new List<IEnumerable<KeyValuePair<ByteArray, ByteArray>>>();
            
            // Now check the files on disk
            using (var manifestSnapshot = _manifest.GetLatestManifest()) {
                // Main MemTable
                enumerators.Add(_currentJournaledMemTable.EnumerateSnapshot());

                // Capture copy of the rotated table if there is one
                var rotatedMemTable = _rotatedJournaledMemTable;
                if (rotatedMemTable != null) {
                    enumerators.Add(rotatedMemTable.EnumerateSnapshot());
                }

                List<SortedBlockTable> tables = new List<SortedBlockTable>();
                try {
                    for (int i = 0; i < manifestSnapshot.NumLevels; i++) {
                        var pages = manifestSnapshot.GetPagesAtLevel(i)
                            .OrderByDescending(page => page.Level)
                            .Select(page => new SortedBlockTable(_manifest.BaseFileName, page.Level, page.Version));
                        tables.AddRange(pages);
                    }
                    enumerators.AddRange(tables.Select( t => t.Enumerate()));

                    foreach (var pair in MergeEnumerator.Merge(enumerators, t => t.Key)) {
                        if (pair.Value.Length > 0) {
                            yield return new KeyValuePair<byte[], byte[]>(pair.Key.InternalBytes, pair.Value.InternalBytes);
                        }
                    }
                } finally {
                    // make sure all the tables get closed
                    tables.ForEach(table => table.Close());
                }
            }
        }

        public IEnumerable<KeyValuePair<byte[], byte[]>> EnumerateFromKey(byte[] startingKey) {

            var enumerators = new List<IEnumerable<KeyValuePair<ByteArray, ByteArray>>>();
            ByteArray key = new ByteArray(startingKey);

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
                            .Select(page => new SortedBlockTable(_manifest.BaseFileName, page.Level, page.Version));
                        tables.AddRange(pages);
                    }
                    enumerators.AddRange(tables.Select(t => t.EnumerateFromKey(_blockIndexCache, key)));

                    foreach (var pair in MergeEnumerator.Merge(enumerators, t => t.Key)) {
                        if (pair.Value.Length > 0) {
                            yield return new KeyValuePair<byte[], byte[]>(pair.Key.InternalBytes, pair.Value.InternalBytes);
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
