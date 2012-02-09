using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;

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

        public Manifest Manifest { get { return _manifest; } }

        private volatile JournaledMemTable _currentJournaledMemTable;

        public void Truncate() {
            _currentJournaledMemTable.Close();
            _tableManager.Close();

            string basePath = Path.GetFullPath(Manifest.BaseFileName);
            foreach (string file in Directory.GetFiles(basePath, "*.*", SearchOption.TopDirectoryOnly)) {
                File.Delete(file);
            }

            _manifest = new Manifest(basePath);
            _currentJournaledMemTable = new JournaledMemTable(_manifest.BaseFileName, _manifest.CurrentVersion(0));
            _tableManager = new TableManager(_manifest);
            _blockIndexCache = new Cache();
        }

        public void Set(byte[] key, byte[] value) {
            var k = new ByteArray(key);
            var v = new ByteArray(value);

            int adds = 10;
            while (!_currentJournaledMemTable.Add(k, v)) {
                adds--;
                if (adds <= 0)
                    throw new InvalidOperationException("Failed too many times trying to add an item to the JournaledMemTable");
            }

            if (_currentJournaledMemTable.Full) {
                RotateMemTable();
            }
        }

        public byte[] Get(byte[] key) {
            ByteArray lookupKey = new ByteArray(key);
            ByteArray output;
            if (_currentJournaledMemTable.Lookup(lookupKey, out output)) {
                return output.Length == 0 ? null : output.InternalBytes;
            } else {
                using (var manifestSnapshot = _manifest.GetSnapshot()) {
                    // Must check all pages on level 0
                    var zeroPages = manifestSnapshot.Manifest.GetPagesAtLevel(0);
                    foreach (var page in zeroPages) {
                        if (SortedBlockTable.Lookup(manifestSnapshot.Manifest.BaseFileName, page.Level, page.Version, _blockIndexCache, lookupKey, out output)) {
                            return output.Length == 0 ? null : output.InternalBytes;
                        }
                    }
                    // If not found, must check pages on the higher levels, but we can use the page index to make the search quicker
                    for (int level = 1; level < manifestSnapshot.Manifest.NumLevels; level++) {
                        var page = manifestSnapshot.Manifest.FindPageForKey(level, lookupKey);
                        if (page != null && SortedBlockTable.Lookup(manifestSnapshot.Manifest.BaseFileName, page.Level, page.Version, _blockIndexCache, lookupKey, out output)) {
                            return output.Length == 0 ? null : output.InternalBytes;
                        }
                    }
                    return null;
                }
            }
        }

        public void Delete(byte[] key) {
            Set(key, new byte[0]);
        }

        private object memTableRotationLock = new object();
        private JournaledMemTable _oldMemTable = null;
        private ManualResetEvent _rotationEvent = new ManualResetEvent(true);

        public void RotateMemTable() {
            lock (memTableRotationLock) {
                // Double check the flag in case we have multiple threads that make it into this routine
                if (_currentJournaledMemTable.Full) {
                    _rotationEvent.Reset();
                    #pragma warning disable 420
                    _oldMemTable = Interlocked.Exchange<JournaledMemTable>(ref _currentJournaledMemTable, new JournaledMemTable(_manifest.BaseFileName, _manifest.NextVersion(0)));
                    #pragma warning restore 420
                    _oldMemTable.AsyncWriteToSortedBlockTable(_manifest, _rotationEvent);
                }
            }
        }

        public void Dispose() {
            Close();
        }

        public void Close() {
            if (!_rotationEvent.WaitOne(30000)) {
                throw new TimeoutException("Timed out waiting for memtable rotation to complete.");
            }
            if (_tableManager != null) {
                _tableManager.Close();
                _tableManager = null;
            }
            if (_currentJournaledMemTable != null) {
                _currentJournaledMemTable.Close();
                _currentJournaledMemTable = null;
            }
        }
    }

}
