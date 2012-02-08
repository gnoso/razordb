using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;

namespace RazorDB {

    public class Manifest {

        public Manifest(string baseFileName) {
            _baseFileName = baseFileName;
            _pages = new List<PageRecord>[num_levels];
            _mergeKeys = new ByteArray[num_levels];
            for (int i = 0; i < num_levels; i++) {
                _pages[i] = new List<PageRecord>();
                _mergeKeys[i] = new ByteArray(new byte[0]);
            }
            _snapshot = false;
            Read();
        }
        private object manifestLock = new object();
        private List<PageRecord>[] _pages;

        private string _baseFileName;
        public string BaseFileName {
            get { return _baseFileName; }
        }

        private int _manifestVersion = 0;
        public int ManifestVersion {
            get { return _manifestVersion; }
        }

        private const int num_levels = 8;
        private int[] _versions = new int[num_levels];
        public int CurrentVersion(int level) {
            if (level >= num_levels)
                throw new IndexOutOfRangeException();
            lock (manifestLock) {
                return _versions[level];
            }
        }

        // atomically acquires the next version and persists the metadata
        public int NextVersion(int level) {
            if (level >= num_levels)
                throw new IndexOutOfRangeException();
            lock (manifestLock) {
                _versions[level] += 1;
                Write();
                return _versions[level];
            }
        }

        private void Write() {

            // Check for mutability here because any operation that changes the snapshot must commit the writes in order to work properly.
            VerifyMutability();

            string manifestFile = Config.ManifestFile(_baseFileName);
            string tempManifestFile = manifestFile + "~";

            _manifestVersion++;

            if (ManifestVersion > Config.ManifestVersionCount) {
                // Make a backup of the current manifest file
                if (File.Exists(manifestFile))
                    File.Move(manifestFile, tempManifestFile);

                FileStream fs = new FileStream(manifestFile, FileMode.CreateNew, FileAccess.Write, FileShare.None, 1024, false);
                BinaryWriter writer = new BinaryWriter(fs);

                try {
                    WriteManifestContents(writer);
                    writer.Close();
                } catch {
                    writer.Close();
                    File.Delete(manifestFile);
                    File.Move(tempManifestFile, manifestFile);
                    throw;
                }

                // Delete the backup file
                if (File.Exists(tempManifestFile))
                    File.Delete(tempManifestFile);
            } else {
                FileStream fs = new FileStream(manifestFile, FileMode.Append, FileAccess.Write, FileShare.None, 1024, false);
                BinaryWriter writer = new BinaryWriter(fs);
                try {
                    WriteManifestContents(writer);
                } finally {
                    writer.Close();
                }
            }
        }

        private void WriteManifestContents(BinaryWriter writer) {
            long startPos = writer.BaseStream.Position;

            writer.Write7BitEncodedInt(ManifestVersion);
            writer.Write7BitEncodedInt(_versions.Length);
            foreach (var b in _versions) {
                writer.Write7BitEncodedInt(b);
            }
            writer.Write7BitEncodedInt(_pages.Length);
            foreach (var pageList in _pages) {
                writer.Write7BitEncodedInt(pageList.Count);
                foreach (var page in pageList) {
                    writer.Write7BitEncodedInt(page.Level);
                    writer.Write7BitEncodedInt(page.Version);
                    writer.Write7BitEncodedInt(page.FirstKey.Length);
                    writer.Write(page.FirstKey.InternalBytes);
                    writer.Write7BitEncodedInt(page.LastKey.Length);
                    writer.Write(page.LastKey.InternalBytes);
                }
            }
            foreach (var key in _mergeKeys) {
                writer.Write7BitEncodedInt(key.Length);
                writer.Write(key.InternalBytes);
            }

            int size = (int)(writer.BaseStream.Position - startPos);
            writer.Write(size);
        }

        private void ReadManifestContents(BinaryReader reader) {
            ReadManifestContents(reader, this);
        }

        private void ReadManifestContents(BinaryReader reader, Manifest owner) {
            _manifestVersion = reader.Read7BitEncodedInt();
            int num_versions = reader.Read7BitEncodedInt();
            for (int i = 0; i < num_versions; i++) {
                _versions[i] = reader.Read7BitEncodedInt();
            }
            int num_pages = reader.Read7BitEncodedInt();
            for (int j = 0; j < num_pages; j++) {
                int num_page_entries = reader.Read7BitEncodedInt();
                for (int k = 0; k < num_page_entries; k++) {
                    int level = reader.Read7BitEncodedInt();
                    int version = reader.Read7BitEncodedInt();
                    int num_key_bytes = reader.Read7BitEncodedInt();
                    ByteArray startkey = new ByteArray(reader.ReadBytes(num_key_bytes));
                    num_key_bytes = reader.Read7BitEncodedInt();
                    ByteArray endkey = new ByteArray(reader.ReadBytes(num_key_bytes));
                    _pages[j].Add(new PageRecord(owner, level, version, startkey, endkey));
                }
            }
            for (int k = 0; k < num_pages; k++) {
                int num_key_bytes = reader.Read7BitEncodedInt();
                _mergeKeys[k] = new ByteArray(reader.ReadBytes(num_key_bytes));
            }
            AddRefAllPages();
        }

        private Manifest GetSnapshotInstance() {
            // Clone this copy of the manifest
            Manifest clone = new Manifest(BaseFileName);
            clone._manifestVersion = _manifestVersion;
            for (int v=0; v < _versions.Length; v++) {
                clone._versions[v] = _versions[v];
            }
            for (int p = 0; p < _pages.Length; p++) {
                clone._pages[p] = new List<PageRecord>();
                foreach (var page in _pages[p]) {
                    clone._pages[p].Add(page);
                }
            }
            clone._snapshot = true; // Don't allow the user to change the snapshot instance
            return clone;
        }

        public bool IsSnapshot {
            get { return _snapshot; }
        }
        private bool _snapshot; // This flag is set for snapshots so we can throw an error if it's modified
        private void VerifyMutability() {
            if (_snapshot)
                throw new InvalidOperationException("Cannot modify an immutable manifest instance.");
        }

        internal void AddRefAllPages() {
            foreach (var level in _pages) {
                foreach (var page in level) {
                    page.AddRef();
                }
            }
        }
        internal void ReleaseAllPages() {
            foreach (var level in _pages) {
                foreach (var page in level) {
                    page.Release();
                }
            }
        }

        public ManifestSnapshot GetSnapshot() {
            lock (manifestLock) {
                return new ManifestSnapshot(this, GetSnapshotInstance());
            }
        }

        private void Read() {

            string manifestFile = Config.ManifestFile(_baseFileName);
            if (!File.Exists(manifestFile)) {
                return;
            }

            FileStream fs = new FileStream(manifestFile, FileMode.Open, FileAccess.Read, FileShare.None, 1024, false);
            BinaryReader reader = new BinaryReader(fs);

            try {
                // Get the size of the last manifest block
                reader.BaseStream.Seek(-4, SeekOrigin.End);
                int size = reader.ReadInt32();

                // Now seek to that position and read it
                reader.BaseStream.Seek(-size - 4, SeekOrigin.End);

                ReadManifestContents(reader);
            } finally {
                reader.Close();
            }
        }

        public int NumLevels { get { return num_levels; } }

        public int GetNumPagesAtLevel(int level) {
            if (level >= num_levels)
                throw new IndexOutOfRangeException();
            lock (manifestLock) {
                return _pages[level].Count;
            }
        }

        private ByteArray[] _mergeKeys;
        public PageRecord NextMergePage(int level) {
            if (level >= num_levels)
                throw new IndexOutOfRangeException();
            lock (manifestLock) {
                var currentKey = _mergeKeys[level];
                var levelKeys = _pages[level].Select(key => key.FirstKey).ToList();
                int pageNum = levelKeys.BinarySearch(currentKey);
                if (pageNum < 0) { pageNum = ~pageNum - 1; }
                pageNum = Math.Max(0, pageNum);

                int nextPage = pageNum >= levelKeys.Count - 1 ? 0 : pageNum + 1;
                _mergeKeys[level] = _pages[level][nextPage].FirstKey;
                Write();
                return _pages[level][pageNum];
            }
        }

        public PageRecord FindPageForKey(int level, ByteArray key) {
            if (level >= num_levels)
                throw new IndexOutOfRangeException();
            lock (manifestLock) {
                var levelKeys = _pages[level].Select(p => p.FirstKey).ToList();
                int startingPage = levelKeys.BinarySearch(key);
                if (startingPage < 0) { startingPage = ~startingPage - 1; }

                if (startingPage >= 0 && startingPage < _pages[level].Count) {
                    return _pages[level][startingPage];
                } else {
                    return null;
                }
            }
        }

        public PageRecord[] FindPagesForKeyRange(int level, ByteArray startKey, ByteArray endKey) {
            if (level >= num_levels)
                throw new IndexOutOfRangeException();
            lock (manifestLock) {
                var levelKeys = _pages[level].Select(p => p.FirstKey).ToList();
                int startingPage = levelKeys.BinarySearch(startKey);
                if (startingPage < 0) { startingPage = ~startingPage - 1; }
                int endingPage = levelKeys.BinarySearch(endKey);
                if (endingPage < 0) { endingPage = ~endingPage - 1; }
                return _pages[level].Skip(startingPage).Take(endingPage - startingPage + 1).ToArray();
            }
        }

        public PageRecord[] GetPagesAtLevel(int level) {
            if (level >= num_levels)
                throw new IndexOutOfRangeException();
            lock (manifestLock) {
                return _pages[level].ToArray();
            }
        }

        public void AddPage(int level, int version, ByteArray firstKey, ByteArray lastKey) {
            if (level >= num_levels)
                throw new IndexOutOfRangeException();
            lock (manifestLock) {
                var page = new PageRecord(this, level, version, firstKey, lastKey);
                page.AddRef();
                _pages[level].Add(page);
                _pages[level].Sort((x, y) => x.FirstKey.CompareTo(y.FirstKey));
                Write();
            }
        }

        // Atomically add/remove page specifications to/from the manifest
        public void ModifyPages(IEnumerable<PageRecord> addPages, IEnumerable<PageRef> removePages) {
            lock (manifestLock) {
                foreach (var page in addPages) {
                    if (page.Level >= num_levels)
                        throw new IndexOutOfRangeException();
                    page.AddRef();
                    _pages[page.Level].Add(page);
                    _pages[page.Level].Sort((x, y) => x.FirstKey.CompareTo(y.FirstKey));
                }
                foreach (var pageRef in removePages) {
                    if (pageRef.Level >= num_levels)
                        throw new IndexOutOfRangeException();
                    var page = _pages[pageRef.Level].Where(p => p.Version == pageRef.Version).First();
                    _pages[pageRef.Level].Remove(page);
                    _pages[pageRef.Level].Sort((x, y) => x.FirstKey.CompareTo(y.FirstKey));
                    page.Release();
                }
                Write();
            }
        }

        internal void NotifyPageRelease(PageRecord releasedPage) {
            string fileName = Config.SortedBlockTableFile(BaseFileName, releasedPage.Level, releasedPage.Version);
            if (File.Exists(fileName)) {
                File.Delete(fileName);
            }
        }

        public void LogContents() {
            LogMessage("Manifest Version: {0}", ManifestVersion);
            LogMessage("Base Filename: {0}", BaseFileName);
            for (int level = 0; level < NumLevels; level++) {
                LogMessage("-------------------------------------");
                LogMessage("Level: {0} NumPages: {1} MaxPages: {2}", level, GetNumPagesAtLevel(level), Config.MaxPagesOnLevel(level));
                LogMessage("MergeKey: {0}", _mergeKeys[level]);
                LogMessage("Version: {0}", _versions[level]);
                var pages = GetPagesAtLevel(level);
                foreach (var page in pages) {
                    LogMessage("Page {0}-{1} [{2} -> {3}]", page.Level, page.Version, page.FirstKey, page.LastKey);
                }
            }
        }

        public Action<string> Logger { get; set; }

        public void LogMessage(string format, params object[] parms) {
            if (Logger != null) {
                Logger( string.Format(format, parms));   
            }
        }
    }

    public class ManifestSnapshot : IDisposable {

        public ManifestSnapshot(Manifest owner, Manifest manifest) {
            _owner = owner;
            _manifest = manifest;
            _manifest.AddRefAllPages();
        }
        private Manifest _owner;
        private Manifest _manifest;

        public Manifest Owner { get { return _owner; } }
        public Manifest Manifest { get { return _manifest; } }

        public void Dispose() {
            _manifest.ReleaseAllPages();
        }
    }

    public struct PageRef {
        public int Level;
        public int Version;
    }

    public static class PageRefConverter {
        public static IEnumerable<PageRef> AsPageRefs(this IEnumerable<PageRecord> pageRecords) {
            return pageRecords.Select(record => new PageRef { Level = record.Level, Version = record.Version });
        }
    }

    public class PageRecord {
        public PageRecord(Manifest owner, int level, int version, ByteArray firstKey, ByteArray lastKey) {
            _level = level;
            _version = version;
            _firstKey = firstKey;
            _lastKey = lastKey;

            if (owner.IsSnapshot)
                throw new InvalidOperationException("PageRecord owner cannot be a snapshot manifest.");
            _snapshotReferenceCount = 0;
            _owner = owner;
        }
        private int _level;
        public int Level { get { return _level; } }
        private int _version;
        public int Version { get { return _version; } }
        private ByteArray _firstKey;
        public ByteArray FirstKey { get { return _firstKey;  } }
        private ByteArray _lastKey;
        public ByteArray LastKey { get { return _lastKey; } }

        private Manifest _owner;
        private int _snapshotReferenceCount;
        public void AddRef() {
            Interlocked.Increment(ref _snapshotReferenceCount);
        }
        public void Release() {
            int count = Interlocked.Decrement(ref _snapshotReferenceCount);
            if (count == 0) {
                _owner.NotifyPageRelease(this);
            }
        }
    }
}
