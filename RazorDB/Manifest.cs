/*
Copyright 2012, 2013 Gnoso Inc.

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
using System.Diagnostics;

namespace RazorDB {

    public class ManifestImmutable : IDisposable {

        public ManifestImmutable(Manifest manifest) {
            _manifest = manifest;
            _pages = new List<PageRecord>[MaxLevels];
            _mergeKeys = new Key[MaxLevels];
            for (int i = 0; i < MaxLevels; i++) {
                _pages[i] = new List<PageRecord>();
                _mergeKeys[i] = Key.Empty;
            }
        }

        private ManifestImmutable Clone() {
            // Clone this copy of the manifest
            var clone = new ManifestImmutable(_manifest);

            for (int v = 0; v < _versions.Length; v++) {
                clone._versions[v] = _versions[v];
            }
            for (int p = 0; p < _pages.Length; p++) {
                clone._pages[p] = new List<PageRecord>();
                foreach (var page in _pages[p]) {
                    clone._pages[p].Add(page);
                }
            }
            return clone;
        }

        private Key[] _mergeKeys;
        private List<PageRecord>[] _pages;

        private int[] _versions = new int[MaxLevels];
        public int CurrentVersion(int level) {
            if (level >= MaxLevels)
                throw new IndexOutOfRangeException();
            return _versions[level];
        }

        public const int MaxLevels = 8;
        public int NumLevels { get { return MaxLevels; } }

        public int GetNumPagesAtLevel(int level) {
            if (level >= MaxLevels)
                throw new IndexOutOfRangeException();
            return _pages[level].Count;
        }

        public int FindPageIndexForKey(int level, Key key) {
            if (level >= MaxLevels)
                throw new IndexOutOfRangeException();
            var levelKeys = _pages[level].Select(p => p.FirstKey).ToList();
            int startingPage = levelKeys.BinarySearch(key);
            if (startingPage < 0) { startingPage = ~startingPage - 1; }

            if (startingPage >= 0 && startingPage < _pages[level].Count) {
                return startingPage;
            } else {
                return -1;
            }
        }

        public PageRecord FindPageForKey(int level, Key key) {
            int index = FindPageIndexForKey(level, key);
            return index < 0 ? null : _pages[level][index];
        }

        public PageRecord[] FindPagesForKeyRange(int level, Key startKey, Key endKey) {
            if (level >= MaxLevels)
                throw new IndexOutOfRangeException();
            var levelKeys = _pages[level].Select(p => p.FirstKey).ToList();
            int startingPage = levelKeys.BinarySearch(startKey);
            if (startingPage < 0) { startingPage = ~startingPage - 1; }
            int endingPage = levelKeys.BinarySearch(endKey);
            if (endingPage < 0) { endingPage = ~endingPage - 1; }
            return _pages[level].Skip(startingPage).Take(endingPage - startingPage + 1).ToArray();
        }

        // find the maximum key that we can span to avoid covering too much upper level data.
        public Key FindSpanningLimit(int level, Key startingKey) {

            // not a valid level
            if (level >= NumLevels)
                return Key.Empty;

            // there are no pages, don't limit
            if (_pages[level].Count == 0) {
                return Key.Empty;
            }

            // Find the page for the starting key
            var index = FindPageIndexForKey(level, startingKey);
            if (index < 0) {
                // startingKey is not included in any of the pages
                if (startingKey.CompareTo(_pages[level][0].FirstKey) < 0) {
                    // startingKey is before the first page, so treat it as if the first page contains the key
                    index = 0;
                } else {
                    // starting Key is after the last page, so don't limit the span
                    return Key.Empty;
                }
            }
            var maxIndex = index + Config.MaxPageSpan - 1;

            if (_pages[level].Count > maxIndex) {
                // The last key in that page is the furthest we can span at the lower level
                return _pages[level][maxIndex].LastKey;
            }
            // There aren't enough pages to trigger the limit.
            return Key.Empty;
        }

        public PageRecord[] GetPagesAtLevel(int level) {
            if (level >= MaxLevels)
                throw new IndexOutOfRangeException();
            return _pages[level].ToArray();
        }

        // Mutation Methods - Must return a new copy of the manifest
        public ManifestImmutable NextVersion(int level, out int version) {
            if (level >= MaxLevels)
                throw new IndexOutOfRangeException();
            var m = Clone();
            m._versions[level] += 1;
            version = m._versions[level];
            return m;
        }

        public ManifestImmutable NextMergePage(int level, out PageRecord page) {
            if (level >= MaxLevels)
                throw new IndexOutOfRangeException();
            var m = Clone();
            var currentKey = _mergeKeys[level];
            var levelKeys = _pages[level].Select(key => key.FirstKey).ToList();
            int pageNum = levelKeys.BinarySearch(currentKey);
            if (pageNum < 0) { pageNum = ~pageNum - 1; }
            pageNum = Math.Max(0, pageNum);

            int nextPage = pageNum >= levelKeys.Count - 1 ? 0 : pageNum + 1;
            m._mergeKeys[level] = _pages[level][nextPage].FirstKey;

            page = _pages[level][pageNum];
            return m;
        }

        // Atomically add page specifications to the manifest
        public ManifestImmutable AddPage(int level, int version, Key firstKey, Key lastKey) {
            if (level >= MaxLevels)
                throw new IndexOutOfRangeException();
            var page = new PageRecord(level, version, firstKey, lastKey);
            page.AddRef();
            var m = Clone();
            m._pages[level].Add(page);
            m._pages[level].Sort((x, y) => x.FirstKey.CompareTo(y.FirstKey));
            return m;
        }

        // Atomically add/remove page specifications to/from the manifest
        public ManifestImmutable ModifyPages(IEnumerable<PageRecord> addPages, IEnumerable<PageRef> removePages) {
            var m = Clone();
            foreach (var page in addPages) {
                if (page.Level >= MaxLevels)
                    throw new IndexOutOfRangeException();
                page.AddRef();
                m._pages[page.Level].Add(page);
                m._pages[page.Level].Sort((x, y) => x.FirstKey.CompareTo(y.FirstKey));
            }
            foreach (var pageRef in removePages) {
                if (pageRef.Level >= MaxLevels)
                    throw new IndexOutOfRangeException();
                var page = _pages[pageRef.Level].Where(p => p.Version == pageRef.Version).First();
                m._pages[pageRef.Level].Remove(page);
                m._pages[pageRef.Level].Sort((x, y) => x.FirstKey.CompareTo(y.FirstKey));
                page.Release();
            }
            return m;
        }


        // Read/Write manifest data
        public void WriteManifestContents(BinaryWriter writer) {
            long startPos = writer.BaseStream.Position;

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

        internal void ReadManifestContents(BinaryReader reader) {
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
                    Key startkey = Key.FromBytes(reader.ReadBytes(num_key_bytes));
                    num_key_bytes = reader.Read7BitEncodedInt();
                    Key endkey = Key.FromBytes(reader.ReadBytes(num_key_bytes));
                    var page = new PageRecord(level, version, startkey, endkey);
                    page.AddRef();
                    _pages[j].Add(page);
                }
            }
            for (int k = 0; k < num_pages; k++) {
                int num_key_bytes = reader.Read7BitEncodedInt();
                _mergeKeys[k] = Key.FromBytes(reader.ReadBytes(num_key_bytes));
            }
        }

        public void LogContents(Manifest manifest) {
            for (int level = 0; level < NumLevels; level++) {
                manifest.LogMessage("-------------------------------------");
                manifest.LogMessage("Level: {0} NumPages: {1} MaxPages: {2}", level, GetNumPagesAtLevel(level), Config.MaxPagesOnLevel(level));
                manifest.LogMessage("MergeKey: {0}", _mergeKeys[level]);
                manifest.LogMessage("Version: {0}", _versions[level]);
                var pages = GetPagesAtLevel(level);
                foreach (var page in pages) {
                    var mergePages = FindPagesForKeyRange(level + 1, page.FirstKey, page.LastKey).Count();
                    var pageName = string.Format("{0}-{1}.sbt", page.Level, page.Version);
                    var pageFile = Path.Combine(manifest.BaseFileName, pageName);
                    var fileFound = File.Exists(pageFile);
                    manifest.LogMessage("Page {0}-{1} [{2} -> {3}] Ref({4}) Overlap({5}) File Exists({6})", page.Level, page.Version, page.FirstKey, page.LastKey, page.RefCount, mergePages, fileFound ? "True" : "***NOT FOUND***");
                }
            }
        }

        private Manifest _manifest;

        public void AddRef() {
            foreach (var level in _pages) {
                foreach (var page in level) {
                    page.AddRef();
                }
            }
        }

        public void Dispose() {
            foreach (var level in _pages) {
                foreach (var page in level) {
                    int count = page.Release();
                    if (count == 0)
                        _manifest.NotifyPageReleased(page);
                }
            }
        }
    }

    public class Manifest {

        private Manifest() { }
        public Manifest(string baseFileName) {
            _baseFileName = baseFileName;
            Read();
        }
        public static Manifest NewDummyManifest() {
            return new Manifest();
        }
        private object manifestLock = new object();
        private LinkedList<ManifestImmutable> _manifests = new LinkedList<ManifestImmutable>();

        private string _baseFileName;
        public string BaseFileName {
            get { return _baseFileName; }
        }

        private int _manifestVersion = 0;
        public int ManifestVersion {
            get { return _manifestVersion; }
        }

        private int _altManifestVersion = Config.ManifestVersionCount / 4;
        public int AltManifestVersion {
            get { return _altManifestVersion; }
        }

        public int CurrentVersion(int level) {
            lock (manifestLock) {
                return LastManifest.CurrentVersion(level);
            }
        }

        private void CommitManifest(ManifestImmutable manifest) {
            lock (manifestLock) {
                Write(manifest);
                _manifests.AddLast(manifest);
            }
        }

        public ManifestImmutable GetLatestManifest() {
            lock (manifestLock) {
                var manifest = _manifests.Last.Value;
                manifest.AddRef();
                return manifest;
            }
        }

        private void ReleaseManifest(ManifestImmutable manifest) {
            lock (manifestLock) {
                _manifests.Remove(manifest);
            }
        }

        private ManifestImmutable LastManifest { get { return _manifests.Last.Value; } }

        public void LogContents() {
            LastManifest.LogContents(this);
        }

        // atomically acquires the next version and persists the metadata
        public int NextVersion(int level) {
            lock (manifestLock) {
                int version;
                var m = LastManifest.NextVersion(level, out version);
                CommitManifest(m);
                return version;
            }
        }

        // atomically acquires the next merge page and persists the metadata
        public PageRecord NextMergePage(int level) {
            lock (manifestLock) {
                PageRecord page;
                var m = LastManifest.NextMergePage(level, out page);
                CommitManifest(m);
                return page;
            }
        }

        public void AddPage(int level, int version, Key firstKey, Key lastKey) {
            lock (manifestLock) {
                var m = LastManifest.AddPage(level, version, firstKey, lastKey);
                CommitManifest(m);
            }
        }

        // Atomically add/remove page specifications to/from the manifest
        public void ModifyPages(IEnumerable<PageRecord> addPages, IEnumerable<PageRef> removePages) {
            lock (manifestLock) {
                var m = LastManifest.ModifyPages(addPages, removePages);
                CommitManifest(m);
            }
        }

        public void NotifyPageReleased(PageRecord pageRec) {
            string path = Config.SortedBlockTableFile(BaseFileName, pageRec.Level, pageRec.Version);
            if (File.Exists(path))
                File.Delete(path);
        }

        private void Write(ManifestImmutable m) {

            // Get an in-memory copy of the all the bytes that will be written to the manifest
            var ms = new MemoryStream();
            var writer = new BinaryWriter(ms);
            m.WriteManifestContents(writer);
            writer.Close();
            var manifestBytes = ms.ToArray();

            // Increment manifest versions, we're getting ready to write another copy
            _manifestVersion++;
            _altManifestVersion++;

            // Write primary manifest
            FileMode fileMode = FileMode.Append;
            if (_manifestVersion > Config.ManifestVersionCount) {
                fileMode = FileMode.Create;
                _manifestVersion = 0;
            }
            using (FileStream mfs = new FileStream(Config.ManifestFile(BaseFileName), fileMode, FileAccess.Write, FileShare.None, 8, FileOptions.WriteThrough)) {
                IAsyncResult manifestWrite = mfs.BeginWrite(manifestBytes, 0, manifestBytes.Length, null, null);

                // Write alternative manifest
                fileMode = FileMode.Append;
                if (_altManifestVersion > Config.ManifestVersionCount) {
                    fileMode = FileMode.Create;
                    _altManifestVersion = 0;
                }
                using (FileStream afs = new FileStream(Config.AltManifestFile(BaseFileName), fileMode, FileAccess.Write, FileShare.None, 8, FileOptions.WriteThrough)) {
                    IAsyncResult altManifestWrite = afs.BeginWrite(manifestBytes, 0, manifestBytes.Length, null, null);

                    // Wait for i/o's to complete
                    mfs.EndWrite(manifestWrite);
                    afs.EndWrite(altManifestWrite);
                }
            }
        }

        private void Read() {

            string manifestFile = Config.ManifestFile(_baseFileName);
            if (!File.Exists(manifestFile)) {
                _manifests.AddLast(new ManifestImmutable(this));
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

                var m = new ManifestImmutable(this);
                m.ReadManifestContents(reader);
                _manifests.AddLast(m);
            } catch (Exception ex) {
                LogMessage("Error reading manifest file: {0}", _baseFileName);
            } finally {
                reader.Close();
            }
        }

        public static IEnumerable<ManifestImmutable> ReadAllManifests(string baseFileName) {

            string manifestFile = Config.ManifestFile(baseFileName);
            if (!File.Exists(manifestFile)) {
                throw new FileNotFoundException("Could not find the manifest file.", manifestFile);
            }

            FileStream fs = new FileStream(manifestFile, FileMode.Open, FileAccess.Read, FileShare.None, 1024, false);
            BinaryReader reader = new BinaryReader(fs);

            try {
                do {
                    var m = new ManifestImmutable(null);
                    m.ReadManifestContents(reader);
                    yield return m;

                    reader.ReadInt32();  // Consume the size encoded at the end of each manifest
                } while (true);
            } finally {
                reader.Close();
            }
        }

        private Action<string> _logger;
        public Action<string> Logger {
            get { return _logger; }
            set { _logger = value; } 
        }

        public void LogMessage(string format, params object[] parms) {
            if (Logger != null) {
                Logger( string.Format(format, parms));   
            }
        }

        [Conditional("DEBUG")]
        public void DebugMessage(string format, params object[] parms) {
            if (Logger != null) {
                Logger(string.Format(format, parms));
            }
        }
    }

    public struct PageRef : IComparable<PageRef> {
        public int Level;
        public int Version;

        // Define the order of pages by the merging priority
        // Higher priority is "less than"
        public int CompareTo(PageRef other) {
            // compare first on level, lower level is higher priority and "less than"
            int r = Level.CompareTo(other.Level);
            if (r == 0) {
                // compare next on version, higher version is higher priority and "less than"
                return -Version.CompareTo(other.Version);
            } else {
                return r;
            }
        }
    }

    public struct Ranked<T> {
        public Ranked(T value, int rank) {
            Value = value;
            Rank = rank;
        }
        public T Value;
        public int Rank;
    }

    public static class PageRefConverter {
        public static IEnumerable<Ranked<T>> AsRanked<T>(this IEnumerable<T> items) {
            int i = 0;
            foreach (var item in items) {
                yield return new Ranked<T>(item, i);
                i++;
            }
        }
        public static IEnumerable<PageRef> OrderByPagePriority(this IEnumerable<PageRef> pages) {
            return pages.OrderBy(page => page);
        }
        public static IEnumerable<PageRef> AsPageRefs(this IEnumerable<PageRecord> pageRecords) {
            return pageRecords.Select(record => new PageRef { Level = record.Level, Version = record.Version });
        }
    }

    public class PageRecord {
        public PageRecord(int level, int version, Key firstKey, Key lastKey) {
            _level = level;
            _version = version;
            _firstKey = firstKey;
            _lastKey = lastKey;
            _snapshotReferenceCount = 0;
        }
        private int _level;
        public int Level { get { return _level; } }
        private int _version;
        public int Version { get { return _version; } }
        private Key _firstKey;
        public Key FirstKey { get { return _firstKey; } }
        private Key _lastKey;
        public Key LastKey { get { return _lastKey; } }

        private int _snapshotReferenceCount;
        public int RefCount { get { return _snapshotReferenceCount; } }

        public void AddRef() {
            Interlocked.Increment(ref _snapshotReferenceCount);
        }
        public int Release() {
            return Interlocked.Decrement(ref _snapshotReferenceCount);
        }
    }
}
