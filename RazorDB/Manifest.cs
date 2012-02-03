using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace RazorDB {

    public class Manifest {

        public Manifest(string baseFileName) {
            _baseFileName = baseFileName;
            _pages = new List<PageRecord>[num_levels]; 
            for ( int i=0; i < num_levels; i++) {
                _pages[i] = new List<PageRecord>();
            }
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

        private const int num_levels = 10;
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
                }
            }

            int size = (int)(writer.BaseStream.Position - startPos);
            writer.Write(size);
        }

        private void Read() {

            string manifestFile = Config.ManifestFile(_baseFileName);
            if (!File.Exists(manifestFile)) {
                return;
            }

            FileStream fs = new FileStream(manifestFile, FileMode.Open, FileAccess.Read, FileShare.None, 1024, false);
            BinaryReader reader = new BinaryReader(fs);

            // Get the size of the last manifest block
            reader.BaseStream.Seek(-4, SeekOrigin.End);
            int size = reader.ReadInt32();

            // Now seek to that position and read it
            reader.BaseStream.Seek(-size - 4, SeekOrigin.End);

            try {
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
                        ByteArray key = new ByteArray(reader.ReadBytes(num_key_bytes));
                        _pages[j].Add(new PageRecord(level, version, key));
                    }
                }

            } finally {
                reader.Close();
            }
        }

        public PageRecord[] GetPagesAtLevel(int level) {
            if (level >= num_levels)
                throw new IndexOutOfRangeException();
            lock (manifestLock) {
                return _pages[level].ToArray();
            }
        }

        public void AddPage(int level, int version, ByteArray firstKey) {
            if (level >= num_levels)
                throw new IndexOutOfRangeException();
            lock (manifestLock) {
                _pages[level].Add( new PageRecord(level, version, firstKey) );
                _pages[level].Sort((x, y) => x.FirstKey.CompareTo(y.FirstKey));
                Write();
            }
        }

        // Atomically add/remove page specifications to/from the manifest
        public void ModifyPages( IEnumerable<PageRecord> addPages, IEnumerable<PageRef> removePages ) {
            lock (manifestLock) {
                foreach (var page in addPages) {
                    if (page.Level >= num_levels)
                        throw new IndexOutOfRangeException();
                    _pages[page.Level].Add(page);
                    _pages[page.Level].Sort((x, y) => x.FirstKey.CompareTo(y.FirstKey));
                }
                foreach (var pageRef in removePages) {
                    if (pageRef.Level >= num_levels)
                        throw new IndexOutOfRangeException();
                    _pages[pageRef.Level].RemoveAll(p => p.Version == pageRef.Version);
                    _pages[pageRef.Level].Sort((x, y) => x.FirstKey.CompareTo(y.FirstKey));
                }
                Write();
            }
        }
    }

    public struct PageRef {
        public int Level;
        public int Version;
    }

    public struct PageRecord {
        public PageRecord(int level, int version, ByteArray firstKey) {
            _level = level;
            _version = version;
            _firstKey = firstKey;
        }
        private int _level;
        public int Level { get { return _level; } }
        private int _version;
        public int Version { get { return _version; } }
        private ByteArray _firstKey;
        public ByteArray FirstKey { get { return _firstKey;  } }
    }
}
