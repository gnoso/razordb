using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace RazorDB {

    public class Manifest {

        public Manifest(string baseFileName) {
            _baseFileName = baseFileName;
            Read();
        }
        private object manifestLock = new object();

        private string _baseFileName;
        public string BaseFileName {
            get { return _baseFileName; }
        }

        private int _manifestVersion = 0;
        public int ManifestVersion {
            get { return _manifestVersion; }
        }

        private const int num_versions = 10;
        private int[] _versions = new int[num_versions];
        public int CurrentVersion(int level) {
            if (level >= num_versions)
                throw new IndexOutOfRangeException();
            lock (manifestLock) {
                return _versions[level];
            }
        }

        // atomically acquires the next version and persists the metadata
        public int NextVersion(int level) {
            if (level >= num_versions)
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

            writer.Write(ManifestVersion);
            writer.Write(_versions.Length);
            foreach (var b in _versions) {
                writer.Write(b);
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
                _manifestVersion = reader.ReadInt32();
                int num_versions = reader.ReadInt32();
                for (int i = 0; i < num_versions; i++) {
                    _versions[i] = reader.ReadInt32();
                }
            } finally {
                reader.Close();
            }
        }

    }
}
