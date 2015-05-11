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
using System.Threading;
using System.IO;

namespace RazorDB {

    public class JournaledMemTable {

        public JournaledMemTable(string baseFileName, int version) {
            _baseFileName = baseFileName;
            _version = version;
            _memTable = new MemTable();

            // If the journal exists from a previous run, then load its data into the memtable
            string journalFile = Config.JournalFile(baseFileName, version);
            if (File.Exists(journalFile)) {
                var journalReader = new JournalReader(baseFileName, version);
                try {
                    foreach (var pair in journalReader.Enumerate()) {
                        _memTable.Add(pair.Key, pair.Value);
                    }
                } finally {
                    journalReader.Close();
                }
                _journal = new JournalWriter(baseFileName, version, true);
            } else {
                _journal = new JournalWriter(baseFileName, version, false);
            }

        }

        private JournalWriter _journal;
        private MemTable _memTable;
        private string _baseFileName;
        private int _version;

        public IEnumerable<KeyValuePair<Key, Value>> EnumerateSnapshot() {
            // Grab sorted copy of the internal memtable contents
            return _memTable.GetEnumerableSnapshot();
        }
        public IEnumerable<KeyValuePair<Key, Value>> EnumerateSnapshotFromKey(Key key) {
            // Grab sorted copy of the internal memtable contents
            return _memTable.GetEnumerableSnapshot().Where(pair => pair.Key.CompareTo(key) >= 0);
        }

        public int Version { get { return _version; } }

        public bool Add(Key key, Value value) {

            if (_journal == null || _memTable == null)
                return false;

            if (_journal.Add(key, value)) {
                _memTable.Add(key, value);
                return true;
            } else {
                return false;
            }

        }

        public bool Lookup(Key key, out Value value) {
            return _memTable.Lookup(key, out value);
        }

        public bool Full {
            get { return _memTable.Full; }
        }

        public Key FirstKey {
            get { return _memTable.FirstKey; }
        }

        public Key LastKey {
            get { return _memTable.LastKey; }
        }

        public void WriteToSortedBlockTable(Manifest manifest) {
            // Close the journal file, we don't need it anymore
            _journal.Close();
            // Write out the contents of the memtable to our level-0 sbt log
            _memTable.WriteToSortedBlockTable(_baseFileName, 0, _version);
            // Commit the new pages to the manifest
            manifest.AddPage(0, _version, FirstKey, LastKey);
            // Remove the journal file
            _journal.Delete();
        }

        public void Close() {
            if (_journal != null)
                _journal.Close();
            _journal = null;
            _memTable = null;
        }

    }
}
