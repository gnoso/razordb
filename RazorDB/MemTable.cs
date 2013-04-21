using System;
using System.Collections.Generic;
using System.Linq;

namespace RazorDB {

    public class MemTable {

        private RazorDB.C5.TreeDictionary<KeyEx, Value> _internalTable = new RazorDB.C5.TreeDictionary<KeyEx, Value>();
        private int _totalKeySize = 0;
        private int _totalValueSize = 0;
        private object _tableLock = new object();

        public void Add(KeyEx key, Value value) {
            lock (_tableLock) {
                _totalKeySize += key.Length;
                _totalValueSize += value.Length;

                // Set value in the hashtable
                _internalTable[key] = value;
            }
        }

        public bool Lookup(KeyEx key, out Value value) {
            lock (_tableLock) {
                return _internalTable.Find(key, out value);
            }
        }

        public int Size {
            get { lock (_tableLock) { return _totalKeySize + _totalValueSize; } }
        }

        public bool Full {
            get { return Size > Config.MaxMemTableSize; }
        }

        public KeyEx FirstKey {
            get { lock (_tableLock) { return _internalTable.FindMin().Key; } }
        }

        public KeyEx LastKey {
            get { lock (_tableLock) { return _internalTable.FindMax().Key; } }
        }

        public void WriteToSortedBlockTable(string baseFileName, int level, int version, SortedBlockTableFormat format = SortedBlockTableFormat.Default) {

            lock (_tableLock) {
                SortedBlockTableWriter tableWriter = null;
                try {
                    tableWriter = new SortedBlockTableWriter(baseFileName, level, version, format);

                    foreach ( var pair in this.Enumerate() ) {
                        tableWriter.WritePair(pair.Key, pair.Value);
                    }
                } finally {
                    if (tableWriter != null)
                        tableWriter.Close();
                }
            }
        }

        public IEnumerable<KeyValuePair<KeyEx, Value>> Enumerate() {
            return _internalTable
                .Select(pair => new KeyValuePair<KeyEx, Value>(pair.Key, pair.Value));
        }

        public IEnumerable<KeyValuePair<KeyEx, Value>> GetEnumerableSnapshot() {
            lock (_tableLock) {
                return _internalTable.Snapshot().Select(pair => new KeyValuePair<KeyEx, Value>(pair.Key, pair.Value));
            }
        }

        public void ReadFromJournal(string fileName, int version) {
            lock (_tableLock) {
                JournalReader jr = new JournalReader(fileName, version);
                try {
                    foreach (var pair in jr.Enumerate()) {
                        Add(pair.Key, pair.Value);
                    }
                } finally {
                    jr.Close();
                }
            }
        }
    }
}