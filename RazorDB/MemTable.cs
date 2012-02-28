using System;
using System.Collections.Generic;
using System.Linq;

namespace RazorDB {

    public class MemTable {

        private Dictionary<Key, Value> _internalTable = new Dictionary<Key, Value>();
        private int _totalKeySize = 0;
        private int _totalValueSize = 0;
        private object _tableLock = new object();

        public void Add(Key key, Value value) {
            lock (_tableLock) {
                _totalKeySize += key.Length;
                _totalValueSize += value.Length;

                Value currentValue;
                if (_internalTable.TryGetValue(key, out currentValue)) {
                    // if we are replacing a value, then subtract its size from our object accounting
                    _totalKeySize -= key.Length;
                    _totalValueSize -= currentValue.Length;
                }
                // Set value in the hashtable
                _internalTable[key] = value;
            }
        }

        public bool Lookup(Key key, out Value value) {
            lock (_tableLock) {
                return _internalTable.TryGetValue(key, out value);
            }
        }

        public int Size {
            get { lock (_tableLock) { return _totalKeySize + _totalValueSize; } }
        }

        public bool Full {
            get { return Size > Config.MaxMemTableSize; }
        }

        public Key FirstKey {
            get { lock (_tableLock) { return _internalTable.Keys.Min(); } }
        }

        public Key LastKey {
            get { lock (_tableLock) { return _internalTable.Keys.Max(); } }
        }

        public void WriteToSortedBlockTable(string baseFileName, int level, int version) {

            lock (_tableLock) {
                SortedBlockTableWriter tableWriter = null;
                try {
                    tableWriter = new SortedBlockTableWriter(baseFileName, level, version);

                    foreach ( var pair in this.Enumerate() ) {
                        tableWriter.WritePair(pair.Key, pair.Value);
                    }
                } finally {
                    if (tableWriter != null)
                        tableWriter.Close();
                }
            }
        }

        public IEnumerable<KeyValuePair<Key, Value>> Enumerate() {
            return _internalTable
                .Select(pair => new KeyValuePair<Key, Value>(pair.Key, pair.Value))
                .OrderBy((pair) => pair.Key);
        }

        public IEnumerable<KeyValuePair<Key, Value>> GetEnumerableSnapshot() {
            lock (_tableLock) {
                return Enumerate().ToList();
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
