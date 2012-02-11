using System;
using System.Collections.Generic;
using System.Linq;

namespace RazorDB {

    public class MemTable {

        internal class VersionedValue {
            internal VersionedValue(ByteArray value, int sequenceNum) {
                Value = value;
                SequenceNum = sequenceNum;
            }
            public ByteArray Value { get; private set; }
            public int SequenceNum { get; private set; }
        }

        private Dictionary<ByteArray, List<VersionedValue> > _internalTable = new Dictionary<ByteArray, List<VersionedValue> >();
        private int _totalKeySize = 0;
        private int _totalValueSize = 0;
        private int _sequenceNum = 0;
        private object _tableLock = new object();

        public int SequenceNum {
            get { lock (_tableLock) { return _sequenceNum; } }
        }

        public void Add(ByteArray key, ByteArray value) {
            lock (_tableLock) {
                _totalKeySize += key.Length;
                _totalValueSize += value.Length;
                List<VersionedValue> values;
                if (_internalTable.TryGetValue(key, out values)) {
                    values.Add(new VersionedValue(value, _sequenceNum));
                } else {
                    values = new List<VersionedValue>();
                    values.Add(new VersionedValue(value, _sequenceNum));
                    _internalTable.Add(key, values);
                }
                _sequenceNum++;
            }
        }

        public bool Lookup(ByteArray key, out ByteArray value) {
            lock (_tableLock) {
                List<VersionedValue> values;
                if (_internalTable.TryGetValue(key, out values)) {
                    value = values.Last().Value;
                    return true;
                } else {
                    value = new ByteArray();
                    return false;
                }
            }
        }

        public int Size {
            get { lock (_tableLock) { return _totalKeySize + _totalValueSize; } }
        }

        public bool Full {
            get { return Size > Config.MaxMemTableSize; }
        }

        public ByteArray FirstKey {
            get { lock (_tableLock) { return _internalTable.Keys.Min(); } }
        }

        public ByteArray LastKey {
            get { lock (_tableLock) { return _internalTable.Keys.Max(); } }
        }

        public void WriteToSortedBlockTable(string baseFileName, int level, int version) {

            lock (_tableLock) {
                SortedBlockTableWriter tableWriter = null;
                try {
                    tableWriter = new SortedBlockTableWriter(baseFileName, level, version);

                    foreach (var pair in _internalTable.Select(pair => new KeyValuePair<ByteArray,ByteArray>(pair.Key, pair.Value.Last().Value)).OrderBy((pair) => pair.Key) ) {
                        tableWriter.WritePair(pair.Key, pair.Value);
                    }
                } finally {
                    if (tableWriter != null)
                        tableWriter.Close();
                }
            }
        }

        public IEnumerable<KeyValuePair<ByteArray, ByteArray>> Enumerate() {
            return _internalTable
                .Select(pair => new KeyValuePair<ByteArray, ByteArray>(pair.Key, pair.Value.Last().Value))
                .OrderBy((pair) => pair.Key);
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
