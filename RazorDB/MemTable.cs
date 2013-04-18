﻿using System;
using System.Collections.Generic;
using System.Linq;

namespace RazorDB {
    public class MemTable {
        RazorDB.C5.TreeDictionary<Key, Value> _internalTable = new RazorDB.C5.TreeDictionary<Key, Value>();
        int _totalKeySize = 0;
        int _totalValueSize = 0;
        object _tableLock = new object();

        public void Add(Key key, Value value) {
            lock (_tableLock) {
                _totalKeySize += key.Length;
                _totalValueSize += value.Length;
                // Set value in the hashtable
                _internalTable[key] = value;
            }
        }

        public bool Lookup(Key key, out Value value) {
            lock (_tableLock) {
                return _internalTable.Find(key, out value);
            }
        }

        public int Size {
            get {
				lock (_tableLock) {
					return _totalKeySize + _totalValueSize;
				}
			}
        }

        public bool Full {
            get {
				return Size > Config.MaxMemTableSize;
			}
        }

        public Key FirstKey {
            get {
				lock (_tableLock) {
					return _internalTable.FindMin().Key;
				}
			}
        }

        public Key LastKey {
            get {
				lock (_tableLock) {
					return _internalTable.FindMax().Key;
				}
			}
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
                .Select(pair => new KeyValuePair<Key, Value>(pair.Key, pair.Value));
        }

        public IEnumerable<KeyValuePair<Key, Value>> GetEnumerableSnapshot() {
            lock (_tableLock) {
                return _internalTable.Snapshot().Select(pair => new KeyValuePair<Key, Value>(pair.Key, pair.Value));
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