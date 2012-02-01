using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;

namespace RazorDB {
    
    public class KeyValueStore : IDisposable {

        public KeyValueStore(string baseFileName) {
            _baseFileName = baseFileName;
            string directoryName = Path.GetDirectoryName(baseFileName);
            if (!Directory.Exists(directoryName))
                Directory.CreateDirectory(directoryName);

            _currentMemTable = new MemTable();
            _currentJournal = new JournalWriter(_baseFileName, _journal_version);
        }

        ~KeyValueStore() {
            Dispose();
        }

        private string _baseFileName;
        private MemTable _currentMemTable;
        private JournalWriter _currentJournal;
        private int _journal_version = 0;
        private int _level_0_version = 0;

        public void Set(byte[] key, byte[] value) {
            var k = new ByteArray(key);
            var v = new ByteArray(value);
            _currentJournal.Add( k, v );
            _currentMemTable.Add( k, v );
            if (_currentMemTable.Full) {
                FlushCurrentMemTable();
            }
        }

        public byte[] Get(byte[] key) {
            ByteArray output;
            if (_currentMemTable.Lookup(new ByteArray(key), out output)) {
                return output.InternalBytes;
            } else {
                return null;
            }
        }

        private object memTableFlushLock = new object();

        public void FlushCurrentMemTable() {
            lock (memTableFlushLock) {
                // Double check the flag in case we have multiple threads that make it into this routine
                if (_currentMemTable.Full) {
                    var oldMemTable = Interlocked.Exchange<MemTable>(ref _currentMemTable, new MemTable());
                    var version = _level_0_version++;
                    new Thread(() => {
                        oldMemTable.WriteToSortedBlockTable(Config.SBTFile(_baseFileName, 0, version));
                    }).Start();
                }
            }
        }

        public void Dispose() {
            Close();
        }

        public void Close() {
            if (_currentJournal != null) {
                _currentJournal.Close();
                _currentJournal = null;
            }
            _currentMemTable = null;
        }
    }

}
