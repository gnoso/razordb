using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;

namespace RazorDB {
    
    public class KeyValueStore : IDisposable {

        public KeyValueStore(string baseFileName) {
            _manifest = new Manifest(baseFileName);
            _currentJournaledMemTable = new JournaledMemTable(_manifest.BaseFileName, _manifest.CurrentVersion(0));
        }

        ~KeyValueStore() {
            Dispose();
        }

        private Manifest _manifest;

        private volatile JournaledMemTable _currentJournaledMemTable;

        public void Set(byte[] key, byte[] value) {
            var k = new ByteArray(key);
            var v = new ByteArray(value);

            int adds = 10;
            while (!_currentJournaledMemTable.Add(k, v)) {
                adds--;
                if (adds <= 0)
                    throw new InvalidOperationException("Failed too many times trying to add an item to the JournaledMemTable");
            }

            if (_currentJournaledMemTable.Full) {
                RotateMemTable();
            }
        }

        public byte[] Get(byte[] key) {
            ByteArray output;
            if (_currentJournaledMemTable.Lookup(new ByteArray(key), out output)) {
                return output.InternalBytes;
            } else {
                return null;
            }
        }

        private object memTableRotationLock = new object();

        public void RotateMemTable() {
            lock (memTableRotationLock) {
                // Double check the flag in case we have multiple threads that make it into this routine
                if (_currentJournaledMemTable.Full) {
                    #pragma warning disable 420
                    var oldMemTable = Interlocked.Exchange<JournaledMemTable>(ref _currentJournaledMemTable, new JournaledMemTable(_manifest.BaseFileName, _manifest.NextVersion(0)));
                    #pragma warning restore 420
                    _manifest.AddPage(0, oldMemTable.Version, oldMemTable.FirstKey);
                    oldMemTable.AsyncWriteToSortedBlockTable();
                }
            }
        }

        public void Dispose() {
            Close();
        }

        public void Close() {
            if (_currentJournaledMemTable != null) {
                _currentJournaledMemTable.Close();
                _currentJournaledMemTable = null;
            }
        }
    }

}
