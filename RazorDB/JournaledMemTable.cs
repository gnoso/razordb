using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace RazorDB {

    public class JournaledMemTable {

        public JournaledMemTable(string baseFileName, int version) {
            _baseFileName = baseFileName;
            _version = version;
            _journal = new JournalWriter(baseFileName, version);
            _memTable = new MemTable();
        }

        private JournalWriter _journal;
        private MemTable _memTable;
        private string _baseFileName;
        private int _version;

        public void Add(ByteArray key, ByteArray value) {
            _journal.Add(key, value);
            _memTable.Add(key, value);
        }

        public bool Lookup(ByteArray key, out ByteArray value) {
            return _memTable.Lookup(key, out value);
        }

        public bool Full {
            get { return _memTable.Full; }
        }

        public void AsyncWriteToSortedBlockTable() {
            ThreadPool.QueueUserWorkItem((o) => {
                // Close the journal file, we don't need it anymore
                _journal.Close();
                // Write out the contents of the memtable to our level-0 sbt log
                _memTable.WriteToSortedBlockTable(Config.SBTFile(_baseFileName, 0, _version));
                // Remove the journal
                _journal.Delete();
            });
        }

        public void Close() {
            if (_journal != null) 
                _journal.Close();
            _journal = null;
            _memTable = null;
        }

    }
}
