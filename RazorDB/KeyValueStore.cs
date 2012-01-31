using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RazorDB {
    
    public class KeyValueStore {

        public KeyValueStore(string folder) {
            _currentMemTable = new MemTable();
        }

        private MemTable _currentMemTable;

        public void Set(byte[] key, byte[] value) {
            _currentMemTable.Add( new ByteArray(key), new ByteArray(value) );
        }

        public byte[] Get(byte[] key) {
            ByteArray output;
            if (_currentMemTable.Lookup(new ByteArray(key), out output)) {
                // Found
            }
            return output.InternalBytes;
        }

    }

}
