using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RazorDB {

    public class Cache {

        public Cache() {
            _blockTableIndexCache = new Dictionary<string, WeakReference>();
        }

        private Dictionary<string, WeakReference> _blockTableIndexCache;
        
        public ByteArray[] GetBlockTableIndex(string baseName, int level, int version) {
            string fileName = Config.SortedBlockTableFile(baseName, level, version);
            WeakReference indexRef;
            if (_blockTableIndexCache.TryGetValue(fileName, out indexRef)) {
                object idx = indexRef.Target;
                if (idx != null)
                    return (ByteArray[])idx;
            }
            var sbt = new SortedBlockTable(baseName, level, version);
            try {
                ByteArray[] index = sbt.GetIndex();
                _blockTableIndexCache[fileName] = new WeakReference(index);
                return index;
            } finally {
                sbt.Close();
            }
        }

    }
}
