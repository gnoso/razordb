using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RazorDB {

    internal class CacheEntry<T> {
        internal T Value;
        internal LinkedListNode<CacheEntry<T>> ListNode;
    }

    public class Cache<T> {
        public Cache(int sizeLimit) {
            _sizeLimit = sizeLimit;
        }
        private int _sizeLimit;
        private int _currentSize;
        private Dictionary<ByteArray, CacheEntry<T>> _hash = new Dictionary<ByteArray,CacheEntry<T>>();
        private LinkedList<CacheEntry<T>> _list = new LinkedList<CacheEntry<T>>();
    
        public bool TryGetValue(ByteArray key, out T value) {
            CacheEntry<T> val;
            bool res = _hash.TryGetValue(key, out val);
            value = res ? val.Value : default(T);
            return res;
        }

        public void Set(ByteArray key, T value) {
            var ce = new CacheEntry<T> { Value = value };
            var node = _list.AddFirst(ce);
            ce.ListNode = node;
            _hash.Add(key,ce);
        }
    }
    
    public class RazorCache {

        public RazorCache() {
            _indexCache = new Dictionary<ByteArray, ByteArray[]>();
            _blockTableIndexCache = new Dictionary<string, WeakReference>();
        }

        private Dictionary<string, WeakReference> _blockTableIndexCache;
        private Dictionary<ByteArray, ByteArray[]> _indexCache;

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
