using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RazorDB {

    internal class CacheEntry<T> {
        internal ByteArray Key;
        internal T Value;
        internal int Size;
        internal LinkedListNode<CacheEntry<T>> ListNode;
    }

    public class Cache<T> {
        public Cache(int sizeLimit, Func<T,int> sizer) {
            _sizeLimit = sizeLimit;
            _sizer = sizer;
        }
        private int _sizeLimit;
        private int _currentSize;
        public int CurrentSize {
            get { return _currentSize; }
        }
        private Func<T, int> _sizer;

        private Dictionary<ByteArray, CacheEntry<T>> _hash = new Dictionary<ByteArray, CacheEntry<T>>();
        private LinkedList<CacheEntry<T>> _list = new LinkedList<CacheEntry<T>>();
        private object _lock = new object();

        public bool TryGetValue(ByteArray key, out T value) {
            lock (_lock) {
                CacheEntry<T> val;
                bool exists = _hash.TryGetValue(key, out val);
                if (exists) {
                    // move the item to the top of the LRU list
                    _list.Remove(val.ListNode);
                    _list.AddFirst(val.ListNode);
                }
                // Set the output parameter
                value = exists ? val.Value : default(T);
                return exists;
            }
        }

        public void Set(ByteArray key, T value) {
            lock (_lock) {

                // If the hash already contains the key, we are probably in a race condition, so go ahead and abort.
                if (_hash.ContainsKey(key))
                    return;

                var cacheEntry = new CacheEntry<T> { Value = value, Size = _sizer(value), Key = key };
                var node = _list.AddFirst(cacheEntry);
                cacheEntry.ListNode = node;
                _hash.Add(key, cacheEntry);
                _currentSize += _sizer(value);

                CheckCacheSizeAndEvict();
            }
        }

        private void CheckCacheSizeAndEvict() {
            while (_currentSize > _sizeLimit) {

                var lastEntry = _list.Last;
                var cacheEntry = lastEntry.Value;

                // Subtract the last entry from the size
                _currentSize -= cacheEntry.Size;
                // Remove from list
                _list.RemoveLast();
                // Remove from hash
                _hash.Remove(cacheEntry.Key);
            }
        }
    }
    
    public class RazorCache {

        public RazorCache() {
            _blockIndexCache = new Cache<ByteArray[]>(Config.IndexCacheSize, index => index.Sum(ba => ba.Length));
        }

        private Cache<ByteArray[]> _blockIndexCache;

        public ByteArray[] GetBlockTableIndex(string baseName, int level, int version) {

            string fileName = Config.SortedBlockTableFile(baseName, level, version);
            ByteArray key = new ByteArray(Encoding.UTF8.GetBytes(fileName));
            ByteArray[] index;

            if (_blockIndexCache.TryGetValue(key, out index)) {
                return index;
            }
            var sbt = new SortedBlockTable(baseName, level, version);
            try {
                index = sbt.GetIndex();
                _blockIndexCache.Set(key, index);
                return index;
            } finally {
                sbt.Close();
            }
        }

    }
}
