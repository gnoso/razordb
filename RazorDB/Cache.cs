using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RazorDB {

    internal class CacheEntry<T> {
        internal string Key;
        internal T Value;
        internal int Size;
        internal LinkedListNode<CacheEntry<T>> ListNode;
    }

    public class Cache<T> {
        public Cache(int sizeLimit, Func<T,int> sizer) {
            _sizeLimit = sizeLimit;
            _sizer = sizer;
        }
        int _sizeLimit;
        int _currentSize;
        public int CurrentSize {
            get { return _currentSize; }
        }
        Func<T, int> _sizer;

        Dictionary<string, CacheEntry<T>> _hash = new Dictionary<string, CacheEntry<T>>();
        LinkedList<CacheEntry<T>> _list = new LinkedList<CacheEntry<T>>();
        object _lock = new object();

        public bool TryGetValue(string key, out T value) {
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

        public void Set(string key, T value) {
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

        void CheckCacheSizeAndEvict() {
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
            _blockIndexCache = new Cache<KeyEx[]>(Config.IndexCacheSize, index => index.Sum(ba => ba.Length));
            _blockDataCache = new Cache<byte[]>(Config.DataBlockCacheSize, block => block.Length);
        }

        Cache<KeyEx[]> _blockIndexCache;
        Cache<byte[]> _blockDataCache;

        public KeyEx[] GetBlockTableIndex(string baseName, int level, int version) {

            string fileName = Config.SortedBlockTableFile(baseName, level, version);
            KeyEx[] index;

            if (_blockIndexCache.TryGetValue(fileName, out index)) {
                return index;
            }
            var sbt = new SortedBlockTable(null, baseName, level, version);
            try {
                index = sbt.GetIndex();
                _blockIndexCache.Set(fileName, index);
                return index;
            } finally {
                sbt.Close();
            }
        }

        public byte[] GetBlock(string baseName, int level, int version, int blockNum) {
            string blockKey = Config.SortedBlockTableFile(baseName, level, version) + ":" + blockNum.ToString();
            byte[] block = null;
            _blockDataCache.TryGetValue(blockKey, out block);
            return block;
        }

        public void SetBlock(string baseName, int level, int version, int blockNum, byte[] block) {
            string blockKey = Config.SortedBlockTableFile(baseName, level, version) + ":" + blockNum.ToString();
            _blockDataCache.Set(blockKey, block);
        }
    }
}