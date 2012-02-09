using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;

namespace RazorDB {

    internal class SkipListNode<TKey,TValue> {

        public SkipListNode(TKey key, TValue value, int height) {
            _skipPointers = new SkipListNode<TKey, TValue>[height];
            _key = key;
            _value = value;
        }

        private TKey _key;
        public TKey Key {
            get { return _key; }
        }

        private TValue _value;
        public TValue Value {
            get { return _value; }
            set { _value = value; }
        } 

        internal SkipListNode<TKey,TValue>[] _skipPointers;
    }

    public class SkipList<TKey, TValue> : IDictionary<TKey, TValue> {

        public const int MaxHeight = 12;

        private int _count;
        private int _currentLevel;
        private SkipListNode<TKey, TValue> _header;

        public SkipList() {
            Initialize();
        }

        public void Initialize() {
            _count = 0;
            _currentLevel = 1;
            _header = new SkipListNode<TKey, TValue>(default(TKey), default(TValue), MaxHeight);
            for (int i = 0; i < MaxHeight; i++) {
                _header._skipPointers[i] = _header;
            }
        }

        private const int BranchingFactor = 4;
        private Random _rand = new Random();
        private int GetHeightForNewNode() {
            // Increase height with probability 1 in BranchingFactor
            int height = 1;
            while (height < MaxHeight && ((_rand.Next() % BranchingFactor) == 0)) {
                height++;
            }
            return height;
        }

        public void Add(TKey key, TValue value) {
            if (key == null)
                throw new ArgumentNullException();

            SkipListNode<TKey,TValue>[] update = new SkipListNode<TKey,TValue>[MaxHeight];
            SkipListNode<TKey,TValue> node; 

            // If key does not already exist in the skip list.
            if(!Search(key, out node, update)) {
                // Insert key/value pair into the skip list.
                Insert(key, value, update);
            } else {
                throw new ArgumentException("Key already exists in the collection.");
            }

        }

        private bool Search(TKey key, out SkipListNode<TKey, TValue> foundNode, SkipListNode<TKey, TValue>[] update) {

            bool found = false;
            foundNode = _header;

            // Work our way down from the top of the skip list to the bottom.
            for (int i = _currentLevel - 1; i >= 0; i--) {

                // While we haven't reached the end of the skip list and the 
                // current key is less than the search key.
                while (foundNode._skipPointers[i] != _header && Comparer<TKey>.Default.Compare(foundNode._skipPointers[i].Key, key) < 0) {
                    // Move forward in the skip list.
                    foundNode = foundNode._skipPointers[i];
                }

                // Keep track of each node where we move down a level. This 
                // will be used later to rearrange node references when 
                // inserting a new element.
                update[i] = foundNode;
            }

            // Move ahead in the skip list. If the new key doesn't already 
            // exist in the skip list, this should put us at either the end of
            // the skip list or at a node with a key greater than the search key.
            // If the new key already exists in the skip list, this should put 
            // us at a node with a key equal to the search key.
            foundNode = foundNode._skipPointers[0];

            // If we haven't reached the end of the skip list and the 
            // current key is equal to the search key.
            if (foundNode != _header && Comparer<TKey>.Default.Compare(foundNode.Key, key) == 0) {
                // Indicate that we've found the search key.
                found = true;
            }

            return found;
        } 

        private void Insert(TKey key, TValue val, SkipListNode<TKey,TValue>[] update) { 

            int height = GetHeightForNewNode();
    
            // If this height is higher than anything else in the list, then make sure we extend the pointers in the update to point to the NIL node.
            if (height > _currentLevel) {
                for (int i = _currentLevel; i < height; i++) {
                    update[i] = _header;
                }
                _currentLevel = height;
            }
 
            SkipListNode<TKey, TValue> node = new SkipListNode<TKey, TValue>(key, val, height);
 
            // Insert the new node into the skip list.
            for (int i = 0; i < height; i++) {
                // set up this node to point to the next nodes in the list
                node._skipPointers[i] = update[i]._skipPointers[i];
 
                // Point the update node to our newly inserted node
                update[i]._skipPointers[i] = node;
            }

            _count++;
        }

        public void Add(KeyValuePair<TKey, TValue> item) {
            Add(item.Key, item.Value);
        }

        public bool ContainsKey(TKey key) {
            if (key == null)
                throw new ArgumentNullException();

            SkipListNode<TKey, TValue>[] update = new SkipListNode<TKey, TValue>[MaxHeight];
            SkipListNode<TKey, TValue> foundNode;
            return Search(key, out foundNode, update);
        }

        public ICollection<TKey> Keys {
            get { return this.Select(pair => pair.Key).ToList(); }
        }

        public bool Remove(TKey key) {
            SkipListNode<TKey, TValue>[] update = new SkipListNode<TKey, TValue>[MaxHeight];
            SkipListNode<TKey, TValue> foundNode;

            if (Search(key, out foundNode, update)) {
                // Take the forward references that point to the node to be 
                // removed and reassign them to the nodes that come after it.
                for (int i = 0; i < _currentLevel && update[i]._skipPointers[i] == foundNode; i++) {
                    update[i]._skipPointers[i] = foundNode._skipPointers[i];
                }

                // After removing the node, we may need to lower the current 
                // skip list level if the node had the highest level of all of
                // the nodes.
                while (_currentLevel > 1 && _header._skipPointers[_currentLevel - 1] == _header) {
                    _currentLevel--;
                }

                // Keep track of the number of nodes.
                _count--;

                return true;
            } else {
                return false;
            }
        }

        public bool TryGetValue(TKey key, out TValue value) {
            SkipListNode<TKey, TValue>[] update = new SkipListNode<TKey, TValue>[MaxHeight];
            SkipListNode<TKey, TValue> foundNode;

            if (Search(key, out foundNode, update)) {
                value = foundNode.Value;
                return true;
            } else {
                value = default(TValue);
                return false;
            }
        }

        public ICollection<TValue> Values {
            get { return this.Select(pair => pair.Value).ToList(); }
        }

        public TValue this[TKey key] {
            get {
                if (key == null)
                    throw new ArgumentNullException();

                SkipListNode<TKey,TValue>[] update = new SkipListNode<TKey,TValue>[MaxHeight];
                SkipListNode<TKey,TValue> foundNode;

                if (Search(key, out foundNode, update)) {
                    return foundNode.Value;
                } else {
                    throw new KeyNotFoundException();
                }
            }
            set {
                if (key == null)
                    throw new ArgumentNullException();

                SkipListNode<TKey, TValue>[] update = new SkipListNode<TKey, TValue>[MaxHeight];
                SkipListNode<TKey, TValue> foundNode;

                if (Search(key, out foundNode, update)) {
                    foundNode.Value = value;
                } else {
                    Insert(key, value, update);
                }
            }
        }

        public void Clear() {
            Initialize();
        }

        public bool Contains(KeyValuePair<TKey, TValue> item) {
            return ContainsKey(item.Key);
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex) {
            using (var e = this.GetEnumerator()) {
                for (int i = arrayIndex; i < array.Length; i++) {
                    if (e.MoveNext()) {
                        array[i] = new KeyValuePair<TKey,TValue>(e.Current.Key, e.Current.Value);
                    } else { 
                        return;
                    }
                }
            }
        }

        public int Count {
            get { return _count; }
        }

        public bool IsReadOnly {
            get { return false; }
        }

        public bool Remove(KeyValuePair<TKey, TValue> item) {
            return Remove(item.Key);
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator() {
            SkipListNode<TKey,TValue> foundNode = _header;
            // Advance to the first node
            foundNode = foundNode._skipPointers[0];
            while (foundNode != _header) {
                yield return new KeyValuePair<TKey, TValue>(foundNode.Key, foundNode.Value);
                foundNode = foundNode._skipPointers[0];
            }
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() {
            return GetEnumerator();
        }
    }

}
