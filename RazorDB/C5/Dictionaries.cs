using System;
using System.Diagnostics;
using SCG = System.Collections.Generic;
namespace RazorDB.C5
{
  //
  // An entry in a dictionary from K to V.
  //
  [Serializable]
  public struct KeyValuePair<K, V> : IEquatable<KeyValuePair<K, V>>, IShowable
  {
    //
    // The key field of the entry
    //
    public K Key;

    //
    // The value field of the entry
    //
    public V Value;

    //
    // Create an entry with specified key and value
    //
    // <param name="key">The key</param>
    // <param name="value">The value</param>
    public KeyValuePair(K key, V value) { Key = key; Value = value; }


    //
    // Create an entry with a specified key. The value will be the default value of type <code>V</code>.
    //
    // <param name="key">The key</param>
    public KeyValuePair(K key) { Key = key; Value = default(V); }


    //
    // Pretty print an entry
    //
    // <returns>(key, value)</returns>
    [Tested]
    public override string ToString() { return "(" + Key + ", " + Value + ")"; }


    //
    // Check equality of entries. 
    //
    // <param name="obj">The other object</param>
    // <returns>True if obj is an entry of the same type and has the same key and value</returns>
    [Tested]
    public override bool Equals(object obj)
    {
      if (!(obj is KeyValuePair<K, V>))
        return false;
      KeyValuePair<K, V> other = (KeyValuePair<K, V>)obj;
      return Equals(other);
    }


    //
    // Get the hash code of the pair.
    //
    // <returns>The hash code</returns>
    [Tested]
    public override int GetHashCode() { return EqualityComparer<K>.Default.GetHashCode(Key) + 13984681 * EqualityComparer<V>.Default.GetHashCode(Value); }

    //
    // 
    //
    // <param name="other"></param>
    // <returns></returns>
    public bool Equals(KeyValuePair<K, V> other)
    {
      return EqualityComparer<K>.Default.Equals(Key, other.Key) && EqualityComparer<V>.Default.Equals(Value, other.Value);
    }

    //
    // 
    //
    // <param name="pair1"></param>
    // <param name="pair2"></param>
    // <returns></returns>
    public static bool operator ==(KeyValuePair<K, V> pair1, KeyValuePair<K, V> pair2) { return pair1.Equals(pair2); }

    //
    // 
    //
    // <param name="pair1"></param>
    // <param name="pair2"></param>
    // <returns></returns>
    public static bool operator !=(KeyValuePair<K, V> pair1, KeyValuePair<K, V> pair2) { return !pair1.Equals(pair2); }

    #region IShowable Members

    //
    // 
    //
    // <param name="stringbuilder"></param>
    // <param name="formatProvider"></param>
    // <param name="rest"></param>
    // <returns></returns>
    public bool Show(System.Text.StringBuilder stringbuilder, ref int rest, IFormatProvider formatProvider)
    {
      if (rest < 0)
        return false;
      if (!Showing.Show(Key, stringbuilder, ref rest, formatProvider))
        return false;
      stringbuilder.Append(" => ");
      rest -= 4;
      if (!Showing.Show(Value, stringbuilder, ref rest, formatProvider))
        return false;
      return rest >= 0;
    }
    #endregion

    #region IFormattable Members

    //
    // 
    //
    // <param name="format"></param>
    // <param name="formatProvider"></param>
    // <returns></returns>
    public string ToString(string format, IFormatProvider formatProvider)
    {
      return Showing.ShowString(this, format, formatProvider);
    }

    #endregion
  }



  //
  // Default comparer for dictionary entries in a sorted dictionary.
  // Entry comparisons only look at keys and uses an externally defined comparer for that.
  //
  [Serializable]
  public class KeyValuePairComparer<K, V> : SCG.IComparer<KeyValuePair<K, V>>
  {
    SCG.IComparer<K> comparer;


    //
    // Create an entry comparer for a item comparer of the keys
    //
    // <param name="comparer">Comparer of keys</param>
    public KeyValuePairComparer(SCG.IComparer<K> c)
    {
      if (comparer == null)
        throw new NullReferenceException();
      comparer = c;
    }


    //
    // Compare two entries
    //
    // <param name="entry1">First entry</param>
    // <param name="entry2">Second entry</param>
    // <returns>The result of comparing the keys</returns>
    [Tested]
    public int Compare(KeyValuePair<K, V> entry1, KeyValuePair<K, V> entry2)
    { return comparer.Compare(entry1.Key, entry2.Key); }
  }



  //
  // Default equalityComparer for dictionary entries.
  // Operations only look at keys and uses an externaly defined equalityComparer for that.
  //
  [Serializable]
  public sealed class KeyValuePairEqualityComparer<K, V> : SCG.IEqualityComparer<KeyValuePair<K, V>>
  {
    SCG.IEqualityComparer<K> keyequalityComparer;


    //
    // Create an entry equalityComparer using the default equalityComparer for keys
    //
    public KeyValuePairEqualityComparer() { keyequalityComparer = EqualityComparer<K>.Default; }


    //
    // Create an entry equalityComparer from a specified item equalityComparer for the keys
    //
    // <param name="keyequalityComparer">The key equalityComparer</param>
    public KeyValuePairEqualityComparer(SCG.IEqualityComparer<K> k)
    {
      if (keyequalityComparer == null)
        throw new NullReferenceException("Key equality comparer cannot be null");
      keyequalityComparer = k;
    }


    //
    // Get the hash code of the entry
    //
    // <param name="entry">The entry</param>
    // <returns>The hash code of the key</returns>
    [Tested]
    public int GetHashCode(KeyValuePair<K, V> entry) { return keyequalityComparer.GetHashCode(entry.Key); }


    //
    // Test two entries for equality
    //
    // <param name="entry1">First entry</param>
    // <param name="entry2">Second entry</param>
    // <returns>True if keys are equal</returns>
    [Tested]
    public bool Equals(KeyValuePair<K, V> entry1, KeyValuePair<K, V> entry2)
    { return keyequalityComparer.Equals(entry1.Key, entry2.Key); }
  }



  //
  // A base class for implementing a dictionary based on a set collection implementation.
  // <i>See the source code for <see cref="T:C5.HashDictionary`2"/> for an example</i>
  // 
  //
  [Serializable]
  public abstract class DictionaryBase<K, V> : CollectionValueBase<KeyValuePair<K, V>>, IDictionary<K, V>
  {
    //
    // The set collection of entries underlying this dictionary implementation
    //
    protected ICollection<KeyValuePair<K, V>> pairs;

    SCG.IEqualityComparer<K> keyequalityComparer;

    #region Events
    ProxyEventBlock<KeyValuePair<K, V>> eventBlock;

    //
    // The change event. Will be raised for every change operation on the collection.
    //
    public override event CollectionChangedHandler<KeyValuePair<K, V>> CollectionChanged
    {
      add { (eventBlock ?? (eventBlock = new ProxyEventBlock<KeyValuePair<K, V>>(this, pairs))).CollectionChanged += value; }
      remove { if (eventBlock != null) eventBlock.CollectionChanged -= value; }
    }

    //
    // The change event. Will be raised for every change operation on the collection.
    //
    public override event CollectionClearedHandler<KeyValuePair<K, V>> CollectionCleared
    {
      add { (eventBlock ?? (eventBlock = new ProxyEventBlock<KeyValuePair<K, V>>(this, pairs))).CollectionCleared += value; }
      remove { if (eventBlock != null) eventBlock.CollectionCleared -= value; }
    }

    //
    // The item added  event. Will be raised for every individual addition to the collection.
    //
    public override event ItemsAddedHandler<KeyValuePair<K, V>> ItemsAdded
    {
      add { (eventBlock ?? (eventBlock = new ProxyEventBlock<KeyValuePair<K, V>>(this, pairs))).ItemsAdded += value; }
      remove { if (eventBlock != null) eventBlock.ItemsAdded -= value; }
    }

    //
    // The item added  event. Will be raised for every individual removal from the collection.
    //
    public override event ItemsRemovedHandler<KeyValuePair<K, V>> ItemsRemoved
    {
      add { (eventBlock ?? (eventBlock = new ProxyEventBlock<KeyValuePair<K, V>>(this, pairs))).ItemsRemoved += value; }
      remove { if (eventBlock != null) eventBlock.ItemsRemoved -= value; }
    }

    //
    // 
    //
    public override EventTypeEnum ListenableEvents
    {
      get
      {
        return EventTypeEnum.Basic;
      }
    }

    //
    // 
    //
    public override EventTypeEnum ActiveEvents
    {
      get
      {
        return pairs.ActiveEvents;
      }
    }

    #endregion

    //
    // 
    //
    // <param name="keyequalityComparer"></param>
    protected DictionaryBase(SCG.IEqualityComparer<K> k)
    {
      if (keyequalityComparer == null)
        throw new NullReferenceException("Key equality comparer cannot be null");
      keyequalityComparer = k;
    }

    #region IDictionary<K,V> Members

    //
    // 
    //
    // <value></value>
    public virtual SCG.IEqualityComparer<K> EqualityComparer { get { return keyequalityComparer; } }


    //
    // Add a new (key, value) pair (a mapping) to the dictionary.
    //
    // <exception cref="DuplicateNotAllowedException"> if there already is an entry with the same key. </exception>
    // <param name="key">Key to add</param>
    // <param name="value">Value to add</param>
    [Tested]
    public virtual void Add(K key, V value)
    {
      KeyValuePair<K, V> p = new KeyValuePair<K, V>(key, value);

      if (!pairs.Add(p))
        throw new DuplicateNotAllowedException("Key being added: '" + key + "'");
    }

    //
    // Add the entries from a collection of <see cref="T:C5.KeyValuePair`2"/> pairs to this dictionary.
    // <para><b>TODO: add restrictions L:K and W:V when the .Net SDK allows it </b></para>
    //
    // <exception cref="DuplicateNotAllowedException"> 
    // If the input contains duplicate keys or a key already present in this dictionary.</exception>
    // <param name="entries"></param>
    public virtual void AddAll<L, W>(SCG.IEnumerable<KeyValuePair<L, W>> entries)
      where L : K
      where W : V
    {
      foreach (KeyValuePair<L, W> pair in entries)
      {
        KeyValuePair<K, V> p = new KeyValuePair<K, V>(pair.Key, pair.Value);
        if (!pairs.Add(p))
          throw new DuplicateNotAllowedException("Key being added: '" + pair.Key + "'");
      }
    }

    //
    // Remove an entry with a given key from the dictionary
    //
    // <param name="key">The key of the entry to remove</param>
    // <returns>True if an entry was found (and removed)</returns>
    [Tested]
    public virtual bool Remove(K key)
    {
      KeyValuePair<K, V> p = new KeyValuePair<K, V>(key);

      return pairs.Remove(p);
    }


    //
    // Remove an entry with a given key from the dictionary and report its value.
    //
    // <param name="key">The key of the entry to remove</param>
    // <param name="value">On exit, the value of the removed entry</param>
    // <returns>True if an entry was found (and removed)</returns>
    [Tested]
    public virtual bool Remove(K key, out V value)
    {
      KeyValuePair<K, V> p = new KeyValuePair<K, V>(key);

      if (pairs.Remove(p, out p))
      {
        value = p.Value;
        return true;
      }
      else
      {
        value = default(V);
        return false;
      }
    }


    //
    // Remove all entries from the dictionary
    //
    [Tested]
    public virtual void Clear() { pairs.Clear(); }

    //
    // 
    //
    // <value></value>
    public virtual Speed ContainsSpeed { get { return pairs.ContainsSpeed; } }

    //
    // Check if there is an entry with a specified key
    //
    // <param name="key">The key to look for</param>
    // <returns>True if key was found</returns>
    [Tested]
    public virtual bool Contains(K key)
    {
      KeyValuePair<K, V> p = new KeyValuePair<K, V>(key);

      return pairs.Contains(p);
    }

    [Serializable]
    class LiftedEnumerable<H> : SCG.IEnumerable<KeyValuePair<K, V>> where H : K
    {
      SCG.IEnumerable<H> keys;
      public LiftedEnumerable(SCG.IEnumerable<H> keys) { keys = keys; }
      public SCG.IEnumerator<KeyValuePair<K, V>> GetEnumerator() { foreach (H key in keys) yield return new KeyValuePair<K, V>(key); }

      #region IEnumerable Members

      System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
      {
        throw new Exception("The method or operation is not implemented.");
      }

      #endregion
    }

    //
    // 
    //
    // <param name="keys"></param>
    // <returns></returns>
    public virtual bool ContainsAll<H>(SCG.IEnumerable<H> keys) where H : K
    {
      return pairs.ContainsAll(new LiftedEnumerable<H>(keys));
    }

    //
    // Check if there is an entry with a specified key and report the corresponding
    // value if found. This can be seen as a safe form of "val = this[key]".
    //
    // <param name="key">The key to look for</param>
    // <param name="value">On exit, the value of the entry</param>
    // <returns>True if key was found</returns>
    [Tested]
    public virtual bool Find(K key, out V value)
    {
      return Find(ref key, out value);
    }
    //
    // Check if there is an entry with a specified key and report the corresponding
    // value if found. This can be seen as a safe form of "val = this[key]".
    //
    // <param name="key">The key to look for</param>
    // <param name="value">On exit, the value of the entry</param>
    // <returns>True if key was found</returns>
    public virtual bool Find(ref K key, out V value)
    {
      KeyValuePair<K, V> p = new KeyValuePair<K, V>(key);

      if (pairs.Find(ref p))
      {
        key = p.Key;
        value = p.Value;
        return true;
      }
      else
      {
        value = default(V);
        return false;
      }
    }


    //
    // Look for a specific key in the dictionary and if found replace the value with a new one.
    // This can be seen as a non-adding version of "this[key] = val".
    //
    // <param name="key">The key to look for</param>
    // <param name="value">The new value</param>
    // <returns>True if key was found</returns>
    [Tested]
    public virtual bool Update(K key, V value)
    {
      KeyValuePair<K, V> p = new KeyValuePair<K, V>(key, value);

      return pairs.Update(p);
    }


    //
    // 
    //
    // <param name="key"></param>
    // <param name="value"></param>
    // <param name="oldvalue"></param>
    // <returns></returns>
    public virtual bool Update(K key, V value, out V oldvalue)
    {
      KeyValuePair<K, V> p = new KeyValuePair<K, V>(key, value);

      bool retval = pairs.Update(p, out p);
      oldvalue = p.Value;
      return retval;
    }


    //
    // Look for a specific key in the dictionary. If found, report the corresponding value,
    // else add an entry with the key and the supplied value.
    //
    // <param name="key">On entry the key to look for</param>
    // <param name="value">On entry the value to add if the key is not found.
    // On exit the value found if any.</param>
    // <returns>True if key was found</returns>
    [Tested]
    public virtual bool FindOrAdd(K key, ref V value)
    {
      KeyValuePair<K, V> p = new KeyValuePair<K, V>(key, value);

      if (!pairs.FindOrAdd(ref p))
        return false;
      else
      {
        value = p.Value;
        //key = p.key;
        return true;
      }
    }


    //
    // Update value in dictionary corresponding to key if found, else add new entry.
    // More general than "this[key] = val;" by reporting if key was found.
    //
    // <param name="key">The key to look for</param>
    // <param name="value">The value to add or replace with.</param>
    // <returns>True if entry was updated.</returns>
    [Tested]
    public virtual bool UpdateOrAdd(K key, V value)
    {
      return pairs.UpdateOrAdd(new KeyValuePair<K, V>(key, value));
    }


    //
    // Update value in dictionary corresponding to key if found, else add new entry.
    // More general than "this[key] = val;" by reporting if key was found and the old value if any.
    //
    // <param name="key"></param>
    // <param name="value"></param>
    // <param name="oldvalue"></param>
    // <returns></returns>
    public virtual bool UpdateOrAdd(K key, V value, out V oldvalue)
    {
      KeyValuePair<K, V> p = new KeyValuePair<K, V>(key, value);
      bool retval = pairs.UpdateOrAdd(p, out p);
      oldvalue = p.Value;
      return retval;
    }



    #region Keys,Values support classes
    [Serializable]
    internal class ValuesCollection : CollectionValueBase<V>, ICollectionValue<V>
    {
      ICollection<KeyValuePair<K, V>> pairs;


      internal ValuesCollection(ICollection<KeyValuePair<K, V>> pairs)
      { pairs = pairs; }


      public override V Choose() { return pairs.Choose().Value; }

      [Tested]
      public override SCG.IEnumerator<V> GetEnumerator()
      {
        //Updatecheck is performed by the pairs enumerator
        foreach (KeyValuePair<K, V> p in pairs)
          yield return p.Value;
      }

      public override bool IsEmpty { get { return pairs.IsEmpty; } }

      [Tested]
      public override int Count { [Tested]get { return pairs.Count; } }

      public override Speed CountSpeed { get { return Speed.Constant; } }
    }



    [Serializable]
    internal class KeysCollection : CollectionValueBase<K>, ICollectionValue<K>
    {
      ICollection<KeyValuePair<K, V>> pairs;


      internal KeysCollection(ICollection<KeyValuePair<K, V>> pairs)
      { pairs = pairs; }

      public override K Choose() { return pairs.Choose().Key; }

      [Tested]
      public override SCG.IEnumerator<K> GetEnumerator()
      {
        foreach (KeyValuePair<K, V> p in pairs)
          yield return p.Key;
      }

      public override bool IsEmpty { get { return pairs.IsEmpty; } }

      [Tested]
      public override int Count { [Tested]get { return pairs.Count; } }

      public override Speed CountSpeed { get { return pairs.CountSpeed; } }
    }
    #endregion

    //
    // 
    //
    // <value>A collection containg the all the keys of the dictionary</value>
    [Tested]
    public virtual ICollectionValue<K> Keys { [Tested]get { return new KeysCollection(pairs); } }


    //
    // 
    //
    // <value>A collection containing all the values of the dictionary</value>
    [Tested]
    public virtual ICollectionValue<V> Values { [Tested]get { return new ValuesCollection(pairs); } }

    //
    // 
    //
    public virtual Fun<K, V> Fun { get { return delegate(K k) { return this[k]; }; } }

    //
    // Indexer by key for dictionary. 
    // <para>The get method will throw an exception if no entry is found. </para>
    // <para>The set method behaves like <see cref="M:C5.DictionaryBase`2.UpdateOrAdd(`0,`1)"/>.</para>
    //
    // <exception cref="NoSuchItemException"> On get if no entry is found. </exception>
    // <value>The value corresponding to the key</value>
    [Tested]
    public virtual V this[K key]
    {
      [Tested]
      get
      {
        KeyValuePair<K, V> p = new KeyValuePair<K, V>(key);

        if (pairs.Find(ref p))
          return p.Value;
        else
          throw new NoSuchItemException("Key '" + key.ToString() + "' not present in Dictionary");
      }
      [Tested]
      set
      { pairs.UpdateOrAdd(new KeyValuePair<K, V>(key, value)); }
    }


    //
    // 
    //
    // <value>True if dictionary is read  only</value>
    [Tested]
    public virtual bool IsReadOnly { [Tested]get { return pairs.IsReadOnly; } }


    //
    // Check the integrity of the internal data structures of this dictionary.
    //
    // <returns>True if check does not fail.</returns>
    [Tested]
    public virtual bool Check() { return pairs.Check(); }

    #endregion

    #region ICollectionValue<KeyValuePair<K,V>> Members

    //
    // 
    //
    // <value>True if this collection is empty.</value>
    public override bool IsEmpty { get { return pairs.IsEmpty; } }


    //
    // 
    //
    // <value>The number of entrues in the dictionary</value>
    [Tested]
    public override int Count { [Tested]get { return pairs.Count; } }

    //
    // 
    //
    // <value>The number of entrues in the dictionary</value>
    [Tested]
    public override Speed CountSpeed { [Tested]get { return pairs.CountSpeed; } }

    //
    // Choose some entry in this Dictionary. 
    //
    // <exception cref="NoSuchItemException">if collection is empty.</exception>
    // <returns></returns>
    public override KeyValuePair<K, V> Choose() { return pairs.Choose(); }

    //
    // Create an enumerator for the collection of entries of the dictionary
    //
    // <returns>The enumerator</returns>
    [Tested]
    public override SCG.IEnumerator<KeyValuePair<K, V>> GetEnumerator()
    {
      return pairs.GetEnumerator(); ;
    }

    #endregion

    //
    // 
    //
    // <param name="stringbuilder"></param>
    // <param name="rest"></param>
    // <param name="formatProvider"></param>
    // <returns></returns>
    public override bool Show(System.Text.StringBuilder stringbuilder, ref int rest, IFormatProvider formatProvider)
    {
      return Showing.ShowDictionary<K, V>(this, stringbuilder, ref rest, formatProvider);
    }

    //
    // 
    //
    // <returns></returns>
    public abstract object Clone();

  }

  //
  // A base class for implementing a sorted dictionary based on a sorted set collection implementation.
  // <i>See the source code for <see cref="T:C5.TreeDictionary`2"/> for an example</i>
  // 
  //
  [Serializable]
  public abstract class SortedDictionaryBase<K, V> : DictionaryBase<K, V>, ISortedDictionary<K, V>
  {
    #region Fields

    //
    // 
    //
    protected ISorted<KeyValuePair<K, V>> sortedpairs;
    SCG.IComparer<K> keycomparer;

    //
    // 
    //
    // <param name="keycomparer"></param>
    // <param name="keyequalityComparer"></param>
    protected SortedDictionaryBase(SCG.IComparer<K> keycomparer, SCG.IEqualityComparer<K> keyequalityComparer) : base(keyequalityComparer) { keycomparer = keycomparer; }

    #endregion

    #region ISortedDictionary<K,V> Members

    //
    // The key comparer used by this dictionary.
    //
    // <value></value>
    public SCG.IComparer<K> Comparer { get { return keycomparer; } }

    //
    // 
    //
    // <value></value>
    public new ISorted<K> Keys { get { return new SortedKeysCollection(this, sortedpairs, keycomparer, EqualityComparer); } }

    //
    // Find the entry in the dictionary whose key is the
    // predecessor of the specified key.
    //
    // <param name="key">The key</param>
    // <param name="res">The predecessor, if any</param>
    // <returns>True if key has a predecessor</returns>
    [Tested]
    public bool TryPredecessor(K key, out KeyValuePair<K, V> res)
    {
      return sortedpairs.TryPredecessor(new KeyValuePair<K, V>(key), out res);
    }

    //
    // Find the entry in the dictionary whose key is the
    // successor of the specified key.
    //
    // <param name="key">The key</param>
    // <param name="res">The successor, if any</param>
    // <returns>True if the key has a successor</returns>
    [Tested]
    public bool TrySuccessor(K key, out KeyValuePair<K, V> res)
    {
      return sortedpairs.TrySuccessor(new KeyValuePair<K, V>(key), out res);
    }

    //
    // Find the entry in the dictionary whose key is the
    // weak predecessor of the specified key.
    //
    // <param name="key">The key</param>
    // <param name="res">The predecessor, if any</param>
    // <returns>True if key has a weak predecessor</returns>
    [Tested]
    public bool TryWeakPredecessor(K key, out KeyValuePair<K, V> res)
    {
      return sortedpairs.TryWeakPredecessor(new KeyValuePair<K, V>(key), out res);
    }

    //
    // Find the entry in the dictionary whose key is the
    // weak successor of the specified key.
    //
    // <param name="key">The key</param>
    // <param name="res">The weak successor, if any</param>
    // <returns>True if the key has a weak successor</returns>
    [Tested]
    public bool TryWeakSuccessor(K key, out KeyValuePair<K, V> res)
    {
      return sortedpairs.TryWeakSuccessor(new KeyValuePair<K, V>(key), out res);
    }

    //
    // Get the entry in the dictionary whose key is the
    // predecessor of the specified key.
    //
    // <exception cref="NoSuchItemException"></exception>
    // <param name="key">The key</param>
    // <returns>The entry</returns>
    [Tested]
    public KeyValuePair<K, V> Predecessor(K key)
    {
      return sortedpairs.Predecessor(new KeyValuePair<K, V>(key));
    }

    //
    // Get the entry in the dictionary whose key is the
    // successor of the specified key.
    //
    // <exception cref="NoSuchItemException"></exception>
    // <param name="key">The key</param>
    // <returns>The entry</returns>
    [Tested]
    public KeyValuePair<K, V> Successor(K key)
    {
      return sortedpairs.Successor(new KeyValuePair<K, V>(key));
    }

    //
    // Get the entry in the dictionary whose key is the
    // weak predecessor of the specified key.
    //
    // <exception cref="NoSuchItemException"></exception>
    // <param name="key">The key</param>
    // <returns>The entry</returns>
    [Tested]
    public KeyValuePair<K, V> WeakPredecessor(K key)
    {
      return sortedpairs.WeakPredecessor(new KeyValuePair<K, V>(key));
    }

    //
    // Get the entry in the dictionary whose key is the
    // weak successor of the specified key.
    //
    // <exception cref="NoSuchItemException"></exception>
    // <param name="key">The key</param>
    // <returns>The entry</returns>
    [Tested]
    public KeyValuePair<K, V> WeakSuccessor(K key)
    {
      return sortedpairs.WeakSuccessor(new KeyValuePair<K, V>(key));
    }

    #endregion

    #region ISortedDictionary<K,V> Members

    //
    // 
    //
    // <returns></returns>
    public KeyValuePair<K, V> FindMin()
    {
      return sortedpairs.FindMin();
    }

    //
    // 
    //
    // <returns></returns>
    public KeyValuePair<K, V> DeleteMin()
    {
      return sortedpairs.DeleteMin();
    }

    //
    // 
    //
    // <returns></returns>
    public KeyValuePair<K, V> FindMax()
    {
      return sortedpairs.FindMax();
    }

    //
    // 
    //
    // <returns></returns>
    public KeyValuePair<K, V> DeleteMax()
    {
      return sortedpairs.DeleteMax();
    }

    //
    // 
    //
    // <param name="cutter"></param>
    // <param name="lowEntry"></param>
    // <param name="lowIsValid"></param>
    // <param name="highEntry"></param>
    // <param name="highIsValid"></param>
    // <returns></returns>
    public bool Cut(IComparable<K> cutter, out KeyValuePair<K, V> lowEntry, out bool lowIsValid, out KeyValuePair<K, V> highEntry, out bool highIsValid)
    {
      return sortedpairs.Cut(new KeyValuePairComparable(cutter), out lowEntry, out lowIsValid, out highEntry, out highIsValid);
    }

    //
    // 
    //
    // <param name="bot"></param>
    // <returns></returns>
    public IDirectedEnumerable<KeyValuePair<K, V>> RangeFrom(K bot)
    {
      return sortedpairs.RangeFrom(new KeyValuePair<K, V>(bot));
    }

    //
    // 
    //
    // <param name="bot"></param>
    // <param name="top"></param>
    // <returns></returns>
    public IDirectedEnumerable<KeyValuePair<K, V>> RangeFromTo(K bot, K top)
    {
      return sortedpairs.RangeFromTo(new KeyValuePair<K, V>(bot), new KeyValuePair<K, V>(top));
    }

    //
    // 
    //
    // <param name="top"></param>
    // <returns></returns>
    public IDirectedEnumerable<KeyValuePair<K, V>> RangeTo(K top)
    {
      return sortedpairs.RangeTo(new KeyValuePair<K, V>(top));
    }

    //
    // 
    //
    // <returns></returns>
    public IDirectedCollectionValue<KeyValuePair<K, V>> RangeAll()
    {
      return sortedpairs.RangeAll();
    }

    //
    // 
    //
    // <param name="items"></param>
    public void AddSorted(SCG.IEnumerable<KeyValuePair<K, V>> items)
    {
      sortedpairs.AddSorted(items);
    }

    //
    // 
    //
    // <param name="lowKey"></param>
    public void RemoveRangeFrom(K lowKey)
    {
      sortedpairs.RemoveRangeFrom(new KeyValuePair<K, V>(lowKey));
    }

    //
    // 
    //
    // <param name="lowKey"></param>
    // <param name="highKey"></param>
    public void RemoveRangeFromTo(K lowKey, K highKey)
    {
      sortedpairs.RemoveRangeFromTo(new KeyValuePair<K, V>(lowKey), new KeyValuePair<K, V>(highKey));
    }

    //
    // 
    //
    // <param name="highKey"></param>
    public void RemoveRangeTo(K highKey)
    {
      sortedpairs.RemoveRangeTo(new KeyValuePair<K, V>(highKey));
    }

    #endregion
    [Serializable]
    class KeyValuePairComparable : IComparable<KeyValuePair<K, V>>
    {
      IComparable<K> cutter;

      internal KeyValuePairComparable(IComparable<K> cutter) { cutter = cutter; }

      public int CompareTo(KeyValuePair<K, V> other) { return cutter.CompareTo(other.Key); }

      public bool Equals(KeyValuePair<K, V> other) { return cutter.Equals(other.Key); }
    }

    [Serializable]
    class ProjectedDirectedEnumerable : MappedDirectedEnumerable<KeyValuePair<K, V>, K>
    {
      public ProjectedDirectedEnumerable(IDirectedEnumerable<KeyValuePair<K, V>> directedpairs) : base(directedpairs) { }

      public override K Map(KeyValuePair<K, V> pair) { return pair.Key; }

    }

    [Serializable]
    class ProjectedDirectedCollectionValue : MappedDirectedCollectionValue<KeyValuePair<K, V>, K>
    {
      public ProjectedDirectedCollectionValue(IDirectedCollectionValue<KeyValuePair<K, V>> directedpairs) : base(directedpairs) { }

      public override K Map(KeyValuePair<K, V> pair) { return pair.Key; }

    }

    [Serializable]
    class SortedKeysCollection : SequencedBase<K>, ISorted<K>
    {
      ISortedDictionary<K, V> sorteddict;
      //TODO: eliminate  Only problem is the Find method because we lack method on dictionary that also 
      //      returns the actual key.
      ISorted<KeyValuePair<K, V>> sortedpairs;
      SCG.IComparer<K> comparer;

      internal SortedKeysCollection(ISortedDictionary<K, V> sorteddict, ISorted<KeyValuePair<K, V>> sortedpairs, SCG.IComparer<K> comparer, SCG.IEqualityComparer<K> itemequalityComparer)
        : base(itemequalityComparer)
      {
        sorteddict = sorteddict;
        sortedpairs = sortedpairs;
        comparer = comparer;
      }

      public override K Choose() { return sorteddict.Choose().Key; }

      public override SCG.IEnumerator<K> GetEnumerator()
      {
        foreach (KeyValuePair<K, V> p in sorteddict)
          yield return p.Key;
      }

      public override bool IsEmpty { get { return sorteddict.IsEmpty; } }

      public override int Count { [Tested]get { return sorteddict.Count; } }

      public override Speed CountSpeed { get { return sorteddict.CountSpeed; } }

      #region ISorted<K> Members

      public K FindMin() { return sorteddict.FindMin().Key; }

      public K DeleteMin() { throw new ReadOnlyCollectionException(); }

      public K FindMax() { return sorteddict.FindMax().Key; }

      public K DeleteMax() { throw new ReadOnlyCollectionException(); }

      public SCG.IComparer<K> Comparer { get { return comparer; } }

      public bool TryPredecessor(K item, out K res)
      {
          KeyValuePair<K, V> pRes;
          bool success = sorteddict.TryPredecessor(item, out pRes);
          res = pRes.Key;
          return success;
      }

      public bool TrySuccessor(K item, out K res)
      {
          KeyValuePair<K, V> pRes;
          bool success = sorteddict.TrySuccessor(item, out pRes);
          res = pRes.Key;
          return success;
      }

      public bool TryWeakPredecessor(K item, out K res)
      {
          KeyValuePair<K, V> pRes;
          bool success = sorteddict.TryWeakPredecessor(item, out pRes);
          res = pRes.Key;
          return success;
      }

      public bool TryWeakSuccessor(K item, out K res)
      {
          KeyValuePair<K, V> pRes;
          bool success = sorteddict.TryWeakSuccessor(item, out pRes);
          res = pRes.Key;
          return success;
      }

      public K Predecessor(K item) { return sorteddict.Predecessor(item).Key; }

      public K Successor(K item) { return sorteddict.Successor(item).Key; }

      public K WeakPredecessor(K item) { return sorteddict.WeakPredecessor(item).Key; }

      public K WeakSuccessor(K item) { return sorteddict.WeakSuccessor(item).Key; }

      public bool Cut(IComparable<K> c, out K low, out bool lowIsValid, out K high, out bool highIsValid)
      {
        KeyValuePair<K, V> lowpair, highpair;
        bool retval = sorteddict.Cut(c, out lowpair, out lowIsValid, out highpair, out highIsValid);
        low = lowpair.Key;
        high = highpair.Key;
        return retval;
      }

      public IDirectedEnumerable<K> RangeFrom(K bot)
      {
        return new ProjectedDirectedEnumerable(sorteddict.RangeFrom(bot));
      }

      public IDirectedEnumerable<K> RangeFromTo(K bot, K top)
      {
        return new ProjectedDirectedEnumerable(sorteddict.RangeFromTo(bot, top));
      }

      public IDirectedEnumerable<K> RangeTo(K top)
      {
        return new ProjectedDirectedEnumerable(sorteddict.RangeTo(top));
      }

      public IDirectedCollectionValue<K> RangeAll()
      {
        return new ProjectedDirectedCollectionValue(sorteddict.RangeAll());
      }

      public void AddSorted<U>(SCG.IEnumerable<U> items) where U : K { throw new ReadOnlyCollectionException(); }

      public void RemoveRangeFrom(K low) { throw new ReadOnlyCollectionException(); }

      public void RemoveRangeFromTo(K low, K hi) { throw new ReadOnlyCollectionException(); }

      public void RemoveRangeTo(K hi) { throw new ReadOnlyCollectionException(); }
      #endregion

      #region ICollection<K> Members
      public Speed ContainsSpeed { get { return sorteddict.ContainsSpeed; } }

      public bool Contains(K key) { return sorteddict.Contains(key); ;      }

      public int ContainsCount(K item) { return sorteddict.Contains(item) ? 1 : 0; }

      //
      // 
      //
      // <returns></returns>
      public virtual ICollectionValue<K> UniqueItems()
      {
        return this;
      }

      //
      // 
      //
      // <returns></returns>
      public virtual ICollectionValue<KeyValuePair<K, int>> ItemMultiplicities()
      {
        return new MultiplicityOne<K>(this);
      }


      public bool ContainsAll<U>(SCG.IEnumerable<U> items) where U : K
      {
        //TODO: optimize?
        foreach (K item in items)
          if (!sorteddict.Contains(item))
            return false;
        return true;
      }

      public bool Find(ref K item)
      {
        KeyValuePair<K, V> p = new KeyValuePair<K, V>(item);
        bool retval = sortedpairs.Find(ref p);
        item = p.Key;
        return retval;
      }

      public bool FindOrAdd(ref K item) { throw new ReadOnlyCollectionException(); }

      public bool Update(K item) { throw new ReadOnlyCollectionException(); }

      public bool Update(K item, out K olditem) { throw new ReadOnlyCollectionException(); }

      public bool UpdateOrAdd(K item) { throw new ReadOnlyCollectionException(); }

      public bool UpdateOrAdd(K item, out K olditem) { throw new ReadOnlyCollectionException(); }

      public bool Remove(K item) { throw new ReadOnlyCollectionException(); }

      public bool Remove(K item, out K removeditem) { throw new ReadOnlyCollectionException(); }

      public void RemoveAllCopies(K item) { throw new ReadOnlyCollectionException(); }

      public void RemoveAll<U>(SCG.IEnumerable<U> items) where U : K { throw new ReadOnlyCollectionException(); }

      public void Clear() { throw new ReadOnlyCollectionException(); }

      public void RetainAll<U>(SCG.IEnumerable<U> items) where U : K { throw new ReadOnlyCollectionException(); }

      #endregion

      #region IExtensible<K> Members
      public override bool IsReadOnly { get { return true; } }

      public bool AllowsDuplicates { get { return false; } }

      public bool DuplicatesByCounting { get { return true; } }

      public bool Add(K item) { throw new ReadOnlyCollectionException(); }

      void SCG.ICollection<K>.Add(K item) { throw new ReadOnlyCollectionException(); }

      public void AddAll(System.Collections.Generic.IEnumerable<K> items) { throw new ReadOnlyCollectionException(); }

      public void AddAll<U>(System.Collections.Generic.IEnumerable<U> items) where U : K { throw new ReadOnlyCollectionException(); }

      public bool Check() { return sorteddict.Check(); }

      #endregion

      #region IDirectedCollectionValue<K> Members

      public override IDirectedCollectionValue<K> Backwards()
      {
        return RangeAll().Backwards();
      }

      #endregion

      #region IDirectedEnumerable<K> Members

      IDirectedEnumerable<K> IDirectedEnumerable<K>.Backwards() { return Backwards(); }
      #endregion

      #region ICloneable Members

      //TODO: test
      //
      // Make a shallow copy of this SortedKeysCollection.
      //
      // <returns></returns>
      public virtual object Clone()
      {
        //
        SortedArrayDictionary<K, V> dictclone = new SortedArrayDictionary<K, V>(sortedpairs.Count, comparer, EqualityComparer);
        SortedArray<KeyValuePair<K, V>> sortedpairsclone = (SortedArray<KeyValuePair<K, V>>)(dictclone.sortedpairs);
        foreach (K key in sorteddict.Keys)
        {
          sortedpairsclone.Add(new KeyValuePair<K, V>(key, default(V)));
        }
        return new SortedKeysCollection(dictclone, sortedpairsclone, comparer, EqualityComparer);
      }

      #endregion

    }

    //
    // 
    //
    // <param name="stringbuilder"></param>
    // <param name="rest"></param>
    // <param name="formatProvider"></param>
    // <returns></returns>
    public override bool Show(System.Text.StringBuilder stringbuilder, ref int rest, IFormatProvider formatProvider)
    {
      return Showing.ShowDictionary<K, V>(this, stringbuilder, ref rest, formatProvider);
    }

  }

  [Serializable]
  class SortedArrayDictionary<K, V> : SortedDictionaryBase<K, V>, IDictionary<K, V>, ISortedDictionary<K, V>
  {
    #region Constructors

    public SortedArrayDictionary() : this(Comparer<K>.Default, EqualityComparer<K>.Default) { }

    //
    // Create a red-black tree dictionary using an external comparer for keys.
    //
    // <param name="comparer">The external comparer</param>
    public SortedArrayDictionary(SCG.IComparer<K> comparer) : this(comparer, new ComparerZeroHashCodeEqualityComparer<K>(comparer)) { }

    //
    // 
    //
    // <param name="comparer"></param>
    // <param name="equalityComparer"></param>
    public SortedArrayDictionary(SCG.IComparer<K> comparer, SCG.IEqualityComparer<K> equalityComparer)
      : base(comparer, equalityComparer)
    {
      pairs = sortedpairs = new SortedArray<KeyValuePair<K, V>>(new KeyValuePairComparer<K, V>(comparer));
    }

    //
    // 
    //
    // <param name="comparer"></param>
    // <param name="equalityComparer"></param>
    // <param name="capacity"></param>
    public SortedArrayDictionary(int capacity, SCG.IComparer<K> comparer, SCG.IEqualityComparer<K> equalityComparer)
      : base(comparer, equalityComparer)
    {
      pairs = sortedpairs = new SortedArray<KeyValuePair<K, V>>(capacity, new KeyValuePairComparer<K, V>(comparer));
    }
    #endregion

    //
    // 
    //
    // <returns></returns>
    public override object Clone()
    {
      SortedArrayDictionary<K, V> clone = new SortedArrayDictionary<K, V>(Comparer, EqualityComparer);
      clone.sortedpairs.AddSorted(sortedpairs);
      return clone;
    }

  }
}