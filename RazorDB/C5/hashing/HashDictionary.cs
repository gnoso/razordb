using System;
using System.Diagnostics;
using SCG = System.Collections.Generic;

namespace RazorDB.C5
{
  /// <summary>
  /// A generic dictionary class based on a hash set class <see cref="T:C5.HashSet`1"/>. 
  /// </summary>
  [Serializable]
  public class HashDictionary<K, V> : DictionaryBase<K, V>, IDictionary<K, V>
  {
    /// <summary>
    /// Create a hash dictionary using a default equalityComparer for the keys.
    /// Initial capacity of internal table will be 16 entries and threshold for 
    /// expansion is 66% fill.
    /// </summary>
    public HashDictionary() : this(EqualityComparer<K>.Default) { }

    /// <summary>
    /// Create a hash dictionary using a custom equalityComparer for the keys.
    /// Initial capacity of internal table will be 16 entries and threshold for 
    /// expansion is 66% fill.
    /// </summary>
    /// <param name="keyequalityComparer">The external key equalityComparer</param>
    public HashDictionary(SCG.IEqualityComparer<K> keyequalityComparer) : base(keyequalityComparer)
    {
      pairs = new HashSet<KeyValuePair<K, V>>(new KeyValuePairEqualityComparer<K, V>(keyequalityComparer));
    }

    /// <summary>
    /// Create a hash dictionary using a custom equalityComparer and prescribing the 
    /// initial size of the dictionary and a non-default threshold for internal table expansion.
    /// </summary>
    /// <param name="capacity">The initial capacity. Will be rounded upwards to nearest
    /// power of 2, at least 16.</param>
    /// <param name="fill">The expansion threshold. Must be between 10% and 90%.</param>
    /// <param name="keyequalityComparer">The external key equalityComparer</param>
    public HashDictionary(int capacity, double fill, SCG.IEqualityComparer<K> keyequalityComparer): base(keyequalityComparer)
    {
      pairs = new HashSet<KeyValuePair<K, V>>(capacity, fill, new KeyValuePairEqualityComparer<K, V>(keyequalityComparer));
    }

    /// <summary>
    /// 
    /// </summary>
    /// <returns></returns>
    public override object Clone()
    {
      HashDictionary<K, V> clone = new HashDictionary<K, V>(EqualityComparer);
      clone.pairs.AddAll(pairs);
      return clone;
    }

  }
}