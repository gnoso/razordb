using System;
using System.Diagnostics;
using SCG = System.Collections.Generic;
namespace RazorDB.C5
{
  // The symbolic characterization of the speed of lookups for a collection.
  // The values may refer to worst-case, amortized and/or expected asymtotic 
  // complexity wrt. the collection size.
  public enum Speed : short
  {
    // Counting the collection with the <code>Count property</code> may not return
    // (for a synthetic and potentially infinite collection).
    PotentiallyInfinite = 1,
    // Lookup operations like <code>Contains(T item)</code> or the <code>Count</code>
    // property may take time O(n),
    // where n is the size of the collection.
    Linear = 2,
    // Lookup operations like <code>Contains(T item)</code> or the <code>Count</code>
    // property  takes time O(log n),
    // where n is the size of the collection.
    Log = 3,
    // Lookup operations like <code>Contains(T item)</code> or the <code>Count</code>
    // property  takes time O(1),
    // where n is the size of the collection.
    Constant = 4
  }
  /*
  public enum ItemEqualityTypeEnum
  {
    // Only an Equals(T,T)
    Equator,
    // Equals(T,T) and GetHashCode(T)
    HashingEqualityComparer,
    // Compare(T,T)
    Comparer,
    // Compatible Compare(T,T) and GetHashCode(T)
    Both
  }
*/
  // Direction of enumeration order relative to original collection.
  public enum EnumerationDirection
  {
    // Same direction
    Forwards,
    // Opposite direction
    Backwards
  }
}