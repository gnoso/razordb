#define IMPROVED_COLLECTION_HASHFUNCTION

using System;
using System.Diagnostics;
using SCG = System.Collections.Generic;
namespace RazorDB.C5
{
  //
  // A base class for implementing an IEnumerable&lt;T&gt;
  //
  [Serializable]
  public abstract class EnumerableBase<T> : SCG.IEnumerable<T>
  {
    //
    // Create an enumerator for this collection.
    //
    // <returns>The enumerator</returns>
    public abstract SCG.IEnumerator<T> GetEnumerator();

    //
    // Count the number of items in an enumerable by enumeration
    //
    // <param name="items">The enumerable to count</param>
    // <returns>The size of the enumerable</returns>
    [Tested]
    protected static int countItems(SCG.IEnumerable<T> items)
    {
      ICollectionValue<T> jtems = items as ICollectionValue<T>;

      if (jtems != null)
        return jtems.Count;

      int count = 0;

      using (SCG.IEnumerator<T> e = items.GetEnumerator())
        while (e.MoveNext()) count++;

      return count;
    }

    #region IEnumerable Members

    System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
    {
      return GetEnumerator();
    }

    #endregion
  }


  //
  // Base class for classes implementing ICollectionValue[T]
  //
  [Serializable]
  public abstract class CollectionValueBase<T> : EnumerableBase<T>, ICollectionValue<T>, IShowable
  {
    #region Event handling
    EventBlock<T> eventBlock;
    //
    // 
    //
    // <value></value>
    public virtual EventTypeEnum ListenableEvents { get { return 0; } }

    //
    // A flag bitmap of the events currently subscribed to by this collection.
    //
    // <value></value>
    public virtual EventTypeEnum ActiveEvents { get { return eventBlock == null ? 0 : eventBlock.events; } }

    void checkWillListen(EventTypeEnum eventType)
    {
      if ((ListenableEvents & eventType) == 0)
        throw new UnlistenableEventException();
    }

    //
    // The change event. Will be raised for every change operation on the collection.
    //
    public virtual event CollectionChangedHandler<T> CollectionChanged
    {
      add { checkWillListen(EventTypeEnum.Changed); (eventBlock ?? (eventBlock = new EventBlock<T>())).CollectionChanged += value; }
      remove
      {
        checkWillListen(EventTypeEnum.Changed);
        if (eventBlock != null)
        {
          eventBlock.CollectionChanged -= value;
          if (eventBlock.events == 0) eventBlock = null;
        }
      }
    }
    //
    // Fire the CollectionChanged event
    //
    protected virtual void raiseCollectionChanged()
    { if (eventBlock != null) eventBlock.raiseCollectionChanged(this); }

    //
    // The clear event. Will be raised for every Clear operation on the collection.
    //
    public virtual event CollectionClearedHandler<T> CollectionCleared
    {
      add { checkWillListen(EventTypeEnum.Cleared); (eventBlock ?? (eventBlock = new EventBlock<T>())).CollectionCleared += value; }
      remove
      {
        checkWillListen(EventTypeEnum.Cleared);
        if (eventBlock != null)
        {
          eventBlock.CollectionCleared -= value;
          if (eventBlock.events == 0) eventBlock = null;
        }
      }
    }
    //
    // Fire the CollectionCleared event
    //
    protected virtual void raiseCollectionCleared(bool full, int count)
    { if (eventBlock != null) eventBlock.raiseCollectionCleared(this, full, count); }

    //
    // Fire the CollectionCleared event
    //
    protected virtual void raiseCollectionCleared(bool full, int count, int? offset)
    { if (eventBlock != null) eventBlock.raiseCollectionCleared(this, full, count, offset); }

    //
    // The item added  event. Will be raised for every individual addition to the collection.
    //
    public virtual event ItemsAddedHandler<T> ItemsAdded
    {
      add { checkWillListen(EventTypeEnum.Added); (eventBlock ?? (eventBlock = new EventBlock<T>())).ItemsAdded += value; }
      remove
      {
        checkWillListen(EventTypeEnum.Added);
        if (eventBlock != null)
        {
          eventBlock.ItemsAdded -= value;
          if (eventBlock.events == 0) eventBlock = null;
        }
      }
    }
    //
    // Fire the ItemsAdded event
    //
    // <param name="item">The item that was added</param>
    // <param name="count"></param>
    protected virtual void raiseItemsAdded(T item, int count)
    { if (eventBlock != null) eventBlock.raiseItemsAdded(this, item, count); }

    //
    // The item removed event. Will be raised for every individual removal from the collection.
    //
    public virtual event ItemsRemovedHandler<T> ItemsRemoved
    {
      add { checkWillListen(EventTypeEnum.Removed); (eventBlock ?? (eventBlock = new EventBlock<T>())).ItemsRemoved += value; }
      remove
      {
        checkWillListen(EventTypeEnum.Removed);
        if (eventBlock != null)
        {
          eventBlock.ItemsRemoved -= value;
          if (eventBlock.events == 0) eventBlock = null;
        }
      }
    }
    //
    // Fire the ItemsRemoved event
    //
    // <param name="item">The item that was removed</param>
    // <param name="count"></param>
    protected virtual void raiseItemsRemoved(T item, int count)
    { if (eventBlock != null) eventBlock.raiseItemsRemoved(this, item, count); }

    //
    // The item added  event. Will be raised for every individual addition to the collection.
    //
    public virtual event ItemInsertedHandler<T> ItemInserted
    {
      add { checkWillListen(EventTypeEnum.Inserted); (eventBlock ?? (eventBlock = new EventBlock<T>())).ItemInserted += value; }
      remove
      {
        checkWillListen(EventTypeEnum.Inserted);
        if (eventBlock != null)
        {
          eventBlock.ItemInserted -= value;
          if (eventBlock.events == 0) eventBlock = null;
        }
      }
    }
    //
    // Fire the ItemInserted event
    //
    // <param name="item">The item that was added</param>
    // <param name="index"></param>
    protected virtual void raiseItemInserted(T item, int index)
    { if (eventBlock != null) eventBlock.raiseItemInserted(this, item, index); }

    //
    // The item removed event. Will be raised for every individual removal from the collection.
    //
    public virtual event ItemRemovedAtHandler<T> ItemRemovedAt
    {
      add { checkWillListen(EventTypeEnum.RemovedAt); (eventBlock ?? (eventBlock = new EventBlock<T>())).ItemRemovedAt += value; }
      remove
      {
        checkWillListen(EventTypeEnum.RemovedAt);
        if (eventBlock != null)
        {
          eventBlock.ItemRemovedAt -= value;
          if (eventBlock.events == 0) eventBlock = null;
        }
      }
    }
    // 
    // Fire the ItemRemovedAt event
    //
    // <param name="item">The item that was removed</param>
    // <param name="index"></param>
    protected virtual void raiseItemRemovedAt(T item, int index)
    { if (eventBlock != null) eventBlock.raiseItemRemovedAt(this, item, index); }

    #region Event support for IList
    //
    // 
    //
    // <param name="index"></param>
    // <param name="value"></param>
    // <param name="item"></param>
    protected virtual void raiseForSetThis(int index, T value, T item)
    {
      if (ActiveEvents != 0)
      {
        raiseItemsRemoved(item, 1);
        raiseItemRemovedAt(item, index);
        raiseItemsAdded(value, 1);
        raiseItemInserted(value, index);
        raiseCollectionChanged();
      }
    }
    //
    // 
    //
    // <param name="i"></param>
    // <param name="item"></param>
    protected virtual void raiseForInsert(int i, T item)
    {
      if (ActiveEvents != 0)
      {
        raiseItemInserted(item, i);
        raiseItemsAdded(item, 1);
        raiseCollectionChanged();
      }
    }

    //
    // 
    //
    // <param name="item"></param>
    protected void raiseForRemove(T item)
    {
      if (ActiveEvents != 0)
      {
        raiseItemsRemoved(item, 1);
        raiseCollectionChanged();
      }
    }

    //
    // 
    //
    // <param name="item"></param>
    // <param name="count"></param>
    protected void raiseForRemove(T item, int count)
    {
      if (ActiveEvents != 0)
      {
        raiseItemsRemoved(item, count);
        raiseCollectionChanged();
      }
    }

    //
    // 
    //
    // <param name="index"></param>
    // <param name="item"></param>
    protected void raiseForRemoveAt(int index, T item)
    {
      if (ActiveEvents != 0)
      {
        raiseItemRemovedAt(item, index);
        raiseItemsRemoved(item, 1);
        raiseCollectionChanged();
      }
    }

    #endregion

    #region Event  Support for ICollection
    //
    // 
    //
    // <param name="newitem"></param>
    // <param name="olditem"></param>
    protected virtual void raiseForUpdate(T newitem, T olditem)
    {
      if (ActiveEvents != 0)
      {
        raiseItemsRemoved(olditem, 1);
        raiseItemsAdded(newitem, 1);
        raiseCollectionChanged();
      }
    }
    //
    // 
    //
    // <param name="newitem"></param>
    // <param name="olditem"></param>
    // <param name="count"></param>
    protected virtual void raiseForUpdate(T newitem, T olditem, int count)
    {
      if (ActiveEvents != 0)
      {
        raiseItemsRemoved(olditem, count);
        raiseItemsAdded(newitem, count);
        raiseCollectionChanged();
      }
    }
    //
    // 
    //
    // <param name="item"></param>
    protected virtual void raiseForAdd(T item)
    {
      if (ActiveEvents != 0)
      {
        raiseItemsAdded(item, 1);
        raiseCollectionChanged();
      }
    }
    //
    // 
    //
    // <param name="wasRemoved"></param>
    protected virtual void raiseForRemoveAll(ICollectionValue<T> wasRemoved)
    {
      if ((ActiveEvents & EventTypeEnum.Removed) != 0)
        foreach (T item in wasRemoved)
          raiseItemsRemoved(item, 1);
      if (wasRemoved != null && wasRemoved.Count > 0)
        raiseCollectionChanged();
    }

    //
    // 
    //
    protected class RaiseForRemoveAllHandler
    {
      CollectionValueBase<T> collection;
      CircularQueue<T> wasRemoved;
      bool wasChanged = false;

      //
      // 
      //
      // <param name="collection"></param>
      public RaiseForRemoveAllHandler(CollectionValueBase<T> c)
      {
        collection = c;
        mustFireRemoved = (collection.ActiveEvents & EventTypeEnum.Removed) != 0;
        MustFire = (collection.ActiveEvents & (EventTypeEnum.Removed | EventTypeEnum.Changed)) != 0;
      }

      bool mustFireRemoved;
      //
      // 
      //
      public readonly bool MustFire;

      //
      // 
      //
      // <param name="item"></param>
      public void Remove(T item)
      {
        if (mustFireRemoved)
        {
          if (wasRemoved == null)
            wasRemoved = new CircularQueue<T>();
          wasRemoved.Enqueue(item);
        }
        if (!wasChanged)
          wasChanged = true;
      }

      //
      // 
      //
      public void Raise()
      {
        if (wasRemoved != null)
          foreach (T item in wasRemoved)
            collection.raiseItemsRemoved(item, 1);
        if (wasChanged)
          collection.raiseCollectionChanged();
      }
    }
    #endregion

    #endregion

    //
    // Check if collection is empty.
    //
    // <value>True if empty</value>
    public abstract bool IsEmpty { get;}

    //
    // The number of items in this collection.
    //
    // <value></value>
    public abstract int Count { get;}

    //
    // The value is symbolic indicating the type of asymptotic complexity
    // in terms of the size of this collection (worst-case or amortized as
    // relevant).
    //
    // <value>A characterization of the speed of the 
    // <code>Count</code> property in this collection.</value>
    public abstract Speed CountSpeed { get; }

    //
    // Copy the items of this collection to part of an array.
    //
    // <exception cref="ArgumentOutOfRangeException"> if <code>index</code> 
    // is not a valid index
    // into the array (i.e. negative or greater than the size of the array)
    // or the array does not have room for the items.</exception>
    // <param name="array">The array to copy to.</param>
    // <param name="index">The starting index.</param>
    [Tested]
    public virtual void CopyTo(T[] array, int index)
    {
      if (index < 0 || index + Count > array.Length)
        throw new ArgumentOutOfRangeException();

      foreach (T item in this) array[index++] = item;
    }

    //
    // Create an array with the items of this collection (in the same order as an
    // enumerator would output them).
    //
    // <returns>The array</returns>
    //[Tested]
    public virtual T[] ToArray()
    {
      T[] res = new T[Count];
      int i = 0;

      foreach (T item in this) res[i++] = item;

      return res;
    }

    //
    // Apply an single argument action, <see cref="T:C5.Act`1"/> to this enumerable
    //
    // <param name="action">The action delegate</param>
    [Tested]
    public virtual void Apply(Act<T> action)
    {
      foreach (T item in this)
        action(item);
    }


    //
    // Check if there exists an item  that satisfies a
    // specific predicate in this collection.
    //
    // <param name="predicate">A delegate 
    // (<see cref="T:C5.Fun`2"/> with <code>R = bool</code>) 
    // defining the predicate</param>
    // <returns>True if such an item exists</returns>
    [Tested]
    public virtual bool Exists(Fun<T, bool> predicate)
    {
      foreach (T item in this)
        if (predicate(item))
          return true;

      return false;
    }

    //
    // Check if there exists an item  that satisfies a
    // specific predicate in this collection and return the first one in enumeration order.
    //
    // <param name="predicate">A delegate 
    // (<see cref="T:C5.Fun`2"/> with <code>R == bool</code>) defining the predicate</param>
    // <param name="item"></param>
    // <returns>True is such an item exists</returns>
    public virtual bool Find(Fun<T, bool> predicate, out T item)
    {
      foreach (T jtem in this)
        if (predicate(jtem))
        {
          item = jtem;
          return true;
        }
      item = default(T);
      return false;
    }

    //
    // Check if all items in this collection satisfies a specific predicate.
    //
    // <param name="predicate">A delegate 
    // (<see cref="T:C5.Fun`2"/> with <code>R = bool</code>) 
    // defining the predicate</param>
    // <returns>True if all items satisfies the predicate</returns>
    [Tested]
    public virtual bool All(Fun<T, bool> predicate)
    {
      foreach (T item in this)
        if (!predicate(item))
          return false;

      return true;
    }

    //
    // Create an enumerable, enumerating the items of this collection that satisfies 
    // a certain condition.
    //
    // <param name="predicate">A delegate 
    // (<see cref="T:C5.Fun`2"/> with <code>R = bool</code>) 
    // defining the predicate</param>
    // <returns>The filtered enumerable</returns>
    public virtual SCG.IEnumerable<T> Filter(Fun<T, bool> predicate)
    {
      foreach (T item in this)
        if (predicate(item))
          yield return item;
    }

    //
    // Choose some item of this collection. 
    //
    // <exception cref="NoSuchItemException">if collection is empty.</exception>
    // <returns></returns>
    public abstract T Choose();


    //
    // Create an enumerator for this collection.
    //
    // <returns>The enumerator</returns>
    public override abstract SCG.IEnumerator<T> GetEnumerator();

    #region IShowable Members

    //
    // 
    //
    // <param name="stringbuilder"></param>
    // <param name="rest"></param>
    // <param name="formatProvider"></param>
    // <returns></returns>
    public virtual bool Show(System.Text.StringBuilder stringbuilder, ref int rest, IFormatProvider formatProvider)
    {
      return Showing.ShowCollectionValue<T>(this, stringbuilder, ref rest, formatProvider);
    }
    #endregion

    #region IFormattable Members

    //
    // 
    //
    // <param name="format"></param>
    // <param name="formatProvider"></param>
    // <returns></returns>
    public virtual string ToString(string format, IFormatProvider formatProvider)
    {
      return Showing.ShowString(this, format, formatProvider);
    }

    #endregion

    //
    // 
    //
    // <returns></returns>
    public override string ToString()
    {
      return ToString(null, null);
    }

  }

  //
  // 
  //
  // <typeparam name="T"></typeparam>
  public abstract class DirectedCollectionValueBase<T> : CollectionValueBase<T>, IDirectedCollectionValue<T>
  {
    //
    // <code>Forwards</code> if same, else <code>Backwards</code>
    //
    // <value>The enumeration direction relative to the original collection.</value>
    public virtual EnumerationDirection Direction { [Tested]get { return EnumerationDirection.Forwards; } }

    //
    // 
    //
    // <returns></returns>
    public abstract IDirectedCollectionValue<T> Backwards();

    IDirectedEnumerable<T> IDirectedEnumerable<T>.Backwards() { return Backwards(); }

    //
    // Check if there exists an item  that satisfies a
    // specific predicate in this collection and return the first one in enumeration order.
    //
    // <param name="predicate">A delegate 
    // (<see cref="T:C5.Fun`2"/> with <code>R == bool</code>) defining the predicate</param>
    // <param name="item"></param>
    // <returns>True is such an item exists</returns>
    public virtual bool FindLast(Fun<T, bool> predicate, out T item)
    {
      foreach (T jtem in Backwards())
        if (predicate(jtem))
        {
          item = jtem;
          return true;
        }
      item = default(T);
      return false;
    }
  }

  //
  // Base class (abstract) for ICollection implementations.
  //
  [Serializable]
  public abstract class CollectionBase<T> : CollectionValueBase<T>
  {
    #region Fields

    //
    // The underlying field of the ReadOnly property
    //
    protected bool isReadOnlyBase = false;

    //
    // The current stamp value
    //
    protected int stamp;

    //
    // The number of items in the collection
    //
    protected int size;

    //
    // The item equalityComparer of the collection
    //
    protected readonly SCG.IEqualityComparer<T> itemequalityComparer;

    int iUnSequencedHashCode, iUnSequencedHashCodeStamp = -1;

    #endregion

    //
    // 
    //
    // <param name="itemequalityComparer"></param>
    protected CollectionBase(SCG.IEqualityComparer<T> i)
    {
      if (itemequalityComparer == null)
        throw new NullReferenceException("Item EqualityComparer cannot be null.");
      itemequalityComparer = i;
    }

    #region Util

    //
    // Utility method for range checking.
    //
    // <exception cref="ArgumentOutOfRangeException"> if the start or count is negative or
    //  if the range does not fit within collection size.</exception>
    // <param name="start">start of range</param>
    // <param name="count">size of range</param>
    [Tested]
    protected void checkRange(int start, int count)
    {
      if (start < 0 || count < 0 || start + count > size)
        throw new ArgumentOutOfRangeException();
    }


    //
    // Compute the unsequenced hash code of a collection
    //
    // <param name="items">The collection to compute hash code for</param>
    // <param name="itemequalityComparer">The item equalityComparer</param>
    // <returns>The hash code</returns>
    [Tested]
    public static int ComputeHashCode(ICollectionValue<T> items, SCG.IEqualityComparer<T> itemequalityComparer)
    {
      int h = 0;

#if IMPROVED_COLLECTION_HASHFUNCTION
      //But still heuristic: 
      //Note: the three odd factors should really be random, 
      //but there will be a problem with serialization/deserialization!
      //Two products is too few
      foreach (T item in items)
      {
        uint h1 = (uint)itemequalityComparer.GetHashCode(item);

        h += (int)((h1 * 1529784657 + 1) ^ (h1 * 2912831877) ^ (h1 * 1118771817 + 2));
      }

      return h;
      /*
            The pairs (-1657792980, -1570288808) and (1862883298, -272461342) gives the same
            unsequenced hashcode with this hashfunction. The pair was found with code like

            HashDictionary<int, int[]> set = new HashDictionary<int, int[]>();
            Random rnd = new C5Random(12345);
            while (true)
            {
                int[] a = new int[2];
                a[0] = rnd.Next(); a[1] = rnd.Next();
                int h = unsequencedhashcode(a);
                int[] b = a;
                if (set.FindOrAdd(h, ref b))
                {
                    Console.WriteLine("Code {5}, Pair ({1},{2}) number {0} matched other pair ({3},{4})", set.Count, a[0], a[1], b[0], b[1], h);
                }
            }
            */
#else
            foreach (T item in items)
				h ^= itemequalityComparer.GetHashCode(item);

			return (items.Count << 16) + h;
#endif
    }

    //
    // Examine if collection1 and collection2 are equal as unsequenced collections
    // using the specified item equalityComparer (assumed compatible with the two collections).
    //
    // <param name="collection1">The first collection</param>
    // <param name="collection2">The second collection</param>
    // <param name="itemequalityComparer">The item equalityComparer to use for comparison</param>
    // <returns>True if equal</returns>
    [Tested]
    public static bool StaticEquals(ICollection<T> collection1, ICollection<T> collection2, SCG.IEqualityComparer<T> itemequalityComparer)
    {
      if (object.ReferenceEquals(collection1, collection2))
        return true;

      // bug20070227:
      if (collection1 == null || collection2 == null)
        return false;

      if (collection1.Count != collection2.Count)
        return false;

      //This way we might run through both enumerations twice, but
      //probably not (if the hash codes are good)
      //TODO: check equal equalityComparers, at least here!
      if (collection1.GetUnsequencedHashCode() != collection2.GetUnsequencedHashCode())
        return false;

      //TODO: move this to the sorted implementation classes? 
      //Really depends on speed of InstanceOfType: we could save a cast
      {
        ISorted<T> stit, stat;
        if ((stit = collection1 as ISorted<T>) != null && (stat = collection2 as ISorted<T>) != null && stit.Comparer == stat.Comparer)
        {
          using (SCG.IEnumerator<T> dat = collection2.GetEnumerator(), dit = collection1.GetEnumerator())
          {
            while (dit.MoveNext())
            {
              dat.MoveNext();
              if (!itemequalityComparer.Equals(dit.Current, dat.Current))
                return false;
            }
            return true;
          }
        }
      }

      if (!collection1.AllowsDuplicates && (collection2.AllowsDuplicates || collection2.ContainsSpeed >= collection1.ContainsSpeed))
      {
        foreach (T x in collection1) if (!collection2.Contains(x)) return false;
      }
      else if (!collection2.AllowsDuplicates)
      {
        foreach (T x in collection2) if (!collection1.Contains(x)) return false;
      }
      // Now tit.AllowsDuplicates && tat.AllowsDuplicates
      else if (collection1.DuplicatesByCounting && collection2.DuplicatesByCounting)
      {
        foreach (T item in collection2) if (collection1.ContainsCount(item) != collection2.ContainsCount(item)) return false;
      }
      else
      {
        // To avoid an O(n^2) algorithm, we make an aux hashtable to hold the count of items
        // bug20101103: HashDictionary<T, int> dict = new HashDictionary<T, int>();
        HashDictionary<T, int> dict = new HashDictionary<T, int>(itemequalityComparer);
        foreach (T item in collection2)
        {
          int count = 1;
          if (dict.FindOrAdd(item, ref count))
            dict[item] = count + 1;
        }
        foreach (T item in collection1)
        {
          int count;
          if (dict.Find(item, out count) && count > 0)
            dict[item] = count - 1;
          else
            return false;
        }
        return true;
      }

      return true;
    }


    //
    // Get the unsequenced collection hash code of this collection: from the cached 
    // value if present and up to date, else (re)compute.
    //
    // <returns>The hash code</returns>
    public virtual int GetUnsequencedHashCode()
    {
      if (iUnSequencedHashCodeStamp == stamp)
        return iUnSequencedHashCode;

      iUnSequencedHashCode = ComputeHashCode(this, itemequalityComparer);
      iUnSequencedHashCodeStamp = stamp;
      return iUnSequencedHashCode;
    }


    //
    // Check if the contents of otherCollection is equal to the contents of this
    // in the unsequenced sense.  Uses the item equality comparer of this collection
    //
    // <param name="otherCollection">The collection to compare to.</param>
    // <returns>True if  equal</returns>
    public virtual bool UnsequencedEquals(ICollection<T> otherCollection)
    {
      return otherCollection != null && StaticEquals((ICollection<T>)this, otherCollection, itemequalityComparer);
    }


    //
    // Check if the collection has been modified since a specified time, expressed as a stamp value.
    //
    // <exception cref="CollectionModifiedException"> if this collection has been updated 
    // since a target time</exception>
    // <param name="thestamp">The stamp identifying the target time</param>
    protected virtual void modifycheck(int thestamp)
    {
      if (stamp != thestamp)
        throw new CollectionModifiedException();
    }


    //
    // Check if it is valid to perform update operations, and if so increment stamp.
    //
    // <exception cref="ReadOnlyCollectionException">If colection is read-only</exception>
    protected virtual void updatecheck()
    {
      if (isReadOnlyBase)
        throw new ReadOnlyCollectionException();

      stamp++;
    }

    #endregion

    #region ICollection<T> members

    //
    // 
    //
    // <value>True if this collection is read only</value>
    [Tested]
    public virtual bool IsReadOnly { [Tested]get { return isReadOnlyBase; } }

    #endregion

    #region ICollectionValue<T> members
    //
    // 
    //
    // <value>The size of this collection</value>
    [Tested]
    public override int Count { [Tested]get { return size; } }

    //
    // The value is symbolic indicating the type of asymptotic complexity
    // in terms of the size of this collection (worst-case or amortized as
    // relevant).
    //
    // <value>A characterization of the speed of the 
    // <code>Count</code> property in this collection.</value>
    public override Speed CountSpeed { get { return Speed.Constant; } }


    #endregion

    #region IExtensible<T> members

    //
    // 
    //
    // <value></value>
    public virtual SCG.IEqualityComparer<T> EqualityComparer { get { return itemequalityComparer; } }

    //
    // 
    //
    // <value>True if this collection is empty</value>
    [Tested]
    public override bool IsEmpty { [Tested]get { return size == 0; } }

    #endregion

    #region IEnumerable<T> Members
    //
    // Create an enumerator for this collection.
    //
    // <returns>The enumerator</returns>
    public override abstract SCG.IEnumerator<T> GetEnumerator();
    #endregion
  }

  //
  // 
  //
  // <typeparam name="T"></typeparam>
  [Serializable]
  public abstract class DirectedCollectionBase<T> : CollectionBase<T>, IDirectedCollectionValue<T>
  {
    //
    // 
    //
    // <param name="itemequalityComparer"></param>
    protected DirectedCollectionBase(SCG.IEqualityComparer<T> itemequalityComparer) : base(itemequalityComparer) { }
    //
    // <code>Forwards</code> if same, else <code>Backwards</code>
    //
    // <value>The enumeration direction relative to the original collection.</value>
    public virtual EnumerationDirection Direction { [Tested]get { return EnumerationDirection.Forwards; } }

    //
    // 
    //
    // <returns></returns>
    public abstract IDirectedCollectionValue<T> Backwards();

    IDirectedEnumerable<T> IDirectedEnumerable<T>.Backwards() { return Backwards(); }

    //
    // Check if there exists an item  that satisfies a
    // specific predicate in this collection and return the first one in enumeration order.
    //
    // <param name="predicate">A delegate 
    // (<see cref="T:C5.Fun`2"/> with <code>R == bool</code>) defining the predicate</param>
    // <param name="item"></param>
    // <returns>True is such an item exists</returns>
    public virtual bool FindLast(Fun<T, bool> predicate, out T item)
    {
      foreach (T jtem in Backwards())
        if (predicate(jtem))
        {
          item = jtem;
          return true;
        }
      item = default(T);
      return false;
    }
  }

  //
  // Base class (abstract) for sequenced collection implementations.
  //
  [Serializable]
  public abstract class SequencedBase<T> : DirectedCollectionBase<T>, IDirectedCollectionValue<T>
  {
    #region Fields

    int iSequencedHashCode, iSequencedHashCodeStamp = -1;

    #endregion

    //
    // 
    //
    // <param name="itemequalityComparer"></param>
    protected SequencedBase(SCG.IEqualityComparer<T> itemequalityComparer) : base(itemequalityComparer) { }

    #region Util

    //TODO: make random for release
    const int HASHFACTOR = 31;

    //
    // Compute the unsequenced hash code of a collection
    //
    // <param name="items">The collection to compute hash code for</param>
    // <param name="itemequalityComparer">The item equalityComparer</param>
    // <returns>The hash code</returns>
    [Tested]
    public static int ComputeHashCode(ISequenced<T> items, SCG.IEqualityComparer<T> itemequalityComparer)
    {
      //NOTE: It must be possible to devise a much stronger combined hashcode, 
      //but unfortunately, it has to be universal. OR we could use a (strong)
      //family and initialise its parameter randomly at load time of this class!
      //(We would not want to have yet a flag to check for invalidation?!)
      //NBNBNB: the current hashcode has the very bad property that items with hashcode 0
      // is ignored.
      int iIndexedHashCode = 0;

      foreach (T item in items)
        iIndexedHashCode = iIndexedHashCode * HASHFACTOR + itemequalityComparer.GetHashCode(item);

      return iIndexedHashCode;
    }


    //
    // Examine if tit and tat are equal as sequenced collections
    // using the specified item equalityComparer (assumed compatible with the two collections).
    //
    // <param name="collection1">The first collection</param>
    // <param name="collection2">The second collection</param>
    // <param name="itemequalityComparer">The item equalityComparer to use for comparison</param>
    // <returns>True if equal</returns>
    [Tested]
    public static bool StaticEquals(ISequenced<T> collection1, ISequenced<T> collection2, SCG.IEqualityComparer<T> itemequalityComparer)
    {
      if (object.ReferenceEquals(collection1, collection2))
        return true;

      if (collection1.Count != collection2.Count)
        return false;

      //This way we might run through both enumerations twice, but
      //probably not (if the hash codes are good)
      if (collection1.GetSequencedHashCode() != collection2.GetSequencedHashCode())
        return false;

      using (SCG.IEnumerator<T> dat = collection2.GetEnumerator(), dit = collection1.GetEnumerator())
      {
        while (dit.MoveNext())
        {
          dat.MoveNext();
          if (!itemequalityComparer.Equals(dit.Current, dat.Current))
            return false;
        }
      }

      return true;
    }


    //
    // Get the sequenced collection hash code of this collection: from the cached 
    // value if present and up to date, else (re)compute.
    //
    // <returns>The hash code</returns>
    public virtual int GetSequencedHashCode()
    {
      if (iSequencedHashCodeStamp == stamp)
        return iSequencedHashCode;

      iSequencedHashCode = ComputeHashCode((ISequenced<T>)this, itemequalityComparer);
      iSequencedHashCodeStamp = stamp;
      return iSequencedHashCode;
    }


    //
    // Check if the contents of that is equal to the contents of this
    // in the sequenced sense. Using the item equalityComparer of this collection.
    //
    // <param name="otherCollection">The collection to compare to.</param>
    // <returns>True if  equal</returns>
    public virtual bool SequencedEquals(ISequenced<T> otherCollection)
    {
      return StaticEquals((ISequenced<T>)this, otherCollection, itemequalityComparer);
    }


    #endregion

    //
    // Create an enumerator for this collection.
    //
    // <returns>The enumerator</returns>
    public override abstract SCG.IEnumerator<T> GetEnumerator();

    //
    // <code>Forwards</code> if same, else <code>Backwards</code>
    //
    // <value>The enumeration direction relative to the original collection.</value>
    [Tested]
    public override EnumerationDirection Direction { [Tested]get { return EnumerationDirection.Forwards; } }

    //
    // Check if there exists an item  that satisfies a
    // specific predicate in this collection and return the index of the first one.
    //
    // <param name="predicate">A delegate 
    // (<see cref="T:C5.Fun`2"/> with <code>R == bool</code>) defining the predicate</param>
    // <returns>the index, if found, a negative value else</returns>
    public int FindIndex(Fun<T, bool> predicate)
    {
      int index = 0;
      foreach (T item in this)
      {
        if (predicate(item))
          return index;
        index++;
      }
      return -1;
    }

    //
    // Check if there exists an item  that satisfies a
    // specific predicate in this collection and return the index of the last one.
    //
    // <param name="predicate">A delegate 
    // (<see cref="T:C5.Fun`2"/> with <code>R == bool</code>) defining the predicate</param>
    // <returns>the index, if found, a negative value else</returns>
    public int FindLastIndex(Fun<T, bool> predicate)
    {
      int index = Count - 1;
      foreach (T item in Backwards())
      {
        if (predicate(item))
          return index;
        index--;
      }
      return -1;
    }

  }


  //
  // Base class for collection classes of dynamic array type implementations.
  //
  [Serializable]
  public abstract class ArrayBase<T> : SequencedBase<T>
  {
    #region Fields
    //
    // The actual internal array container. Will be extended on demand.
    //
    protected T[] array;

    //
    // The offset into the internal array container of the first item. The offset is 0 for a 
    // base dynamic array and may be positive for an updatable view into a base dynamic array.
    //
    protected int offset;
    #endregion

    #region Util
    //
    // Double the size of the internal array.
    //
    protected virtual void expand()
    {
      expand(2 * array.Length, size);
    }


    //
    // Expand the internal array container.
    //
    // <param name="newcapacity">The new size of the internal array - 
    // will be rounded upwards to a power of 2.</param>
    // <param name="newsize">The (new) size of the (base) collection.</param>
    protected virtual void expand(int newcapacity, int newsize)
    {
      Debug.Assert(newcapacity >= newsize);

      int newlength = array.Length;

      while (newlength < newcapacity) newlength *= 2;

      T[] newarray = new T[newlength];

      Array.Copy(array, newarray, newsize);
      array = newarray;
    }


    //
    // Insert an item at a specific index, moving items to the right
    // upwards and expanding the array if necessary.
    //
    // <param name="i">The index at which to insert.</param>
    // <param name="item">The item to insert.</param>
    protected virtual void insert(int i, T item)
    {
      if (size == array.Length)
        expand();

      if (i < size)
        Array.Copy(array, i, array, i + 1, size - i);

      array[i] = item;
      size++;
    }

    #endregion

    #region Constructors

    //
    // Create an empty ArrayBase object.
    //
    // <param name="capacity">The initial capacity of the internal array container.
    // Will be rounded upwards to the nearest power of 2 greater than or equal to 8.</param>
    // <param name="itemequalityComparer">The item equalityComparer to use, primarily for item equality</param>
    protected ArrayBase(int capacity, SCG.IEqualityComparer<T> itemequalityComparer)
      : base(itemequalityComparer)
    {
      int newlength = 8;
      while (newlength < capacity) newlength *= 2;
      array = new T[newlength];
    }

    #endregion

    #region IIndexed members

    //
    //
    // <exception cref="ArgumentOutOfRangeException">If the arguments does not describe a 
    // valid range in the indexed collection, cf. <see cref="M:C5.CollectionBase`1.checkRange(System.Int32,System.Int32)"/>.</exception>
    // <value>The directed collection of items in a specific index interval.</value>
    // <param name="start">The low index of the interval (inclusive).</param>
    // <param name="count">The size of the range.</param>
    [Tested]
    public virtual IDirectedCollectionValue<T> this[int start, int count]
    {
      [Tested]
      get
      {
        checkRange(start, count);
        return new Range(this, start, count, true);
      }
    }

    #endregion

    #region IEditableCollection members
    //
    // Remove all items and reset size of internal array container.
    //
    [Tested]
    public virtual void Clear()
    {
      updatecheck();
      array = new T[8];
      size = 0;
    }


    //
    // Create an array containing (copies) of the items of this collection in enumeration order.
    //
    // <returns>The new array</returns>
    [Tested]
    public override T[] ToArray()
    {
      T[] res = new T[size];

      Array.Copy(array, offset, res, 0, size);
      return res;
    }


    //
    // Perform an internal consistency (invariant) test on the array base.
    //
    // <returns>True if test succeeds.</returns>
    [Tested]
    public virtual bool Check()
    {
      bool retval = true;

      if (size > array.Length)
      {
        Console.WriteLine("Bad size ({0}) > array.Length ({1})", size, array.Length);
        return false;
      }

      for (int i = 0; i < size; i++)
      {
        if ((object)(array[i]) == null)
        {
          Console.WriteLine("Bad element: null at index {0}", i);
          return false;
        }
      }

      return retval;
    }

    #endregion

    #region IDirectedCollection<T> Members

    //
    // Create a directed collection with the same contents as this one, but 
    // opposite enumeration sequence.
    //
    // <returns>The mirrored collection.</returns>
    [Tested]
    public override IDirectedCollectionValue<T> Backwards() { return this[0, size].Backwards(); }

    #endregion

    //
    // Choose some item of this collection. The result is the last item in the internal array,
    // making it efficient to remove.
    //
    // <exception cref="NoSuchItemException">if collection is empty.</exception>
    // <returns></returns>
    public override T Choose() { if (size > 0) return array[size - 1]; throw new NoSuchItemException(); }

    #region IEnumerable<T> Members
    //
    // Create an enumerator for this array based collection.
    //
    // <returns>The enumerator</returns>
    [Tested]
    public override SCG.IEnumerator<T> GetEnumerator()
    {
      int thestamp = stamp, theend = size + offset, thestart = offset;

      for (int i = thestart; i < theend; i++)
      {
        modifycheck(thestamp);
        yield return array[i];
      }
    }
    #endregion

    #region Range nested class
    //
    // A helper class for defining results of interval queries on array based collections.
    //
    protected class Range : DirectedCollectionValueBase<T>, IDirectedCollectionValue<T>
    {
      int start, count, delta, stamp;

      ArrayBase<T> thebase;

      internal Range(ArrayBase<T> a, int s, int c, bool forwards)
      {
        thebase = a; stamp = thebase.stamp;
        delta = forwards ? 1 : -1;
        start = s + thebase.offset; count = c;
      }

      //
      // 
      //
      // <exception cref="CollectionModifiedException">if underlying collection has been modified.</exception>
      // <value>True if this collection is empty.</value>
      public override bool IsEmpty { get { thebase.modifycheck(stamp); return count == 0; } }


      //
      // 
      //
      // <exception cref="CollectionModifiedException">if underlying collection has been modified.</exception>
      // <value>The number of items in the range</value>
      [Tested]
      public override int Count { [Tested]get { thebase.modifycheck(stamp); return count; } }

      //
      // The value is symbolic indicating the type of asymptotic complexity
      // in terms of the size of this collection (worst-case or amortized as
      // relevant).
      //
      // <value>A characterization of the speed of the 
      // <exception cref="CollectionModifiedException">if underlying collection has been modified.</exception>
      // <code>Count</code> property in this collection.</value>
      public override Speed CountSpeed { get { thebase.modifycheck(stamp); return Speed.Constant; } }

      //
      // Choose some item of this collection. 
      //
      // <exception cref="CollectionModifiedException">if underlying collection has been modified.</exception>
      // <exception cref="NoSuchItemException">if range is empty.</exception>
      // <returns></returns>
      public override T Choose()
      {
        thebase.modifycheck(stamp);
        if (count == 0)
          throw new NoSuchItemException();
        return thebase.array[start];
      }


      //
      // Create an enumerator for this range of an array based collection.
      //
      // <exception cref="CollectionModifiedException">if underlying collection has been modified.</exception>
      // <returns>The enumerator</returns>
      [Tested]
      public override SCG.IEnumerator<T> GetEnumerator()
      {
        for (int i = 0; i < count; i++)
        {
          thebase.modifycheck(stamp);
          yield return thebase.array[start + delta * i];
        }
      }


      //
      // Create a araay collection range with the same contents as this one, but 
      // opposite enumeration sequence.
      //
      // <exception cref="CollectionModifiedException">if underlying collection has been modified.</exception>
      // <returns>The mirrored collection.</returns>
      [Tested]
      public override IDirectedCollectionValue<T> Backwards()
      {
        thebase.modifycheck(stamp);

        Range res = (Range)MemberwiseClone();

        res.delta = -delta;
        res.start = start + (count - 1) * delta;
        return res;
      }


      IDirectedEnumerable<T> C5.IDirectedEnumerable<T>.Backwards()
      {
        return Backwards();
      }


      //
      // <code>Forwards</code> if same, else <code>Backwards</code>
      //
      // <exception cref="CollectionModifiedException">if underlying collection has been modified.</exception>
      // <value>The enumeration direction relative to the original collection.</value>
      [Tested]
      public override EnumerationDirection Direction
      {
        [Tested]
        get
        {
          thebase.modifycheck(stamp);
          return delta > 0 ? EnumerationDirection.Forwards : EnumerationDirection.Backwards;
        }
      }
    }
    #endregion
  }
}
