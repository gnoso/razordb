using System;
using SCG = System.Collections.Generic;
namespace RazorDB.C5
{
  //
  // A generic collection, that can be enumerated backwards.
  //
  public interface IDirectedEnumerable<T> : SCG.IEnumerable<T>
  {
    //
    // Create a collection containing the same items as this collection, but
    // whose enumerator will enumerate the items backwards. The new collection
    // will become invalid if the original is modified. Method typically used as in
    // <code>foreach (T x in coll.Backwards()) {...}</code>
    //
    // <returns>The backwards collection.</returns>
    IDirectedEnumerable<T> Backwards();


    //
    // <code>Forwards</code> if same, else <code>Backwards</code>
    //
    // <value>The enumeration direction relative to the original collection.</value>
    EnumerationDirection Direction { get;}
  }

  //
  // A generic collection that may be enumerated and can answer
  // efficiently how many items it contains. Like <code>IEnumerable&lt;T&gt;</code>,
  // this interface does not prescribe any operations to initialize or update the 
  // collection. The main usage for this interface is to be the return type of 
  // query operations on generic collection.
  //
  public interface ICollectionValue<T> : SCG.IEnumerable<T>, IShowable
  {
    //
    // A flag bitmap of the events subscribable to by this collection.
    //
    // <value></value>
    EventTypeEnum ListenableEvents { get;}

    //
    // A flag bitmap of the events currently subscribed to by this collection.
    //
    // <value></value>
    EventTypeEnum ActiveEvents { get;}

    //
    // The change event. Will be raised for every change operation on the collection.
    //
    event CollectionChangedHandler<T> CollectionChanged;

    //
    // The change event. Will be raised for every clear operation on the collection.
    //
    event CollectionClearedHandler<T> CollectionCleared;

    //
    // The item added  event. Will be raised for every individual addition to the collection.
    //
    event ItemsAddedHandler<T> ItemsAdded;

    //
    // The item inserted  event. Will be raised for every individual insertion to the collection.
    //
    event ItemInsertedHandler<T> ItemInserted;

    //
    // The item removed event. Will be raised for every individual removal from the collection.
    //
    event ItemsRemovedHandler<T> ItemsRemoved;

    //
    // The item removed at event. Will be raised for every individual removal at from the collection.
    //
    event ItemRemovedAtHandler<T> ItemRemovedAt;

    //
    // 
    //
    // <value>True if this collection is empty.</value>
    bool IsEmpty { get;}

    //
    //
    // <value>The number of items in this collection</value>
    int Count { get;}

    //
    // The value is symbolic indicating the type of asymptotic complexity
    // in terms of the size of this collection (worst-case or amortized as
    // relevant).
    //
    // <value>A characterization of the speed of the 
    // <code>Count</code> property in this collection.</value>
    Speed CountSpeed { get;}

    //
    // Copy the items of this collection to a contiguous part of an array.
    //
    // <param name="array">The array to copy to</param>
    // <param name="index">The index at which to copy the first item</param>
    void CopyTo(T[] array, int index);

    //
    // Create an array with the items of this collection (in the same order as an
    // enumerator would output them).
    //
    // <returns>The array</returns>
    T[] ToArray();

    //
    // Apply a delegate to all items of this collection.
    //
    // <param name="action">The delegate to apply</param>
    void Apply(Act<T> action);


    //
    // Check if there exists an item  that satisfies a
    // specific predicate in this collection.
    //
    // <param name="predicate">A  delegate 
    // (<see cref="T:C5.Fun`2"/> with <code>R == bool</code>) defining the predicate</param>
    // <returns>True is such an item exists</returns>
    bool Exists(Fun<T, bool> predicate);

    //
    // Check if there exists an item  that satisfies a
    // specific predicate in this collection and return the first one in enumeration order.
    //
    // <param name="predicate">A delegate 
    // (<see cref="T:C5.Fun`2"/> with <code>R == bool</code>) defining the predicate</param>
    // <param name="item"></param>
    // <returns>True is such an item exists</returns>
    bool Find(Fun<T, bool> predicate, out T item);


    //
    // Check if all items in this collection satisfies a specific predicate.
    //
    // <param name="predicate">A delegate 
    // (<see cref="T:C5.Fun`2"/> with <code>R == bool</code>) defining the predicate</param>
    // <returns>True if all items satisfies the predicate</returns>
    bool All(Fun<T, bool> predicate);

    //
    // Choose some item of this collection. 
    // <para>Implementations must assure that the item 
    // returned may be efficiently removed.</para>
    // <para>Implementors may decide to implement this method in a way such that repeated
    // calls do not necessarily give the same result, i.e. so that the result of the following 
    // test is undetermined:
    // <code>coll.Choose() == coll.Choose()</code></para>
    //
    // <exception cref="NoSuchItemException">if collection is empty.</exception>
    // <returns></returns>
    T Choose();

    //
    // Create an enumerable, enumerating the items of this collection that satisfies 
    // a certain condition.
    //
    // <param name="filter">The T->bool filter delegate defining the condition</param>
    // <returns>The filtered enumerable</returns>
    SCG.IEnumerable<T> Filter(Fun<T, bool> filter);
  }



  //
  // A sized generic collection, that can be enumerated backwards.
  //
  public interface IDirectedCollectionValue<T> : ICollectionValue<T>, IDirectedEnumerable<T>
  {
    //
    // Create a collection containing the same items as this collection, but
    // whose enumerator will enumerate the items backwards. The new collection
    // will become invalid if the original is modified. Method typically used as in
    // <code>foreach (T x in coll.Backwards()) {...}</code>
    //
    // <returns>The backwards collection.</returns>
    new IDirectedCollectionValue<T> Backwards();

    //
    // Check if there exists an item  that satisfies a
    // specific predicate in this collection and return the first one in enumeration order.
    //
    // <param name="predicate">A delegate 
    // (<see cref="T:C5.Fun`2"/> with <code>R == bool</code>) defining the predicate</param>
    // <param name="item"></param>
    // <returns>True is such an item exists</returns>
    bool FindLast(Fun<T, bool> predicate, out T item);
  }


  //
  // A generic collection to which one may add items. This is just the intersection
  // of the main stream generic collection interfaces and the priority queue interface,
  // <see cref="T:C5.ICollection`1"/> and <see cref="T:C5.IPriorityQueue`1"/>.
  //
  public interface IExtensible<T> : ICollectionValue<T>, ICloneable
  {
    //
    // If true any call of an updating operation will throw an
    // <code>ReadOnlyCollectionException</code>
    //
    // <value>True if this collection is read-only.</value>
    bool IsReadOnly { get;}

    //TODO: wonder where the right position of this is
    //
    // 
    //
    // <value>False if this collection has set semantics, true if bag semantics.</value>
    bool AllowsDuplicates { get;}

    //TODO: wonder where the right position of this is. And the semantics.
    //
    // (Here should be a discussion of the role of equalityComparers. Any ). 
    //
    // <value>The equalityComparer used by this collection to check equality of items. 
    // Or null (????) if collection does not check equality at all or uses a comparer.</value>
    SCG.IEqualityComparer<T> EqualityComparer { get;}

    //ItemEqualityTypeEnum ItemEqualityType {get ;}

    //TODO: find a good name

    //
    // By convention this is true for any collection with set semantics.
    //
    // <value>True if only one representative of a group of equal items 
    // is kept in the collection together with the total count.</value>
    bool DuplicatesByCounting { get;}

    //
    // Add an item to this collection if possible. If this collection has set
    // semantics, the item will be added if not already in the collection. If
    // bag semantics, the item will always be added.
    //
    // <param name="item">The item to add.</param>
    // <returns>True if item was added.</returns>
    bool Add(T item);

    //
    // Add the elements from another collection with a more specialized item type 
    // to this collection. If this
    // collection has set semantics, only items not already in the collection
    // will be added.
    //
    // <typeparam name="U">The type of items to add</typeparam>
    // <param name="items">The items to add</param>
    void AddAll<U>(SCG.IEnumerable<U> items) where U : T;

    //void Clear(); // for priority queue
    //int Count why not?
    //
    // Check the integrity of the internal data structures of this collection.
    // <i>This is only relevant for developers of the library</i>
    //
    // <returns>True if check was passed.</returns>
    bool Check();
  }

  //
  // The simplest interface of a main stream generic collection
  // with lookup, insertion and removal operations. 
  //
  public interface ICollection<T> : IExtensible<T>, SCG.ICollection<T>
  {
    //This is somewhat similar to the RandomAccess marker itf in java
    //
    // The value is symbolic indicating the type of asymptotic complexity
    // in terms of the size of this collection (worst-case or amortized as
    // relevant). 
    // <para>See <see cref="T:C5.Speed"/> for the set of symbols.</para>
    //
    // <value>A characterization of the speed of lookup operations
    // (<code>Contains()</code> etc.) of the implementation of this collection.</value>
    Speed ContainsSpeed { get;}

    //
    //
    // <value>The number of items in this collection</value>
    new int Count { get; }

    //
    // If true any call of an updating operation will throw an
    // <code>ReadOnlyCollectionException</code>
    //
    // <value>True if this collection is read-only.</value>
    new bool IsReadOnly { get; }

    //
    // Add an item to this collection if possible. If this collection has set
    // semantics, the item will be added if not already in the collection. If
    // bag semantics, the item will always be added.
    //
    // <param name="item">The item to add.</param>
    // <returns>True if item was added.</returns>
    new bool Add(T item);

    //
    // Copy the items of this collection to a contiguous part of an array.
    //
    // <param name="array">The array to copy to</param>
    // <param name="index">The index at which to copy the first item</param>
    new void CopyTo(T[] array, int index);

    //
    // The unordered collection hashcode is defined as the sum of 
    // <code>h(hashcode(item))</code> over the items
    // of the collection, where the function <code>h</code> is a function from 
    // int to int of the form <code> t -> (a0*t+b0)^(a1*t+b1)^(a2*t+b2)</code>, where 
    // the ax and bx are the same for all collection classes. 
    // <para>The current implementation uses fixed values for the ax and bx, 
    // specified as constants in the code.</para>
    //
    // <returns>The unordered hashcode of this collection.</returns>
    int GetUnsequencedHashCode();


    //
    // Compare the contents of this collection to another one without regards to
    // the sequence order. The comparison will use this collection's itemequalityComparer
    // to compare individual items.
    //
    // <param name="otherCollection">The collection to compare to.</param>
    // <returns>True if this collection and that contains the same items.</returns>
    bool UnsequencedEquals(ICollection<T> otherCollection);


    //
    // Check if this collection contains (an item equivalent to according to the
    // itemequalityComparer) a particular value.
    //
    // <param name="item">The value to check for.</param>
    // <returns>True if the items is in this collection.</returns>
    new bool Contains(T item);


    //
    // Count the number of items of the collection equal to a particular value.
    // Returns 0 if and only if the value is not in the collection.
    //
    // <param name="item">The value to count.</param>
    // <returns>The number of copies found.</returns>
    int ContainsCount(T item);


    //
    // 
    //
    // <returns></returns>
    ICollectionValue<T> UniqueItems();

    //
    // 
    //
    // <returns></returns>
    ICollectionValue<KeyValuePair<T, int>> ItemMultiplicities();

    //
    // Check whether this collection contains all the values in another collection.
    // If this collection has bag semantics (<code>AllowsDuplicates==true</code>)
    // the check is made with respect to multiplicities, else multiplicities
    // are not taken into account.
    //
    // <param name="items">The </param>
    // <typeparam name="U"></typeparam>
    // <returns>True if all values in <code>items</code>is in this collection.</returns>
    bool ContainsAll<U>(SCG.IEnumerable<U> items) where U : T;


    //
    // Check if this collection contains an item equivalent according to the
    // itemequalityComparer to a particular value. If so, return in the ref argument (a
    // binary copy of) the actual value found.
    //
    // <param name="item">The value to look for.</param>
    // <returns>True if the items is in this collection.</returns>
    bool Find(ref T item);


    //This should probably just be bool Add(ref T item); !!!
    //
    // Check if this collection contains an item equivalent according to the
    // itemequalityComparer to a particular value. If so, return in the ref argument (a
    // binary copy of) the actual value found. Else, add the item to the collection.
    //
    // <param name="item">The value to look for.</param>
    // <returns>True if the item was found (hence not added).</returns>
    bool FindOrAdd(ref T item);


    //
    // Check if this collection contains an item equivalent according to the
    // itemequalityComparer to a particular value. If so, update the item in the collection 
    // with a (binary copy of) the supplied value. If the collection has bag semantics,
    // it depends on the value of DuplicatesByCounting if this updates all equivalent copies in
    // the collection or just one.
    //
    // <param name="item">Value to update.</param>
    // <returns>True if the item was found and hence updated.</returns>
    bool Update(T item);

    //
    // Check if this collection contains an item equivalent according to the
    // itemequalityComparer to a particular value. If so, update the item in the collection 
    // with a (binary copy of) the supplied value. If the collection has bag semantics,
    // it depends on the value of DuplicatesByCounting if this updates all equivalent copies in
    // the collection or just one.
    //
    // <param name="item">Value to update.</param>
    // <param name="olditem">On output the olditem, if found.</param>
    // <returns>True if the item was found and hence updated.</returns>
    bool Update(T item, out T olditem);


    //
    // Check if this collection contains an item equivalent according to the
    // itemequalityComparer to a particular value. If so, update the item in the collection 
    // to with a binary copy of the supplied value; else add the value to the collection. 
    //
    // <param name="item">Value to add or update.</param>
    // <returns>True if the item was found and updated (hence not added).</returns>
    bool UpdateOrAdd(T item);


    //
    // Check if this collection contains an item equivalent according to the
    // itemequalityComparer to a particular value. If so, update the item in the collection 
    // to with a binary copy of the supplied value; else add the value to the collection. 
    //
    // <param name="item">Value to add or update.</param>
    // <param name="olditem">On output the olditem, if found.</param>
    // <returns>True if the item was found and updated (hence not added).</returns>
    bool UpdateOrAdd(T item, out T olditem);

    //
    // Remove a particular item from this collection. If the collection has bag
    // semantics only one copy equivalent to the supplied item is removed. 
    //
    // <param name="item">The value to remove.</param>
    // <returns>True if the item was found (and removed).</returns>
    new bool Remove(T item);


    //
    // Remove a particular item from this collection if found. If the collection
    // has bag semantics only one copy equivalent to the supplied item is removed,
    // which one is implementation dependent. 
    // If an item was removed, report a binary copy of the actual item removed in 
    // the argument.
    //
    // <param name="item">The value to remove.</param>
    // <param name="removeditem">The value removed if any.</param>
    // <returns>True if the item was found (and removed).</returns>
    bool Remove(T item, out T removeditem);


    //
    // Remove all items equivalent to a given value.
    //
    // <param name="item">The value to remove.</param>
    void RemoveAllCopies(T item);


    //
    // Remove all items in another collection from this one. If this collection
    // has bag semantics, take multiplicities into account.
    //
    // <typeparam name="U"></typeparam>
    // <param name="items">The items to remove.</param>
    void RemoveAll<U>(SCG.IEnumerable<U> items) where U : T;

    //void RemoveAll(Fun<T, bool> predicate);

    //
    // Remove all items from this collection.
    //
    new void Clear();


    //
    // Remove all items not in some other collection from this one. If this collection
    // has bag semantics, take multiplicities into account.
    //
    // <typeparam name="U"></typeparam>
    // <param name="items">The items to retain.</param>
    void RetainAll<U>(SCG.IEnumerable<U> items) where U : T;

    //void RetainAll(Fun<T, bool> predicate);
    //IDictionary<T> UniqueItems()
  }



  //
  // An editable collection maintaining a definite sequence order of the items.
  //
  // <i>Implementations of this interface must compute the hash code and 
  // equality exactly as prescribed in the method definitions in order to
  // be consistent with other collection classes implementing this interface.</i>
  // <i>This interface is usually implemented by explicit interface implementation,
  // not as ordinary virtual methods.</i>
  //
  public interface ISequenced<T> : ICollection<T>, IDirectedCollectionValue<T>
  {
    //
    // The hashcode is defined as <code>h(...h(h(h(x1),x2),x3),...,xn)</code> for
    // <code>h(a,b)=CONSTANT*a+b</code> and the x's the hash codes of the items of 
    // this collection.
    //
    // <returns>The sequence order hashcode of this collection.</returns>
    int GetSequencedHashCode();


    //
    // Compare this sequenced collection to another one in sequence order.
    //
    // <param name="otherCollection">The sequenced collection to compare to.</param>
    // <returns>True if this collection and that contains equal (according to
    // this collection's itemequalityComparer) in the same sequence order.</returns>
    bool SequencedEquals(ISequenced<T> otherCollection);
  }



  //
  // A sequenced collection, where indices of items in the order are maintained
  //
  public interface IIndexed<T> : ISequenced<T>
  {
    //
    //
    // <exception cref="IndexOutOfRangeException"> if <code>index</code> is negative or
    // &gt;= the size of the collection.</exception>
    // <value>The <code>index</code>'th item of this list.</value>
    // <param name="index">the index to lookup</param>
    T this[int index] { get;}

    //
    // 
    //
    // <value></value>
    Speed IndexingSpeed { get;}

    //
    //
    // <exception cref="ArgumentOutOfRangeException"></exception>
    // <value>The directed collection of items in a specific index interval.</value>
    // <param name="start">The low index of the interval (inclusive).</param>
    // <param name="count">The size of the range.</param>
    IDirectedCollectionValue<T> this[int start, int count] { get;}


    //
    // Searches for an item in the list going forwards from the start. 
    //
    // <param name="item">Item to search for.</param>
    // <returns>Index of item from start. A negative number if item not found, 
    // namely the one's complement of the index at which the Add operation would put the item.</returns>
    int IndexOf(T item);


    //
    // Searches for an item in the list going backwards from the end.
    //
    // <param name="item">Item to search for.</param>
    // <returns>Index of of item from the end. A negative number if item not found, 
    // namely the two-complement of the index at which the Add operation would put the item.</returns>
    int LastIndexOf(T item);

    //
    // Check if there exists an item  that satisfies a
    // specific predicate in this collection and return the index of the first one.
    //
    // <param name="predicate">A delegate 
    // (<see cref="T:C5.Fun`2"/> with <code>R == bool</code>) defining the predicate</param>
    // <returns>the index, if found, a negative value else</returns>
    int FindIndex(Fun<T, bool> predicate);

    //
    // Check if there exists an item  that satisfies a
    // specific predicate in this collection and return the index of the last one.
    //
    // <param name="predicate">A delegate 
    // (<see cref="T:C5.Fun`2"/> with <code>R == bool</code>) defining the predicate</param>
    // <returns>the index, if found, a negative value else</returns>
    int FindLastIndex(Fun<T, bool> predicate);


    //
    // Remove the item at a specific position of the list.
    //
    // <exception cref="IndexOutOfRangeException"> if <code>index</code> is negative or
    // &gt;= the size of the collection.</exception>
    // <param name="index">The index of the item to remove.</param>
    // <returns>The removed item.</returns>
    T RemoveAt(int index);


    //
    // Remove all items in an index interval.
    //
    // <exception cref="ArgumentOutOfRangeException"> if start or count 
    // is negative or start+count &gt; the size of the collection.</exception>
    // <param name="start">The index of the first item to remove.</param>
    // <param name="count">The number of items to remove.</param>
    void RemoveInterval(int start, int count);
  }

  //TODO: decide if this should extend ICollection
  //
  // The interface describing the operations of a LIFO stack data structure.
  //
  // <typeparam name="T">The item type</typeparam>
  public interface IStack<T> : IDirectedCollectionValue<T>
  {
    //
    // 
    //
    // <value></value>
    bool AllowsDuplicates { get;}
    //
    // Get the <code>index</code>'th element of the stack.  The bottom of the stack has index 0.
    //
    // <param name="index"></param>
    // <returns></returns>
    T this[int index] { get;}
    //
    // Push an item to the top of the stack.
    //
    // <param name="item">The item</param>
    void Push(T item);
    //
    // Pop the item at the top of the stack from the stack.
    //
    // <returns>The popped item.</returns>
    T Pop();
  }

  //
  // The interface describing the operations of a FIFO queue data structure.
  //
  // <typeparam name="T">The item type</typeparam>
  public interface IQueue<T> : IDirectedCollectionValue<T>
  {
    //
    // 
    //
    // <value></value>
    bool AllowsDuplicates { get;}
    //
    // Get the <code>index</code>'th element of the queue.  The front of the queue has index 0.
    //
    // <param name="index"></param>
    // <returns></returns>
    T this[int index] { get;}
    //
    // Enqueue an item at the back of the queue. 
    //
    // <param name="item">The item</param>
    void Enqueue(T item);
    //
    // Dequeue an item from the front of the queue.
    //
    // <returns>The item</returns>
    T Dequeue();
  }


  //
  // This is an indexed collection, where the item order is chosen by 
  // the user at insertion time.
  //
  // NBNBNB: we need a description of the view functionality here!
  //
  public interface IList<T> : IIndexed<T>, IDisposable, SCG.IList<T>, System.Collections.IList
  {
    //
    //
    // <exception cref="NoSuchItemException"> if this list is empty.</exception>
    // <value>The first item in this list.</value>
    T First { get;}

    //
    //
    // <exception cref="NoSuchItemException"> if this list is empty.</exception>
    // <value>The last item in this list.</value>
    T Last { get;}

    //
    // Since <code>Add(T item)</code> always add at the end of the list,
    // this describes if list has FIFO or LIFO semantics.
    //
    // <value>True if the <code>Remove()</code> operation removes from the
    // start of the list, false if it removes from the end.</value>
    bool FIFO { get; set;}

    //
    // 
    //
    //bool IsFixedSize { get; }

    //
    // On this list, this indexer is read/write.
    //
    // <exception cref="IndexOutOfRangeException"> if index is negative or
    // &gt;= the size of the collection.</exception>
    // <value>The index'th item of this list.</value>
    // <param name="index">The index of the item to fetch or store.</param>
    new T this[int index] { get; set;}

    #region Ambiguous calls when extending SCG.IList<T>

    #region SCG.ICollection<T>
    //
    // 
    //
    new int Count { get; }

    //
    // 
    //
    new bool IsReadOnly { get; }

    //
    // 
    //
    // <param name="item"></param>
    // <returns></returns>
    new bool Add(T item);

    //
    // 
    //
    new void Clear();

    //
    // 
    //
    // <param name="item"></param>
    // <returns></returns>
    new bool Contains(T item);

    //
    // 
    //
    // <param name="array"></param>
    // <param name="index"></param>
    new void CopyTo(T[] array, int index);

    //
    // 
    //
    // <param name="item"></param>
    // <returns></returns>
    new bool Remove(T item);

    #endregion

    #region SCG.IList<T> proper

    //
    // Searches for an item in the list going forwards from the start. 
    //
    // <param name="item">Item to search for.</param>
    // <returns>Index of item from start. A negative number if item not found, 
    // namely the one's complement of the index at which the Add operation would put the item.</returns>
    new int IndexOf(T item);

    //
    // Remove the item at a specific position of the list.
    //
    // <exception cref="IndexOutOfRangeException"> if <code>index</code> is negative or
    // &gt;= the size of the collection.</exception>
    // <param name="index">The index of the item to remove.</param>
    // <returns>The removed item.</returns>
    new T RemoveAt(int index);

    #endregion

    #endregion

    /*//
    // Insert an item at a specific index location in this list. 
    //
    // <exception cref="IndexOutOfRangeException"> if <code>index</code> is negative or
    // &gt; the size of the collection.</exception>
    // <exception cref="DuplicateNotAllowedException"> if the list has
    // <code>AllowsDuplicates==false</code> and the item is 
    // already in the list.</exception>
    // <param name="index">The index at which to insert.</param>
    // <param name="item">The item to insert.</param>
    void Insert(int index, T item);*/

    //
    // Insert an item at the end of a compatible view, used as a pointer.
    // <para>The <code>pointer</code> must be a view on the same list as
    // <code>this</code> and the endpoitn of <code>pointer</code> must be
    // a valid insertion point of <code>this</code></para>
    //
    // <exception cref="IncompatibleViewException">If <code>pointer</code> 
    // is not a view on the same list as <code>this</code></exception>
    // <exception cref="IndexOutOfRangeException"><b>??????</b> if the endpoint of 
    //  <code>pointer</code> is not inside <code>this</code></exception>
    // <exception cref="DuplicateNotAllowedException"> if the list has
    // <code>AllowsDuplicates==false</code> and the item is 
    // already in the list.</exception>
    // <param name="pointer"></param>
    // <param name="item"></param>
    void Insert(IList<T> pointer, T item);

    //
    // Insert an item at the front of this list.
    // <exception cref="DuplicateNotAllowedException"/> if the list has
    // <code>AllowsDuplicates==false</code> and the item is 
    // already in the list.
    //
    // <param name="item">The item to insert.</param>
    void InsertFirst(T item);

    //
    // Insert an item at the back of this list.
    // <exception cref="DuplicateNotAllowedException"/> if the list has
    // <code>AllowsDuplicates==false</code> and the item is 
    // already in the list.
    //
    // <param name="item">The item to insert.</param>
    void InsertLast(T item);

    //
    // Insert into this list all items from an enumerable collection starting 
    // at a particular index.
    //
    // <exception cref="IndexOutOfRangeException"> if <code>index</code> is negative or
    // &gt; the size of the collection.</exception>
    // <exception cref="DuplicateNotAllowedException"> if the list has 
    // <code>AllowsDuplicates==false</code> and one of the items to insert is
    // already in the list.</exception>
    // <param name="index">Index to start inserting at</param>
    // <param name="items">Items to insert</param>
    // <typeparam name="U"></typeparam>
    void InsertAll<U>(int index, SCG.IEnumerable<U> items) where U : T;

    //
    // Create a new list consisting of the items of this list satisfying a 
    // certain predicate.
    //
    // <param name="filter">The filter delegate defining the predicate.</param>
    // <returns>The new list.</returns>
    IList<T> FindAll(Fun<T, bool> filter);

    //
    // Create a new list consisting of the results of mapping all items of this
    // list. The new list will use the default equalityComparer for the item type V.
    //
    // <typeparam name="V">The type of items of the new list</typeparam>
    // <param name="mapper">The delegate defining the map.</param>
    // <returns>The new list.</returns>
    IList<V> Map<V>(Fun<T, V> mapper);

    //
    // Create a new list consisting of the results of mapping all items of this
    // list. The new list will use a specified equalityComparer for the item type.
    //
    // <typeparam name="V">The type of items of the new list</typeparam>
    // <param name="mapper">The delegate defining the map.</param>
    // <param name="equalityComparer">The equalityComparer to use for the new list</param>
    // <returns>The new list.</returns>
    IList<V> Map<V>(Fun<T, V> mapper, SCG.IEqualityComparer<V> equalityComparer);

    //
    // Remove one item from the list: from the front if <code>FIFO</code>
    // is true, else from the back.
    // <exception cref="NoSuchItemException"/> if this list is empty.
    //
    // <returns>The removed item.</returns>
    T Remove();

    //
    // Remove one item from the front of the list.
    // <exception cref="NoSuchItemException"/> if this list is empty.
    //
    // <returns>The removed item.</returns>
    T RemoveFirst();

    //
    // Remove one item from the back of the list.
    // <exception cref="NoSuchItemException"/> if this list is empty.
    //
    // <returns>The removed item.</returns>
    T RemoveLast();

    //
    // Create a list view on this list. 
    // <exception cref="ArgumentOutOfRangeException"/> if the view would not fit into
    // this list.
    //
    // <param name="start">The index in this list of the start of the view.</param>
    // <param name="count">The size of the view.</param>
    // <returns>The new list view.</returns>
    IList<T> View(int start, int count);

    //
    // Create a list view on this list containing the (first) occurrence of a particular item. 
    // <exception cref="NoSuchItemException"/> if the item is not in this list.
    //
    // <param name="item">The item to find.</param>
    // <returns>The new list view.</returns>
    IList<T> ViewOf(T item);

    //
    // Create a list view on this list containing the last occurrence of a particular item. 
    // <exception cref="NoSuchItemException"/> if the item is not in this list.
    //
    // <param name="item">The item to find.</param>
    // <returns>The new list view.</returns>
    IList<T> LastViewOf(T item);

    //
    // Null if this list is not a view.
    //
    // <value>Underlying list for view.</value>
    IList<T> Underlying { get;}

    //
    //
    // <value>Offset for this list view or 0 for an underlying list.</value>
    int Offset { get;}

    //
    // 
    //
    // <value></value>
    bool IsValid { get;}

    //
    // Slide this list view along the underlying list.
    //
    // <exception cref="NotAViewException"> if this list is not a view.</exception>
    // <exception cref="ArgumentOutOfRangeException"> if the operation
    // would bring either end of the view outside the underlying list.</exception>
    // <param name="offset">The signed amount to slide: positive to slide
    // towards the end.</param>
    IList<T> Slide(int offset);

    //
    // Slide this list view along the underlying list, changing its size.
    // 
    //
    // <exception cref="NotAViewException"> if this list is not a view.</exception>
    // <exception cref="ArgumentOutOfRangeException"> if the operation
    // would bring either end of the view outside the underlying list.</exception>
    // <param name="offset">The signed amount to slide: positive to slide
    // towards the end.</param>
    // <param name="size">The new size of the view.</param>
    IList<T> Slide(int offset, int size);

    //
    // 
    //
    // <param name="offset"></param>
    // <returns></returns>
    bool TrySlide(int offset);

    //
    // 
    //
    // <param name="offset"></param>
    // <param name="size"></param>
    // <returns></returns>
    bool TrySlide(int offset, int size);

    //
    // 
    // <para>Returns null if <code>otherView</code> is strictly to the left of this view</para>
    //
    // <param name="otherView"></param>
    // <exception cref="IncompatibleViewException">If otherView does not have the same underlying list as this</exception>
    // <exception cref="ArgumentOutOfRangeException">If <code>otherView</code> is strictly to the left of this view</exception>
    // <returns></returns>
    IList<T> Span(IList<T> otherView);

    //
    // Reverse the list so the items are in the opposite sequence order.
    //
    void Reverse();

    //
    // Check if this list is sorted according to the default sorting order
    // for the item type T, as defined by the <see cref="T:C5.Comparer`1"/> class 
    //
    // <exception cref="NotComparableException">if T is not comparable</exception>
    // <returns>True if the list is sorted, else false.</returns>
    bool IsSorted();

    //
    // Check if this list is sorted according to a specific sorting order.
    //
    // <param name="comparer">The comparer defining the sorting order.</param>
    // <returns>True if the list is sorted, else false.</returns>
    bool IsSorted(SCG.IComparer<T> comparer);

    //
    // Sort the items of the list according to the default sorting order
    // for the item type T, as defined by the <see cref="T:C5.Comparer`1"/> class 
    //
    // <exception cref="NotComparableException">if T is not comparable</exception>
    void Sort();

    //
    // Sort the items of the list according to a specified sorting order.
    // <para>The sorting does not perform duplicate elimination or identify items
    // according to the comparer or itemequalityComparer. I.e. the list as an 
    // unsequenced collection with binary equality, will not change.
    // </para>
    //
    // <param name="comparer">The comparer defining the sorting order.</param>
    void Sort(SCG.IComparer<T> comparer);


    //
    // Randomly shuffle the items of this list. 
    //
    void Shuffle();


    //
    // Shuffle the items of this list according to a specific random source.
    //
    // <param name="rnd">The random source.</param>
    void Shuffle(Random rnd);
  }


  //
  // The base type of a priority queue handle
  //
  // <typeparam name="T"></typeparam>
  public interface IPriorityQueueHandle<T>
  {
    //TODO: make abstract and prepare for double dispatch:
    //public virtual bool Delete(IPriorityQueue<T> q) { throw new InvalidFooException();}
    //bool Replace(T item);
  }


  //
  // A generic collection of items prioritized by a comparison (order) relation.
  // Supports adding items and reporting or removing extremal elements. 
  // <para>
  // 
  // </para>
  // When adding an item, the user may choose to have a handle allocated for this item in the queue. 
  // The resulting handle may be used for deleting the item even if not extremal, and for replacing the item.
  // A priority queue typically only holds numeric priorities associated with some objects
  // maintained separately in other collection objects.
  //
  public interface IPriorityQueue<T> : IExtensible<T>
  {
    //
    // Find the current least item of this priority queue.
    //
    // <returns>The least item.</returns>
    T FindMin();


    //
    // Remove the least item from this  priority queue.
    //
    // <returns>The removed item.</returns>
    T DeleteMin();


    //
    // Find the current largest item of this priority queue.
    //
    // <returns>The largest item.</returns>
    T FindMax();


    //
    // Remove the largest item from this priority queue.
    //
    // <returns>The removed item.</returns>
    T DeleteMax();

    //
    // The comparer object supplied at creation time for this collection
    //
    // <value>The comparer</value>
    SCG.IComparer<T> Comparer { get;}
    //
    // Get or set the item corresponding to a handle. Throws exceptions on 
    // invalid handles.
    //
    // <param name="handle"></param>
    // <returns></returns>
    T this[IPriorityQueueHandle<T> handle] { get; set;}

    //
    // Check if the entry corresponding to a handle is in the priority queue.
    //
    // <param name="handle"></param>
    // <param name="item"></param>
    // <returns></returns>
    bool Find(IPriorityQueueHandle<T> handle, out T item);

    //
    // Add an item to the priority queue, receiving a 
    // handle for the item in the queue, 
    // or reusing an existing unused handle.
    //
    // <param name="handle">On output: a handle for the added item. 
    // On input: null for allocating a new handle, or a currently unused handle for reuse. 
    // A handle for reuse must be compatible with this priority queue, 
    // by being created by a priority queue of the same runtime type, but not 
    // necessarily the same priority queue object.</param>
    // <param name="item"></param>
    // <returns></returns>
    bool Add(ref IPriorityQueueHandle<T> handle, T item);

    //
    // Delete an item with a handle from a priority queue
    //
    // <param name="handle">The handle for the item. The handle will be invalidated, but reusable.</param>
    // <returns>The deleted item</returns>
    T Delete(IPriorityQueueHandle<T> handle);

    //
    // Replace an item with a handle in a priority queue with a new item. 
    // Typically used for changing the priority of some queued object.
    //
    // <param name="handle">The handle for the old item</param>
    // <param name="item">The new item</param>
    // <returns>The old item</returns>
    T Replace(IPriorityQueueHandle<T> handle, T item);

    //
    // Find the current least item of this priority queue.
    //
    // <param name="handle">On return: the handle of the item.</param>
    // <returns>The least item.</returns>
    T FindMin(out IPriorityQueueHandle<T> handle);

    //
    // Find the current largest item of this priority queue.
    //
    // <param name="handle">On return: the handle of the item.</param>
    // <returns>The largest item.</returns>

    T FindMax(out IPriorityQueueHandle<T> handle);

    //
    // Remove the least item from this  priority queue.
    //
    // <param name="handle">On return: the handle of the removed item.</param>
    // <returns>The removed item.</returns>

    T DeleteMin(out IPriorityQueueHandle<T> handle);

    //
    // Remove the largest item from this  priority queue.
    //
    // <param name="handle">On return: the handle of the removed item.</param>
    // <returns>The removed item.</returns>
    T DeleteMax(out IPriorityQueueHandle<T> handle);
  }



  //
  // A sorted collection, i.e. a collection where items are maintained and can be searched for in sorted order.
  // Thus the sequence order is given as a sorting order.
  // 
  // <para>The sorting order is defined by a comparer, an object of type IComparer&lt;T&gt; 
  // (<see cref="T:C5.IComparer`1"/>). Implementors of this interface will normally let the user 
  // define the comparer as an argument to a constructor. 
  // Usually there will also be constructors without a comparer argument, in which case the 
  // comparer should be the defalt comparer for the item type, <see cref="P:C5.Comparer`1.Default"/>.</para>
  // 
  // <para>The comparer of the sorted collection is available as the <code>Comparer</code> property 
  // (<see cref="P:C5.ISorted`1.Comparer"/>).</para>
  // 
  // <para>The methods are grouped according to
  // <list>
  // <item>Extrema: report or report and delete an extremal item. This is reminiscent of simplified priority queues.</item>
  // <item>Nearest neighbor: report predecessor or successor in the collection of an item. Cut belongs to this group.</item>
  // <item>Range: report a view of a range of elements or remove all elements in a range.</item>
  // <item>AddSorted: add a collection of items known to be sorted in the same order (should be faster) (to be removed?)</item>
  // </list>
  // </para>
  // 
  // <para>Since this interface extends ISequenced&lt;T&gt;, sorted collections will also have an 
  // item equalityComparer (<see cref="P:C5.IExtensible`1.EqualityComparer"/>). This equalityComparer will not be used in connection with 
  // the inner workings of the sorted collection, but will be used if the sorted collection is used as 
  // an item in a collection of unsequenced or sequenced collections, 
  // (<see cref="T:C5.ICollection`1"/> and <see cref="T:C5.ISequenced`1"/>)</para>
  // 
  // <para>Note that code may check if two sorted collections has the same sorting order 
  // by checking if the Comparer properties are equal. This is done a few places in this library
  // for optimization purposes.</para>
  //
  public interface ISorted<T> : ISequenced<T>
  {
    //
    // Find the current least item of this sorted collection.
    //
    // <exception cref="NoSuchItemException"> if the collection is empty.</exception>
    // <returns>The least item.</returns>
    T FindMin();


    //
    // Remove the least item from this sorted collection.
    //
    // <exception cref="NoSuchItemException"> if the collection is empty.</exception>
    // <returns>The removed item.</returns>
    T DeleteMin();


    //
    // Find the current largest item of this sorted collection.
    //
    // <exception cref="NoSuchItemException"> if the collection is empty.</exception>
    // <returns>The largest item.</returns>
    T FindMax();


    //
    // Remove the largest item from this sorted collection.
    //
    // <exception cref="NoSuchItemException"> if the collection is empty.</exception>
    // <returns>The removed item.</returns>
    T DeleteMax();

    //
    // The comparer object supplied at creation time for this sorted collection.
    //
    // <value>The comparer</value>
    SCG.IComparer<T> Comparer { get; }

    //
    // Find the strict predecessor of item in the sorted collection,
    // that is, the greatest item in the collection smaller than the item.
    //
    // <param name="item">The item to find the predecessor for.</param>
    // <param name="res">The predecessor, if any; otherwise the default value for T.</param>
    // <returns>True if item has a predecessor; otherwise false.</returns>
    bool TryPredecessor(T item, out T res);


    //
    // Find the strict successor of item in the sorted collection,
    // that is, the least item in the collection greater than the supplied value.
    //
    // <param name="item">The item to find the successor for.</param>
    // <param name="res">The successor, if any; otherwise the default value for T.</param>
    // <returns>True if item has a successor; otherwise false.</returns>
    bool TrySuccessor(T item, out T res);


    //
    // Find the weak predecessor of item in the sorted collection,
    // that is, the greatest item in the collection smaller than or equal to the item.
    //
    // <param name="item">The item to find the weak predecessor for.</param>
    // <param name="res">The weak predecessor, if any; otherwise the default value for T.</param>
    // <returns>True if item has a weak predecessor; otherwise false.</returns>
    bool TryWeakPredecessor(T item, out T res);


    //
    // Find the weak successor of item in the sorted collection,
    // that is, the least item in the collection greater than or equal to the supplied value.
    //
    // <param name="item">The item to find the weak successor for.</param>
    // <param name="res">The weak successor, if any; otherwise the default value for T.</param>
    // <returns>True if item has a weak successor; otherwise false.</returns>
    bool TryWeakSuccessor(T item, out T res);


    //
    // Find the strict predecessor in the sorted collection of a particular value,
    // that is, the largest item in the collection less than the supplied value.
    //
    // <exception cref="NoSuchItemException"> if no such element exists (the
    // supplied  value is less than or equal to the minimum of this collection.)</exception>
    // <param name="item">The item to find the predecessor for.</param>
    // <returns>The predecessor.</returns>
    T Predecessor(T item);


    //
    // Find the strict successor in the sorted collection of a particular value,
    // that is, the least item in the collection greater than the supplied value.
    //
    // <exception cref="NoSuchItemException"> if no such element exists (the
    // supplied  value is greater than or equal to the maximum of this collection.)</exception>
    // <param name="item">The item to find the successor for.</param>
    // <returns>The successor.</returns>
    T Successor(T item);


    //
    // Find the weak predecessor in the sorted collection of a particular value,
    // that is, the largest item in the collection less than or equal to the supplied value.
    //
    // <exception cref="NoSuchItemException"> if no such element exists (the
    // supplied  value is less than the minimum of this collection.)</exception>
    // <param name="item">The item to find the weak predecessor for.</param>
    // <returns>The weak predecessor.</returns>
    T WeakPredecessor(T item);


    //
    // Find the weak successor in the sorted collection of a particular value,
    // that is, the least item in the collection greater than or equal to the supplied value.
    //
    // <exception cref="NoSuchItemException"> if no such element exists (the
    // supplied  value is greater than the maximum of this collection.)</exception>
    //<param name="item">The item to find the weak successor for.</param>
    // <returns>The weak successor.</returns>
    T WeakSuccessor(T item);


    //
    // Given a "cut" function from the items of the sorted collection to <code>int</code>
    // whose only sign changes when going through items in increasing order
    // can be 
    // <list>
    // <item>from positive to zero</item>
    // <item>from positive to negative</item>
    // <item>from zero to negative</item>
    // </list>
    // The "cut" function is supplied as the <code>CompareTo</code> method 
    // of an object <code>c</code> implementing 
    // <code>IComparable&lt;T&gt;</code>. 
    // A typical example is the case where <code>T</code> is comparable and 
    // <code>cutFunction</code> is itself of type <code>T</code>.
    // <para>This method performs a search in the sorted collection for the ranges in which the
    // "cut" function is negative, zero respectively positive. If <code>T</code> is comparable
    // and <code>c</code> is of type <code>T</code>, this is a safe way (no exceptions thrown) 
    // to find predecessor and successor of <code>c</code>.
    // </para>
    // <para> If the supplied cut function does not satisfy the sign-change condition, 
    // the result of this call is undefined.
    // </para>
    // 
    //
    // <param name="cutFunction">The cut function <code>T</code> to <code>int</code>, given
    // by the <code>CompareTo</code> method of an object implementing 
    // <code>IComparable&lt;T&gt;</code>.</param>
    // <param name="low">Returns the largest item in the collection, where the
    // cut function is positive (if any).</param>
    // <param name="lowIsValid">Returns true if the cut function is positive somewhere
    // on this collection.</param>
    // <param name="high">Returns the least item in the collection, where the
    // cut function is negative (if any).</param>
    // <param name="highIsValid">Returns true if the cut function is negative somewhere
    // on this collection.</param>
    // <returns>True if the cut function is zero somewhere
    // on this collection.</returns>
    bool Cut(IComparable<T> cutFunction, out T low, out bool lowIsValid, out T high, out bool highIsValid);


    //
    // Query this sorted collection for items greater than or equal to a supplied value.
    // <para>The returned collection is not a copy but a view into the collection.</para>
    // <para>The view is fragile in the sense that changes to the underlying collection will 
    // invalidate the view so that further operations on the view throws InvalidView exceptions.</para>
    //
    // <param name="bot">The lower bound (inclusive).</param>
    // <returns>The result directed collection.</returns>
    IDirectedEnumerable<T> RangeFrom(T bot);


    //
    // Query this sorted collection for items between two supplied values.
    // <para>The returned collection is not a copy but a view into the collection.</para>
    // <para>The view is fragile in the sense that changes to the underlying collection will 
    // invalidate the view so that further operations on the view throws InvalidView exceptions.</para>
    //
    // <param name="bot">The lower bound (inclusive).</param>
    // <param name="top">The upper bound (exclusive).</param>
    // <returns>The result directed collection.</returns>
    IDirectedEnumerable<T> RangeFromTo(T bot, T top);


    //
    // Query this sorted collection for items less than a supplied value.
    // <para>The returned collection is not a copy but a view into the collection.</para>
    // <para>The view is fragile in the sense that changes to the underlying collection will 
    // invalidate the view so that further operations on the view throws InvalidView exceptions.</para>
    //
    // <param name="top">The upper bound (exclusive).</param>
    // <returns>The result directed collection.</returns>
    IDirectedEnumerable<T> RangeTo(T top);


    //
    // Create a directed collection with the same items as this collection.
    // <para>The returned collection is not a copy but a view into the collection.</para>
    // <para>The view is fragile in the sense that changes to the underlying collection will 
    // invalidate the view so that further operations on the view throws InvalidView exceptions.</para>
    //
    // <returns>The result directed collection.</returns>
    IDirectedCollectionValue<T> RangeAll();


    //TODO: remove now that we assume that we can check the sorting order?
    //
    // Add all the items from another collection with an enumeration order that 
    // is increasing in the items.
    //
    // <exception cref="ArgumentException"> if the enumerated items turns out
    // not to be in increasing order.</exception>
    // <param name="items">The collection to add.</param>
    // <typeparam name="U"></typeparam>
    void AddSorted<U>(SCG.IEnumerable<U> items) where U : T;


    //
    // Remove all items of this collection above or at a supplied threshold.
    //
    // <param name="low">The lower threshold (inclusive).</param>
    void RemoveRangeFrom(T low);


    //
    // Remove all items of this collection between two supplied thresholds.
    //
    // <param name="low">The lower threshold (inclusive).</param>
    // <param name="hi">The upper threshold (exclusive).</param>
    void RemoveRangeFromTo(T low, T hi);


    //
    // Remove all items of this collection below a supplied threshold.
    //
    // <param name="hi">The upper threshold (exclusive).</param>
    void RemoveRangeTo(T hi);
  }



  //
  // A collection where items are maintained in sorted order together
  // with their indexes in that order.
  //
  public interface IIndexedSorted<T> : ISorted<T>, IIndexed<T>
  {
    //
    // Determine the number of items at or above a supplied threshold.
    //
    // <param name="bot">The lower bound (inclusive)</param>
    // <returns>The number of matcing items.</returns>
    int CountFrom(T bot);


    //
    // Determine the number of items between two supplied thresholds.
    //
    // <param name="bot">The lower bound (inclusive)</param>
    // <param name="top">The upper bound (exclusive)</param>
    // <returns>The number of matcing items.</returns>
    int CountFromTo(T bot, T top);


    //
    // Determine the number of items below a supplied threshold.
    //
    // <param name="top">The upper bound (exclusive)</param>
    // <returns>The number of matcing items.</returns>
    int CountTo(T top);


    //
    // Query this sorted collection for items greater than or equal to a supplied value.
    //
    // <param name="bot">The lower bound (inclusive).</param>
    // <returns>The result directed collection.</returns>
    new IDirectedCollectionValue<T> RangeFrom(T bot);


    //
    // Query this sorted collection for items between two supplied values.
    //
    // <param name="bot">The lower bound (inclusive).</param>
    // <param name="top">The upper bound (exclusive).</param>
    // <returns>The result directed collection.</returns>
    new IDirectedCollectionValue<T> RangeFromTo(T bot, T top);


    //
    // Query this sorted collection for items less than a supplied value.
    //
    // <param name="top">The upper bound (exclusive).</param>
    // <returns>The result directed collection.</returns>
    new IDirectedCollectionValue<T> RangeTo(T top);


    //
    // Create a new indexed sorted collection consisting of the items of this
    // indexed sorted collection satisfying a certain predicate.
    //
    // <param name="predicate">The filter delegate defining the predicate.</param>
    // <returns>The new indexed sorted collection.</returns>
    IIndexedSorted<T> FindAll(Fun<T, bool> predicate);


    //
    // Create a new indexed sorted collection consisting of the results of
    // mapping all items of this list.
    // <exception cref="ArgumentException"/> if the map is not increasing over 
    // the items of this collection (with respect to the two given comparison 
    // relations).
    //
    // <param name="mapper">The delegate definging the map.</param>
    // <param name="comparer">The comparion relation to use for the result.</param>
    // <returns>The new sorted collection.</returns>
    IIndexedSorted<V> Map<V>(Fun<T, V> mapper, SCG.IComparer<V> comparer);
  }



  //
  // The type of a sorted collection with persistence
  //
  public interface IPersistentSorted<T> : ISorted<T>, IDisposable
  {
    //
    // Make a (read-only) snap shot of this collection.
    //
    // <returns>The snap shot.</returns>
    ISorted<T> Snapshot();
  }



  /*************************************************************************/
  //
  // A dictionary with keys of type K and values of type V. Equivalent to a
  // finite partial map from K to V.
  //
  public interface IDictionary<K, V> : ICollectionValue<KeyValuePair<K, V>>, ICloneable
  {
    //
    // The key equalityComparer.
    //
    // <value></value>
    SCG.IEqualityComparer<K> EqualityComparer { get;}

    //
    // Indexer for dictionary.
    //
    // <exception cref="NoSuchItemException"> if no entry is found. </exception>
    // <value>The value corresponding to the key</value>
    V this[K key] { get; set;}


    //
    // 
    //
    // <value>True if dictionary is read-only</value>
    bool IsReadOnly { get;}


    //
    // 
    //
    // <value>A collection containg the all the keys of the dictionary</value>
    ICollectionValue<K> Keys { get;}


    //
    // 
    //
    // <value>A collection containing all the values of the dictionary</value>
    ICollectionValue<V> Values { get;}

    //
    // 
    //
    // <value>A delegate of type <see cref="T:C5.Fun`2"/> defining the partial function from K to V give by the dictionary.</value>
    Fun<K, V> Fun { get; }


    //TODO: resolve inconsistency: Add thows exception if key already there, AddAll ignores keys already There?
    //
    // Add a new (key, value) pair (a mapping) to the dictionary.
    //
    // <exception cref="DuplicateNotAllowedException"> if there already is an entry with the same key. </exception>>
    // <param name="key">Key to add</param>
    // <param name="val">Value to add</param>
    void Add(K key, V val);

    //
    // Add the entries from a collection of <see cref="T:C5.KeyValuePair`2"/> pairs to this dictionary.
    //
    // <exception cref="DuplicateNotAllowedException"> 
    // If the input contains duplicate keys or a key already present in this dictionary.</exception>
    // <param name="entries"></param>
    void AddAll<U, W>(SCG.IEnumerable<KeyValuePair<U, W>> entries)
        where U : K
        where W : V
      ;

    //
    // The value is symbolic indicating the type of asymptotic complexity
    // in terms of the size of this collection (worst-case or amortized as
    // relevant). 
    // <para>See <see cref="T:C5.Speed"/> for the set of symbols.</para>
    //
    // <value>A characterization of the speed of lookup operations
    // (<code>Contains()</code> etc.) of the implementation of this dictionary.</value>
    Speed ContainsSpeed { get;}

    //
    // Check whether this collection contains all the values in another collection.
    // If this collection has bag semantics (<code>AllowsDuplicates==true</code>)
    // the check is made with respect to multiplicities, else multiplicities
    // are not taken into account.
    //
    // <param name="items">The </param>
    // <returns>True if all values in <code>items</code>is in this collection.</returns>
      bool ContainsAll<H>(SCG.IEnumerable<H> items) where H : K;

    //
    // Remove an entry with a given key from the dictionary
    //
    // <param name="key">The key of the entry to remove</param>
    // <returns>True if an entry was found (and removed)</returns>
    bool Remove(K key);


    //
    // Remove an entry with a given key from the dictionary and report its value.
    //
    // <param name="key">The key of the entry to remove</param>
    // <param name="val">On exit, the value of the removed entry</param>
    // <returns>True if an entry was found (and removed)</returns>
    bool Remove(K key, out V val);


    //
    // Remove all entries from the dictionary
    //
    void Clear();


    //
    // Check if there is an entry with a specified key
    //
    // <param name="key">The key to look for</param>
    // <returns>True if key was found</returns>
    bool Contains(K key);


    //
    // Check if there is an entry with a specified key and report the corresponding
    // value if found. This can be seen as a safe form of "val = this[key]".
    //
    // <param name="key">The key to look for</param>
    // <param name="val">On exit, the value of the entry</param>
    // <returns>True if key was found</returns>
    bool Find(K key, out V val);

    //
    // Check if there is an entry with a specified key and report the corresponding
    // value if found. This can be seen as a safe form of "val = this[key]".
    //
    // <param name="key">The key to look for</param>
    // <param name="val">On exit, the value of the entry</param>
    // <returns>True if key was found</returns>
    bool Find(ref K key, out V val);


    //
    // Look for a specific key in the dictionary and if found replace the value with a new one.
    // This can be seen as a non-adding version of "this[key] = val".
    //
    // <param name="key">The key to look for</param>
    // <param name="val">The new value</param>
    // <returns>True if key was found</returns>
    bool Update(K key, V val);          //no-adding				    	


    //
    // Look for a specific key in the dictionary and if found replace the value with a new one.
    // This can be seen as a non-adding version of "this[key] = val" reporting the old value.
    //
    // <param name="key">The key to look for</param>
    // <param name="val">The new value</param>
    // <param name="oldval">The old value if any</param>
    // <returns>True if key was found</returns>
    bool Update(K key, V val, out V oldval);          //no-adding				    	

    //
    // Look for a specific key in the dictionary. If found, report the corresponding value,
    // else add an entry with the key and the supplied value.
    //
    // <param name="key">The key to look for</param>
    // <param name="val">On entry the value to add if the key is not found.
    // On exit the value found if any.</param>
    // <returns>True if key was found</returns>
    bool FindOrAdd(K key, ref V val);   //mixture


    //
    // Update value in dictionary corresponding to key if found, else add new entry.
    // More general than "this[key] = val;" by reporting if key was found.
    //
    // <param name="key">The key to look for</param>
    // <param name="val">The value to add or replace with.</param>
    // <returns>True if key was found and value updated.</returns>
    bool UpdateOrAdd(K key, V val);


    //
    // Update value in dictionary corresponding to key if found, else add new entry.
    // More general than "this[key] = val;" by reporting if key was found.
    //
    // <param name="key">The key to look for</param>
    // <param name="val">The value to add or replace with.</param>
    // <param name="oldval">The old value if any</param>
    // <returns>True if key was found and value updated.</returns>
    bool UpdateOrAdd(K key, V val, out V oldval);


    //
    // Check the integrity of the internal data structures of this dictionary.
    // Only avaliable in DEBUG builds???
    //
    // <returns>True if check does not fail.</returns>
    bool Check();
  }



  //
  // A dictionary with sorted keys.
  //
  public interface ISortedDictionary<K, V> : IDictionary<K, V>
  {
    //
    // 
    //
    // <value></value>
    new ISorted<K> Keys { get;}

    //
    // Find the current least item of this sorted collection.
    //
    // <exception cref="NoSuchItemException"> if the collection is empty.</exception>
    // <returns>The least item.</returns>
    KeyValuePair<K, V> FindMin();


    //
    // Remove the least item from this sorted collection.
    //
    // <exception cref="NoSuchItemException"> if the collection is empty.</exception>
    // <returns>The removed item.</returns>
    KeyValuePair<K, V> DeleteMin();


    //
    // Find the current largest item of this sorted collection.
    //
    // <exception cref="NoSuchItemException"> if the collection is empty.</exception>
    // <returns>The largest item.</returns>
    KeyValuePair<K, V> FindMax();


    //
    // Remove the largest item from this sorted collection.
    //
    // <exception cref="NoSuchItemException"> if the collection is empty.</exception>
    // <returns>The removed item.</returns>
    KeyValuePair<K, V> DeleteMax();

    //
    // The key comparer used by this dictionary.
    //
    // <value></value>
    SCG.IComparer<K> Comparer { get;}

    //
    // Find the entry in the dictionary whose key is the
    // predecessor of the specified key.
    //
    // <param name="key">The key</param>
    // <param name="res">The predecessor, if any</param>
    // <returns>True if key has a predecessor</returns>
    bool TryPredecessor(K key, out KeyValuePair<K, V> res);

    //
    // Find the entry in the dictionary whose key is the
    // successor of the specified key.
    //
    // <param name="key">The key</param>
    // <param name="res">The successor, if any</param>
    // <returns>True if the key has a successor</returns>
    bool TrySuccessor(K key, out KeyValuePair<K, V> res);

    //
    // Find the entry in the dictionary whose key is the
    // weak predecessor of the specified key.
    //
    // <param name="key">The key</param>
    // <param name="res">The predecessor, if any</param>
    // <returns>True if key has a weak predecessor</returns>
    bool TryWeakPredecessor(K key, out KeyValuePair<K, V> res);

    //
    // Find the entry in the dictionary whose key is the
    // weak successor of the specified key.
    //
    // <param name="key">The key</param>
    // <param name="res">The weak successor, if any</param>
    // <returns>True if the key has a weak successor</returns>
    bool TryWeakSuccessor(K key, out KeyValuePair<K, V> res);

    //
    // Find the entry with the largest key less than a given key.
    //
    // <exception cref="NoSuchItemException"> if there is no such entry. </exception>
    // <param name="key">The key to compare to</param>
    // <returns>The entry</returns>
    KeyValuePair<K, V> Predecessor(K key);


    //
    // Find the entry with the least key greater than a given key.
    //
    // <exception cref="NoSuchItemException"> if there is no such entry. </exception>
    // <param name="key">The key to compare to</param>
    // <returns>The entry</returns>
    KeyValuePair<K, V> Successor(K key);


    //
    // Find the entry with the largest key less than or equal to a given key.
    //
    // <exception cref="NoSuchItemException"> if there is no such entry. </exception>
    // <param name="key">The key to compare to</param>
    // <returns>The entry</returns>
    KeyValuePair<K, V> WeakPredecessor(K key);


    //
    // Find the entry with the least key greater than or equal to a given key.
    //
    // <exception cref="NoSuchItemException"> if there is no such entry. </exception>
    // <param name="key">The key to compare to</param>
    // <returns>The entry</returns>
    KeyValuePair<K, V> WeakSuccessor(K key);

    //
    // Given a "cut" function from the items of the sorted collection to <code>int</code>
    // whose only sign changes when going through items in increasing order
    // can be 
    // <list>
    // <item>from positive to zero</item>
    // <item>from positive to negative</item>
    // <item>from zero to negative</item>
    // </list>
    // The "cut" function is supplied as the <code>CompareTo</code> method 
    // of an object <code>c</code> implementing 
    // <code>IComparable&lt;K&gt;</code>. 
    // A typical example is the case where <code>K</code> is comparable and 
    // <code>c</code> is itself of type <code>K</code>.
    // <para>This method performs a search in the sorted collection for the ranges in which the
    // "cut" function is negative, zero respectively positive. If <code>K</code> is comparable
    // and <code>c</code> is of type <code>K</code>, this is a safe way (no exceptions thrown) 
    // to find predecessor and successor of <code>c</code>.
    // </para>
    // <para> If the supplied cut function does not satisfy the sign-change condition, 
    // the result of this call is undefined.
    // </para>
    // 
    //
    // <param name="cutFunction">The cut function <code>K</code> to <code>int</code>, given
    // by the <code>CompareTo</code> method of an object implementing 
    // <code>IComparable&lt;K&gt;</code>.</param>
    // <param name="lowEntry">Returns the largest item in the collection, where the
    // cut function is positive (if any).</param>
    // <param name="lowIsValid">Returns true if the cut function is positive somewhere
    // on this collection.</param>
    // <param name="highEntry">Returns the least item in the collection, where the
    // cut function is negative (if any).</param>
    // <param name="highIsValid">Returns true if the cut function is negative somewhere
    // on this collection.</param>
    // <returns>True if the cut function is zero somewhere
    // on this collection.</returns>
    bool Cut(IComparable<K> cutFunction, out KeyValuePair<K, V> lowEntry, out bool lowIsValid, out KeyValuePair<K, V> highEntry, out bool highIsValid);

    //
    // Query this sorted collection for items greater than or equal to a supplied value.
    // <para>The returned collection is not a copy but a view into the collection.</para>
    // <para>The view is fragile in the sense that changes to the underlying collection will 
    // invalidate the view so that further operations on the view throws InvalidView exceptions.</para>
    //
    // <param name="bot">The lower bound (inclusive).</param>
    // <returns>The result directed collection.</returns>
    IDirectedEnumerable<KeyValuePair<K, V>> RangeFrom(K bot);


    //
    // Query this sorted collection for items between two supplied values.
    // <para>The returned collection is not a copy but a view into the collection.</para>
    // <para>The view is fragile in the sense that changes to the underlying collection will 
    // invalidate the view so that further operations on the view throws InvalidView exceptions.</para>
    //
    // <param name="lowerBound">The lower bound (inclusive).</param>
    // <param name="upperBound">The upper bound (exclusive).</param>
    // <returns>The result directed collection.</returns>
    IDirectedEnumerable<KeyValuePair<K, V>> RangeFromTo(K lowerBound, K upperBound);


    //
    // Query this sorted collection for items less than a supplied value.
    // <para>The returned collection is not a copy but a view into the collection.</para>
    // <para>The view is fragile in the sense that changes to the underlying collection will 
    // invalidate the view so that further operations on the view throws InvalidView exceptions.</para>
    //
    // <param name="top">The upper bound (exclusive).</param>
    // <returns>The result directed collection.</returns>
    IDirectedEnumerable<KeyValuePair<K, V>> RangeTo(K top);


    //
    // Create a directed collection with the same items as this collection.
    // <para>The returned collection is not a copy but a view into the collection.</para>
    // <para>The view is fragile in the sense that changes to the underlying collection will 
    // invalidate the view so that further operations on the view throws InvalidView exceptions.</para>
    //
    // <returns>The result directed collection.</returns>
    IDirectedCollectionValue<KeyValuePair<K, V>> RangeAll();


    //TODO: remove now that we assume that we can check the sorting order?
    //
    // Add all the items from another collection with an enumeration order that 
    // is increasing in the items.
    //
    // <exception cref="ArgumentException"> if the enumerated items turns out
    // not to be in increasing order.</exception>
    // <param name="items">The collection to add.</param>
    void AddSorted(SCG.IEnumerable<KeyValuePair<K, V>> items);


    //
    // Remove all items of this collection above or at a supplied threshold.
    //
    // <param name="low">The lower threshold (inclusive).</param>
    void RemoveRangeFrom(K low);


    //
    // Remove all items of this collection between two supplied thresholds.
    //
    // <param name="low">The lower threshold (inclusive).</param>
    // <param name="hi">The upper threshold (exclusive).</param>
    void RemoveRangeFromTo(K low, K hi);


    //
    // Remove all items of this collection below a supplied threshold.
    //
    // <param name="hi">The upper threshold (exclusive).</param>
    void RemoveRangeTo(K hi);
  }



  /*******************************************************************/
  /*//
  // The type of an item comparer
  // <i>Implementations of this interface must asure that the method is self-consistent
  // and defines a sorting order on items, or state precise conditions under which this is true.</i>
  // <i>Implementations <b>must</b> assure that repeated calls of
  // the method to the same (in reference or binary identity sense) arguments 
  // will return values with the same sign (-1, 0 or +1), or state precise conditions
  // under which the user 
  // can be assured repeated calls will return the same sign.</i>
  // <i>Implementations of this interface must always return values from the method
  // and never throw exceptions.</i>
  // <i>This interface is identical to System.Collections.Generic.IComparer&lt;T&gt;</i>
  //
  public interface IComparer<T>
  {
    //
    // Compare two items with respect to this item comparer
    //
    // <param name="item1">First item</param>
    // <param name="item2">Second item</param>
    // <returns>Positive if item1 is greater than item2, 0 if they are equal, negative if item1 is less than item2</returns>
    int Compare(T item1, T item2);
  }

  //
  // The type of an item equalityComparer. 
  // <i>Implementations of this interface <b>must</b> assure that the methods are 
  // consistent, that is, that whenever two items i1 and i2 satisfies that Equals(i1,i2)
  // returns true, then GetHashCode returns the same value for i1 and i2.</i>
  // <i>Implementations of this interface <b>must</b> assure that repeated calls of
  // the methods to the same (in reference or binary identity sense) arguments 
  // will return the same values, or state precise conditions under which the user 
  // can be assured repeated calls will return the same values.</i>
  // <i>Implementations of this interface must always return values from the methods
  // and never throw exceptions.</i>
  // <i>This interface is similar in function to System.IKeyComparer&lt;T&gt;</i>
  //
  public interface SCG.IEqualityComparer<T>
  {
    //
    // Get the hash code with respect to this item equalityComparer
    //
    // <param name="item">The item</param>
    // <returns>The hash code</returns>
    int GetHashCode(T item);


    //
    // Check if two items are equal with respect to this item equalityComparer
    //
    // <param name="item1">first item</param>
    // <param name="item2">second item</param>
    // <returns>True if equal</returns>
    bool Equals(T item1, T item2);
  }*/
}
