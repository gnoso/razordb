using System;
using System.Text;
using System.Diagnostics;
using SCG = System.Collections.Generic;
namespace RazorDB.C5
{
  //
  // An advanced interface to operations on an array. The array is viewed as an 
  // <see cref="T:C5.IList`1"/> of fixed size, and so all operations that would change the
  // size of the array will be invalid (and throw <see cref="T:C5.FixedSizeCollectionException"/>
  //
  // <typeparam name="T"></typeparam>
  public class WrappedArray<T> : IList<T>, SCG.IList<T>
  {
    class InnerList : ArrayList<T>
    {
      internal InnerList(T[] a) { array = a; size = array.Length; }
    }
    ArrayList<T> innerlist;
    //TODO: remember a ref to the wrapped array in WrappedArray to save a little on indexing?
    WrappedArray<T> underlying;

    //
    // 
    //
    // <param name="wrappedarray"></param>
    public WrappedArray(T[] wrappedarray) { innerlist = new InnerList(wrappedarray); }

    //for views
    WrappedArray(ArrayList<T> arraylist, WrappedArray<T> u) { innerlist = arraylist; underlying = u; }

    #region IList<T> Members

    //
    // 
    //
    // <value></value>
    public T First { get { return innerlist.First; } }

    //
    // 
    //
    // <value></value>
    public T Last { get { return innerlist.Last; } }

    //
    // 
    //
    // <param name="index"></param>
    // <returns></returns>
    public T this[int index]
    {
      get { return innerlist[index]; }
      set { innerlist[index] = value; }
    }

    //
    // 
    //
    // <param name="filter"></param>
    // <returns></returns>
    public IList<T> FindAll(Fun<T, bool> filter) { return innerlist.FindAll(filter); }

    //
    // 
    //
    // <typeparam name="V"></typeparam>
    // <param name="mapper"></param>
    // <returns></returns>
    public IList<V> Map<V>(Fun<T, V> mapper) { return innerlist.Map<V>(mapper); }

    //
    // 
    //
    // <typeparam name="V"></typeparam>
    // <param name="mapper"></param>
    // <param name="equalityComparer"></param>
    // <returns></returns>
    public IList<V> Map<V>(Fun<T, V> mapper, SCG.IEqualityComparer<V> equalityComparer) { return innerlist.Map<V>(mapper, equalityComparer); }

    //
    // ???? should we throw NotRelevantException
    //
    // <value></value>
    public bool FIFO
    {
      get { throw new FixedSizeCollectionException(); }
      set { throw new FixedSizeCollectionException(); }
    }

    //
    // 
    //
    public virtual bool IsFixedSize
    {
      get { return true; }
    }

    //
    // 
    //
    // <param name="index"></param>
    // <param name="item"></param>
    public void Insert(int index, T item)
    {
      throw new FixedSizeCollectionException();
    }

    //
    // 
    //
    // <param name="pointer"></param>
    // <param name="item"></param>
    public void Insert(IList<T> pointer, T item)
    {
      throw new FixedSizeCollectionException();
    }

    //
    // 
    //
    // <param name="item"></param>
    public void InsertFirst(T item)
    {
      throw new FixedSizeCollectionException();
    }

    //
    // 
    //
    // <param name="item"></param>
    public void InsertLast(T item)
    {
      throw new FixedSizeCollectionException();
    }

    //
    // 
    //
    // <typeparam name="U"></typeparam>
    // <param name="i"></param>
    // <param name="items"></param>
    public void InsertAll<U>(int i, System.Collections.Generic.IEnumerable<U> items) where U : T
    {
      throw new FixedSizeCollectionException();
    }

    //
    // 
    //
    // <returns></returns>
    public T Remove()
    {
      throw new FixedSizeCollectionException();
    }

    //
    // 
    //
    // <returns></returns>
    public T RemoveFirst()
    {
      throw new FixedSizeCollectionException();
    }

    //
    // 
    //
    // <returns></returns>
    public T RemoveLast()
    {
      throw new FixedSizeCollectionException();
    }

    //
    // 
    //
    // <param name="start"></param>
    // <param name="count"></param>
    // <returns></returns>
    public IList<T> View(int start, int count)
    {
      return new WrappedArray<T>((ArrayList<T>)innerlist.View(start, count), underlying ?? this);
    }

    //
    // 
    //
    // <param name="item"></param>
    // <returns></returns>
    public IList<T> ViewOf(T item)
    {
      return new WrappedArray<T>((ArrayList<T>)innerlist.ViewOf(item), underlying ?? this);
    }

    //
    // 
    //
    // <param name="item"></param>
    // <returns></returns>
    public IList<T> LastViewOf(T item)
    {
      return new WrappedArray<T>((ArrayList<T>)innerlist.LastViewOf(item), underlying ?? this);
    }

    //
    // 
    //
    // <value></value>
    public IList<T> Underlying { get { return underlying; } }

    //
    // 
    //
    // <value></value>
    public int Offset { get { return innerlist.Offset; } }

    //
    // 
    //
    // <value></value>
    public bool IsValid { get { return innerlist.IsValid; } }

    //
    // 
    //
    // <param name="offset"></param>
    // <returns></returns>
    public IList<T> Slide(int offset) { return innerlist.Slide(offset); }

    //
    // 
    //
    // <param name="offset"></param>
    // <param name="size"></param>
    // <returns></returns>
    public IList<T> Slide(int offset, int size) { return innerlist.Slide(offset, size); }

    //
    // 
    //
    // <param name="offset"></param>
    // <returns></returns>
    public bool TrySlide(int offset) { return innerlist.TrySlide(offset); }

    //
    // 
    //
    // <param name="offset"></param>
    // <param name="size"></param>
    // <returns></returns>
    public bool TrySlide(int offset, int size) { return innerlist.TrySlide(offset, size); }

    //
    // 
    //
    // <param name="otherView"></param>
    // <returns></returns>
    public IList<T> Span(IList<T> otherView) { return innerlist.Span(((WrappedArray<T>)otherView).innerlist); }

    //
    // 
    //
    public void Reverse() { innerlist.Reverse(); }

    //
    // 
    //
    // <returns></returns>
    public bool IsSorted() { return innerlist.IsSorted(); }

    //
    // 
    //
    // <param name="comparer"></param>
    // <returns></returns>
    public bool IsSorted(SCG.IComparer<T> comparer) { return innerlist.IsSorted(comparer); }

    //
    // 
    //
    public void Sort() { innerlist.Sort(); }

    //
    // 
    //
    // <param name="comparer"></param>
    public void Sort(SCG.IComparer<T> comparer) { innerlist.Sort(comparer); }

    //
    // 
    //
    public void Shuffle() { innerlist.Shuffle(); }

    //
    // 
    //
    // <param name="rnd"></param>
    public void Shuffle(Random rnd) { innerlist.Shuffle(rnd); }

    #endregion

    #region IIndexed<T> Members

    //
    // 
    //
    // <value></value>
    public Speed IndexingSpeed { get { return Speed.Constant; } }

    //
    // 
    //
    // <param name="start"></param>
    // <param name="count"></param>
    // <returns></returns>
    public IDirectedCollectionValue<T> this[int start, int count] { get { return innerlist[start, count]; } }

    //
    // 
    //
    // <param name="item"></param>
    // <returns></returns>
    public int IndexOf(T item) { return innerlist.IndexOf(item); }

    //
    // 
    //
    // <param name="item"></param>
    // <returns></returns>
    public int LastIndexOf(T item) { return innerlist.LastIndexOf(item); }

    //
    // 
    //
    // <param name="predicate"></param>
    // <returns></returns>
    public int FindIndex(Fun<T, bool> predicate) { return innerlist.FindIndex(predicate); }

    //
    // 
    //
    // <param name="predicate"></param>
    // <returns></returns>
    public int FindLastIndex(Fun<T, bool> predicate) { return innerlist.FindLastIndex(predicate); }

    //
    // 
    //
    // <param name="i"></param>
    // <returns></returns>
    public T RemoveAt(int i) { throw new FixedSizeCollectionException(); }

    //
    // 
    //
    // <param name="start"></param>
    // <param name="count"></param>
    public void RemoveInterval(int start, int count) { throw new FixedSizeCollectionException(); }

    #endregion

    #region ISequenced<T> Members

    //
    // 
    //
    // <returns></returns>
    public int GetSequencedHashCode() { return innerlist.GetSequencedHashCode(); }

    //
    // 
    //
    // <param name="that"></param>
    // <returns></returns>
    public bool SequencedEquals(ISequenced<T> that) { return innerlist.SequencedEquals(that); }

    #endregion

    #region ICollection<T> Members
    //
    // 
    //
    // <value></value>
    public Speed ContainsSpeed { get { return Speed.Linear; } }

    //
    // 
    //
    // <returns></returns>
    public int GetUnsequencedHashCode() { return innerlist.GetUnsequencedHashCode(); }

    //
    // 
    //
    // <param name="that"></param>
    // <returns></returns>
    public bool UnsequencedEquals(ICollection<T> that) { return innerlist.UnsequencedEquals(that); }

    //
    // 
    //
    // <param name="item"></param>
    // <returns></returns>
    public bool Contains(T item) { return innerlist.Contains(item); }

    //
    // 
    //
    // <param name="item"></param>
    // <returns></returns>
    public int ContainsCount(T item) { return innerlist.ContainsCount(item); }

    //
    // 
    //
    // <returns></returns>
    public ICollectionValue<T> UniqueItems() { return innerlist.UniqueItems(); }

    //
    // 
    //
    // <returns></returns>
    public ICollectionValue<KeyValuePair<T, int>> ItemMultiplicities() { return innerlist.ItemMultiplicities(); }

    //
    // 
    //
    // <typeparam name="U"></typeparam>
    // <param name="items"></param>
    // <returns></returns>
    public bool ContainsAll<U>(System.Collections.Generic.IEnumerable<U> items) where U : T
    { return innerlist.ContainsAll(items); }

    //
    // 
    //
    // <param name="item"></param>
    // <returns></returns>
    public bool Find(ref T item) { return innerlist.Find(ref item); }

    //
    // 
    //
    // <param name="item"></param>
    // <returns></returns>
    public bool FindOrAdd(ref T item) { throw new FixedSizeCollectionException(); }

    //
    // 
    //
    // <param name="item"></param>
    // <returns></returns>
    public bool Update(T item) { throw new FixedSizeCollectionException(); }

    //
    // 
    //
    // <param name="item"></param>
    // <param name="olditem"></param>
    // <returns></returns>
    public bool Update(T item, out T olditem) { throw new FixedSizeCollectionException(); }

    //
    // 
    //
    // <param name="item"></param>
    // <returns></returns>
    public bool UpdateOrAdd(T item) { throw new FixedSizeCollectionException(); }

    //
    // 
    //
    // <param name="item"></param>
    // <param name="olditem"></param>
    // <returns></returns>
    public bool UpdateOrAdd(T item, out T olditem) { throw new FixedSizeCollectionException(); }

    //
    // 
    //
    // <param name="item"></param>
    // <returns></returns>
    public bool Remove(T item) { throw new FixedSizeCollectionException(); }

    //
    // 
    //
    // <param name="item"></param>
    // <param name="removeditem"></param>
    // <returns></returns>
    public bool Remove(T item, out T removeditem) { throw new FixedSizeCollectionException(); }

    //
    // 
    //
    // <param name="item"></param>
    public void RemoveAllCopies(T item) { throw new FixedSizeCollectionException(); }

    //
    // 
    //
    // <typeparam name="U"></typeparam>
    // <param name="items"></param>
    public void RemoveAll<U>(System.Collections.Generic.IEnumerable<U> items) where U : T { throw new FixedSizeCollectionException(); }

    //
    // 
    //
    public void Clear() { throw new FixedSizeCollectionException(); }

    //
    // 
    //
    // <typeparam name="U"></typeparam>
    // <param name="items"></param>
    public void RetainAll<U>(System.Collections.Generic.IEnumerable<U> items) where U : T { throw new FixedSizeCollectionException(); }

    #endregion

    #region IExtensible<T> Members

    //
    // 
    //
    // <value></value>
    public bool IsReadOnly { get { return true; } }

    //
    // 
    //
    // <value></value>
    public bool AllowsDuplicates
    {
      get { return true; }
    }

    //
    // 
    //
    // <value></value>
    public SCG.IEqualityComparer<T> EqualityComparer { get { return innerlist.EqualityComparer; } }

    //
    // 
    //
    // <value></value>
    public bool DuplicatesByCounting
    {
      get { return false; }
    }

    //
    // 
    //
    // <param name="item"></param>
    // <returns></returns>
    public bool Add(T item)
    {
      throw new FixedSizeCollectionException();
    }

    //
    // 
    //
    // <typeparam name="U"></typeparam>
    // <param name="items"></param>
    public void AddAll<U>(System.Collections.Generic.IEnumerable<U> items) where U : T
    {
      throw new FixedSizeCollectionException();
    }

    //
    // 
    //
    // <returns></returns>
    public bool Check()
    {
      return innerlist.Check() && (underlying == null || underlying.innerlist == innerlist.Underlying);
    }

    #endregion

    #region ICollectionValue<T> Members
    //
    // No listeners may be installed
    //
    // <value>0</value>
    public virtual EventTypeEnum ListenableEvents { get { return 0; } }

    //
    // No listeners ever installed
    //
    // <value>0</value>
    public virtual EventTypeEnum ActiveEvents { get { return 0; } }

    //
    // 
    //
    // <value></value>
    public event CollectionChangedHandler<T> CollectionChanged
    {
      add { throw new UnlistenableEventException(); }
      remove { throw new UnlistenableEventException(); }
    }

    //
    // 
    //
    // <value></value>
    public event CollectionClearedHandler<T> CollectionCleared
    {
      add { throw new UnlistenableEventException(); }
      remove { throw new UnlistenableEventException(); }
    }

    //
    // 
    //
    // <value></value>
    public event ItemsAddedHandler<T> ItemsAdded
    {
      add { throw new UnlistenableEventException(); }
      remove { throw new UnlistenableEventException(); }
    }

    //
    // 
    //
    // <value></value>
    public event ItemInsertedHandler<T> ItemInserted
    {
      add { throw new UnlistenableEventException(); }
      remove { throw new UnlistenableEventException(); }
    }

    //
    // 
    //
    // <value></value>
    public event ItemsRemovedHandler<T> ItemsRemoved
    {
      add { throw new UnlistenableEventException(); }
      remove { throw new UnlistenableEventException(); }
    }

    //
    // 
    //
    // <value></value>
    public event ItemRemovedAtHandler<T> ItemRemovedAt
    {
      add { throw new UnlistenableEventException(); }
      remove { throw new UnlistenableEventException(); }
    }

    //
    // 
    //
    // <value></value>
    public bool IsEmpty { get { return innerlist.IsEmpty; } }

    //
    // 
    //
    // <value></value>
    public int Count { get { return innerlist.Count; } }

    //
    // 
    //
    // <value></value>
    public Speed CountSpeed { get { return innerlist.CountSpeed; } }

    //
    // 
    //
    // <param name="array"></param>
    // <param name="index"></param>
    public void CopyTo(T[] array, int index) { innerlist.CopyTo(array, index); }

    //
    // 
    //
    // <returns></returns>
    public T[] ToArray() { return innerlist.ToArray(); }

    //
    // 
    //
    // <param name="action"></param>
    public void Apply(Act<T> action) { innerlist.Apply(action); }

    //
    // 
    //
    // <param name="predicate"></param>
    // <returns></returns>
    public bool Exists(Fun<T, bool> predicate) { return innerlist.Exists(predicate); }

    //
    // 
    //
    // <param name="predicate"></param>
    // <param name="item"></param>
    // <returns></returns>
    public bool Find(Fun<T, bool> predicate, out T item) { return innerlist.Find(predicate, out item); }

    //
    // 
    //
    // <param name="predicate"></param>
    // <returns></returns>
    public bool All(Fun<T, bool> predicate) { return innerlist.All(predicate); }

    //
    // 
    //
    // <returns></returns>
    public T Choose() { return innerlist.Choose(); }

    //
    // 
    //
    // <param name="filter"></param>
    // <returns></returns>
    public SCG.IEnumerable<T> Filter(Fun<T, bool> filter) { return innerlist.Filter(filter); }

    #endregion

    #region IEnumerable<T> Members

    //
    // 
    //
    // <returns></returns>
    public SCG.IEnumerator<T> GetEnumerator() { return innerlist.GetEnumerator(); }
    #endregion

    #region IShowable Members

    //
    // 
    //
    // <param name="stringbuilder"></param>
    // <param name="rest"></param>
    // <param name="formatProvider"></param>
    // <returns></returns>
    public bool Show(StringBuilder stringbuilder, ref int rest, IFormatProvider formatProvider)
    { return innerlist.Show(stringbuilder, ref  rest, formatProvider); }

    #endregion

    #region IFormattable Members

    //
    // 
    //
    // <returns></returns>
    public override string ToString() { return innerlist.ToString(); }


    //
    // 
    //
    // <param name="format"></param>
    // <param name="formatProvider"></param>
    // <returns></returns>
    public virtual string ToString(string format, IFormatProvider formatProvider) { return innerlist.ToString(format, formatProvider); }

    #endregion

    #region IDirectedCollectionValue<T> Members

    //
    // 
    //
    // <returns></returns>
    public IDirectedCollectionValue<T> Backwards() { return innerlist.Backwards(); }

    //
    // 
    //
    // <param name="predicate"></param>
    // <param name="item"></param>
    // <returns></returns>
    public bool FindLast(Fun<T, bool> predicate, out T item) { return innerlist.FindLast(predicate, out item); }

    #endregion

    #region IDirectedEnumerable<T> Members

    IDirectedEnumerable<T> IDirectedEnumerable<T>.Backwards() { return Backwards(); }

    //
    // 
    //
    // <value></value>
    public EnumerationDirection Direction { get { return EnumerationDirection.Forwards; } }

    #endregion

    #region IDisposable Members

    //
    // Dispose this if a view else operation is illegal 
    //
    // <exception cref="FixedSizeCollectionException">If not a view</exception>
    public void Dispose()
    {
      if (underlying == null)
        throw new FixedSizeCollectionException();
      else
        innerlist.Dispose();
    }

    #endregion

    #region ICloneable Members

    //
    // Make a shallow copy of this WrappedArray.
    // 
    // 
    //
    // <returns></returns>
    public virtual object Clone()
    {
      return new WrappedArray<T>(innerlist.ToArray());
    }

    #endregion

    #region System.Collections.Generic.IList<T> Members

    void System.Collections.Generic.IList<T>.RemoveAt(int index)
    {
      throw new FixedSizeCollectionException();
    }

    void System.Collections.Generic.ICollection<T>.Add(T item)
    {
      throw new FixedSizeCollectionException();
    }

    #endregion

    #region System.Collections.ICollection Members

    bool System.Collections.ICollection.IsSynchronized
    {
      get { return false; }
    }

    [Obsolete]
    Object System.Collections.ICollection.SyncRoot
    {
      get { return ((System.Collections.IList)innerlist).SyncRoot; }
    }

    void System.Collections.ICollection.CopyTo(Array arr, int index)
    {
      if (index < 0 || index + Count > arr.Length)
        throw new ArgumentOutOfRangeException();

      foreach (T item in this)
        arr.SetValue(item, index++);
    }
    
    #endregion

    #region System.Collections.IList Members

    Object System.Collections.IList.this[int index]
    {
      get { return this[index]; }
      set { this[index] = (T)value; }
    }

    int System.Collections.IList.Add(Object o)
    {
      bool added = Add((T)o);
      // What position to report if item not added? SC.IList.Add doesn't say
      return added ? Count - 1 : -1;
    }

    bool System.Collections.IList.Contains(Object o)
    {
      return Contains((T)o);
    }

    int System.Collections.IList.IndexOf(Object o)
    {
      return Math.Max(-1, IndexOf((T)o));
    }

    void System.Collections.IList.Insert(int index, Object o)
    {
      Insert(index, (T)o);
    }

    void System.Collections.IList.Remove(Object o)
    {
      Remove((T)o);
    }

    void System.Collections.IList.RemoveAt(int index)
    {
      RemoveAt(index);
    }

    #endregion
    
    #region IEnumerable Members

    System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
    {
      throw new Exception("The method or operation is not implemented.");
    }

    #endregion
  }
}