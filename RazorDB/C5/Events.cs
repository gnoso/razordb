using System;
using System.Diagnostics;
using SCG = System.Collections.Generic;

namespace RazorDB.C5
{
  [Flags]
  public enum EventTypeEnum
  {
    None = 0x00000000,
    Changed = 0x00000001,
    Cleared = 0x00000002,
    Added = 0x00000004,
    Removed = 0x00000008,
    Basic = 0x0000000f,
    Inserted = 0x00000010,
    RemovedAt = 0x00000020,
    All = 0x0000003f
  }

  // Holds the real events for a collection
  // <typeparam name="T"></typeparam>
  [Serializable]
  internal sealed class EventBlock<T>
  {
    internal EventTypeEnum events;

    event CollectionChangedHandler<T> collectionChanged;
    internal event CollectionChangedHandler<T> CollectionChanged
    {
      add
      {
        collectionChanged += value;
        events |= EventTypeEnum.Changed;
      }
      remove
      {
        collectionChanged -= value;
        if (collectionChanged == null)
          events &= ~EventTypeEnum.Changed;
      }
    }
    internal void raiseCollectionChanged(object sender)
    { if (collectionChanged != null) collectionChanged(sender); }

    event CollectionClearedHandler<T> collectionCleared;
    internal event CollectionClearedHandler<T> CollectionCleared
    {
      add
      {
        collectionCleared += value;
        events |= EventTypeEnum.Cleared;
      }
      remove
      {
        collectionCleared -= value;
        if (collectionCleared == null)
          events &= ~EventTypeEnum.Cleared;
      }
    }
    internal void raiseCollectionCleared(object sender, bool full, int count)
    { if (collectionCleared != null) collectionCleared(sender, new ClearedEventArgs(full, count)); }
    internal void raiseCollectionCleared(object sender, bool full, int count, int? start)
    { if (collectionCleared != null) collectionCleared(sender, new ClearedRangeEventArgs(full, count, start)); }

    event ItemsAddedHandler<T> itemsAdded;
    internal event ItemsAddedHandler<T> ItemsAdded
    {
      add
      {
        itemsAdded += value;
        events |= EventTypeEnum.Added;
      }
      remove
      {
        itemsAdded -= value;
        if (itemsAdded == null)
          events &= ~EventTypeEnum.Added;
      }
    }
    internal void raiseItemsAdded(object sender, T item, int count)
    { if (itemsAdded != null) itemsAdded(sender, new ItemCountEventArgs<T>(item, count)); }

    event ItemsRemovedHandler<T> itemsRemoved;
    internal event ItemsRemovedHandler<T> ItemsRemoved
    {
      add
      {
        itemsRemoved += value;
        events |= EventTypeEnum.Removed;
      }
      remove
      {
        itemsRemoved -= value;
        if (itemsRemoved == null)
          events &= ~EventTypeEnum.Removed;
      }
    }
    internal void raiseItemsRemoved(object sender, T item, int count)
    { if (itemsRemoved != null) itemsRemoved(sender, new ItemCountEventArgs<T>(item, count)); }

    event ItemInsertedHandler<T> itemInserted;
    internal event ItemInsertedHandler<T> ItemInserted
    {
      add
      {
        itemInserted += value;
        events |= EventTypeEnum.Inserted;
      }
      remove
      {
        itemInserted -= value;
        if (itemInserted == null)
          events &= ~EventTypeEnum.Inserted;
      }
    }
    internal void raiseItemInserted(object sender, T item, int index)
    { if (itemInserted != null) itemInserted(sender, new ItemAtEventArgs<T>(item, index)); }

    event ItemRemovedAtHandler<T> itemRemovedAt;
    internal event ItemRemovedAtHandler<T> ItemRemovedAt
    {
      add
      {
        itemRemovedAt += value;
        events |= EventTypeEnum.RemovedAt;
      }
      remove
      {
        itemRemovedAt -= value;
        if (itemRemovedAt == null)
          events &= ~EventTypeEnum.RemovedAt;
      }
    }
    internal void raiseItemRemovedAt(object sender, T item, int index)
    { if (itemRemovedAt != null) itemRemovedAt(sender, new ItemAtEventArgs<T>(item, index)); }
  }

  // Tentative, to conserve memory in GuardedCollectionValueBase
  // This should really be nested in Guarded collection value, only have a guardereal field
  // <typeparam name="T"></typeparam>
  [Serializable]
  internal sealed class ProxyEventBlock<T>
  {
    ICollectionValue<T> proxy, real;

    internal ProxyEventBlock(ICollectionValue<T> p, ICollectionValue<T> r)
    { proxy = p; real = r; }

    event CollectionChangedHandler<T> collectionChanged;
    CollectionChangedHandler<T> collectionChangedProxy;
    internal event CollectionChangedHandler<T> CollectionChanged
    {
      add
      {
        if (collectionChanged == null)
        {
          if (collectionChangedProxy == null)
            collectionChangedProxy = delegate(object sender) { collectionChanged(proxy); };
          real.CollectionChanged += collectionChangedProxy;
        }
        collectionChanged += value;
      }
      remove
      {
        collectionChanged -= value;
        if (collectionChanged == null)
          real.CollectionChanged -= collectionChangedProxy;
      }
    }

    event CollectionClearedHandler<T> collectionCleared;
    CollectionClearedHandler<T> collectionClearedProxy;
    internal event CollectionClearedHandler<T> CollectionCleared
    {
      add
      {
        if (collectionCleared == null)
        {
          if (collectionClearedProxy == null)
            collectionClearedProxy = delegate(object sender, ClearedEventArgs e) { collectionCleared(proxy, e); };
          real.CollectionCleared += collectionClearedProxy;
        }
        collectionCleared += value;
      }
      remove
      {
        collectionCleared -= value;
        if (collectionCleared == null)
          real.CollectionCleared -= collectionClearedProxy;
      }
    }

    event ItemsAddedHandler<T> itemsAdded;
    ItemsAddedHandler<T> itemsAddedProxy;
    internal event ItemsAddedHandler<T> ItemsAdded
    {
      add
      {
        if (itemsAdded == null)
        {
          if (itemsAddedProxy == null)
            itemsAddedProxy = delegate(object sender, ItemCountEventArgs<T> e) { itemsAdded(proxy, e); };
          real.ItemsAdded += itemsAddedProxy;
        }
        itemsAdded += value;
      }
      remove
      {
        itemsAdded -= value;
        if (itemsAdded == null)
          real.ItemsAdded -= itemsAddedProxy;
      }
    }

    event ItemInsertedHandler<T> itemInserted;
    ItemInsertedHandler<T> itemInsertedProxy;
    internal event ItemInsertedHandler<T> ItemInserted
    {
      add
      {
        if (itemInserted == null)
        {
          if (itemInsertedProxy == null)
            itemInsertedProxy = delegate(object sender, ItemAtEventArgs<T> e) { itemInserted(proxy, e); };
          real.ItemInserted += itemInsertedProxy;
        }
        itemInserted += value;
      }
      remove
      {
        itemInserted -= value;
        if (itemInserted == null)
          real.ItemInserted -= itemInsertedProxy;
      }
    }

    event ItemsRemovedHandler<T> itemsRemoved;
    ItemsRemovedHandler<T> itemsRemovedProxy;
    internal event ItemsRemovedHandler<T> ItemsRemoved
    {
      add
      {
        if (itemsRemoved == null)
        {
          if (itemsRemovedProxy == null)
            itemsRemovedProxy = delegate(object sender, ItemCountEventArgs<T> e) { itemsRemoved(proxy, e); };
          real.ItemsRemoved += itemsRemovedProxy;
        }
        itemsRemoved += value;
      }
      remove
      {
        itemsRemoved -= value;
        if (itemsRemoved == null)
          real.ItemsRemoved -= itemsRemovedProxy;
      }
    }

    event ItemRemovedAtHandler<T> itemRemovedAt;
    ItemRemovedAtHandler<T> itemRemovedAtProxy;
    internal event ItemRemovedAtHandler<T> ItemRemovedAt
    {
      add
      {
        if (itemRemovedAt == null)
        {
          if (itemRemovedAtProxy == null)
            itemRemovedAtProxy = delegate(object sender, ItemAtEventArgs<T> e) { itemRemovedAt(proxy, e); };
          real.ItemRemovedAt += itemRemovedAtProxy;
        }
        itemRemovedAt += value;
      }
      remove
      {
        itemRemovedAt -= value;
        if (itemRemovedAt == null)
          real.ItemRemovedAt -= itemRemovedAtProxy;
      }
    }
  }

  // <typeparam name="T"></typeparam>
  public class ItemAtEventArgs<T> : EventArgs
  {
    public readonly T Item;
    public readonly int Index;
    // <param name="item"></param>
    // <param name="index"></param>
    public ItemAtEventArgs(T item, int index) { Item = item; Index = index; }
    public override string ToString()
    {
      return String.Format("(ItemAtEventArgs {0} '{1}')", Index, Item);
    }
  }

  public class ItemCountEventArgs<T> : EventArgs
  {
    public readonly T Item;
    public readonly int Count;
    public ItemCountEventArgs(T item, int count) { Item = item; Count = count; }
    public override string ToString()
    {
      return String.Format("(ItemCountEventArgs {0} '{1}')", Count, Item);
    }
  }

  public class ClearedEventArgs : EventArgs
  {
    public readonly bool Full;
    public readonly int Count;
    // <param name="full">True if the operation cleared all of the collection</param>
    // <param name="count">The number of items removed by the clear.</param>
    public ClearedEventArgs(bool full, int count) { Full = full; Count = count; }
    public override string ToString()
    {
      return String.Format("(ClearedEventArgs {0} {1})", Count, Full);
    }
  }

  public class ClearedRangeEventArgs : ClearedEventArgs
  {
    // could we let this be of type int to allow?
    public readonly int? Start;
    // <param name="full"></param>
    // <param name="count"></param>
    // <param name="start"></param>
    public ClearedRangeEventArgs(bool full, int count, int? start) : base(full,count) { Start = start; }
    public override string ToString()
    {
      return String.Format("(ClearedRangeEventArgs {0} {1} {2})", Count, Full, Start);
    }
  }

  // The type of event raised after an operation on a collection has changed its contents.
  // Normally, a multioperation like AddAll, 
  // <see cref="M:C5.IExtensible`1.AddAll(System.Collections.Generic.IEnumerable{`0})"/> 
  // will only fire one CollectionChanged event. Any operation that changes the collection
  // must fire CollectionChanged as its last event.
  public delegate void CollectionChangedHandler<T>(object sender);

  // The type of event raised after the Clear() operation on a collection.
  // Note: The Clear() operation will not fire ItemsRemoved events.
  // <param name="sender"></param>
  // <param name="eventArgs"></param>
  public delegate void CollectionClearedHandler<T>(object sender, ClearedEventArgs eventArgs);

  // The type of event raised after an item has been added to a collection.
  // The event will be raised at a point of time, where the collection object is 
  // in an internally consistent state and before the corresponding CollectionChanged 
  // event is raised.
  // Note: an Update operation will fire an ItemsRemoved and an ItemsAdded event.
  // Note: When an item is inserted into a list (<see cref="T:C5.IList`1"/>), both
  // ItemInserted and ItemsAdded events will be fired.
  // <param name="sender"></param>
  // <param name="eventArgs">An object with the item that was added</param>
  public delegate void ItemsAddedHandler<T>(object sender, ItemCountEventArgs<T> eventArgs);

  // The type of event raised after an item has been removed from a collection.
  // The event will be raised at a point of time, where the collection object is 
  // in an internally consistent state and before the corresponding CollectionChanged 
  // event is raised.
  // Note: The Clear() operation will not fire ItemsRemoved events.
  // Note: an Update operation will fire an ItemsRemoved and an ItemsAdded event.
  // Note: When an item is removed from a list by the RemoveAt operation, both an 
  // ItemsRemoved and an ItemRemovedAt event will be fired.
  // <param name="sender"></param>
  // <param name="eventArgs">An object with the item that was removed</param>
  public delegate void ItemsRemovedHandler<T>(object sender, ItemCountEventArgs<T> eventArgs);

  // The type of event raised after an item has been inserted into a list by an Insert, 
  // InsertFirst or InsertLast operation.
  // The event will be raised at a point of time, where the collection object is 
  // in an internally consistent state and before the corresponding CollectionChanged 
  // event is raised.
  // Note: an ItemsAdded event will also be fired.
  // <param name="sender"></param>
  // <param name="eventArgs"></param>
  public delegate void ItemInsertedHandler<T>(object sender, ItemAtEventArgs<T> eventArgs);

  // The type of event raised after an item has been removed from a list by a RemoveAt(int i)
  // operation (or RemoveFirst(), RemoveLast(), Remove() operation).
  // The event will be raised at a point of time, where the collection object is 
  // in an internally consistent state and before the corresponding CollectionChanged 
  // event is raised.
  // Note: an ItemRemoved event will also be fired.
  // <param name="sender"></param>
  // <param name="eventArgs"></param>
  public delegate void ItemRemovedAtHandler<T>(object sender, ItemAtEventArgs<T> eventArgs);
}