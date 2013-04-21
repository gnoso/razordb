using RazorDB.C5;
using System;
using System.Reflection;
using System.Reflection.Emit;
using System.Diagnostics;
using SCG = System.Collections.Generic;

namespace RazorDB.C5
{
  //
  // A default item comparer for an item type that is either generic (IComparable&lt;T&gt;)
  // or ordinarily (System.IComparable) comparable.
  //
  public static class Comparer<T>
  {
    readonly static Type naturalComparerO = typeof(NaturalComparerO<>);

    readonly static Type naturalComparer = typeof(NaturalComparer<>);

    static SCG.IComparer<T> cachedComparer = null;

    //
    // Create a default comparer. 
    // <para>The IComparer[T] object is constructed when this class is initialised, i.e. 
    // its static constructors called. Thus, the property will be the same object 
    // for the duration of an invocation of the runtime, but a value serialized in 
    // another invocation and deserialized here will not be the same object.</para>
    //
    // <exception cref="NotComparableException">If T is not comparable</exception>
    // <value>The comparer</value>
    [Tested]
    public static SCG.IComparer<T> Default
    {
      get
      {
        if (cachedComparer != null)
          return cachedComparer;

        Type t = typeof(T);

        if (t.IsValueType)
        {
          if (t.Equals(typeof(char)))
            return cachedComparer = (SCG.IComparer<T>)(new CharComparer());

          if (t.Equals(typeof(sbyte)))
            return cachedComparer = (SCG.IComparer<T>)(new SByteComparer());

          if (t.Equals(typeof(byte)))
            return cachedComparer = (SCG.IComparer<T>)(new ByteComparer());

          if (t.Equals(typeof(short)))
            return cachedComparer = (SCG.IComparer<T>)(new ShortComparer());

          if (t.Equals(typeof(ushort)))
            return cachedComparer = (SCG.IComparer<T>)(new UShortComparer());

          if (t.Equals(typeof(int)))
            return cachedComparer = (SCG.IComparer<T>)(new IntComparer());

          if (t.Equals(typeof(uint)))
            return cachedComparer = (SCG.IComparer<T>)(new UIntComparer());

          if (t.Equals(typeof(long)))
            return cachedComparer = (SCG.IComparer<T>)(new LongComparer());

          if (t.Equals(typeof(ulong)))
            return cachedComparer = (SCG.IComparer<T>)(new ULongComparer());

          if (t.Equals(typeof(float)))
            return cachedComparer = (SCG.IComparer<T>)(new FloatComparer());

          if (t.Equals(typeof(double)))
            return cachedComparer = (SCG.IComparer<T>)(new DoubleComparer());

          if (t.Equals(typeof(decimal)))
            return cachedComparer = (SCG.IComparer<T>)(new DecimalComparer());
        }

        if (typeof(IComparable<T>).IsAssignableFrom(t))
        {
          Type c = naturalComparer.MakeGenericType(new Type[] { t });

          return cachedComparer = (SCG.IComparer<T>)(c.GetConstructor(System.Type.EmptyTypes).Invoke(null));
        }

        if (t.GetInterface("System.IComparable") != null)
        {
          Type c = naturalComparerO.MakeGenericType(new Type[] { t });

          return cachedComparer = (SCG.IComparer<T>)(c.GetConstructor(System.Type.EmptyTypes).Invoke(null));
        }

        throw new NotComparableException(String.Format("Cannot make comparer for type {0}", t));
      }
    }
  }

  //
  // A natural generic IComparer for an IComparable&lt;T&gt; item type
  //
  // <typeparam name="T"></typeparam>
  [Serializable]
  public class NaturalComparer<T> : SCG.IComparer<T>
      where T : IComparable<T>
  {
    //
    // Compare two items
    //
    // <param name="item1">First item</param>
    // <param name="item2">Second item</param>
    // <returns>item1 &lt;=&gt; item2</returns>
    [Tested]
    public int Compare(T item1, T item2) { return item1 != null ? item1.CompareTo(item2) : item2 != null ? -1 : 0; }
  }

  //
  // A natural generic IComparer for a System.IComparable item type
  //
  // <typeparam name="T"></typeparam>
  [Serializable]
  public class NaturalComparerO<T> : SCG.IComparer<T> where T : System.IComparable
  {
    //
    // Compare two items
    //
    // <param name="item1">First item</param>
    // <param name="item2">Second item</param>
    // <returns>item1 &lt;=&gt; item2</returns>
    [Tested]
    public int Compare(T item1, T item2) { return item1 != null ? item1.CompareTo(item2) : item2 != null ? -1 : 0; }
  }

  //
  // A generic comparer for type T based on a Comparison[T] delegate
  //
  // <typeparam name="T"></typeparam>
  [Serializable]
  public class DelegateComparer<T> : SCG.IComparer<T>
  {
    readonly Comparison<T> cmp;
    //
    // 
    //
    // <param name="comparison"></param>
    public DelegateComparer(Comparison<T> comparison)
    {
      if (comparison == null)
        throw new NullReferenceException("Comparison cannot be null");
      cmp = comparison;
    }
    //
    // 
    //
    // <param name="item1">First item</param>
    // <param name="item2">Second item</param>
    // <returns>item1 &lt;=&gt; item2</returns>
    public int Compare(T item1, T item2) { return cmp(item1, item2); }
  }
}