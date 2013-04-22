using RazorDB.C5;
using System;
using System.Reflection;
using System.Reflection.Emit;
using System.Diagnostics;
using SCG = System.Collections.Generic;

namespace RazorDB.C5
{
  // Utility class for building default generic equalityComparers.
  // <typeparam name="T"></typeparam>
  public static class EqualityComparer<T>
  {
    readonly static Type isequenced = typeof(ISequenced<>);

    readonly static Type icollection = typeof(ICollection<>);

    readonly static Type orderedcollectionequalityComparer = typeof(SequencedCollectionEqualityComparer<,>);

    readonly static Type unorderedcollectionequalityComparer = typeof(UnsequencedCollectionEqualityComparer<,>);

    readonly static Type equalityequalityComparer = typeof(EquatableEqualityComparer<>);

    readonly static Type iequalitytype = typeof(IEquatable<T>);

    static SCG.IEqualityComparer<T> cachedDefault = null;

    //TODO: find the right word for initialized+invocation
    // A default generic equality comparer for type T. The procedure is as follows:
    // <item>If T is a primitive type (char, sbyte, byte, short, ushort, int, uint, float, double, decimal), 
    // the equalityComparer will be a standard equalityComparer for that type</item>
    // <item>If the actual generic argument T implements the generic interface
    // <see cref="T:C5.ISequenced`1"/> for some value W of its generic parameter T,
    // the equalityComparer will be <see cref="T:C5.SequencedCollectionEqualityComparer`2"/></item>
    // <item>If the actual generic argument T implements 
    // <see cref="T:C5.ICollection`1"/> for some value W of its generic parameter T,
    // the equalityComparer will be <see cref="T:C5.UnsequencedCollectionEqualityComparer`2"/></item>
    // <item>If T is a type implementing <see cref="T:C5.IEquatable`1"/>, the equalityComparer
    // will be <see cref="T:C5.EquatableEqualityComparer`1"/></item>
    // <item>If T is a type not implementing <see cref="T:C5.IEquatable`1"/>, the equalityComparer
    // will be <see cref="T:C5.NaturalEqualityComparer`1"/> </item>
    // The <see cref="T:C5.IEqualityComparer`1"/> object is constructed when this class is initialised, i.e. 
    // its static constructors called. Thus, the property will be the same object 
    // for the duration of an invocation of the runtime, but a value serialized in 
    // another invocation and deserialized here will not be the same object.
    public static SCG.IEqualityComparer<T> Default
    {
      get
      {
        if (cachedDefault != null)
          return cachedDefault;

        Type t = typeof(T);

        if (t.IsValueType)
        {
          if (t.Equals(typeof(char)))
            return cachedDefault = (SCG.IEqualityComparer<T>)(CharEqualityComparer.Default);

          if (t.Equals(typeof(sbyte)))
            return cachedDefault = (SCG.IEqualityComparer<T>)(SByteEqualityComparer.Default);

          if (t.Equals(typeof(byte)))
            return cachedDefault = (SCG.IEqualityComparer<T>)(ByteEqualityComparer.Default);

          if (t.Equals(typeof(short)))
            return cachedDefault = (SCG.IEqualityComparer<T>)(ShortEqualityComparer.Default);

          if (t.Equals(typeof(ushort)))
            return cachedDefault = (SCG.IEqualityComparer<T>)(UShortEqualityComparer.Default);
          
          if (t.Equals(typeof(int)))
            return cachedDefault = (SCG.IEqualityComparer<T>)(IntEqualityComparer.Default);

          if (t.Equals(typeof(uint)))
            return cachedDefault = (SCG.IEqualityComparer<T>)(UIntEqualityComparer.Default);

          if (t.Equals(typeof(long)))
            return cachedDefault = (SCG.IEqualityComparer<T>)(LongEqualityComparer.Default);

          if (t.Equals(typeof(ulong)))
            return cachedDefault = (SCG.IEqualityComparer<T>)(ULongEqualityComparer.Default);

          if (t.Equals(typeof(float)))
            return cachedDefault = (SCG.IEqualityComparer<T>)(FloatEqualityComparer.Default);
          
          if (t.Equals(typeof(double)))
            return cachedDefault = (SCG.IEqualityComparer<T>)(DoubleEqualityComparer.Default);

          if (t.Equals(typeof(decimal)))
            return cachedDefault = (SCG.IEqualityComparer<T>)(DecimalEqualityComparer.Default);
        }
        Type[] interfaces = t.GetInterfaces();
        if (t.IsGenericType && t.GetGenericTypeDefinition().Equals(isequenced))
          return createAndCache(orderedcollectionequalityComparer.MakeGenericType(new Type[] { t, t.GetGenericArguments()[0] }));
        foreach (Type ty in interfaces)
        {
          if (ty.IsGenericType && ty.GetGenericTypeDefinition().Equals(isequenced))
            return createAndCache(orderedcollectionequalityComparer.MakeGenericType(new Type[] { t, ty.GetGenericArguments()[0] }));
        }
        if (t.IsGenericType && t.GetGenericTypeDefinition().Equals(icollection))
          return createAndCache(unorderedcollectionequalityComparer.MakeGenericType(new Type[] { t, t.GetGenericArguments()[0] }));
        foreach (Type ty in interfaces)
        {
          if (ty.IsGenericType && ty.GetGenericTypeDefinition().Equals(icollection))
            return createAndCache(unorderedcollectionequalityComparer.MakeGenericType(new Type[] { t, ty.GetGenericArguments()[0] }));
        }
        if (iequalitytype.IsAssignableFrom(t))
          return createAndCache(equalityequalityComparer.MakeGenericType(new Type[] { t }));
        else
          return cachedDefault = NaturalEqualityComparer<T>.Default;
      }
    }
    static SCG.IEqualityComparer<T> createAndCache(Type equalityComparertype)
    {
      return cachedDefault = (SCG.IEqualityComparer<T>)(equalityComparertype.GetProperty("Default", BindingFlags.Static | BindingFlags.Public).GetValue(null, null));
    }
  }

  // A default item equalityComparer calling through to
  // the GetHashCode and Equals methods inherited from System.Object.
  [Serializable]
  public sealed class NaturalEqualityComparer<T> : SCG.IEqualityComparer<T>
  {
    static NaturalEqualityComparer<T> cached;
    NaturalEqualityComparer() { }
    public static NaturalEqualityComparer<T> Default { get { return cached ?? (cached = new NaturalEqualityComparer<T>()); } }
    //TODO: check if null check is reasonable
    //Answer: if we have struct C<T> { T t; int i;} and implement GetHashCode as
    //the sum of hashcodes, and T may be any type, we cannot make the null check 
    //inside the definition of C<T> in a reasonable way.
    // Get the hash code with respect to this item equalityComparer
    // <param name="item">The item</param>
    // <returns>The hash code</returns>
    [Tested]
    public int GetHashCode(T item) { return item == null ? 0 : item.GetHashCode(); }

    // Check if two items are equal with respect to this item equalityComparer
    // <param name="item1">first item</param>
    // <param name="item2">second item</param>
    // <returns>True if equal</returns>
    [Tested]
    public bool Equals(T item1, T item2)
    {
      return item1 == null ? item2 == null : item1.Equals(item2);
    }
  }

  // A default equality comparer for a type T that implements System.IEquatable<typeparamref name="T"/>. 
  // 
  // The equality comparer forwards calls to GetHashCode and Equals to the IEquatable methods 
  // on T, so Equals(T) is called, not Equals(object). 
  // This will save boxing abd unboxing if T is a value type
  // and in general saves a runtime type check.
  // <typeparam name="T"></typeparam>
  [Serializable]
  public class EquatableEqualityComparer<T> : SCG.IEqualityComparer<T> where T : IEquatable<T>
  {
    static EquatableEqualityComparer<T> cached = new EquatableEqualityComparer<T>();
    EquatableEqualityComparer() { }
    public static EquatableEqualityComparer<T> Default { 
      get { return cached ?? (cached = new EquatableEqualityComparer<T>()); } 
    }
    // <param name="item"></param>
    public int GetHashCode(T item) { return item == null ? 0 : item.GetHashCode(); }

    // <param name="item1"></param>
    // <param name="item2"></param>
    public bool Equals(T item1, T item2) { return item1 == null ? item2 == null : item1.Equals(item2); }
  }

  // A equalityComparer for a reference type that uses reference equality for equality and the hash code from object as hash code.
  // <typeparam name="T">The item type. Must be a reference type.</typeparam>
  [Serializable]
  public class ReferenceEqualityComparer<T> : SCG.IEqualityComparer<T> where T : class
  {
    static ReferenceEqualityComparer<T> cached;
    ReferenceEqualityComparer() { }
    public static ReferenceEqualityComparer<T> Default { 
      get { return cached ?? (cached = new ReferenceEqualityComparer<T>()); } 
    }
    // <param name="item"></param>
    public int GetHashCode(T item)
    {
      return System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(item);
    }

    // <param name="i1"></param>
    // <param name="i2"></param>
    public bool Equals(T i1, T i2)
    {
      return object.ReferenceEquals(i1, i2);
    }
  }

  // An equalityComparer compatible with a given comparer. All hash codes are 0, 
  // meaning that anything based on hash codes will be quite inefficient.
  // <para><b>Note: this will give a new EqualityComparer each time created!</b></para>
  // <typeparam name="T"></typeparam>
  [Serializable]
  public class ComparerZeroHashCodeEqualityComparer<T> : SCG.IEqualityComparer<T>
  {
    SCG.IComparer<T> comparer;
    // Create a trivial <see cref="T:C5.IEqualityComparer`1"/> compatible with the 
    // <see cref="T:C5.IComparer`1"/> <code>comparer</code>
    // <param name="comparer"></param>
    public ComparerZeroHashCodeEqualityComparer(SCG.IComparer<T> c)
    {
      if (comparer == null)
        throw new NullReferenceException("Comparer cannot be null");
      comparer = c;
    }
    // A trivial, inefficient hash fuction. Compatible with any equality relation.
    // <param name="item"></param>
    public int GetHashCode(T item) { return 0; }
    // Equality of two items as defined by the comparer.
    // <param name="item1"></param>
    // <param name="item2"></param>
    public bool Equals(T item1, T item2) { return comparer.Compare(item1, item2) == 0; }
  }

  // Prototype for a sequenced equalityComparer for something (T) that implements ISequenced[W].
  // This will use ISequenced[W] specific implementations of the equality comparer operations.
  // <typeparam name="T"></typeparam>
  // <typeparam name="W"></typeparam>
  [Serializable]
  public class SequencedCollectionEqualityComparer<T, W> : SCG.IEqualityComparer<T>
      where T : ISequenced<W>
  {
    static SequencedCollectionEqualityComparer<T, W> cached;
    SequencedCollectionEqualityComparer() { }
    public static SequencedCollectionEqualityComparer<T, W> Default { 
      get { return cached ?? (cached = new SequencedCollectionEqualityComparer<T, W>()); } 
    }
    // Get the hash code with respect to this sequenced equalityComparer
    // <param name="collection">The collection</param>
    [Tested]
    public int GetHashCode(T collection) { return collection.GetSequencedHashCode(); }

    // Check if two items are equal with respect to this sequenced equalityComparer
    // <param name="collection1">first collection</param>
    // <param name="collection2">second collection</param>
    // <returns>True if equal</returns>
    [Tested]
    public bool Equals(T collection1, T collection2) { return collection1 == null ? collection2 == null : collection1.SequencedEquals(collection2); }
  }

  // Prototype for an unsequenced equalityComparer for something (T) that implements ICollection[W]
  // This will use ICollection[W] specific implementations of the equalityComparer operations
  // <typeparam name="T"></typeparam>
  // <typeparam name="W"></typeparam>
  [Serializable]
  public class UnsequencedCollectionEqualityComparer<T, W> : SCG.IEqualityComparer<T>
      where T : ICollection<W>
  {
    static UnsequencedCollectionEqualityComparer<T, W> cached;
    UnsequencedCollectionEqualityComparer() { }
    public static UnsequencedCollectionEqualityComparer<T, W> Default { get { return cached ?? (cached = new UnsequencedCollectionEqualityComparer<T, W>()); } }
    // Get the hash code with respect to this unsequenced equalityComparer
    // <param name="collection">The collection</param>
    // <returns>The hash code</returns>
    [Tested]
    public int GetHashCode(T collection) { return collection.GetUnsequencedHashCode(); }

    // Check if two collections are equal with respect to this unsequenced equalityComparer
    // <param name="collection1">first collection</param>
    // <param name="collection2">second collection</param>
    // <returns>True if equal</returns>
    [Tested]
    public bool Equals(T collection1, T collection2) { return collection1 == null ? collection2 == null : collection1.UnsequencedEquals(collection2); }
  }

}


#if EXPERIMENTAL
namespace RazorDB.C5.EqualityComparerBuilder
{
  // IEqualityComparer factory class: examines at instatiation time if T is an
  // interface implementing "int GetHashCode()" and "bool Equals(T)".
  // If those are not present, MakeEqualityComparer will return a default equalityComparer,
  // else this class will implement IequalityComparer[T] via Invoke() on the
  // reflected method infos.
  public class ByInvoke<T> : SCG.IEqualityComparer<T>
  {
    internal static readonly System.Reflection.MethodInfo hinfo, einfo;

    static ByInvoke()
    {
      Type t = typeof(T);

      if (!t.IsInterface) return;

      BindingFlags f = BindingFlags.Public | BindingFlags.Instance;

      hinfo = t.GetMethod("GetHashCode", f, null, new Type[0], null);
      einfo = t.GetMethod("Equals", f, null, new Type[1] { t }, null);
    }


    ByInvoke() { }

    public static SCG.IEqualityComparer<T> MakeEqualityComparer()
    {
      if (hinfo != null && einfo != null)
        return new ByInvoke<T>();
      else
        return NaturalEqualityComparer<T>.Default;
    }

// <param name="item"></param>
    public int GetHashCode(T item)
    {
      return (int)(hinfo.Invoke(item, null));
    }

// <param name="i1"></param>
// <param name="i2"></param>
    public bool Equals(T i1, T i2)
    {
      return (bool)(einfo.Invoke(i1, new object[1] { i2 }));
    }
  }

  // Like ByInvoke, but tries to build a equalityComparer by RTCG to
  // avoid the Invoke() overhead.
  public class ByRTCG
  {
    static ModuleBuilder moduleBuilder;

    static AssemblyBuilder assemblyBuilder;

    // <param name="hinfo"></param>
    // <param name="einfo"></param>
    public static SCG.IEqualityComparer<T> CreateEqualityComparer<T>(MethodInfo hinfo, MethodInfo einfo)
    {
      if (moduleBuilder == null)
      {
        string assmname = "LeFake";
        string filename = assmname + ".dll";
        AssemblyName assemblyName = new AssemblyName("LeFake");
        AppDomain appdomain = AppDomain.CurrentDomain;
        AssemblyBuilderAccess acc = AssemblyBuilderAccess.RunAndSave;

        assemblyBuilder = appdomain.DefineDynamicAssembly(assemblyName, acc);
        moduleBuilder = assemblyBuilder.DefineDynamicModule(assmname, filename);
      }

      Type t = typeof(T);
      Type o_t = typeof(object);
      Type h_t = typeof(SCG.IEqualityComparer<T>);
      Type i_t = typeof(int);
      //TODO: protect uid for thread safety!
      string name = "C5.Dynamic.EqualityComparer_" + Guid.NewGuid().ToString();
      TypeAttributes tatt = TypeAttributes.Class | TypeAttributes.Public | TypeAttributes.Sealed;
      TypeBuilder tb = moduleBuilder.DefineType(name, tatt, o_t, new Type[1] { h_t });
      MethodAttributes matt = MethodAttributes.Public | MethodAttributes.Virtual;
      MethodBuilder mb = tb.DefineMethod("GetHashCode", matt, i_t, new Type[1] { t });
      ILGenerator ilg = mb.GetILGenerator();

      ilg.Emit(OpCodes.Ldarg_1);
      ilg.Emit(OpCodes.Callvirt, hinfo);
      ilg.Emit(OpCodes.Ret);
      mb = tb.DefineMethod("Equals", matt, typeof(bool), new Type[2] { t, t });
      ilg = mb.GetILGenerator();
      ilg.Emit(OpCodes.Ldarg_1);
      ilg.Emit(OpCodes.Ldarg_2);
      ilg.Emit(OpCodes.Callvirt, einfo);
      ilg.Emit(OpCodes.Ret);

      Type equalityComparer_t = tb.CreateType();
      object equalityComparer = equalityComparer_t.GetConstructor(new Type[0]).Invoke(null);

      return (SCG.IEqualityComparer<T>)equalityComparer;
    }

// <typeparam name="T"></typeparam>
    public static SCG.IEqualityComparer<T> build<T>()
    {
      MethodInfo hinfo = ByInvoke<T>.hinfo, einfo = ByInvoke<T>.einfo;

      if (hinfo != null && einfo != null)
        return CreateEqualityComparer<T>(hinfo, einfo);
      else
        return EqualityComparer<T>.Default;
    }

    public void dump()
    {
      assemblyBuilder.Save("LeFake.dll");
    }
  }
}
#endif