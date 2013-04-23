using System;
using System.Diagnostics;
using SCG = System.Collections.Generic;
namespace RazorDB.C5
{
  #region char comparer and equality comparer
  class CharComparer : SCG.IComparer<char>
  {
    public int Compare(char item1, char item2) { 
      return item1 > item2 ? 1 : item1 < item2 ? -1 : 0; 
    }
  }

  //
  // An equality comparer for type char, also known as System.Char.
  //
  public class CharEqualityComparer : SCG.IEqualityComparer<char>
  {
    static CharEqualityComparer cached = new CharEqualityComparer();
    CharEqualityComparer() { }
    //
    // 
    //
    // <value></value>
    public static CharEqualityComparer Default { get { return cached ?? (cached = new CharEqualityComparer()); } }

    //
    // Get the hash code of this char
    //
    // <param name="item">The char</param>
    // <returns>The same</returns>
    public int GetHashCode(char item) { return item.GetHashCode(); }


    //
    // Check if two chars are equal
    //
    // <param name="item1">first char</param>
    // <param name="item2">second char</param>
    // <returns>True if equal</returns>
    public bool Equals(char item1, char item2) { return item1 == item2; }
  }
  #endregion
  
  #region sbyte comparer and equality comparer
  [Serializable]
  class SByteComparer : SCG.IComparer<sbyte>
  {
    [Tested]
    public int Compare(sbyte item1, sbyte item2) { 
      return item1 > item2 ? 1 : item1 < item2 ? -1 : 0; 
    }
  }

  //
  // An equality comparer for type sbyte, also known as System.SByte. 
  // <para>This class is a singleton and the instance can be accessed
  // via the static <see cref="P:C5.SByteEqualityComparer.Default"/> property</para>
  //
  [Serializable]
  public class SByteEqualityComparer : SCG.IEqualityComparer<sbyte>
  {
    static SByteEqualityComparer cached;
    SByteEqualityComparer() { }

    //
    // 
    //
    // <value></value>
    [Tested]
    public static SByteEqualityComparer Default { get { return cached ?? (cached = new SByteEqualityComparer()); } }
    //
    // Get the hash code of this sbyte, that is, itself
    //
    // <param name="item">The sbyte</param>
    // <returns>The same</returns>
    [Tested]
    public int GetHashCode(sbyte item) { return item.GetHashCode(); }


    //
    // Determine whether two sbytes are equal
    //
    // <param name="item1">first sbyte</param>
    // <param name="item2">second sbyte</param>
    // <returns>True if equal</returns>
    [Tested]
    public bool Equals(sbyte item1, sbyte item2) { return item1 == item2; }
  }

  #endregion

  #region byte comparer and equality comparer
  class ByteComparer : SCG.IComparer<byte>
  {
    public int Compare(byte item1, byte item2) { 
      return item1 > item2 ? 1 : item1 < item2 ? -1 : 0; 
    }
  }

  //
  // An equality comparer for type byte, also known as System.Byte.
  // <para>This class is a singleton and the instance can be accessed
  // via the <see cref="P:C5.ByteEqualityComparer.Default"/> property</para>
  //
  public class ByteEqualityComparer : SCG.IEqualityComparer<byte>
  {
    static ByteEqualityComparer cached = new ByteEqualityComparer();
    ByteEqualityComparer() { }
    //
    // 
    //
    // <value></value>
    public static ByteEqualityComparer Default { get { return cached ?? (cached = new ByteEqualityComparer()); } }
    //
    // Get the hash code of this byte, i.e. itself
    //
    // <param name="item">The byte</param>
    // <returns>The same</returns>
    public int GetHashCode(byte item) { return item.GetHashCode(); }

    //
    // Check if two bytes are equal
    //
    // <param name="item1">first byte</param>
    // <param name="item2">second byte</param>
    // <returns>True if equal</returns>
    public bool Equals(byte item1, byte item2) { return item1 == item2; }
  }
  #endregion

  #region short comparer and equality comparer
  [Serializable]
  class ShortComparer : SCG.IComparer<short>
  {
    [Tested]
    public int Compare(short item1, short item2) { 
      return item1 > item2 ? 1 : item1 < item2 ? -1 : 0; 
    }
  }

  //
  // An equality comparer for type short, also known as System.Int16. 
  // <para>This class is a singleton and the instance can be accessed
  // via the static <see cref="P:C5.ShortEqualityComparer.Default"/> property</para>
  //
  [Serializable]
  public class ShortEqualityComparer : SCG.IEqualityComparer<short>
  {
    static ShortEqualityComparer cached;
    ShortEqualityComparer() { }

    //
    // 
    //
    // <value></value>
    [Tested]
    public static ShortEqualityComparer Default { get { return cached ?? (cached = new ShortEqualityComparer()); } }
    //
    // Get the hash code of this short, that is, itself
    //
    // <param name="item">The short</param>
    // <returns>The same</returns>
    [Tested]
    public int GetHashCode(short item) { return item.GetHashCode(); }


    //
    // Determine whether two shorts are equal
    //
    // <param name="item1">first short</param>
    // <param name="item2">second short</param>
    // <returns>True if equal</returns>
    [Tested]
    public bool Equals(short item1, short item2) { return item1 == item2; }
  }

  #endregion

  #region ushort comparer and equality comparer
  [Serializable]
  class UShortComparer : SCG.IComparer<ushort>
  {
    [Tested]
    public int Compare(ushort item1, ushort item2)
    {
      return item1 > item2 ? 1 : item1 < item2 ? -1 : 0;
    }
  }

  //
  // An equality comparer for type ushort, also known as System.UInt16. 
  // <para>This class is a singleton and the instance can be accessed
  // via the static <see cref="P:C5.UShortEqualityComparer.Default"/> property</para>
  //
  [Serializable]
  public class UShortEqualityComparer : SCG.IEqualityComparer<ushort>
  {
    static UShortEqualityComparer cached;
    UShortEqualityComparer() { }

    //
    // 
    //
    // <value></value>
    [Tested]
    public static UShortEqualityComparer Default { get { return cached ?? (cached = new UShortEqualityComparer()); } }
    //
    // Get the hash code of this ushort, that is, itself
    //
    // <param name="item">The ushort</param>
    // <returns>The same</returns>
    [Tested]
    public int GetHashCode(ushort item) { return item.GetHashCode(); }


    //
    // Determine whether two ushorts are equal
    //
    // <param name="item1">first ushort</param>
    // <param name="item2">second ushort</param>
    // <returns>True if equal</returns>
    [Tested]
    public bool Equals(ushort item1, ushort item2) { return item1 == item2; }
  }

  #endregion

  #region int comparer and equality comparer
  [Serializable]
  class IntComparer : SCG.IComparer<int>
  {
    [Tested]
    public int Compare(int item1, int item2) { 
      return item1 > item2 ? 1 : item1 < item2 ? -1 : 0; 
    }
  }

  //
  // An equality comparer for type int, also known as System.Int32. 
  // <para>This class is a singleton and the instance can be accessed
  // via the static <see cref="P:C5.IntEqualityComparer.Default"/> property</para>
  //
  [Serializable]
  public class IntEqualityComparer : SCG.IEqualityComparer<int>
  {
    static IntEqualityComparer cached;
    IntEqualityComparer() { }
    //
    // 
    //
    // <value></value>
    [Tested]
    public static IntEqualityComparer Default { get { return cached ?? (cached = new IntEqualityComparer()); } }
    //
    // Get the hash code of this integer, that is, itself
    //
    // <param name="item">The integer</param>
    // <returns>The same</returns>
    [Tested]
    public int GetHashCode(int item) { return item; }


    //
    // Determine whether two integers are equal
    //
    // <param name="item1">first integer</param>
    // <param name="item2">second integer</param>
    // <returns>True if equal</returns>
    [Tested]
    public bool Equals(int item1, int item2) { return item1 == item2; }
  }

  #endregion

  #region uint comparer and equality comparer
  [Serializable]
  class UIntComparer : SCG.IComparer<uint>
  {
    [Tested]
    public int Compare(uint item1, uint item2)
    {
      return item1 > item2 ? 1 : item1 < item2 ? -1 : 0;
    }
  }

  //
  // An equality comparer for type uint, also known as System.UInt32. 
  // <para>This class is a singleton and the instance can be accessed
  // via the static <see cref="P:C5.UIntEqualityComparer.Default"/> property</para>
  //
  [Serializable]
  public class UIntEqualityComparer : SCG.IEqualityComparer<uint>
  {
    static UIntEqualityComparer cached;
    UIntEqualityComparer() { }
    //
    // 
    //
    // <value></value>
    [Tested]
    public static UIntEqualityComparer Default { get { return cached ?? (cached = new UIntEqualityComparer()); } }
    //
    // Get the hash code of this unsigned integer
    //
    // <param name="item">The integer</param>
    // <returns>The same bit pattern as a signed integer</returns>
    [Tested]
    public int GetHashCode(uint item) { return item.GetHashCode(); }


    //
    // Determine whether two unsigned integers are equal
    //
    // <param name="item1">first unsigned integer</param>
    // <param name="item2">second unsigned integer</param>
    // <returns>True if equal</returns>
    [Tested]
    public bool Equals(uint item1, uint item2) { return item1 == item2; }
  }

  #endregion

  #region long comparer and equality comparer
  [Serializable]
  class LongComparer : SCG.IComparer<long>
  {
    [Tested]
    public int Compare(long item1, long item2)
    {
      return item1 > item2 ? 1 : item1 < item2 ? -1 : 0;
    }
  }

  //
  // An equality comparer for type long, also known as System.Int64. 
  // <para>This class is a singleton and the instance can be accessed
  // via the static <see cref="P:C5.LongEqualityComparer.Default"/> property</para>
  //
  [Serializable]
  public class LongEqualityComparer : SCG.IEqualityComparer<long>
  {
    static LongEqualityComparer cached;
    LongEqualityComparer() { }
    //
    // 
    //
    // <value></value>
    [Tested]
    public static LongEqualityComparer Default { get { return cached ?? (cached = new LongEqualityComparer()); } }
    //
    // Get the hash code of this long integer
    //
    // <param name="item">The long integer</param>
    // <returns>The hash code</returns>
    [Tested]
    public int GetHashCode(long item) { return item.GetHashCode(); }


    //
    // Determine whether two long integers are equal
    //
    // <param name="item1">first long integer</param>
    // <param name="item2">second long integer</param>
    // <returns>True if equal</returns>
    [Tested]
    public bool Equals(long item1, long item2) { return item1 == item2; }
  }

  #endregion

  #region ulong comparer and equality comparer
  [Serializable]
  class ULongComparer : SCG.IComparer<ulong>
  {
    [Tested]
    public int Compare(ulong item1, ulong item2)
    {
      return item1 > item2 ? 1 : item1 < item2 ? -1 : 0;
    }
  }

  //
  // An equality comparer for type uint, also known as System.UInt64. 
  // <para>This class is a singleton and the instance can be accessed
  // via the static <see cref="P:C5.ULongEqualityComparer.Default"/> property</para>
  //
  [Serializable]
  public class ULongEqualityComparer : SCG.IEqualityComparer<ulong>
  {
    static ULongEqualityComparer cached;
    ULongEqualityComparer() { }
    //
    // 
    //
    // <value></value>
    [Tested]
    public static ULongEqualityComparer Default { get { return cached ?? (cached = new ULongEqualityComparer()); } }
    //
    // Get the hash code of this unsigned long integer
    //
    // <param name="item">The unsigned long integer</param>
    // <returns>The hash code</returns>
    [Tested]
    public int GetHashCode(ulong item) { return item.GetHashCode(); }


    //
    // Determine whether two unsigned long integers are equal
    //
    // <param name="item1">first unsigned long integer</param>
    // <param name="item2">second unsigned long integer</param>
    // <returns>True if equal</returns>
    [Tested]
    public bool Equals(ulong item1, ulong item2) { return item1 == item2; }
  }

  #endregion

  #region float comparer and equality comparer
  class FloatComparer : SCG.IComparer<float>
  {
    public int Compare(float item1, float item2)
    {
      return item1 > item2 ? 1 : item1 < item2 ? -1 : 0;
    }
  }

  //
  // An equality comparer for type float, also known as System.Single. 
  // <para>This class is a singleton and the instance can be accessed
  // via the static <see cref="P:C5.FloatEqualityComparer.Default"/> property</para>
  //
  public class FloatEqualityComparer : SCG.IEqualityComparer<float>
  {
    static FloatEqualityComparer cached;
    FloatEqualityComparer() { }
    //
    // 
    //
    // <value></value>
    [Tested]
    public static FloatEqualityComparer Default { get { return cached ?? (cached = new FloatEqualityComparer()); } }
    //
    // Get the hash code of this float
    //
    // <param name="item">The float</param>
    // <returns>The same</returns>
    [Tested]
    public int GetHashCode(float item) { return item.GetHashCode(); }


    //
    // Check if two floats are equal
    //
    // <param name="item1">first float</param>
    // <param name="item2">second float</param>
    // <returns>True if equal</returns>
    [Tested]
    public bool Equals(float item1, float item2) { return item1 == item2; }
  }
  #endregion

  #region double comparer and equality comparer
  class DoubleComparer : SCG.IComparer<double>
  {
    public int Compare(double item1, double item2) { 
      return item1 > item2 ? 1 : item1 < item2 ? -1 : 0; 
    }
  }

  //
  // An equality comparer for type double, also known as System.Double.
  // <para>This class is a singleton and the instance can be accessed
  // via the static <see cref="P:C5.DoubleEqualityComparer.Default"/> property</para>
  //
  public class DoubleEqualityComparer : SCG.IEqualityComparer<double>
  {
    static DoubleEqualityComparer cached;
    DoubleEqualityComparer() { }
    //
    // 
    //
    // <value></value>
    [Tested]
    public static DoubleEqualityComparer Default { get { return cached ?? (cached = new DoubleEqualityComparer()); } }
    //
    // Get the hash code of this double
    //
    // <param name="item">The double</param>
    // <returns>The same</returns>
    [Tested]
    public int GetHashCode(double item) { return item.GetHashCode(); }


    //
    // Check if two doubles are equal
    //
    // <param name="item1">first double</param>
    // <param name="item2">second double</param>
    // <returns>True if equal</returns>
    [Tested]
    public bool Equals(double item1, double item2) { return item1 == item2; }
  }
  #endregion

  #region decimal comparer and equality comparer
  [Serializable]
  class DecimalComparer : SCG.IComparer<decimal>
  {
    [Tested]
    public int Compare(decimal item1, decimal item2)
    {
      return item1 > item2 ? 1 : item1 < item2 ? -1 : 0;
    }
  }

  //
  // An equality comparer for type decimal, also known as System.Decimal. 
  // <para>This class is a singleton and the instance can be accessed
  // via the static <see cref="P:C5.DecimalEqualityComparer.Default"/> property</para>
  //
  [Serializable]
  public class DecimalEqualityComparer : SCG.IEqualityComparer<decimal>
  {
    static DecimalEqualityComparer cached;
    DecimalEqualityComparer() { }
    //
    // 
    //
    // <value></value>
    [Tested]
    public static DecimalEqualityComparer Default { get { return cached ?? (cached = new DecimalEqualityComparer()); } }
    //
    // Get the hash code of this decimal.
    //
    // <param name="item">The decimal</param>
    // <returns>The hash code</returns>
    [Tested]
    public int GetHashCode(decimal item) { return item.GetHashCode(); }


    //
    // Determine whether two decimals are equal
    //
    // <param name="item1">first decimal</param>
    // <param name="item2">second decimal</param>
    // <returns>True if equal</returns>
    [Tested]
    public bool Equals(decimal item1, decimal item2) { return item1 == item2; }
  }

  #endregion
}