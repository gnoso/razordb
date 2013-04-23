using RazorDB.C5;
using System;
using System.Reflection;
using System.Reflection.Emit;
using System.Diagnostics;

namespace RazorDB.C5
{
  struct RecConst
  {
    public const int HASHFACTOR = 387281;
  }
  //
  // A generic record type with two fields. 
  // <para>
  // Equality is defined field by field, using the <code>Equals</code> method 
  // inherited from <code>System.Object</code> (i.e. using <see cref="T:C5.NaturalEqualityComparer`1"/>).
  // </para>
  // <para>
  // This type is similar to <see cref="T:C5.KeyValuePair`2"/>, but the latter
  // uses <see cref="P:C5.EqualityComparer`1.Default"/> to define field equality instead of <see cref="T:C5.NaturalEqualityComparer`1"/>.
  // </para>
  //
  // <typeparam name="T1"></typeparam>
  // <typeparam name="T2"></typeparam>
  public struct Rec<T1, T2> : IEquatable<Rec<T1, T2>>, IShowable
  {
    //
    // 
    //
    public readonly T1 X1;
    //
    // 
    //
    public readonly T2 X2;

    //
    // 
    //
    // <param name="x1"></param>
    // <param name="x2"></param>
    [Tested]
    public Rec(T1 x1, T2 x2)
    {
      X1 = x1; X2 = x2;
    }

    //
    // 
    //
    // <param name="other"></param>
    // <returns></returns>
    [Tested]
    public bool Equals(Rec<T1, T2> other)
    {
      return
        (X1 == null ? other.X1 == null : X1.Equals(other.X1)) &&
        (X2 == null ? other.X2 == null : X2.Equals(other.X2))
        ;
    }
    //
    // 
    //
    // <param name="obj"></param>
    // <returns></returns>
    [Tested]
    public override bool Equals(object obj)
    {
      return obj is Rec<T1, T2> ? Equals((Rec<T1, T2>)obj) : false;
    }
    //
    // 
    //
    // <param name="record1"></param>
    // <param name="record2"></param>
    // <returns></returns>
    [Tested]
    public static bool operator ==(Rec<T1, T2> record1, Rec<T1, T2> record2)
    {
      return record1.Equals(record2);
    }
    //
    // 
    //
    // <param name="record1"></param>
    // <param name="record2"></param>
    // <returns></returns>
    [Tested]
    public static bool operator !=(Rec<T1, T2> record1, Rec<T1, T2> record2)
    {
      return !record1.Equals(record2);
    }
    //
    // 
    //
    // <returns></returns>
    [Tested]
    public override int GetHashCode()
    {
      //TODO: don't use 0 as hashcode for null, but something else!
      int hashcode = X1 == null ? 0 : X1.GetHashCode();
      hashcode = hashcode * RecConst.HASHFACTOR + (X2 == null ? 0 : X2.GetHashCode());
      return hashcode;
    }

    //
    // 
    //
    // <returns></returns>
    public override string ToString()
    {
      return String.Format("({0}, {1})", X1, X2);
    }

    #region IShowable Members

    //
    // 
    //
    // <param name="stringbuilder"></param>
    // <param name="rest"></param>
    // <param name="formatProvider"></param>
    // <returns></returns>
    public bool Show(System.Text.StringBuilder stringbuilder, ref int rest, IFormatProvider formatProvider)
    {
      bool incomplete = true;
      stringbuilder.Append("(");
      rest -= 2;
      try
      {
        if (incomplete = !Showing.Show(X1, stringbuilder, ref rest, formatProvider))
          return false;
        stringbuilder.Append(", ");
        rest -= 2;
        if (incomplete = !Showing.Show(X2, stringbuilder, ref rest, formatProvider))
          return false;
      }
      finally
      {
        if (incomplete)
        {
          stringbuilder.Append("...");
          rest -= 3;
        }
        stringbuilder.Append(")");
      }
      return true;
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
  // 
  //
  // <typeparam name="T1"></typeparam>
  // <typeparam name="T2"></typeparam>
  // <typeparam name="T3"></typeparam>
  public struct Rec<T1, T2, T3> : IEquatable<Rec<T1, T2, T3>>, IShowable
  {
    //
    // 
    //
    public readonly T1 X1;
    //
    // 
    //
    public readonly T2 X2;
    //
    // 
    //
    public readonly T3 X3;
    //
    // 
    //
    // <param name="x1"></param>
    // <param name="x2"></param>
    // <param name="x3"></param>
    [Tested]
    public Rec(T1 x1, T2 x2, T3 x3)
    {
      X1 = x1; X2 = x2; X3 = x3;
    }
    //
    // 
    //
    // <param name="other"></param>
    // <returns></returns>
    [Tested]
    public bool Equals(Rec<T1, T2, T3> other)
    {
      return
        (X1 == null ? other.X1 == null : X1.Equals(other.X1)) &&
        (X2 == null ? other.X2 == null : X2.Equals(other.X2)) &&
        (X3 == null ? other.X3 == null : X3.Equals(other.X3))
        ;
    }
    //
    // 
    //
    // <param name="obj"></param>
    // <returns></returns>
    [Tested]
    public override bool Equals(object obj)
    {
      return obj is Rec<T1, T2, T3> ? Equals((Rec<T1, T2, T3>)obj) : false;
    }
    //
    // 
    //
    // <param name="record1"></param>
    // <param name="record2"></param>
    // <returns></returns>
    [Tested]
    public static bool operator ==(Rec<T1, T2, T3> record1, Rec<T1, T2, T3> record2)
    {
      return record1.Equals(record2);
    }
    //
    // 
    //
    // <param name="record1"></param>
    // <param name="record2"></param>
    // <returns></returns>
    [Tested]
    public static bool operator !=(Rec<T1, T2, T3> record1, Rec<T1, T2, T3> record2)
    {
      return !record1.Equals(record2);
    }
    //
    // 
    //
    // <returns></returns>
    [Tested]
    public override int GetHashCode()
    {
      //TODO: don't use 0 as hashcode for null, but something else!
      int hashcode = X1 == null ? 0 : X1.GetHashCode();
      hashcode = hashcode * RecConst.HASHFACTOR + (X2 == null ? 0 : X2.GetHashCode());
      hashcode = hashcode * RecConst.HASHFACTOR + (X3 == null ? 0 : X3.GetHashCode());
      return hashcode;
    }

    //
    // 
    //
    // <returns></returns>
    public override string ToString()
    {
      return String.Format("({0}, {1}, {2})", X1, X2, X3);
    }
    #region IShowable Members

    //
    // 
    //
    // <param name="stringbuilder"></param>
    // <param name="rest"></param>
    // <param name="formatProvider"></param>
    // <returns></returns>
    public bool Show(System.Text.StringBuilder stringbuilder, ref int rest, IFormatProvider formatProvider)
    {
      bool incomplete = true;
      stringbuilder.Append("(");
      rest -= 2;
      try
      {
        if (incomplete = !Showing.Show(X1, stringbuilder, ref rest, formatProvider))
          return false;
        stringbuilder.Append(", ");
        rest -= 2;
        if (incomplete = !Showing.Show(X2, stringbuilder, ref rest, formatProvider))
          return false;
        stringbuilder.Append(", ");
        rest -= 2;
        if (incomplete = !Showing.Show(X3, stringbuilder, ref rest, formatProvider))
          return false;
      }
      finally
      {
        if (incomplete)
        {
          stringbuilder.Append("...");
          rest -= 3;
        }
        stringbuilder.Append(")");
      }
      return true;
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
  // 
  //
  // <typeparam name="T1"></typeparam>
  // <typeparam name="T2"></typeparam>
  // <typeparam name="T3"></typeparam>
  // <typeparam name="T4"></typeparam>
  public struct Rec<T1, T2, T3, T4> : IEquatable<Rec<T1, T2, T3, T4>>, IShowable
  {
    //
    // 
    //
    public readonly T1 X1;
    //
    // 
    //
    public readonly T2 X2;
    //
    // 
    //
    public readonly T3 X3;
    //
    // 
    //
    public readonly T4 X4;
    //
    // 
    //
    // <param name="x1"></param>
    // <param name="x2"></param>
    // <param name="x3"></param>
    // <param name="x4"></param>
    [Tested]
    public Rec(T1 x1, T2 x2, T3 x3, T4 x4)
    {
      X1 = x1; X2 = x2; X3 = x3; X4 = x4;
    }
    //
    // 
    //
    // <param name="other"></param>
    // <returns></returns>
    [Tested]
    public bool Equals(Rec<T1, T2, T3, T4> other)
    {
      return
        (X1 == null ? other.X1 == null : X1.Equals(other.X1)) &&
        (X2 == null ? other.X2 == null : X2.Equals(other.X2)) &&
        (X3 == null ? other.X3 == null : X3.Equals(other.X3)) &&
        (X4 == null ? other.X4 == null : X4.Equals(other.X4))
        ;
    }
    //
    // 
    //
    // <param name="obj"></param>
    // <returns></returns>
    [Tested]
    public override bool Equals(object obj)
    {
      return obj is Rec<T1, T2, T3, T4> ? Equals((Rec<T1, T2, T3, T4>)obj) : false;
    }

    //
    // 
    //
    // <param name="record1"></param>
    // <param name="record2"></param>
    // <returns></returns>
    [Tested]
    public static bool operator ==(Rec<T1, T2, T3, T4> record1, Rec<T1, T2, T3, T4> record2)
    {
      return record1.Equals(record2);
    }
    //
    // 
    //
    // <param name="record1"></param>
    // <param name="record2"></param>
    // <returns></returns>
    [Tested]
    public static bool operator !=(Rec<T1, T2, T3, T4> record1, Rec<T1, T2, T3, T4> record2)
    {
      return !record1.Equals(record2);
    }

    //
    // 
    //
    // <returns></returns>
    [Tested]
    public override int GetHashCode()
    {
      //TODO: don't use 0 as hashcode for null, but something else!
      int hashcode = X1 == null ? 0 : X1.GetHashCode();
      hashcode = hashcode * RecConst.HASHFACTOR + (X2 == null ? 0 : X2.GetHashCode());
      hashcode = hashcode * RecConst.HASHFACTOR + (X3 == null ? 0 : X3.GetHashCode());
      hashcode = hashcode * RecConst.HASHFACTOR + (X4 == null ? 0 : X4.GetHashCode());
      return hashcode;
    }

    //
    // 
    //
    // <returns></returns>
    public override string ToString()
    {
      return String.Format("({0}, {1}, {2}, {3})", X1, X2, X3, X4);
    }
    #region IShowable Members

    //
    // 
    //
    // <param name="stringbuilder"></param>
    // <param name="rest"></param>
    // <param name="formatProvider"></param>
    // <returns></returns>
    public bool Show(System.Text.StringBuilder stringbuilder, ref int rest, IFormatProvider formatProvider)
    {
      bool incomplete = true;
      stringbuilder.Append("(");
      rest -= 2;
      try
      {
        if (incomplete = !Showing.Show(X1, stringbuilder, ref rest, formatProvider))
          return false;
        stringbuilder.Append(", ");
        rest -= 2;
        if (incomplete = !Showing.Show(X2, stringbuilder, ref rest, formatProvider))
          return false;
        stringbuilder.Append(", ");
        rest -= 2;
        if (incomplete = !Showing.Show(X3, stringbuilder, ref rest, formatProvider))
          return false;
        stringbuilder.Append(", ");
        rest -= 2;
        if (incomplete = !Showing.Show(X4, stringbuilder, ref rest, formatProvider))
          return false;
      }
      finally
      {
        if (incomplete)
        {
          stringbuilder.Append("...");
          rest -= 3;
        }
        stringbuilder.Append(")");
      }
      return true;
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
}
