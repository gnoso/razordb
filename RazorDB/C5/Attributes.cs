using System;
using System.Diagnostics;
using SCG = System.Collections.Generic;

namespace RazorDB.C5
{
  /// <summary>
  /// A custom attribute to mark methods and properties as being tested 
  /// sufficiently in the regression test suite.
  /// </summary>
  [AttributeUsage(AttributeTargets.All, AllowMultiple = true)]
  public sealed class TestedAttribute : Attribute
  {

    /// <summary>
    /// Optional reference to test case
    /// </summary>
    [Tested]
    public string via;


    /// <summary>
    /// Pretty print attribute value
    /// </summary>
    /// <returns>"Tested via " + via</returns>
    [Tested]
    public override string ToString() { return "Tested via " + via; }
  }
}