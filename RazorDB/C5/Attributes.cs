using System;
using System.Diagnostics;
using SCG = System.Collections.Generic;

namespace RazorDB.C5
{
  // A custom attribute to mark methods and properties as being tested 
  // sufficiently in the regression test suite.
  [AttributeUsage(AttributeTargets.All, AllowMultiple = true)]
  public sealed class TestedAttribute : Attribute
  {
    // Optional reference to test case
    [Tested]
    public string via;
    // Pretty print attribute value
    // <returns>"Tested via " + via</returns>
    [Tested]
    public override string ToString() { return "Tested via " + via; }
  }
}