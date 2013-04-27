/*
 Copyright (c) 2003-2006 Niels Kokholm and Peter Sestoft
 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:
 
 The above copyright notice and this permission notice shall be included in
 all copies or substantial portions of the Software.
 
 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 SOFTWARE.
*/
using System;
using SCG = System.Collections.Generic;

namespace RazorDB.C5
{
	/// <summary>
	/// A list collection based on a plain dynamic array data structure.
	/// Expansion of the internal array is performed by doubling on demand. 
	/// The internal array is only shrinked by the Clear method. 
	///
	/// <i>When the FIFO property is set to false this class works fine as a stack of T.
	/// When the FIFO property is set to true the class will function as a (FIFO) queue
	/// but very inefficiently, use a LinkedList (<see cref="T:C5.LinkedList`1"/>) instead.</i>
	/// </summary>
	public class ArrayLogger
	{
		private static Action<string> _log;
		
		/// <summary>
		/// Gets or sets the log.
		/// </summary>
		/// <example>The following is an example of assigning a observer to the logging module:
		///   <code>
		///     Logger.Log = x => Console.WriteLine(x);
		///   </code>
		/// </example>
		/// <remarks>
		/// If Log is not set it will return a dummy action
		/// <c>x => { return; })</c>
		/// eliminating the need for null-reference checks.
		/// </remarks>
		/// <value>
		/// The log.
		/// </value>
		public static Action<string> Log
		{
			get { return _log ?? (x => { return; }); }
			set { _log = value; }
		}
	}
}