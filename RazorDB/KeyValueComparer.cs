/*
Copyright 2012, 2013 Gnoso Inc.

This software is licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except for what is in compliance with the License.

You may obtain a copy of this license at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either expressed or implied.

See the License for the specific language governing permissions and limitations.
*/
using System;
using RazorDB.C5;

namespace RazorDB
{
	public class KeyValueComparer<T> : System.Collections.Generic.IComparer<KeyValuePair<T, int>> where T : IComparable<T>
	{
		public KeyValueComparer (IList<T> lst)
		{
		}

		public int Compare (KeyValuePair<T, int> p1, KeyValuePair<T, int> p2)
		{
			return p1.Key.CompareTo (p2.Key);
		}
	}
}
