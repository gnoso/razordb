/*
Copyright 2012-2015 Gnoso Inc.

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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;

namespace RazorDB {

    // This class takes a collection of IEnumerable<T> objects (assuming in sorted order) and returns 
    // a merged version with the result being in sorted order as well.
    public static class MergeEnumerator {

        public static IEnumerable<T> Merge<T>(IEnumerable<IEnumerable<T>> enumerables) {
            return Merge(enumerables, o => o);
        }

        public static IEnumerable<T> Merge<T, TKey>(IEnumerable<IEnumerable<T>> enumerables, Func<T, TKey> keyExtractor) {

            // Get enumerators for each enumerable
            var enumerators = enumerables.Select(e => e.GetEnumerator()).AsRanked();
            var nonEmptyEnums = new List<Ranked<IEnumerator<T>>>();

            // move ahead and prune out empty enumerators
            foreach (var e in enumerators) {
                if (e.Value.MoveNext()) {
                    nonEmptyEnums.Add(e);
                } else {
                    e.Value.Dispose();
                }
            }

            // Construct the expression to compare the enumerators, taking rank into account
            Comparison<Ranked<IEnumerator<T>>> comparer = (x, y) => {
                int c = Comparer<TKey>.Default.Compare(keyExtractor(x.Value.Current), keyExtractor(y.Value.Current));
                if (c == 0) {
                    // If they are equal, then compare the ranks
                    return x.Rank.CompareTo(y.Rank);
                } else {
                    return c;
                }
            };

            // order them by the first (current) element and put into a linked list
            nonEmptyEnums.Sort(comparer);
            var workingEnums = new LinkedList<Ranked<IEnumerator<T>>>(nonEmptyEnums);

            try {
                int totalEnumerators = workingEnums.Count;
                TKey lastKeyValue = default(TKey);
                while (totalEnumerators > 0) {

                    var firstEnum = workingEnums.First;
                    T yieldValue = firstEnum.Value.Value.Current;

                    // Yield the value if the key isn't the same as the previously yielded value
                    if (Comparer<TKey>.Default.Compare(keyExtractor(yieldValue), lastKeyValue) != 0) {
                        yield return yieldValue;
                    }
                    lastKeyValue = keyExtractor(yieldValue);

                    // advance this enumerator to the next spot
                    if (!firstEnum.Value.Value.MoveNext()) {
                        // ok this enumerator is done, so remove it
                        firstEnum.Value.Value.Dispose();
                        workingEnums.RemoveFirst();
                        totalEnumerators--;
                    } else {
                        // push this enumerator to the proper sort position in the list
                        var e = firstEnum.Value;
                        workingEnums.RemoveFirst();
                        var currentNode = workingEnums.First;
                        do {
                            if (currentNode == null) {
                                workingEnums.AddLast(e);
                                break;
                            }
                            if (comparer(currentNode.Value, e) >= 0) {
                                workingEnums.AddBefore(currentNode, e);
                                break;
                            }
                            currentNode = currentNode.Next;
                            if (currentNode == null) {
                                workingEnums.AddLast(e);
                                break;
                            }
                        } while (currentNode != null);
                    }
                }
            } finally {
                // Loop through the enumerator list and make sure that any leftovers are properly disposed
                // This should really only happen if an exception is thrown from inside the yield.
                while (workingEnums.First != null) {
                    workingEnums.First.Value.Value.Dispose();
                    workingEnums.RemoveFirst();
                }
            }
        }
    }

}
