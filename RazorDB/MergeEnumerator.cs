using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;

namespace RazorDB {

    // This class takes a collection of IEnumerable<T> objects (assuming in sorted order) and returns 
    // a merged version with the result being in sorted order as well.
    public static class MergeEnumerator {

        public static IEnumerable<T> Merge<T>(IEnumerable<IOrderedEnumerable<T>> enumerables) {

            // Get enumerators for each enumerable
            var enumerators = enumerables.Select( e => e.GetEnumerator() );
            var nonEmptyEnums = new List<IEnumerator<T>>();

            // move ahead and prune out empty enumerators
            foreach (var e in enumerators) {
                if (e.MoveNext()) {
                    nonEmptyEnums.Add(e);
                } else {
                    e.Dispose();
                }
            }

            // order them by the first (current) element and put into a linked list
            var workingEnums = new LinkedList<IEnumerator<T>>(nonEmptyEnums.OrderBy( e => e.Current ));

            int totalEnumerators = workingEnums.Count;
            while (totalEnumerators > 0) {

                var firstEnum = workingEnums.First;
                yield return firstEnum.Value.Current;

                // advance this enumerator to the next spot
                if (!firstEnum.Value.MoveNext()) {
                    // ok this enumerator is done, so remove it
                    firstEnum.Value.Dispose();
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
                        if (Comparer<T>.Default.Compare(currentNode.Value.Current, e.Current) >= 0) {
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
        }
    }

}
