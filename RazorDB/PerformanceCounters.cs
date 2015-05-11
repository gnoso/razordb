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
using System.Diagnostics;
using System.Linq;

namespace RazorDB {

    public static class PerformanceCounters {

        private static RazorPerformanceCounter _SBTReadMetadataCached;
        public static RazorPerformanceCounter SBTReadMetadataCached { get { if (_SBTReadMetadataCached == null) Initialize(); return _SBTReadMetadataCached; } }

        private static RazorPerformanceCounter _SBTReadMetadata;
        public static RazorPerformanceCounter SBTReadMetadata { get { if (_SBTReadMetadata == null) Initialize(); return _SBTReadMetadata; } }

        private static RazorPerformanceCounter _SBTConstructed;
        public static RazorPerformanceCounter SBTConstructed { get { if (_SBTConstructed == null) Initialize(); return _SBTConstructed; } }

        private static RazorPerformanceCounter _SBTEnumerateFromKey;
        public static RazorPerformanceCounter SBTEnumerateFromKey { get { if (_SBTEnumerateFromKey == null) Initialize(); return _SBTEnumerateFromKey; } }

        private static RazorPerformanceCounter _SBTGetBlockTableIndex;
        public static RazorPerformanceCounter SBTGetBlockTableIndex { get { if (_SBTGetBlockTableIndex == null) Initialize(); return _SBTGetBlockTableIndex; } }

        private static RazorPerformanceCounter _SBTLookup;
        public static RazorPerformanceCounter SBTLookup { get { if (_SBTLookup == null) Initialize(); return _SBTLookup; } }

        private static RazorPerformanceCounter _SBTEnumerateMergedTablesPrecached;
        public static RazorPerformanceCounter SBTEnumerateMergedTablesPrecached { get { if (_SBTEnumerateMergedTablesPrecached == null) Initialize(); return _SBTEnumerateMergedTablesPrecached; } }

        private const string perfCategoryName = "RazorDb";
        private static object _perfCtrLock = new object();
        private static bool _initialized = false;
        private static void Initialize() {

            lock (_perfCtrLock) {
                // once entered make sure initialization still needded
                if (_initialized)
                    return;

                try {
                    // remove any previous definitions
                    if (PerformanceCounterCategory.Exists(perfCategoryName))
                        PerformanceCounterCategory.Delete(perfCategoryName);

                    AddPerformanceCounterData("SBTConstructed", "Number of times SBT constructor is called", PerformanceCounterType.NumberOfItems64);
                    AddPerformanceCounterData("SBTReadMetadata", "Number of times ReadMetadata goes to disk", PerformanceCounterType.NumberOfItems64);
                    AddPerformanceCounterData("SBTReadMetadata Cached", "Number of times ReadMetadata comeds from cache", PerformanceCounterType.NumberOfItems64);
                    AddPerformanceCounterData("SBTEnumerateFromKey", "Number of SBT created for EnumerateFromKey", PerformanceCounterType.NumberOfItems64);
                    AddPerformanceCounterData("SBTGetBlockTableIndex", "Number of SBT created for GetBlockTableIndex", PerformanceCounterType.NumberOfItems64);
                    AddPerformanceCounterData("SBTLookup", "Number of SBT created for Lookup", PerformanceCounterType.NumberOfItems64);
                    AddPerformanceCounterData("SBTEnumerateMergedTablesPrecached", "Number of SBT created for EnumerateMergedTablesPrecached", PerformanceCounterType.NumberOfItems64);

                    try {
                        // Create the category and pass the collection to it.
                        System.Diagnostics.PerformanceCounterCategory.Create(
                            perfCategoryName, "Peformance counters for internal operations of RazorDb",
                            PerformanceCounterCategoryType.SingleInstance, _ccData);
                    } catch {
                        // not sure what to do with this exception.
                        // have only seen an exception here when
                    }
                } catch {
                    // exception at this level probably means lack of permissions
                }

                // Create static counter refs
                _SBTConstructed = new RazorPerformanceCounter(perfCategoryName, "SBTConstructed", false);
                _SBTEnumerateFromKey = new RazorPerformanceCounter(perfCategoryName, "SBTEnumerateFromKey", false);
                _SBTEnumerateMergedTablesPrecached = new RazorPerformanceCounter(perfCategoryName, "SBTEnumerateMergedTablesPrecached", false);
                _SBTGetBlockTableIndex = new RazorPerformanceCounter(perfCategoryName, "SBTGetBlockTableIndex", false);
                _SBTLookup = new RazorPerformanceCounter(perfCategoryName, "SBTLookup", false);
                _SBTReadMetadata = new RazorPerformanceCounter(perfCategoryName, "SBTReadMetadata", false);
                _SBTReadMetadataCached = new RazorPerformanceCounter(perfCategoryName, "SBTReadMetadataCached", false);

                _initialized = true;
            }
        }


        private static CounterCreationDataCollection _ccData = new CounterCreationDataCollection();
        private static void AddPerformanceCounterData(string name, string help, PerformanceCounterType type) {
            var ccd = new CounterCreationData() {
                CounterName = name,
                CounterHelp = help,
                CounterType = type
            };
            _ccData.Add(ccd);
        }
    }

    /// <summary>
    /// RazorPerformanceCounter will allow calls to Increment
    /// even if the true performance counter can't be created
    /// in windows.
    /// </summary>
    public class RazorPerformanceCounter {
        private PerformanceCounter _perfCtr;

        public RazorPerformanceCounter(string category, string name, bool readOnly) {
            try {
                _perfCtr = new PerformanceCounter(category, name, readOnly);
            } catch {
                _perfCtr = null;
            }
        }

        public void Increment() {
            try {
                if (_perfCtr != null)
                    _perfCtr.Increment();
            } catch(Exception) {
                 //don't let perf counter increment interfere with any operation
            }
        }
    }

}
