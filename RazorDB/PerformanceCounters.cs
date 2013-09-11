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
using System.Diagnostics;
using System.Linq;

namespace RazorDB {

    public static class PerformanceCounters {

        private static PerformanceCounter _SBTReadMetadataCached;
        public static PerformanceCounter SBTReadMetadataCached { get { if (_SBTReadMetadataCached == null) Initialize(); return _SBTReadMetadataCached; } }

        private static PerformanceCounter _SBTReadMetadata;
        public static PerformanceCounter SBTReadMetadata { get { if (_SBTReadMetadata == null) Initialize(); return _SBTReadMetadata; } }

        private static PerformanceCounter _SBTConstructed;
        public static PerformanceCounter SBTConstructed { get { if (_SBTConstructed == null) Initialize(); return _SBTConstructed; } }

        private static PerformanceCounter _SBTEnumerateFromKey;
        public static PerformanceCounter SBTEnumerateFromKey { get { if (_SBTEnumerateFromKey == null) Initialize(); return _SBTEnumerateFromKey; } }

        private static PerformanceCounter _SBTGetBlockTableIndex;
        public static PerformanceCounter SBTGetBlockTableIndex { get { if (_SBTGetBlockTableIndex == null) Initialize(); return _SBTGetBlockTableIndex; } }

        private static PerformanceCounter _SBTLookup;
        public static PerformanceCounter SBTLookup { get { if (_SBTLookup == null) Initialize(); return _SBTLookup; } }

        private static PerformanceCounter _SBTEnumerateMergedTablesPrecached;
        public static PerformanceCounter SBTEnumerateMergedTablesPrecached { get { if (_SBTEnumerateMergedTablesPrecached == null) Initialize(); return _SBTEnumerateMergedTablesPrecached; } }

        private const string perfCategoryName = "RazorDb";
        private static void Initialize() {
            // remove any previous definitions
            if (PerformanceCounterCategory.Exists(perfCategoryName))
                PerformanceCounterCategory.Delete(perfCategoryName);
            
            AddPerformanceCounterData("SBTConstructed", "Number of times SBT constructor is called", PerformanceCounterType.NumberOfItems64);
            AddPerformanceCounterData("SBTReadMetadata", "Number of times ReadMetadata goes to disk", PerformanceCounterType.NumberOfItems64);
            AddPerformanceCounterData("SBTReadMetadata Cached","Number of times ReadMetadata comeds from cache",PerformanceCounterType.NumberOfItems64);
            AddPerformanceCounterData("SBTEnumerateFromKey", "Number of SBT created for EnumerateFromKey", PerformanceCounterType.NumberOfItems64);
            AddPerformanceCounterData("SBTGetBlockTableIndex", "Number of SBT created for GetBlockTableIndex", PerformanceCounterType.NumberOfItems64);
            AddPerformanceCounterData("SBTLookup", "Number of SBT created for Lookup", PerformanceCounterType.NumberOfItems64);
            AddPerformanceCounterData("SBTEnumerateMergedTablesPrecached", "Number of SBT created for EnumerateMergedTablesPrecached", PerformanceCounterType.NumberOfItems64);

            // Create the category and pass the collection to it.
            System.Diagnostics.PerformanceCounterCategory.Create(
                perfCategoryName, "Peformance counters for internal operations of RazorDb",
                PerformanceCounterCategoryType.SingleInstance, _ccData);

            // Create static counter refs
            _SBTConstructed = new PerformanceCounter(perfCategoryName, "SBTConstructed", false);
            _SBTEnumerateFromKey = new PerformanceCounter(perfCategoryName, "SBTEnumerateFromKey", false);
            _SBTEnumerateMergedTablesPrecached = new PerformanceCounter(perfCategoryName, "SBTEnumerateMergedTablesPrecached", false);
            _SBTGetBlockTableIndex = new PerformanceCounter(perfCategoryName, "SBTGetBlockTableIndex", false);
            _SBTLookup = new PerformanceCounter(perfCategoryName, "SBTLookup", false);
            _SBTReadMetadata = new PerformanceCounter(perfCategoryName, "SBTReadMetadata", false);
            _SBTReadMetadataCached = new PerformanceCounter(perfCategoryName, "SBTReadMetadataCached",false);

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
}
