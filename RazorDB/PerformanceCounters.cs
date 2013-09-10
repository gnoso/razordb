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

        private static CounterCreationDataCollection _ccData = new CounterCreationDataCollection();
        private static PerformanceCounterCategory _pcCategory;
        private static void Initialize() {

            _SBTConstructed = CreatePerformanceCounter("SBT Constructed", "Number of times SBT constructor is called", PerformanceCounterType.NumberOfItems64);
            _SBTReadMetadata = CreatePerformanceCounter("SBT ReadMetadata", "Number of times ReadMetadata goes to disk", PerformanceCounterType.NumberOfItems64);
            _SBTReadMetadataCached = CreatePerformanceCounter("SBT ReadMetadata Cached","Number of times ReadMetadata comeds from cache",PerformanceCounterType.NumberOfItems64);
            _SBTEnumerateFromKey = CreatePerformanceCounter("SBT EnumerateFromKey", "Number of SBT created for EnumerateFromKey", PerformanceCounterType.NumberOfItems64);
            _SBTGetBlockTableIndex = CreatePerformanceCounter("SBT GetBlockTableIndex", "Number of SBT created for GetBlockTableIndex", PerformanceCounterType.NumberOfItems64);
            _SBTLookup = CreatePerformanceCounter("SBT Lookup", "Number of SBT created for Lookup", PerformanceCounterType.NumberOfItems64);
            _SBTEnumerateMergedTablesPrecached = CreatePerformanceCounter("SBT EnumerateMergedTablesPrecached", "Number of SBT created for EnumerateMergedTablesPrecached", PerformanceCounterType.NumberOfItems64);
            
        }

        private static PerformanceCounter CreatePerformanceCounter(string name, string help, PerformanceCounterType type) {
            var ccd = new CounterCreationData() {
                CounterName = name,
                CounterHelp = help,
                CounterType = type
            };
            _ccData.Add(ccd);

            // remove any previous definitions
            if (PerformanceCounterCategory.Exists("RazorDb"))
                PerformanceCounterCategory.Delete("RazorDb");

            // Create the category and pass the collection to it.
            _pcCategory = System.Diagnostics.PerformanceCounterCategory.Create(
                "RazorDb", "Peformance counters for internal operations of RazorDb",
                PerformanceCounterCategoryType.SingleInstance, _ccData);

            var pc = new PerformanceCounter("RazorDb", name, false);
            pc.RawValue = 0L;
            return pc;
        }
    }
}
