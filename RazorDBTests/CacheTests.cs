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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using RazorDB;

namespace RazorDBTests {

    [TestFixture]
    public class CacheTests {

        [Test]
        public void BasicAdd() {

            var cache = new Cache<ByteArray>(500 * 1024, ba => ba.Length );
            var items = new List<KeyValuePair<string, ByteArray>>();

            for (int i = 0; i < 100; i++) {
                var key = ByteArray.Random(40).ToString();
                var val = ByteArray.Random(256);
                cache.Set(key, val);
                items.Add(new KeyValuePair<string, ByteArray>(key, val));
            }

            ByteArray oval;
            for (int i = 0; i < 100; i++) {
                var data = items[i];
                Assert.IsTrue(cache.TryGetValue(data.Key, out oval));
                Assert.AreEqual(data.Value, oval);
            }
            Assert.IsFalse(cache.TryGetValue(ByteArray.Random(40).ToString(), out oval));
        }

        [Test]
        public void CheckAddWithSize() {

            var cache = new Cache<ByteArray>(500 * 1024, ba => ba.Length);

            int size = 0;
            for (int i = 0; i < 100; i++) {
                var key = ByteArray.Random(40).ToString();
                var val = ByteArray.Random(256);
                cache.Set(key, val);
                size += val.Length;
                Assert.AreEqual(size, cache.CurrentSize);
            }
        }

        [Test]
        public void CheckOverSizeLimit() {

            int limit = 100 * 256;
            var cache = new Cache<ByteArray>(limit, ba => ba.Length);
            var items = new List<string>();

            int size = 0;
            for (int i = 0; i < 200; i++) {
                var key = ByteArray.Random(40).ToString();
                var val = ByteArray.Random(256);
                cache.Set(key, val);
                size += val.Length;

                items.Add(key);
            }

            Assert.GreaterOrEqual(limit, cache.CurrentSize);
            ByteArray output;
            // First 100 items should have been evicted
            for (int i = 0; i < 100; i++) {
                Assert.IsFalse(cache.TryGetValue(items[i], out output));
            }

            // Next 100 items should still be there
            for (int i = 100; i < 200; i++) {
                Assert.IsTrue(cache.TryGetValue(items[i], out output));
            }
        }

        [Test]
        public void CheckLRU() {

            int limit = 100 * 256;
            var cache = new Cache<ByteArray>(limit, ba => ba.Length);
            var items = new List<string>();

            for (int i = 0; i < 100; i++) {
                var key = ByteArray.Random(40).ToString();
                var val = ByteArray.Random(256);
                cache.Set(key, val);

                items.Add(key);
            }

            ByteArray output;
            // Touch the first 50 items again
            for (int i = 0; i < 50; i++) {
                Assert.IsTrue(cache.TryGetValue(items[i], out output));
            }

            // Add 10 more items
            for (int i = 0; i < 10; i++) {
                var key = ByteArray.Random(40).ToString();
                var val = ByteArray.Random(256);
                cache.Set(key, val);

                items.Add(key);
            }

            // First 50 items should still be there
            for (int i = 0; i < 50; i++) {
                Assert.IsTrue(cache.TryGetValue(items[i], out output));
            }

            // Next 10 items should be evicted
            for (int i = 50; i < 60; i++) {
                Assert.IsFalse(cache.TryGetValue(items[i], out output));
            }

            // Next 50 items should still be there
            for (int i = 60; i < 110; i++) {
                Assert.IsTrue(cache.TryGetValue(items[i], out output));
            }

            Assert.GreaterOrEqual(limit, cache.CurrentSize);
       }

    }
}
