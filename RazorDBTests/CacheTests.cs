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
            var items = new List<KeyValuePair<ByteArray, ByteArray>>();

            for (int i = 0; i < 100; i++) {
                var key = ByteArray.Random(40);
                var val = ByteArray.Random(256);
                cache.Set(key, val);
                items.Add(new KeyValuePair<ByteArray, ByteArray>(key, val));
            }

            ByteArray oval;
            for (int i = 0; i < 100; i++) {
                var data = items[i];
                Assert.IsTrue(cache.TryGetValue(data.Key, out oval));
                Assert.AreEqual(data.Value, oval);
            }
            Assert.IsFalse(cache.TryGetValue(ByteArray.Random(40), out oval));
        }

        [Test]
        public void CheckAddWithSize() {

            var cache = new Cache<ByteArray>(500 * 1024, ba => ba.Length);

            int size = 0;
            for (int i = 0; i < 100; i++) {
                var key = ByteArray.Random(40);
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
            var items = new List<ByteArray>();

            int size = 0;
            for (int i = 0; i < 200; i++) {
                var key = ByteArray.Random(40);
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
            var items = new List<ByteArray>();

            for (int i = 0; i < 100; i++) {
                var key = ByteArray.Random(40);
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
                var key = ByteArray.Random(40);
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
