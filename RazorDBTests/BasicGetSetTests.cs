using System;
using NUnit.Framework;
using System.Text;
using RazorDB;

namespace RazorDBTests {

    [TestFixture]
    public class BasicGetSetTests {

        [Test]
        public void BasicGetAndSet() {

            var db = new KeyValueStore("TestData");

            for (int i = 0; i < 10; i++) {
                byte[] key = BitConverter.GetBytes(i);
                byte[] value = Encoding.UTF8.GetBytes("Number " + i.ToString());
                db.Set(key, value);
            }

            for (int j = 0; j < 15; j++) {
                byte[] key = BitConverter.GetBytes(j);
                
                byte[] value = db.Get(key);
                if (j < 10) {
                    Assert.AreEqual(Encoding.UTF8.GetBytes("Number " + j.ToString()), value);
                } else {
                    Assert.IsNull(value);
                }
            }
        }

    }

}