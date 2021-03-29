using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace LiteDB.Tests
{
    [TestClass]
    public class CorruptedTest
    {
        [TestMethod]
        public void Engine_Delete_Test()
        {
            using (var file = new TempFile("Resources/corrupted/Db_with_leak.db"))
            {
                using (var db = new LiteDatabase("Filename=" + file.Filename + ";Mode=Exclusive"))
                {
                    var colNames = db.GetCollectionNames ();

                    foreach (var colName in colNames)
                    {
                        db.GetCollection (colName).Delete (Query.All ());
                    }
                }
            }
        }
    }
}