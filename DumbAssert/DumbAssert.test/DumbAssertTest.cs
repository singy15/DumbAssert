using System;
using System.Data;
using System.Data.SQLite;
using NUnit.Framework;
using DumbAssertNS;

namespace DumbAssertTestNS
{
    public class DumbAssertTests
    {
        private readonly string TEST_DIR = "../../../../../T__1__sample";

        private readonly string SQLITE_DB_CONNECTION_STRING = "Data Source=../../../../../test.db";

        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public void TestTableDataCtor() 
        {
            TableData data = new TableData(TEST_DIR + "/R__m_item.csv");
            Assert.IsTrue(data.TableName == "m_item");
            Assert.IsTrue(data.Data.Count > 0);
            Assert.IsTrue(data.Columns.Length > 0);
            Assert.IsTrue(data.Rows.Count > 0);
        }

        [Test]
        public void TestTableDataGenerateInsertSQL() 
        {
            TableData data = new TableData(TEST_DIR + "/R__m_item.csv");
            string sql = data.GenerateInsertSQL();
            Assert.IsNotNull(sql);
            Assert.That(sql, Is.EqualTo(
                "DELETE FROM m_item;" + Environment.NewLine
                + "INSERT INTO m_item (item_id,name,desc) VALUES ('1','item1',NULL);" + Environment.NewLine
                + "INSERT INTO m_item (item_id,name,desc) VALUES ('2','item2','desc2');"));
        }

        [Test]
        public void TestTestDataCtor() 
        {
            TestData data = new TestData(TEST_DIR);
            Assert.IsTrue(data.TestId == "1");
            Assert.IsTrue(data.Desc == "sample");
            Assert.IsTrue(data.Prerequisite.Count == 2);
            Assert.IsTrue(data.Expected.Count == 2);
        }

        [Test]
        public void TestTestDataGeneratePrepareSQL() 
        {
            TestData data = new TestData(TEST_DIR);
            string sql = data.GeneratePrepareSQL();
            Assert.IsNotNull(sql);
            Assert.That(sql, Is.EqualTo(
                "DELETE FROM m_item;" + Environment.NewLine
                + "INSERT INTO m_item (item_id,name,desc) VALUES ('1','item1',NULL);" + Environment.NewLine
                + "INSERT INTO m_item (item_id,name,desc) VALUES ('2','item2','desc2');" + Environment.NewLine
                + "DELETE FROM t_sale;" + Environment.NewLine
                + "INSERT INTO t_sale (sale_id,item_id,qty,payed) VALUES ('1','1','100','1');" + Environment.NewLine
                + "INSERT INTO t_sale (sale_id,item_id,qty,payed) VALUES ('2','2','200','0');"
                ));
        }

        [Test]
        public void TestPrepare() 
        {
            DumbAssertConfig.TestDataBaseDir = "../../../../../";
            using(IDbConnection conn = new SQLiteConnection(SQLITE_DB_CONNECTION_STRING)) {
                conn.Open();
                (new DumbAssert(conn)).Prepare("1");
                conn.Close();
            }
        }

        [Test]
        public void TestAssertTable() 
        {
            DumbAssertConfig.TestDataBaseDir = "../../../../../";
            using(IDbConnection conn = new SQLiteConnection(SQLITE_DB_CONNECTION_STRING)) {
                conn.Open();
                DumbAssert du = new DumbAssert(conn);
                du.Prepare("1");
                du.Assert("1");
                conn.Close();
            }
        }
    }
}