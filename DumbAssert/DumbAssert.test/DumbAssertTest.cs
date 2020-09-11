using System;
using System.Data;
using System.Data.SQLite;
using Npgsql;
using NUnit.Framework;
using DumbAssertNS;

namespace DumbAssertTestNS
{
    public class DumbAssertTests
    {
        private readonly string TEST_DIR_SQLITE = "../../../../../test_resource/data/T__1-sqlite__sample";

        private readonly string TEST_DATA_BASE_DIR = "../../../../../test_resource/data/";

        private readonly string SQLITE_DB_CONNECTION_STRING 
            = "Data Source=../../../../../test_resource/test.db";

        private readonly string PGSQL_DB_CONNECTION_STRING 
            = "Server=127.0.0.1;Port=5432;Database=testdb;User Id=postgres;Password=postgres;";

        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public void TestTableDataCtor() 
        {
            TableData data = new TableData(TEST_DIR_SQLITE + "/R__m_item.csv");
            Assert.IsTrue(data.TableName == "m_item");
            Assert.IsTrue(data.DataType == TableData.TableDataType.Table);
            Assert.IsTrue(data.Data.Count > 0);
            Assert.IsTrue(data.Columns.Length > 0);
            Assert.IsTrue(data.Rows.Count > 0);
        }

        [Test]
        public void TestTableDataCtorQuery() 
        {
            TableData data = new TableData(TEST_DIR_SQLITE + "/E__query.csv");
            Assert.IsTrue(data.TableName == "");
            Assert.IsTrue(data.DataType == TableData.TableDataType.Query);
            Assert.IsTrue(data.Query == "select name, 'foo' as bar from m_item");
            Assert.IsTrue(data.Data.Count > 0);
            Assert.IsTrue(data.Columns.Length > 0);
            Assert.IsTrue(data.Rows.Count > 0);
        }

        [Test]
        public void TestTableDataGenerateInsertSQL() 
        {
            TableData data = new TableData(TEST_DIR_SQLITE + "/R__m_item.csv");
            string sql = data.GenerateInsertSQL();
            Assert.IsNotNull(sql);
            Assert.That(sql, Is.EqualTo(
                "DELETE FROM m_item;" + Environment.NewLine
                + @"INSERT INTO m_item (""item_id"",""name"",""desc"") VALUES ('1','item1',NULL);" + Environment.NewLine
                + @"INSERT INTO m_item (""item_id"",""name"",""desc"") VALUES ('2','item2','desc2');"));
        }

        [Test]
        public void TestTestDataCtor() 
        {
            TestData data = new TestData(TEST_DIR_SQLITE);
            Assert.IsTrue(data.TestId == "1-sqlite");
            Assert.IsTrue(data.Desc == "sample");
            Assert.IsTrue(data.Prerequisite.Count == 2);
            Assert.IsTrue(data.Expected.Count == 3);
        }

        [Test]
        public void TestTestDataGeneratePrepareSQL() 
        {
            TestData data = new TestData(TEST_DIR_SQLITE);
            string sql = data.GeneratePrepareSQL();
            Assert.IsNotNull(sql);
            Assert.That(sql, Is.EqualTo(
                @"DELETE FROM m_item;" + Environment.NewLine
                + @"INSERT INTO m_item (""item_id"",""name"",""desc"") VALUES ('1','item1',NULL);" + Environment.NewLine
                + @"INSERT INTO m_item (""item_id"",""name"",""desc"") VALUES ('2','item2','desc2');" + Environment.NewLine
                + @"DELETE FROM t_sale;" + Environment.NewLine
                + @"INSERT INTO t_sale (""sale_id"",""item_id"",""qty"",""payed"") VALUES ('1','1','100','1');" + Environment.NewLine
                + @"INSERT INTO t_sale (""sale_id"",""item_id"",""qty"",""payed"") VALUES ('2','2','200','0');"
                ));
        }

        [Test]
        public void TestAssertTable() 
        {
            DumbAssertConfig.TestDataBaseDir = TEST_DATA_BASE_DIR;
            using(IDbConnection conn = new SQLiteConnection(SQLITE_DB_CONNECTION_STRING)) {
                conn.Open();
                DumbAssert du = new DumbAssert(conn);
                du.Prepare("1-sqlite");
                du.Assert("1-sqlite");
                conn.Close();
            }
        }

        [Test]
        public void TestAssertTablePg() 
        {
            DumbAssertConfig.TestDataBaseDir = TEST_DATA_BASE_DIR;
            using(IDbConnection conn = new NpgsqlConnection(PGSQL_DB_CONNECTION_STRING)) {
                conn.Open();
                DumbAssert du = new DumbAssert(conn);
                du.Prepare("1-pg");
                du.Assert("1-pg");
                conn.Close();
            }
        }

        [Test]
        public void TestUseExistingTransaction() 
        {
            DumbAssertConfig.TestDataBaseDir = TEST_DATA_BASE_DIR;
            using(IDbConnection conn = new NpgsqlConnection(PGSQL_DB_CONNECTION_STRING)) {
                conn.Open();
                var tx = conn.BeginTransaction();
                DumbAssert du = new DumbAssert(conn, tx);
                du.Prepare("1-pg");
                du.Assert("1-pg");
                tx.Commit();
                conn.Close();
            }
        }
    }
}