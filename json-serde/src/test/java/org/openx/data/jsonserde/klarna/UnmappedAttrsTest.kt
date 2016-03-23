package org.openx.data.jsonserde.klarna

import com.klarna.hiverunner.HiveShell
import com.klarna.hiverunner.StandaloneHiveRunner
import com.klarna.hiverunner.annotations.HiveSQL
import org.apache.commons.io.FileUtils
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith


@RunWith(StandaloneHiveRunner::class)
class UnmappedAttrsTest {

    @Suppress("unused")
    @field:HiveSQL(files = arrayOf())
    var hiveShell:HiveShell? = null

    @Before
    fun prepare() {
        val tmpDir = TemporaryFolder()
        tmpDir.create()
        FileUtils.copyInputStreamToFile(
            this.javaClass.getResourceAsStream("/unmapped_attrs.txt"),
            tmpDir.newFile()
        )
        hiveShell!!.execute("""
            DROP TABLE IF EXISTS test_input;
            CREATE EXTERNAL TABLE test_input (
              listed1 INT,
              listed2 INT,
              unmapped_cols MAP<STRING, STRING>
            )
            ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            WITH SERDEPROPERTIES (
              "unmapped.attr.key" = "unmapped_cols"
            )
            LOCATION '${tmpDir.root.absolutePath}';
        """)
    }

    @Test
    fun verifyFullRead() {
        var results = hiveShell!!.executeQuery("SELECT * FROM test_input")
        Assert.assertNotNull(results)
        Assert.assertEquals(1, results.size)
        println(results.first())
        val cols = results.first().split("\t")
        Assert.assertEquals("1", cols[0])
        Assert.assertEquals("2", cols[1])
        Assert.assertEquals(
            """{"f":"true","g":null,"d":"[1,2,3]","e":"{\"b\":2,\"a\":1}","b":"1.1","c":"\"hello\"","a":"1"}""",
            cols[2]
        )
    }

}