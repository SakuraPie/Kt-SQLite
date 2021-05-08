import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.util.logging.Logger

object KtDataBase{

    private val LOG: Logger = Logger.getLogger(KtDataBase::class.java.name)

    @Synchronized
    fun dbExecute(connection: Connection, sql:String, args: List<Any>? = null):Boolean{
        val ps = connection.prepareStatement(sql)
        if (args != null) {
            for (i in args.indices){
                ps.setObject(i + 1, args[i])
            }
        }
        return try {
            ps.executeUpdate()
            true
        }catch (e:SQLException){
            LOG.warning(e.toString())
            false
        }
    }

    @Synchronized
    fun dbExecuteQuery(connection: Connection, sql:String, args: List<Any>? = null): List<MutableMap<String, Any>>?{
        val lst = mutableListOf<MutableMap<String, Any>>()
        return try {
            val ps = connection.prepareStatement(sql)
            if (args != null) {
                for (i in args.indices){
                    ps.setObject(i + 1, args[i])
                }
            }
            val rs = ps.executeQuery()
            while (rs.next()){
                val map = rs.metaData.columnCount.downTo(1).reversed().associate {
                    rs.metaData.getColumnName(it) to rs.getObject(it)
                }.toMutableMap()
                lst.add(map)
            }
            lst
        }catch (e:SQLException){
            LOG.warning("DataBase SELECT error -> $e -> sql -> '$sql'")
            null
        }
    }

    fun initDatabase(filename:String): Connection? {
        val dbExist = File(filename)
        if (dbExist.exists()) {
            LOG.info("database file has been created!")
            return DriverManager.getConnection("jdbc:sqlite:$filename")
        }
        Class.forName("org.sqlite.JDBC")
        try {
            val connection = DriverManager.getConnection("jdbc:sqlite:$filename")
            TODO("Create...")
            return connection
        }catch(e:SQLException) {
            println(e)
            return null
        }
    }
}

