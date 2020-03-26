
##JDBC存储
```bash 
 <bean id="mysql-ds" class="org.apache.commons.dbcp.BasicDataSource" destroymethod="close">
 <property name="driverClassName" value="com.mysql.jdbc.Driver"/>
 <property name="url" value="jdbc:mysql://localhost/activemq?relaxAutoCommit=true"/>
 <property name="username" value="username"/>
 <property name="password" value="password"/>
 <property name="maxActive" value="200"/>
 <property name="poolPreparedStatements" value="true"/>
 </bean>

 <persistenceAdapter>
 <jdbcPersistenceAdapter dataSource="#mysql-ds"
 createTablesOnStartup="true" />
 </persistenceAdapter>
```

#消息发送同步异步
| |开启事务|关闭事务|
|:---|---:|:---:|
|持久化|异步|同步（等待MQ的ack确认，保证不丢消息）|
|非持久化|异步|异步|