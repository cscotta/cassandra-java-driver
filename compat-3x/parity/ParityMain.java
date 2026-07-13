import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.policies.*;
import com.datastax.driver.core.utils.Bytes;
import com.datastax.driver.core.querybuilder.*;
import com.datastax.driver.core.schemabuilder.*;
import com.datastax.driver.mapping.*;
import com.datastax.driver.mapping.annotations.*;
import io.netty.bootstrap.Bootstrap;
import java.net.InetAddress;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Differential parity harness. Written ONLY against the 3.12.1 public API. Compiled once, then run
 * under (A) real cassandra-driver 3.12.1 and (B) the shim + 4.x, both against the same live C* node.
 * Each scenario prints stable KEY=VALUE observations; the two runs are diffed. Identical => parity.
 * Also implicitly proves binary compat: bytecode compiled vs 3.12.1 must link against the shim at runtime.
 */
public class ParityMain {
  static final String KS = "parity_ks";
  static StringBuilder out = new StringBuilder();
  static void o(String k, Object v){ out.append(k).append('=').append(String.valueOf(v)).append('\n'); }
  interface TR { void run() throws Exception; }
  static void guard(String name, TR r){ try { r.run(); } catch (Throwable t){ o(name+".EXC", t.getClass().getSimpleName()); } }

  public static void main(String[] args) {
    String host = args.length > 0 ? args[0] : "127.0.0.1";
    Cluster cluster = null;
    try {
      cluster = Cluster.builder().addContactPoint(host).build();
      Session s = cluster.connect();
      scConnect(cluster, s);
      scSchema(s);
      scSimpleCrud(s);
      scPrepared(s);
      scBatch(s);
      scTypes(s);
      scNulls(s);
      scPaging(s);
      scCollections(s);
      scMetadata(cluster);
      scExceptions(s);
      scQueryBuilder(s);
      scSchemaBuilder();
      scUdtTuple(cluster, s);
      scMoreTypes(s);
      scAsync(s);
      scConsistency(s);
      scExtras();
      scMapper(s);
      scLwt(s);
      scCounter(s);
      scTtl(s);
      scStatic(s);
      scPooling(cluster);
      // ---- new scenarios exercising the confirmed-bug fixes ----
      scQbBoundValues(s);
      scMetaCaseInsensitive(cluster, s);
      scSimpleStatementCodecs(s);
      scReadTimeoutZero(s);
      scPagingStateRoundTrip(s);
      scPoliciesNonNull(cluster);
      scEmptyTrailingPage(s);
      scClosedClusterConnect(host);
      scCustomLbp(host);
      scCustomSpecex(host);
      scPercentileSpecex(host);
      scNettyThreadingOptions(host);
      scTimestamp(host);
      scRetry(host);
      scReconnection(host);
      scHostListener(cluster);
      scSchemaListener(cluster, s);
      scQueryOptions(host);
      scPooling();
      scMetrics(cluster, s);
      scEndPointFactory(host);
    } catch (Throwable t) {
      o("FATAL", t.getClass().getName()+": "+t.getMessage());
    } finally {
      if (cluster != null) try { cluster.close(); } catch (Throwable ignore) {}
    }
    System.out.print(out);
  }

  static void scConnect(Cluster c, Session s){
    guard("connect", () -> {
      Row r = s.execute("SELECT release_version FROM system.local").one();
      o("connect.release_version", r.getString("release_version"));
      o("connect.protocol", c.getConfiguration().getProtocolOptions().getProtocolVersion());
      o("connect.clusterNameNonNull", c.getMetadata().getClusterName()!=null);
    });
  }
  static void scSchema(Session s){
    guard("schema", () -> {
      s.execute("DROP KEYSPACE IF EXISTS "+KS);
      s.execute("CREATE KEYSPACE "+KS+" WITH replication={'class':'SimpleStrategy','replication_factor':1}");
      s.execute("CREATE TABLE "+KS+".t(id int PRIMARY KEY, name text, tags list<text>)");
      s.execute("CREATE INDEX IF NOT EXISTS myidx ON "+KS+".t(name)");
      o("schema.ok", true);
    });
  }
  static void scSimpleCrud(Session s){
    guard("crud", () -> {
      s.execute("INSERT INTO "+KS+".t(id,name) VALUES(1,'alice')");
      Row r = s.execute("SELECT id,name,tags FROM "+KS+".t WHERE id=1").one();
      o("crud.id", r.getInt("id")); o("crud.name", r.getString("name"));
      o("crud.isNullTags", r.isNull("tags")); o("crud.colCount", r.getColumnDefinitions().size());
    });
  }
  static void scPrepared(Session s){
    guard("prepared", () -> {
      PreparedStatement ps = s.prepare("INSERT INTO "+KS+".t(id,name,tags) VALUES(?,?,?)");
      BoundStatement b = ps.bind(2, "bob", Arrays.asList("x","y"));
      s.execute(b);
      Row r = s.execute("SELECT name,tags FROM "+KS+".t WHERE id=2").one();
      o("prepared.name", r.getString("name")); o("prepared.tags", r.getList("tags", String.class));
      o("prepared.boundIsSet", b.isSet(0));
    });
  }
  static void scBatch(Session s){
    guard("batch", () -> {
      PreparedStatement ps = s.prepare("INSERT INTO "+KS+".t(id,name) VALUES(?,?)");
      BatchStatement bs = new BatchStatement(BatchStatement.Type.LOGGED);
      bs.add(ps.bind(10,"ten")); bs.add(ps.bind(11,"eleven"));
      bs.add(new SimpleStatement("INSERT INTO "+KS+".t(id,name) VALUES(12,'twelve')"));
      s.execute(bs);
      o("batch.count", s.execute("SELECT count(*) FROM "+KS+".t WHERE id IN (10,11,12)").one().getLong(0));
    });
  }
  static void scTypes(Session s){
    guard("types", () -> {
      s.execute("CREATE TABLE "+KS+".types(pk int PRIMARY KEY, a bigint, b boolean, d double, f float, "
        +"g uuid, h timestamp, i blob, j decimal, k varint, l inet, m text)");
      UUID u = UUID.fromString("00000000-0000-0000-0000-000000000001");
      Date dt = new Date(1234567890000L); ByteBuffer blob = ByteBuffer.wrap(new byte[]{1,2,3,4});
      PreparedStatement ps = s.prepare("INSERT INTO "+KS+".types(pk,a,b,d,f,g,h,i,j,k,l,m) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)");
      s.execute(ps.bind(1, 9000000000L, true, 3.14159d, 2.5f, u, dt, blob,
        new BigDecimal("12345.6789"), new BigInteger("99999999999999999999"), InetAddress.getByName("10.0.0.1"), "héllo"));
      Row r = s.execute("SELECT * FROM "+KS+".types WHERE pk=1").one();
      o("types.a", r.getLong("a")); o("types.b", r.getBool("b")); o("types.d", r.getDouble("d"));
      o("types.f", r.getFloat("f")); o("types.g", r.getUUID("g")); o("types.h", r.getTimestamp("h").getTime());
      o("types.i", Bytes.toHexString(r.getBytes("i"))); o("types.j", r.getDecimal("j"));
      o("types.k", r.getVarint("k")); o("types.l", r.getInet("l").getHostAddress()); o("types.m", r.getString("m"));
    });
  }
  static void scNulls(Session s){
    guard("nulls", () -> {
      s.execute("INSERT INTO "+KS+".t(id) VALUES(99)");
      Row r = s.execute("SELECT id,name,tags FROM "+KS+".t WHERE id=99").one();
      o("nulls.nameNull", r.isNull("name")); o("nulls.getStringNull", r.getString("name"));
    });
  }
  static void scPaging(Session s){
    guard("paging", () -> {
      s.execute("CREATE TABLE "+KS+".pg(pk int, ck int, PRIMARY KEY(pk,ck))");
      PreparedStatement ps = s.prepare("INSERT INTO "+KS+".pg(pk,ck) VALUES(0,?)");
      for (int i=0;i<25;i++) s.execute(ps.bind(i));
      Statement st = new SimpleStatement("SELECT ck FROM "+KS+".pg WHERE pk=0"); st.setFetchSize(5);
      ResultSet rs = s.execute(st);
      int count=0, sum=0; for (Row r : rs){ count++; sum+=r.getInt("ck"); }
      o("paging.count", count); o("paging.sum", sum);
    });
  }
  static void scCollections(Session s){
    guard("collections", () -> {
      s.execute("CREATE TABLE "+KS+".coll(pk int PRIMARY KEY, s1 set<int>, m1 map<text,int>)");
      Set<Integer> set = new TreeSet<>(Arrays.asList(3,1,2));
      Map<String,Integer> map = new TreeMap<>(); map.put("a",1); map.put("b",2);
      s.execute(s.prepare("INSERT INTO "+KS+".coll(pk,s1,m1) VALUES(?,?,?)").bind(1, set, map));
      Row r = s.execute("SELECT s1,m1 FROM "+KS+".coll WHERE pk=1").one();
      o("collections.set", new TreeSet<>(r.getSet("s1", Integer.class)));
      o("collections.map", new TreeMap<>(r.getMap("m1", String.class, Integer.class)));
    });
  }
  static void scMetadata(Cluster c){
    guard("metadata", () -> {
      KeyspaceMetadata ks = c.getMetadata().getKeyspace(KS);
      o("metadata.ksName", ks.getName());
      TableMetadata t = ks.getTable("types");
      o("metadata.tableColCount", t.getColumns().size());
      o("metadata.pkName", t.getPartitionKey().get(0).getName());
      o("metadata.colType.j", t.getColumn("j").getType().getName());
      o("metadata.colType.l", t.getColumn("l").getType().getName());
      StringBuilder cols = new StringBuilder();
      for (ColumnMetadata cm : t.getColumns()) cols.append(cm.getName()).append(',');
      o("metadata.colOrder", cols.toString());
    });
  }
  static void scExceptions(Session s){
    guard("exc.syntax", () -> { s.execute("SELECT bogus FROM "); });
    guard("exc.invalidQuery", () -> {
      try { s.execute("SELECT * FROM "+KS+".no_such_table"); }
      catch (InvalidQueryException e){ o("exc.invalidQuery.type","InvalidQueryException"); throw e; }
    });
    guard("exc.hierarchy", () -> o("exc.hierarchy", NoHostAvailableException.class.getSuperclass().getSimpleName()));
  }
  static void scQueryBuilder(Session s){
    guard("qb", () -> {
      o("qb.select", QueryBuilder.select().all().from(KS,"t").where(QueryBuilder.eq("id",1)).getQueryString());
      o("qb.selectCols", QueryBuilder.select("id","name").from(KS,"t").limit(5).getQueryString());
      o("qb.insert", QueryBuilder.insertInto(KS,"t").value("id",5).value("name","e").getQueryString());
      o("qb.update", QueryBuilder.update(KS,"t").with(QueryBuilder.set("name","z")).where(QueryBuilder.eq("id",5)).getQueryString());
      o("qb.delete", QueryBuilder.delete().from(KS,"t").where(QueryBuilder.eq("id",5)).getQueryString());
      o("qb.in", QueryBuilder.select().all().from(KS,"t").where(QueryBuilder.in("id",1,2,3)).getQueryString());
      BuiltStatement bs = QueryBuilder.select().all().from(KS,"t").where(QueryBuilder.eq("id",1));
      o("qb.execRows", s.execute(bs).all().size());
    });
  }
  static void scSchemaBuilder(){
    guard("sb", () -> {
      o("sb.createTable", SchemaBuilder.createTable(KS,"sb_t").addPartitionKey("id", DataType.cint()).addColumn("name", DataType.text()).getQueryString());
      o("sb.createIndex", SchemaBuilder.createIndex("idx_sb").onTable(KS,"sb_t").andColumn("name").getQueryString());
      o("sb.dropTable", SchemaBuilder.dropTable(KS,"sb_t").ifExists().getQueryString());
    });
  }
  static void scUdtTuple(Cluster c, Session s){
    guard("udt", () -> {
      s.execute("CREATE TYPE IF NOT EXISTS "+KS+".addr(street text, zip int)");
      s.execute("CREATE TABLE IF NOT EXISTS "+KS+".u(id int PRIMARY KEY, a frozen<addr>, tp tuple<int,text>)");
      UserType at = c.getMetadata().getKeyspace(KS).getUserType("addr");
      UDTValue uv = at.newValue().setString("street","main").setInt("zip",12345);
      TupleType tt = c.getMetadata().newTupleType(DataType.cint(), DataType.text());
      TupleValue tv = tt.newValue().setInt(0,7).setString(1,"seven");
      s.execute(s.prepare("INSERT INTO "+KS+".u(id,a,tp) VALUES(?,?,?)").bind(1, uv, tv));
      Row r = s.execute("SELECT a,tp FROM "+KS+".u WHERE id=1").one();
      UDTValue g = r.getUDTValue("a"); o("udt.street", g.getString("street")); o("udt.zip", g.getInt("zip"));
      TupleValue gt = r.getTupleValue("tp"); o("tuple.0", gt.getInt(0)); o("tuple.1", gt.getString(1));
    });
  }
  static void scMoreTypes(Session s){
    guard("mt", () -> {
      s.execute("CREATE TABLE "+KS+".mt(pk int PRIMARY KEY, d date, tm time, si smallint, ti tinyint, nl list<frozen<list<int>>>)");
      LocalDate ld = LocalDate.fromYearMonthDay(2020,2,29);
      s.execute(s.prepare("INSERT INTO "+KS+".mt(pk,d,tm,si,ti,nl) VALUES(?,?,?,?,?,?)")
        .bind(1, ld, 123456789L, (short)300, (byte)7, Arrays.asList(Arrays.asList(1,2), Arrays.asList(3))));
      Row r = s.execute("SELECT * FROM "+KS+".mt WHERE pk=1").one();
      o("mt.date", r.getDate("d")); o("mt.time", r.getTime("tm"));
      o("mt.si", r.getShort("si")); o("mt.ti", r.getByte("ti")); o("mt.nl", r.getObject("nl"));
    });
  }
  static void scAsync(Session s){
    guard("async", () -> {
      ResultSetFuture f = s.executeAsync("SELECT release_version FROM system.local");
      o("async.rv", f.getUninterruptibly().one().getString("release_version"));
      o("async.isListenable", (f instanceof com.google.common.util.concurrent.ListenableFuture));
    });
  }
  static void scConsistency(Session s){
    guard("cl", () -> {
      Statement st = new SimpleStatement("SELECT * FROM "+KS+".t WHERE id=1").setConsistencyLevel(ConsistencyLevel.ONE);
      ResultSet rs = s.execute(st);
      o("cl.consistency", st.getConsistencyLevel());
      o("cl.achieved", rs.getExecutionInfo().getAchievedConsistencyLevel());
      o("cl.queriedHostNonNull", rs.getExecutionInfo().getQueriedHost()!=null);
    });
  }
  static void scExtras(){
    guard("extras", () -> {
      com.datastax.driver.extras.codecs.enums.EnumNameCodec<java.util.concurrent.TimeUnit> enc =
        new com.datastax.driver.extras.codecs.enums.EnumNameCodec<>(java.util.concurrent.TimeUnit.class);
      o("extras.enumName.fmt", enc.format(java.util.concurrent.TimeUnit.SECONDS));
      o("extras.enumName.parse", enc.parse("'MINUTES'"));
      com.datastax.driver.extras.codecs.enums.EnumOrdinalCodec<java.util.concurrent.TimeUnit> eoc =
        new com.datastax.driver.extras.codecs.enums.EnumOrdinalCodec<>(java.util.concurrent.TimeUnit.class);
      o("extras.enumOrd.fmt", eoc.format(java.util.concurrent.TimeUnit.SECONDS));
    });
  }
  @Table(keyspace="parity_ks", name="mapped")
  public static class MappedEntity {
    @PartitionKey @Column(name="id") private int id;
    @Column(name="name") private String name;
    public MappedEntity(){}
    public int getId(){return id;} public void setId(int i){id=i;}
    public String getName(){return name;} public void setName(String n){name=n;}
  }
  static void scMapper(Session s){
    guard("mapper", () -> {
      s.execute("CREATE TABLE IF NOT EXISTS "+KS+".mapped(id int PRIMARY KEY, name text)");
      MappingManager mm = new MappingManager(s);
      Mapper<MappedEntity> mapper = mm.mapper(MappedEntity.class);
      MappedEntity e = new MappedEntity(); e.setId(100); e.setName("mapped-name");
      mapper.save(e);
      Row raw = s.execute("SELECT id,name FROM "+KS+".mapped WHERE id=100").one();
      o("mapper.rawName", raw==null?"NOROW":raw.getString("name"));
      MappedEntity got = mapper.get(100);
      o("mapper.getName", got==null?null:got.getName());
      o("mapper.getId", got==null?"NULL":String.valueOf(got.getId()));
      mapper.delete(100);
      o("mapper.afterDeleteNull", mapper.get(100)==null);
    });
  }
  static void scLwt(Session s){
    guard("lwt", () -> {
      s.execute("CREATE TABLE IF NOT EXISTS "+KS+".lwt(id int PRIMARY KEY, v text)");
      ResultSet r1 = s.execute("INSERT INTO "+KS+".lwt(id,v) VALUES(1,'a') IF NOT EXISTS");
      o("lwt.firstApplied", r1.wasApplied());
      ResultSet r2 = s.execute("INSERT INTO "+KS+".lwt(id,v) VALUES(1,'b') IF NOT EXISTS");
      o("lwt.secondApplied", r2.wasApplied());
      o("lwt.existingV", r2.one().getString("v"));
      ResultSet r3 = s.execute("UPDATE "+KS+".lwt SET v='c' WHERE id=1 IF v='a'");
      o("lwt.updApplied", r3.wasApplied());
    });
  }
  static void scCounter(Session s){
    guard("counter", () -> {
      s.execute("CREATE TABLE IF NOT EXISTS "+KS+".cnt(id int PRIMARY KEY, c counter)");
      s.execute("UPDATE "+KS+".cnt SET c = c + 5 WHERE id=1");
      s.execute("UPDATE "+KS+".cnt SET c = c + 3 WHERE id=1");
      o("counter.value", s.execute("SELECT c FROM "+KS+".cnt WHERE id=1").one().getLong("c"));
    });
  }
  static void scTtl(Session s){
    guard("ttl", () -> {
      s.execute("INSERT INTO "+KS+".t(id,name) VALUES(200,'ttl') USING TTL 1000");
      Row r = s.execute("SELECT name, TTL(name) AS ttlv FROM "+KS+".t WHERE id=200").one();
      int ttl = r.getInt("ttlv");
      o("ttl.name", r.getString("name"));
      o("ttl.hasTtl", ttl > 0 && ttl <= 1000);
    });
  }
  static void scStatic(Session s){
    guard("static", () -> {
      s.execute("CREATE TABLE IF NOT EXISTS "+KS+".st(pk int, ck int, sv text static, v text, PRIMARY KEY(pk,ck))");
      s.execute("INSERT INTO "+KS+".st(pk,ck,sv,v) VALUES(1,1,'shared','a')");
      s.execute("INSERT INTO "+KS+".st(pk,ck,v) VALUES(1,2,'b')");
      List<Row> rows = s.execute("SELECT ck,sv,v FROM "+KS+".st WHERE pk=1").all();
      o("static.rowCount", rows.size());
      o("static.sv0", rows.get(0).getString("sv"));
      o("static.sv1", rows.get(1).getString("sv"));
      TableMetadata tm = s.getCluster().getMetadata().getKeyspace(KS).getTable("st");
      o("static.isStatic", tm.getColumn("sv").isStatic());
    });
  }
  static void scPooling(Cluster c){
    guard("pooling", () -> {
      PoolingOptions po = c.getConfiguration().getPoolingOptions();
      po.refreshConnectedHosts();
      Host h = c.getMetadata().getAllHosts().iterator().next();
      po.refreshConnectedHost(h);
      o("pooling.refreshNoThrow", true);
    });
  }

  // (a) QueryBuilder statement carrying non-inlined (bound) values, executed; values must round-trip
  // (finding #1). QueryBuilder inlines only fixed-size numbers, so strings/UUIDs/collections travel
  // as separate getValues() entries that the shim previously dropped.
  static void scQbBoundValues(Session s){
    guard("qbv", () -> {
      java.util.UUID uid = java.util.UUID.fromString("00000000-0000-0000-0000-0000000000aa");
      BuiltStatement ins =
          QueryBuilder.insertInto(KS,"t").value("id",700).value("name","qb-alice");
      s.execute(ins);
      Row r = s.execute("SELECT id,name FROM "+KS+".t WHERE id=700").one();
      o("qbv.name", r==null?"NOROW":r.getString("name"));
      // update carrying a non-inlined collection value
      BuiltStatement upd =
          QueryBuilder.update(KS,"t")
              .with(QueryBuilder.set("tags", Arrays.asList("t1","t2")))
              .where(QueryBuilder.eq("id",700));
      s.execute(upd);
      Row r2 = s.execute("SELECT tags FROM "+KS+".t WHERE id=700").one();
      o("qbv.tags", r2.getList("tags", String.class));
      // marker + provided value via SimpleStatement built by QueryBuilder with a bindMarker
      BuiltStatement sel =
          QueryBuilder.select().all().from(KS,"t").where(QueryBuilder.eq("name", QueryBuilder.bindMarker()));
      o("qbv.hasMarker", sel.getQueryString().contains("?"));
    });
  }

  // (b) case-insensitive metadata lookup (findings #4/#16/#17): mixed-case and quoted names must
  // fold to the stored lowercase form.
  static void scMetaCaseInsensitive(Cluster c, Session s){
    guard("metacase", () -> {
      Metadata md = c.getMetadata();
      KeyspaceMetadata upper = md.getKeyspace("PARITY_KS");
      o("metacase.ksUpper", upper==null?"NULL":upper.getName());
      KeyspaceMetadata quoted = md.getKeyspace("\"parity_ks\"");
      o("metacase.ksQuoted", quoted==null?"NULL":quoted.getName());
      KeyspaceMetadata ks = md.getKeyspace("Parity_Ks");
      o("metacase.ksMixed", ks==null?"NULL":ks.getName());
      TableMetadata tUpper = ks==null?null:ks.getTable("TYPES");
      o("metacase.tableUpper", tUpper==null?"NULL":tUpper.getName());
      UserType udt = ks==null?null:ks.getUserType("ADDR");
      o("metacase.udtUpper", udt==null?"NULL":udt.getTypeName());
      TableMetadata tt = ks==null?null:ks.getTable("T");
      o("metacase.tTable", tt==null?"NULL":tt.getName());
      IndexMetadata idx = tt==null?null:tt.getIndex("MYIDX");
      o("metacase.indexUpper", idx==null?"NULL":idx.getName());
    });
  }

  // (c) SimpleStatement positional/named values requiring the 3.x codec registry (finding #9):
  // java.util.Date has no codec in the 4.x default registry; the shim must serialize on the 3.x side.
  static void scSimpleStatementCodecs(Session s){
    guard("ssv", () -> {
      Date dt = new Date(1234567890000L);
      UUID u = UUID.fromString("00000000-0000-0000-0000-000000000009");
      s.execute(new SimpleStatement(
          "INSERT INTO "+KS+".types(pk,h,g,m) VALUES(?,?,?,?)", 900, dt, u, "sstext"));
      Row r = s.execute("SELECT h,g,m FROM "+KS+".types WHERE pk=900").one();
      o("ssv.h", r.getTimestamp("h").getTime());
      o("ssv.g", r.getUUID("g"));
      o("ssv.m", r.getString("m"));
      Map<String,Object> nv = new HashMap<>();
      nv.put("pk", 901); nv.put("m", "named-text"); nv.put("h", new Date(1000000L));
      s.execute(new SimpleStatement(
          "INSERT INTO "+KS+".types(pk,m,h) VALUES(:pk,:m,:h)", nv));
      Row r2 = s.execute("SELECT m,h FROM "+KS+".types WHERE pk=901").one();
      o("ssv.namedM", r2.getString("m"));
      o("ssv.namedH", r2.getTimestamp("h").getTime());
    });
  }

  // (d) setReadTimeoutMillis(0) must round-trip and not break execution (finding #10). 3.x treats 0
  // as "disable per-statement read timeout"; the shim must map it to Duration.ZERO (not drop it).
  static void scReadTimeoutZero(Session s){
    guard("rt0", () -> {
      Statement st = new SimpleStatement("SELECT release_version FROM system.local");
      st.setReadTimeoutMillis(0);
      o("rt0.value", st.getReadTimeoutMillis());
      ResultSet rs = s.execute(st);
      o("rt0.rv", rs.one().getString("release_version"));
    });
  }

  // (e) ExecutionInfo.getPagingState() non-null round-trip (finding #11): fetch page 1, capture the
  // typed PagingState, resume on a fresh statement, assert the remaining rows.
  static void scPagingStateRoundTrip(Session s){
    guard("pgstate", () -> {
      Statement st = new SimpleStatement("SELECT ck FROM "+KS+".pg WHERE pk=0");
      st.setFetchSize(10);
      ResultSet rs1 = s.execute(st);
      o("pgstate.firstPage", rs1.getAvailableWithoutFetching());
      PagingState ps = rs1.getExecutionInfo().getPagingState();
      o("pgstate.nonNull", ps != null);
      Statement st2 = new SimpleStatement("SELECT ck FROM "+KS+".pg WHERE pk=0");
      st2.setFetchSize(10);
      st2.setPagingState(ps);
      ResultSet rs2 = s.execute(st2);
      int remaining = 0, sum = 0;
      for (Row r : rs2) { remaining++; sum += r.getInt("ck"); }
      o("pgstate.remaining", remaining);
      o("pgstate.sum", sum);
    });
  }

  // (g) getConfiguration().getPolicies() must be non-null with populated defaults (finding #13).
  static void scPoliciesNonNull(Cluster c){
    guard("policies", () -> {
      com.datastax.driver.core.policies.Policies p = c.getConfiguration().getPolicies();
      o("policies.nonNull", p != null);
      o("policies.retryClass", p.getRetryPolicy().getClass().getSimpleName());
      o("policies.lbClass", p.getLoadBalancingPolicy().getClass().getSimpleName());
      o("policies.reconnNonNull", p.getReconnectionPolicy() != null);
      o("policies.specNonNull", p.getSpeculativeExecutionPolicy() != null);
    });
  }

  // (h) isExhausted()/iterator() across an empty trailing page (findings #12/#20). Row count is an
  // exact multiple of the fetch size, which can yield a non-null paging state followed by an empty
  // final page; the 3.x while(!isExhausted()) one() idiom must not call one() on the empty page.
  static void scEmptyTrailingPage(Session s){
    guard("emptypage", () -> {
      s.execute("CREATE TABLE IF NOT EXISTS "+KS+".ep(pk int, ck int, PRIMARY KEY(pk,ck))");
      PreparedStatement ps = s.prepare("INSERT INTO "+KS+".ep(pk,ck) VALUES(0,?)");
      for (int i=0;i<20;i++) s.execute(ps.bind(i));
      Statement st = new SimpleStatement("SELECT ck FROM "+KS+".ep WHERE pk=0");
      st.setFetchSize(10);
      ResultSet rs = s.execute(st);
      int sum = 0, nullHits = 0;
      while (!rs.isExhausted()) {
        Row r = rs.one();
        if (r == null) { nullHits++; break; }
        sum += r.getInt("ck");
      }
      o("emptypage.sum", sum);
      o("emptypage.nullHits", nullHits);
      Statement st2 = new SimpleStatement("SELECT ck FROM "+KS+".ep WHERE pk=0");
      st2.setFetchSize(10);
      int iter = 0; for (Row r : s.execute(st2)) iter++;
      o("emptypage.iterCount", iter);
    });
  }

  // (f) connect() on a closed Cluster must throw IllegalStateException (finding #15).
  static void scClosedClusterConnect(String host){
    guard("closedconnect", () -> {
      Cluster c2 = Cluster.builder().addContactPoint(host).build();
      c2.connect();
      c2.close();
      String type = "NONE";
      try {
        c2.connect();
      } catch (IllegalStateException e) {
        type = "IllegalStateException";
      }
      o("closedconnect.type", type);
    });
  }

  // (i) an ARBITRARY user-written 3.x LoadBalancingPolicy must be honored. Under the real 3.12.1
  // driver the policy is used directly; under the shim it is delegated to through the 4.x
  // load-balancing SPI (com.datastax.shim.bridge.Shim3xLoadBalancingPolicy). Either way the policy's
  // newQueryPlan is exercised (counter > 0), queries return rows, and the session/cluster close. The
  // policy also records the routing key + statement keyspace it observes for a prepared, fully-bound
  // partition-key query, proving the SAME routing key reaches the user policy under both engines. The
  // observations are booleans / small ints / hex strings so exact call counts (which legitimately
  // differ between the two engines) do not affect the diff.
  static final class CountingLbp implements com.datastax.driver.core.policies.LoadBalancingPolicy {
    static final java.util.concurrent.atomic.AtomicInteger PLAN_CALLS =
        new java.util.concurrent.atomic.AtomicInteger();
    static volatile String lastRoutingKeyHex = "NONE";
    static volatile String lastRoutingKeyspace = "NONE";
    private final RoundRobinPolicy delegate = new RoundRobinPolicy();
    public void init(Cluster cluster, java.util.Collection<Host> hosts){ delegate.init(cluster, hosts); }
    public HostDistance distance(Host host){ return HostDistance.LOCAL; }
    public java.util.Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement){
      PLAN_CALLS.incrementAndGet();
      if (statement != null) {
        ByteBuffer rk =
            statement.getRoutingKey(ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE);
        if (rk != null) {
          lastRoutingKeyHex = Bytes.toHexString(rk);
          lastRoutingKeyspace = String.valueOf(statement.getKeyspace());
        }
      }
      return delegate.newQueryPlan(loggedKeyspace, statement);
    }
    public void onAdd(Host host){ delegate.onAdd(host); }
    public void onUp(Host host){ delegate.onUp(host); }
    public void onDown(Host host){ delegate.onDown(host); }
    public void onRemove(Host host){ delegate.onRemove(host); }
    public void close(){ delegate.close(); }
  }

  static void scCustomLbp(String host){
    guard("customlbp", () -> {
      CountingLbp policy = new CountingLbp();
      Cluster c2 = Cluster.builder().addContactPoint(host).withLoadBalancingPolicy(policy).build();
      Session s2 = c2.connect();
      int rows = 0;
      Row r1 = s2.execute("SELECT release_version FROM system.local").one();
      if (r1 != null) rows++;
      Row r2 = s2.execute("SELECT key FROM system.local").one();
      if (r2 != null) rows++;
      // prepared, fully-bound partition key => the driver computes a routing key that the policy sees
      PreparedStatement pst = s2.prepare("SELECT id,name FROM "+KS+".t WHERE id=?");
      Row r3 = s2.execute(pst.bind(1)).one();
      if (r3 != null) rows++;
      o("customlbp.invoked", CountingLbp.PLAN_CALLS.get() > 0);
      o("customlbp.rows", rows);
      o("customlbp.routingKeyHex", CountingLbp.lastRoutingKeyHex);
      o("customlbp.routingKeyspace", CountingLbp.lastRoutingKeyspace);
      s2.close();
      o("customlbp.sessionClosed", s2.isClosed());
      c2.close();
      o("customlbp.clusterClosed", c2.isClosed());
    });
  }

  // (j) an ARBITRARY custom 3.x SpeculativeExecutionPolicy must be honored. Under real 3.12.1 the
  // policy is used directly; under the shim it is delegated through the 4.x specex SPI
  // (com.datastax.shim.bridge.Shim3xSpeculativeExecutionPolicy). The custom policy's newPlan
  // increments a static counter and returns a plan that never speculates (nextExecution == -1), so
  // the proof is timing-independent: after several IDEMPOTENT SELECTs, newPlan must have been
  // invoked under both engines.
  static final class CountingSpecex
      implements com.datastax.driver.core.policies.SpeculativeExecutionPolicy {
    static final java.util.concurrent.atomic.AtomicInteger NEW_PLAN_CALLS =
        new java.util.concurrent.atomic.AtomicInteger();
    public void init(Cluster cluster){}
    public SpeculativeExecutionPolicy.SpeculativeExecutionPlan newPlan(
        String loggedKeyspace, Statement statement){
      NEW_PLAN_CALLS.incrementAndGet();
      return new SpeculativeExecutionPolicy.SpeculativeExecutionPlan(){
        public long nextExecution(Host lastQueried){ return -1; }
      };
    }
    public void close(){}
  }

  static void scCustomSpecex(String host){
    guard("specex", () -> {
      Cluster c3 = Cluster.builder().addContactPoint(host)
          .withSpeculativeExecutionPolicy(new CountingSpecex()).build();
      Session s3 = c3.connect();
      for (int i = 0; i < 20; i++) {
        Statement st = new SimpleStatement("SELECT release_version FROM system.local");
        st.setIdempotent(true);
        s3.execute(st);
      }
      o("specex.customNewPlanInvoked", CountingSpecex.NEW_PLAN_CALLS.get() > 0);
      c3.close();
    });
  }

  // (k) PercentileSpeculativeExecutionPolicy + a PerHostPercentileTracker must be functional: the
  // policy's init registers the tracker on the Cluster, and the shim's build-time RequestTracker
  // feeds per-node latencies into it (mirroring 3.x cluster.register(LatencyTracker)). After a
  // generous warm-up of idempotent SELECTs across several short intervals, the tracker reports a
  // non-negative p99 latency on BOTH engines. Timing-dependent speculation is NOT asserted (a
  // single node cannot fire cross-node speculation).
  static void scPercentileSpecex(String host){
    guard("specexpct", () -> {
      PerHostPercentileTracker tracker = PerHostPercentileTracker.builder(1000)
          .withMinRecordedValues(10)
          .withInterval(20, java.util.concurrent.TimeUnit.MILLISECONDS)
          .build();
      PercentileSpeculativeExecutionPolicy policy =
          new PercentileSpeculativeExecutionPolicy(tracker, 99.0, 2);
      Cluster c4 = Cluster.builder().addContactPoint(host)
          .withSpeculativeExecutionPolicy(policy).build();
      Session s4 = c4.connect();
      Host h = c4.getMetadata().getAllHosts().iterator().next();
      boolean queryOk = true;
      for (int round = 0; round < 15; round++) {
        for (int i = 0; i < 50; i++) {
          Statement st = new SimpleStatement("SELECT release_version FROM system.local");
          st.setIdempotent(true);
          if (s4.execute(st).one() == null) queryOk = false;
        }
        try { Thread.sleep(30); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
      }
      boolean warmed = false;
      for (int attempt = 0; attempt < 60 && !warmed; attempt++) {
        if (tracker.getLatencyAtPercentile(h, null, null, 99.0) >= 0) warmed = true;
        else { try { Thread.sleep(30); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); } }
      }
      o("specex.trackerWarmed", warmed);
      o("specex.queryOk", queryOk);
      c4.close();
    });
  }

  // (l) withNettyOptions + withThreadingOptions must be functional (bridged onto 4.x's internal
  // Netty SPI). A custom 3.x NettyOptions flags its afterBootstrapInitialized hook; a custom 3.x
  // ThreadingOptions flags its createThreadFactory (delegating to super for the real factory). Under
  // real 3.12.1 both are invoked directly; under the shim both are invoked through the bridged
  // context. The cluster is closed so the injected io group / timer shut down (the harness must not
  // hang on their threads).
  static final class CountingNettyOptions extends NettyOptions {
    static volatile boolean bootstrapHookInvoked = false;
    @Override public void afterBootstrapInitialized(Bootstrap bootstrap) {
      bootstrapHookInvoked = true;
      super.afterBootstrapInitialized(bootstrap);
    }
  }

  static final class CountingThreadingOptions extends ThreadingOptions {
    static volatile boolean factoryUsed = false;
    @Override public java.util.concurrent.ThreadFactory createThreadFactory(
        String clusterName, String executorName) {
      factoryUsed = true;
      return super.createThreadFactory(clusterName, executorName);
    }
  }

  static void scNettyThreadingOptions(String host){
    guard("netty", () -> {
      Cluster c5 = Cluster.builder().addContactPoint(host)
          .withNettyOptions(new CountingNettyOptions())
          .withThreadingOptions(new CountingThreadingOptions())
          .build();
      Session s5 = c5.connect();
      Row r = s5.execute("SELECT release_version FROM system.local").one();
      o("netty.queryOk", r != null && r.getString("release_version") != null);
      o("netty.bootstrapHookInvoked", CountingNettyOptions.bootstrapHookInvoked);
      o("threading.factoryUsed", CountingThreadingOptions.factoryUsed);
      c5.close();
      o("netty.clusterClosed", c5.isClosed());
    });
  }

  // (m) withTimestampGenerator: a custom 3.x TimestampGenerator returning a FIXED microsecond value.
  // Real 3.12.1 applies it as the client timestamp; the shim applies it via the 4.x TimestampGenerator
  // SPI bridge. WRITETIME of a freshly-written cell must equal that value on both engines.
  static final class FixedTimestampGenerator implements com.datastax.driver.core.TimestampGenerator {
    static volatile boolean used = false;
    public long next() { used = true; return 1234567890000000L; }
  }

  static void scTimestamp(String host){
    guard("ts", () -> {
      Cluster c = Cluster.builder().addContactPoint(host)
          .withTimestampGenerator(new FixedTimestampGenerator()).build();
      Session s = c.connect();
      // fresh id not touched by other scenarios, so the small fixed timestamp is not shadowed
      s.execute("INSERT INTO "+KS+".t(id,name) VALUES(8888,'ts-test')");
      Row r = s.execute("SELECT WRITETIME(name) AS wt FROM "+KS+".t WHERE id=8888").one();
      o("ts.writetime", r==null?"NOROW":r.getLong("wt"));
      o("ts.generatorUsed", FixedTimestampGenerator.used);
      c.close();
    });
  }

  // (n) withRetryPolicy: a custom 3.x RetryPolicy whose init(Cluster) sets a static flag (decisions
  // RETHROW). init is invoked at cluster/session startup on both engines (a healthy node never
  // triggers the failure paths). Also verifies per-request setRetryPolicy/getRetryPolicy round-trip.
  static final class FlagRetryPolicy implements com.datastax.driver.core.policies.RetryPolicy {
    static volatile boolean initInvoked = false;
    public void init(Cluster cluster) { initInvoked = true; }
    public RetryPolicy.RetryDecision onReadTimeout(
        Statement s, ConsistencyLevel cl, int req, int rec, boolean dr, int nb) {
      return RetryPolicy.RetryDecision.rethrow();
    }
    public RetryPolicy.RetryDecision onWriteTimeout(
        Statement s, ConsistencyLevel cl, WriteType wt, int req, int rec, int nb) {
      return RetryPolicy.RetryDecision.rethrow();
    }
    public RetryPolicy.RetryDecision onUnavailable(
        Statement s, ConsistencyLevel cl, int req, int alive, int nb) {
      return RetryPolicy.RetryDecision.rethrow();
    }
    public RetryPolicy.RetryDecision onRequestError(
        Statement s, ConsistencyLevel cl, DriverException e, int nb) {
      return RetryPolicy.RetryDecision.rethrow();
    }
    public void close() {}
  }

  static void scRetry(String host){
    guard("retry", () -> {
      Cluster c = Cluster.builder().addContactPoint(host)
          .withRetryPolicy(new FlagRetryPolicy()).build();
      Session s = c.connect();
      s.execute("SELECT release_version FROM system.local").one();
      o("retry.clusterInitInvoked", FlagRetryPolicy.initInvoked);
      Statement st = new SimpleStatement("SELECT release_version FROM system.local");
      FlagRetryPolicy p = new FlagRetryPolicy();
      st.setRetryPolicy(p);
      o("retry.perRequestRoundTrip", st.getRetryPolicy() == p);
      c.close();
    });
  }

  // (o) withReconnectionPolicy: the configured instance round-trips through
  // getConfiguration().getPolicies().getReconnectionPolicy() on both engines.
  static void scReconnection(String host){
    guard("recon", () -> {
      ConstantReconnectionPolicy rp = new ConstantReconnectionPolicy(1000L);
      Cluster c = Cluster.builder().addContactPoint(host)
          .withReconnectionPolicy(rp).build();
      c.connect().execute("SELECT release_version FROM system.local").one();
      o("recon.roundTrip", c.getConfiguration().getPolicies().getReconnectionPolicy() == rp);
      c.close();
    });
  }

  // (p) Cluster.register/unregister(Host.StateListener): registering fires onRegister(cluster) and
  // unregistering fires onUnregister(cluster) on both engines (wiring proof; node up/down events
  // aren't induced on a single healthy node).
  static final class FlagHostListener implements Host.StateListener {
    static volatile boolean onReg = false, onUnreg = false;
    public void onAdd(Host host) {}
    public void onUp(Host host) {}
    public void onDown(Host host) {}
    public void onRemove(Host host) {}
    public void onRegister(Cluster cluster) { onReg = true; }
    public void onUnregister(Cluster cluster) { onUnreg = true; }
  }

  static void scHostListener(Cluster c){
    guard("hostlistener", () -> {
      FlagHostListener hl = new FlagHostListener();
      c.register(hl);
      o("hostlistener.onRegister", FlagHostListener.onReg);
      c.unregister(hl);
      o("hostlistener.onUnregister", FlagHostListener.onUnreg);
    });
  }

  // (q) Cluster.register(SchemaChangeListener) — functional proof: after registering, creating a new
  // table fires onTableAdded on both engines (schema events are async, so poll with a bounded wait).
  // The table lives in the freshly-recreated parity keyspace, so it is genuinely new each run.
  static final class FlagSchemaListener extends SchemaChangeListenerBase {
    static volatile boolean onReg = false, tableAdded = false;
    @Override public void onTableAdded(TableMetadata table) { tableAdded = true; }
    @Override public void onRegister(Cluster cluster) { onReg = true; }
  }

  static void scSchemaListener(Cluster c, Session s){
    guard("schemalistener", () -> {
      FlagSchemaListener sl = new FlagSchemaListener();
      c.register(sl);
      s.execute("CREATE TABLE IF NOT EXISTS "+KS+".schemalistener_t(id int PRIMARY KEY, v text)");
      long deadline = System.currentTimeMillis() + 8000;
      while (!FlagSchemaListener.tableAdded && System.currentTimeMillis() < deadline) {
        try { Thread.sleep(100); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
      }
      o("schemalistener.onRegister", FlagSchemaListener.onReg);
      o("schemalistener.tableAdded", FlagSchemaListener.tableAdded);
      c.unregister(sl);
    });
  }

  // (r) QueryOptions node-refresh knobs round-trip through getConfiguration() (and are mapped to the
  // 4.x metadata topology-event debouncer under the hood).
  static void scQueryOptions(String host){
    guard("qopts", () -> {
      QueryOptions qo = new QueryOptions();
      qo.setRefreshNodeIntervalMillis(3000);
      qo.setMaxPendingRefreshNodeRequests(7);
      Cluster c = Cluster.builder().addContactPoint(host).withQueryOptions(qo).build();
      c.connect().execute("SELECT release_version FROM system.local").one();
      QueryOptions got = c.getConfiguration().getQueryOptions();
      o("qopts.refreshNodeInterval", got.getRefreshNodeIntervalMillis());
      o("qopts.maxPendingRefreshNode", got.getMaxPendingRefreshNodeRequests());
      c.close();
    });
  }

  // (s) PoolingOptions unsupported knobs: no-throw + getter round-trip (the shim logs a one-time WARN
  // via SLF4J; the value is retained).
  static void scPooling(){
    guard("pooling", () -> {
      PoolingOptions po = new PoolingOptions();
      po.setNewConnectionThreshold(HostDistance.LOCAL, 42);
      o("pooling.newConnThresholdRoundTrip", po.getNewConnectionThreshold(HostDistance.LOCAL) == 42);
      po.setIdleTimeoutSeconds(123);
      o("pooling.idleTimeoutRoundTrip", po.getIdleTimeoutSeconds() == 123);
    });
  }

  // (t) Metrics report REAL values from the driver: the registry is non-null, the cql-requests timer
  // has recorded the queries run on this cluster, and the JMX flag round-trips (default true). Metric
  // VALUES are non-deterministic, so only booleans are asserted.
  static void scMetrics(Cluster c, Session s){
    guard("metrics", () -> {
      for (int i = 0; i < 5; i++) {
        s.execute("SELECT release_version FROM system.local").one();
      }
      Metrics m = c.getMetrics();
      o("metrics.registryNonNull", m.getRegistry() != null);
      o("metrics.requestsRecorded", m.getRequestsTimer().getCount() > 0);
      o("metrics.jmxFlag", c.getConfiguration().getMetricsOptions().isJMXReportingEnabled());
    });
  }

  // (u) withEndPointFactory: a custom 3.x EndPointFactory whose init(Cluster) sets a static flag
  // (create() delegates to DefaultEndPointFactory). Bridged onto 4.x via ShimTopologyMonitor's
  // buildNodeEndPoint override. On a single node there are no peer rows, so create() isn't exercised;
  // the suite asserts wiring (no-throw build, init invoked, query works) — identical on both engines.
  static final class FlagEndPointFactory implements com.datastax.driver.core.EndPointFactory {
    static volatile boolean initInvoked = false;
    private final DefaultEndPointFactory delegate = new DefaultEndPointFactory();
    public void init(Cluster cluster) { initInvoked = true; delegate.init(cluster); }
    public EndPoint create(Row row) { return delegate.create(row); }
  }

  static void scEndPointFactory(String host){
    guard("epf", () -> {
      Cluster c = Cluster.builder().addContactPoint(host)
          .withEndPointFactory(new FlagEndPointFactory()).build();
      Session s = c.connect();
      boolean queryOk = s.execute("SELECT release_version FROM system.local").one() != null;
      o("epf.buildNoThrow", true);
      o("epf.initInvoked", FlagEndPointFactory.initInvoked);
      o("epf.queryOk", queryOk);
      c.close();
    });
  }
}
