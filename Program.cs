using Microsoft.Data.Sqlite;
using Dapper;

Creation();
ParallelCreation();
ConcurrentCreation();
UpdateAtSource();
UpdateAtDestination();
DeleteAtSource();
DeleteAtDest();
ConcurrentUpdate();
CreateAfterDelete();

Thread.Sleep(100);
Directory.GetFiles(".", "*.db*").ToList().ForEach(o => File.Delete(o));

void Creation() => Test("Create",
                        WithDatabase(out var db1), WithDatabase(out var db2),
                        Create(db1, 1, "One", "One"),
                        Sync(db1, db2));

void ParallelCreation() => Test("Parallel Create",
                                 WithDatabase(out var db1), WithDatabase(out var db2), WithDatabase(out var db3),
                                 Create(db1, 1, "One", "One"),
                                 Create(db2, 2, "Two", "Two"),
                                 Create(db3, 3, "Three", "Three"),
                                 Sync(db1, db2, db3));

void ConcurrentCreation() => Test("Concurrent Create",
                                  WithDatabase(out var db1), WithDatabase(out var db2),
                                  Create(db1, 1, "One", "One"),
                                  Create(db2, 1, "Two", "Two"),
                                  Sync(db1, db2));

void ConcurrentUpdate() => Test("Concurrent Update",
                                WithDatabase(out var db1), WithDatabase(out var db2),
                                Create(db1, 1, "One", "One"),
                                Create(db2, 1, "Two", "Two"),
                                Sync(db1, db2),
                                Update(db1, 1, "1New", "1New"),
                                Update(db2, 1, "2New", "2New"),
                                Sync(db1, db2));

void UpdateAtSource() => Test("Update From Source",
                                WithDatabase(out var db1), WithDatabase(out var db2),
                                Create(db1, 1, "One", "One"),
                                Sync(db1, db2),
                                Update(db1, 1, "New", "New"),
                                Sync(db1, db2));

void UpdateAtDestination() => Test("Update From Destination",
                                     WithDatabase(out var db1), WithDatabase(out var db2),
                                     Create(db1, 1, "One", "One"),
                                     Sync(db1, db2),
                                     Update(db2, 1, "New", "New"),
                                     Sync(db1, db2));

void DeleteAtSource() => Test("Delete From Source",
                                WithDatabase(out var db1), WithDatabase(out var db2),
                                Create(db1, 1, "One", "One"),
                                Sync(db1, db2),
                                Delete(db1, 1),
                                Sync(db1, db2));

void DeleteAtDest() => Test("Delete From Dest",
                              WithDatabase(out var db1), WithDatabase(out var db2),
                              Create(db1, 1, "One", "One"),
                              Sync(db1, db2),
                              Delete(db1, 1),
                              Sync(db1, db2));

void CreateAfterDelete() => Test("Create After Delete",
                                 WithDatabase(out var db1), WithDatabase(out var db2),
                                 Create(db1, 1, "One", "One"),
                                 Sync(db1, db2),
                                 Delete(db1, 1),
                                 Sync(db1, db2),
                                 Create(db1, 1, "New", "New"),
                                 Sync(db1, db2));

void Test(string name, params TestElement[] elements)
{
    try
    {
        DoElements(elements);
        void DoElements(TestElement[] e)
        {
            foreach (var element in e)
            {
                switch (element)
                {
                    case CompoundTestElement cte:
                        DoElements(cte.elements);
                        break;
                    case CreateRecordElement c:
                        CreateImpl(c.conn, c.Id, c.Body, c.Title);
                        break;
                    case SyncElement s:
                        //Sync
                        SyncOneway(s.db1, s.db2);
                        SyncOneway(s.db2, s.db1);
                        //Reconcile
                        var one = Read(s.db1);
                        var two = Read(s.db2);
                        if (!one.SequenceEqual(two)) throw new ReconciliationFailedException(one, two);
                        break;
                    case UpdateElement u:
                        UpdateImpl(u.conn, u.Id, u.Body, u.Title);
                        break;
                    case DeleteElement d:
                        DeleteImpl(d.conn, d.Id);
                        break;
                }
            }
        }
        Console.WriteLine($"✅ {name}");
    }
    catch (ReconciliationFailedException ex)
    {
        Console.WriteLine($"❌ {name}");
        Console.WriteLine("Left");
        ex.Left.ForEach(Console.WriteLine);
        Console.WriteLine("\nRight");
        ex.Right.ForEach(Console.WriteLine);
    }

    foreach (var conn in elements.Where(o => o is DatabaseElement).Cast<DatabaseElement>().Select(o => o.conn))
    {
        conn.Execute("SELECT crsql_finalize();");
        conn.Close();
        conn.Dispose();
    }
}

TestElement WithDatabase(out SqliteConnection conn)
{
    //conn = new SqliteConnection($"Data Source={Guid.NewGuid()}.db");
    conn = new SqliteConnection($"Data Source=");
    conn.Execute("PRAGMA journal_mode=wal");
    conn.LoadExtension("crsqlite");
    conn.Execute("CREATE TABLE Posts(Id INTEGER PRIMARY KEY, ParentId INTEGER, Body TEXT, Title TEXT);");
    conn.Execute("CREATE INDEX Posts_ParentId on Posts(ParentId);");
    conn.Execute("SELECT crsql_as_crr('Posts');");
    return new DatabaseElement(conn);
}
TestElement Create(SqliteConnection conn, int Id, string Body, string Title) => new CreateRecordElement(conn, Id, Body, Title);
T[] RotateRight<T>(T[] t, int shift = 1) => t[^shift..].Concat(t[..^shift]).ToArray();
TestElement Sync(params SqliteConnection[] conns) => new CompoundTestElement(conns.Zip(RotateRight(conns), (l, r) => new SyncElement(l, r)).ToArray());
TestElement Update(SqliteConnection db, int Id, string Body, string Title) => new UpdateElement(db, Id, Body, Title);
TestElement Delete(SqliteConnection db, int Id) => new DeleteElement(db, Id);

void SyncOneway(SqliteConnection source, SqliteConnection destination)
{
    var changes = source.Query<CrSqlChange>("SELECT [table], pk, cid, val, col_version, db_version, COALESCE(site_id, crsql_siteid()) site_id FROM crsql_changes").ToList();
    foreach (var change in changes)
    {
        destination.Execute("INSERT INTO crsql_changes ([table], pk, cid, val, col_version, db_version, site_id) VALUES (@table, @pk, @cid, @val, @col_version, @db_version, @site_id)", change);
    }
}

void UpdateImpl(SqliteConnection conn, int Id, string Body, string Title) => conn.Execute("Update Posts SET Body = @Body, Title = @Title WHERE Id = @Id", new Post(Id, Body, Title));
void CreateImpl(SqliteConnection conn, int Id, string Body, string Title) => conn.Execute("INSERT INTO Posts (Id, Body, Title) VALUES (@Id, @Body, @Title);", new Post(Id, Body, Title));
void DeleteImpl(SqliteConnection conn, int Id) => conn.Execute("DELETE FROM Posts WHERE Id = @Id", new { Id });
List<Post> Read(SqliteConnection conn) => conn.Query<Post>("SELECT * FROM Posts").OrderBy(o => o.Id).ToList();

record TestElement;
record CompoundTestElement(params TestElement[] elements) : TestElement;
record CreateRecordElement(SqliteConnection conn, int Id, string Body, string Title) : TestElement;
record SyncElement(SqliteConnection db1, SqliteConnection db2) : TestElement;
record ReconcileElement(SqliteConnection db1, SqliteConnection db2) : TestElement;
record DatabaseElement(SqliteConnection conn) : TestElement;
record UpdateElement(SqliteConnection conn, int Id, string Body, string Title) : TestElement;
record DeleteElement(SqliteConnection conn, int Id) : TestElement;

public record CrSqlChange(string table, byte[] pk, string cid, string val, string col_version, string db_version, byte[] site_id)
{
    //For Dapper <3
    private CrSqlChange() : this(default!, default!, default!, default!, default!, default!, default!) { }
}

public record Post(int Id, string Body, string Title)
{
    //For Dapper <3
    private Post() : this(default!, default!, default!) { }
}

class ReconciliationFailedException : Exception
{
    public List<Post> Left { get; set; }
    public List<Post> Right { get; set; }

    public ReconciliationFailedException(List<Post> left, List<Post> right)
    {
        Left = left;
        Right = right;
    }
}