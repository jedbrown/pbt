from __future__ import with_statement, print_function
import sqlite3
import os.path


class Node(object):
    def __init__(self, id, dir, body, mtime, elapsed, type):
        self.id = id
        self.dir = dir
        self.body = body
        self.mtime = mtime
        self.elapsed = elapsed
        self.type = type

    def __repr__(self):
        return 'Node(%(id)r,%(dir)r,%(body)r,%(mtime)r,%(elapsed)r,%(type)r)' % self.__dict__

    def __conform__(self, protocol):
        if protocol is sqlite3.PrepareProtocol:
            return '%d;'


class Database(object):
    def __init__(self, filename=':memory:'):
        self.conn = sqlite3.connect(filename)
        self.conn.row_factory = sqlite3.Row
        with self.conn as conn:
            cur = conn.cursor()
            # Assume, for now, that no tables exist
            cur.executescript('''
            CREATE TABLE Node (
                NodeId INTEGER PRIMARY KEY,
                Dir INTEGER,
                Body BLOB,
                MTime INTEGER,
                NodeTypeId INTEGER NOT NULL,
                FOREIGN KEY (NodeTypeId) REFERENCES NodeType,
                UNIQUE (Dir, Body)
            );
            CREATE TABLE Edge (
                Src INTEGER NOT NULL,
                Dest INTEGER NOT NULL,
                --
                InheritTags INT1 NOT NULL,
                PRIMARY KEY (Src, Dest),
                FOREIGN KEY (Src) REFERENCES Node(NodeId),
                FOREIGN KEY (Dest) REFERENCES Node(NodeId)
            );
            CREATE TABLE Tag (
                TagId INTEGER PRIMARY KEY,
                Name VARCHAR(64) NOT NULL,
                --
                Description VARCHAR(4096),
                UNIQUE(Name)
            );
            CREATE TABLE Tag2Node (
                TagId INTEGER NOT NULL,
                NodeId INTEGER NOT NULL,
                PRIMARY KEY (TagId, NodeId),
                FOREIGN KEY (TagId) REFERENCES Tag(TagId),
                FOREIGN KEY (NodeId) REFERENCES Node(NodeId)
            );
            CREATE TABLE NodeType (
                NodeTypeId INTEGER PRIMARY KEY,
                Name VARCHAR(64) NOT NULL,
                UNIQUE(Name)
            );
            CREATE TABLE Config (
                ConfigId INTEGER PRIMARY KEY,
                MachineName VARCHAR(32) NOT NULL,
                PetscVersionHg CHARACTER(40) NOT NULL, -- SHA1
                PetscArch VARCHAR(32) NOT NULL,
                ConfigTime DATETIME NOT NULL,
                -- non-identifying attributes
                ConfigLog BLOB,
                UNIQUE (MachineName, PetscVersionHg, PetscArch, ConfigTime)
            );
            CREATE TABLE Result (
                NodeId INTEGER NOT NULL,
                ConfigId INTEGER NOT NULL,
                CreateTime INTEGER NOT NULL,
                -- non-identifying
                ReturnCode INTEGER NOT NULL,
                StartTime INTEGER NOT NULL,
                Elapsed REAL NOT NULL,
                Stdout BLOB,
                Stderr BLOB,
                PRIMARY KEY (NodeId, ConfigId, CreateTime),
                FOREIGN KEY (NodeId) REFERENCES Node(NodeId),
                FOREIGN KEY (ConfigId) REFERENCES Config(ConfigId)
            );
            CREATE TEMP TABLE RequestedNode (
                NodeId INTEGER PRIMARY KEY,
                --
                FOREIGN KEY (NodeId) REFERENCES Node(NodeId)
            );
            CREATE TEMP TABLE RequestedEdge (
                Src INTEGER NOT NULL,
                Dest INTEGER NOT NULL,
                --
                Depth INTEGER NOT NULL,
                PRIMARY KEY (Src, Dest),
                FOREIGN KEY (Src) REFERENCES Node(NodeId),
                FOREIGN KEY (Dest) REFERENCES Node(NodeId)
            );
            ''')
            conn.commit()
            cur.executemany('INSERT INTO NodeType (Name) VALUES (?)',
                            [('DIRECTORY',), ('FILE',), ('RULE',), ('TEST',)])

            class NodeType(object):
                pass
            self.nodetype = NodeType()
            for row in cur.execute('SELECT NodeTypeId, Name FROM NodeType'):
                setattr(self.nodetype, row[1], row[0])
        self.dirs = dict()

    def commit(self):
        self.conn.commit()

    def full_path(self, nodeid):
        if nodeid == 0:
            return '/'
        cur = self.conn.cursor()
        cur.execute('SELECT Dir, Body FROM Node WHERE NodeId = ?', (nodeid,))
        dirid, basename = cur.fetchone()
        return os.path.join(self.full_path(dirid), basename)

    def add_node(self, dir, body, nodetype, mtime=0):
        cur = self.conn.cursor()
        cur.execute('INSERT INTO Node (Dir, Body, NodeTypeId, MTime) VALUES (?,?,?,?)', (dir, body, nodetype, mtime))
        return cur.lastrowid

    def nodes(self):
        return self.conn.execute('SELECT NodeId, Dir, Body, NodeTypeId FROM Node')

    def edges(self):
        return self.conn.execute('SELECT Src, Dest, InheritTags FROM Edge')

    def requested_edges(self):
        return self.conn.execute('SELECT Src, Dest, Depth FROM RequestedEdge')

    def add_edge(self, srcid, destid, inherittags=False):
        cur = self.conn.cursor()
        cur.execute('INSERT OR REPLACE INTO Edge (Src, Dest, InheritTags) VALUES (?,?,?)', (srcid, destid, inherittags))

    def add_edges(self, edgelist):
        self.conn.executemany('INSERT OR REPLACE INTO Edge (Src, Dest, InheritTags) VALUES (?,?,?)', edgelist)

    def incoming(self, destids):
        return self.conn.executemany('SELECT (Src, Dest, InheritTags) FROM Edge WHERE Dest=?', ((dest,) for dest in destids))

    def outgoing(self, srcids):
        return self.conn.executemany('SELECT (Src, Dest, InheritTags) FROM Edge WHERE Src=?', ((src,) for src in srcids))

    def dir(self, dirname):
        dirname = os.path.realpath(dirname)
        dirid = self.dirs.get(dirname, None)
        if dirid is None:
            if dirname == '/':
                return 0
            parent, base = os.path.split(dirname)
            parentid = self.dir(parent)
            dirid = self.add_node(parentid, base, self.nodetype.DIRECTORY)
        return dirid

    def add_file(self, dirid, basename, ghost=False):
        if ghost:
            mtime = None
        else:
            dirname = self.full_path(dirid)
            mtime = os.path.getmtime(os.path.join(dirname, basename))
        return self.add_node(dirid, basename, self.nodetype.FILE, mtime)

    def add_test(self, dirid, testname):
        return self.add_node(dirid, testname, self.nodetype.TEST, mtime=0)

    def add_rule(self, dirid, rule, srcs, dests):
        ruleid = self.add_node(dirid, rule, self.nodetype.RULE, mtime=0)
        self.add_edges([(src, ruleid, False) for src in srcs] + [(
            ruleid, dest, False) for dest in dests])
        return ruleid

    def update_node(self, nodeid, mtime):
        self.conn.execute(
            'UPDATE Node SET MTime=? WHERE NodeId=?', (mtime, nodeid))

    def request(self, targets=[], names=[]):
        self._new_requests = True
        cur = self.conn.cursor()
        self.conn.executemany('INSERT INTO RequestedNode (NodeId) VALUES (?)',
                              ((t,) for t in targets))
        self.conn.executemany('INSERT OR IGNORE INTO RequestedNode (NodeId)'
                              '  SELECT t.NodeId FROM Node t WHERE t.Body = ?',
                              ((n,) for n in names))

    def process_requests(self):
        if not self._new_requests:
            return
        cur = self.conn.cursor()
        # Close the graph
        cur.executescript('''
        INSERT INTO RequestedEdge (Src, Dest, Depth) SELECT e.Src, e.Dest, 1 FROM Edge e
            JOIN RequestedNode n ON e.Dest = n.NodeId;
        ''')
        # Extend to closure of dependencies
        while True:
            result = cur.execute('INSERT OR IGNORE INTO RequestedEdge (Src, Dest, Depth) SELECT e.Src, re.Src, re.Depth+1 FROM Edge e JOIN RequestedEdge re ON e.Dest = re.Src')
            if result.rowcount == 0:
                break
        # Drop edges that are already up to date
        while True:
            result = cur.execute('DELETE FROM RequestedEdge'
                                 '  WHERE rowid IN'
                                 '    (SELECT re.rowid FROM RequestedEdge re'
                                 '        JOIN Node src ON src.NodeId = re.Src'
                                 '        JOIN Node dest ON dest.NodeId = re.Dest'
                                 '      WHERE src.MTime < dest.MTime)')
            print('Removed %d up-to-date edges' % (result.rowcount))
            if result.rowcount == 0:
                break
        # Edges from updated sources (but the rule remains)
        result = cur.execute('DELETE FROM RequestedEdge'
                             '  WHERE Src NOT IN'
                             '      (SELECT re.Dest FROM RequestedEdge re)'
                             '    AND Src IN (SELECT n.NodeId FROM Node n WHERE n.NodeId = Src AND n.NodeTypeId = ?)', (self.nodetype.FILE,))
        self._new_requests = False

    def requested_rules(self):
        self.process_requests()
        allrules = self.conn.execute('SELECT COUNT(DISTINCT r.NodeId) FROM Node r JOIN RequestedEdge re WHERE r.NodeId = re.Src AND r.NodeTypeId = ?', (self.nodetype.RULE,))
        for rule in allrules:
            return rule[0]

    def runnable_rules(self):
        self.process_requests()
        cur = self.conn.cursor()
        ready = cur.execute('SELECT r.NodeId, r.Dir, r.Body, re.Depth FROM Node r'
                            '    JOIN RequestedEdge re ON r.NodeId = re.Src'
                            '  WHERE r.NodeTypeId = ? AND r.NodeId NOT IN'
                            '    (SELECT re2.Dest FROM RequestedEdge re2 WHERE re2.Depth > re.Depth)',
                            (self.nodetype.RULE,))
        for item in ready:
            yield item
