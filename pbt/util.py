from __future__ import with_statement, print_function
import os
from .core import rule_decode


def graph_dot(db):
    from cStringIO import StringIO
    g = StringIO()
    g.write('digraph G {\n')
    for nodeid, dir, body, nodetype in db.nodes():
        if nodetype == db.nodetype.RULE:
            body = rule_decode(body)
        g.write('  %d [label="%s"];\n' % (nodeid, body))
    for src, dest, inherittags in db.edges():
        g.write('  %d -> %d;\n' % (src, dest))
    for src, dest, length in db.requested_edges():
        g.write('  %d -> %d [label=%d, color=red, fontcolor=red];\n' %
                (src, dest, length))
    g.write('}\n')
    return g.getvalue()


def plot_dot(graph, pngfile=None):
    import subprocess
    import tempfile
    import os
    if pngfile is None:
        pngfile = tempfile.mktemp(suffix='.png')
        remove = True
    try:
        args = ['dot', '-Tpng', "-o%s" % (pngfile,)]
        p = subprocess.Popen(args, stdin=subprocess.PIPE)
        p.communicate(graph)
        ret = p.wait()
        if ret:
            raise subprocess.CalledProcessError(ret, args)
        subprocess.check_call(['qiv', pngfile])
    finally:
        if remove:
            os.remove(pngfile)


def mkdir_p(path):
    import errno
    try:
        os.makedirs(path)
    except OSError as exc:      # Python >2.5
        if not (exc.errno == errno.EEXIST and os.path.isdir(path)):
            raise
