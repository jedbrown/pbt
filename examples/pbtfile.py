from __future__ import with_statement, print_function

import os.path
import sys
thisdir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(thisdir))
import pbt
import imp

pbt.util.mkdir_p('tmp')
open(os.path.join('tmp', 'pbtfile.py'), 'w').write('''
def do_test(testname, args):
    print('Test %r args %r' % (testname, args))

def register(reg):
    d4 = reg.executable('d4.c')
    d4.test('d4_check', do_test, ('two','args'))
''')
open(os.path.join('tmp', 'd4.c'), 'w').write('''
#include <stdio.h>

int main(int argc, char **argv) {
  return printf("%s: Hello %d\n", argv[0], argc);
}
''')


def add_rule(db, dirid, rule, srcs, dests):
    import cPickle
    return db.add_rule(dirid, cPickle.dumps(rule), srcs, dests)


db = pbt.Database()
env = pbt.Environment()
reg = pbt.Registrar(env, db)
reg.walk('tmp')
if False:                       # create a phony dependency graph
    dirid = db.dir('/tmp/jed')
    a1 = db.add_file(dirid, 'a1')
    b1 = db.add_file(dirid, 'b1')
    c1 = db.add_file(dirid, 'c1')
    d1 = db.add_file(dirid, 'd1', ghost=True)
    a2 = db.add_file(dirid, 'a2')
    b2 = db.add_file(dirid, 'b2')
    c2 = db.add_file(dirid, 'c2')
    d2 = db.add_file(dirid, 'd2', ghost=True)
    e = db.add_file(dirid, 'e', ghost=True)
    f1 = db.add_file(dirid, 'f1', ghost=True)
    f2 = db.add_file(dirid, 'f2', ghost=True)
    rule1 = add_rule(db, dirid, '(a1,b1,c1) -> d1', [a1, b1, c1], [d1])
    rule2 = add_rule(db, dirid, '(a2,b2,c2) -> d2', [a2, b2, c2], [d2])
    rulee = add_rule(db, dirid, '(d1,d2) -> e', [d1, d2], [e])
    rulef = add_rule(
        db, dirid, '(b2,c2,d2) -> (f1,f2)', [b2, c2, d2], [f1, f2])
    db.request(names=['d4', 'e'])
else:
    db.request(names=['d4_check'])
print('Number of requested rules: %d' % db.requested_rules())
for ruleid, dirid, rule, depth in db.runnable_rules():
    print('Runnable: id=%d %r' % (ruleid, pbt.core.rule_decode(rule)))
dot = pbt.graph_dot(db)
pbt.plot_dot(dot)
