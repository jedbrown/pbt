from __future__ import with_statement, print_function
import os
import imp
import cPickle
import json


def rule_encode(rule):
    return buffer(cPickle.dumps(rule))


def rule_decode(rule):
    return cPickle.loads(str(rule))


class CBuilder(object):
    def __init__(self, compiler=None, cflags=None, libs=None):
        if compiler is None:
            compiler = 'gcc'
        self.compiler = compiler
        if cflags is None:
            cflags = ['-Wall']
        self.cflags = cflags
        if libs is None:
            libs = []
        self.libs = libs

    def prepend(self, cflags=[], libs=[]):
        return CBuilder(self.compiler, cflags + self.cflags, libs + self.libs)

    def get_compile(self, src):
        srco = src + '.o'
        return srco, [self.compiler, '-c', src, '-o', srco] + self.cflags

    def get_link(self, exename, objs, extralibs):
        return [self.compiler, '-o', exename] + self.cflags + objs + extralibs + self.libs


class Environment(object):
    def __init__(self):
        self._buildbysuffix = dict()
        self.register_builder('.c', CBuilder())

    def register_builder(self, suffix, builder):
        self._buildbysuffix[suffix] = builder

    def get_builder(self, source):
        suffix = os.path.splitext(source)[1]
        return self._buildbysuffix[suffix]


class ExeHelper(object):
    def __init__(self, reghelper, exeid):
        self._reghelper = reghelper
        self._exeid = exeid

    def test(self, testname, func, args):
        return self._reghelper.test(self._exeid, testname, func, args)


class RegHelper(object):
    def __init__(self, env, db, dirid):
        self._db = db
        self._dirid = dirid
        self._env = env

    def executable(self, sources, exename=None, extralibs=[]):
        if isinstance(sources, str):
            sources = [sources]
        if exename is None:
            exename = os.path.splitext(sources[0])[0]
        mainbuilder = self._env.get_builder(sources[0])
        objids = []
        objnames = []
        for src in sources:
            srcid = self._db.add_file(self._dirid, src)
            builder = self._env.get_builder(src)
            objname, rule = builder.get_compile(src)
            objid = self._db.add_file(self._dirid, objname, ghost=True)
            qrule = rule_encode(rule)
            self._db.add_rule(self._dirid, qrule, [srcid], [objid])
            objids.append(objid)
            objnames.append(objname)
        link = mainbuilder.get_link(exename, objnames, extralibs)
        qlink = rule_encode(link)
        exeid = self._db.add_file(self._dirid, exename, ghost=True)
        self._db.add_rule(self._dirid, qlink, objids, [exeid])
        return ExeHelper(self, exeid)

    def test(self, exeid, testname, func, args):
        testid = self._db.add_test(self._dirid, testname)
        rule = rule_encode((testname, func, args))
        self._db.add_rule(self._dirid, rule, [exeid], [testid])


class Registrar(object):
    def __init__(self, env, db):
        self._env = env
        self._db = db

    def walk(self, path, pbtfile='pbtfile.py'):
        for root, dirs, files in os.walk('tmp'):
            if pbtfile not in files:
                raise RuntimeError('No pbtfile.py at %s' % root)
            sub = imp.load_source('pbt_register', os.path.join(root, pbtfile))
            dirid = self._db.dir(root)
            reg = RegHelper(self._env, self._db, dirid)
            sub.register(reg)
