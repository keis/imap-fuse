#!/usr/bin/env python

#
#	Copyright (C) 2009  David Keijser  <keijser@gmail.com>
#

# usage:
# ./imap.py -o server=mysever,username=keijser,password=cake -d mnt/
# (-d is for delicious spam)

# TODO:
# support more stuff (move, create/delete directory, etc)
# work around timeouts
# login solution that does not put my password in /proc
# get status and other stuff that does not easily map to fs actions through unix-socket in mount dir?
# persistant storage of messages (and for the other caches to?)

import os, stat, errno, time, imaplib, fuse
fuse.fuse_python_api = (0, 2)
from fuse import Fuse

class Stat0(fuse.Stat):
	def __init__(self):
		self.st_mode = 0
		self.st_ino = 0
		self.st_dev = 0
		self.st_nlink = 0
		self.st_uid = 0
		self.st_gid = 0
		self.st_size = 0
		self.st_atime = 0
		self.st_mtime = 0
		self.st_ctime = 0

def parse(input):
	assert isinstance(input, basestring), '%s is not a string' % (str(input),)
	in_quote = False 
	last_pos = 0
	stack,out = [],[]
	push,pop = stack.append,stack.pop
	def add(x):
		out.append(x)

	for i in range(len(input)):
		if input[i] == '"':
			in_quote = not in_quote
		elif not in_quote and input[i] == '(':
			add(input[last_pos:i].replace('"', ''))
			push(out)
			out = []
			last_pos = i+1
		elif not in_quote and input[i] == ')':
			add(input[last_pos:i].replace('"', ''))
			tmp = out
			out = pop()
			add([x for x in tmp if x != ''])
			last_pos = i+1
		elif not in_quote and input[i] == ' ':
			add(input[last_pos:i].replace('"',''))
			last_pos = i+1
	add(input[last_pos:].replace('"',''))
	return [x for x in out if x != '']
		
def padr(l, t, pad=None):
	return l + [pad]*(t-len(l))

def padl(l, t, pad=None):
	return [pad]*(t-len(l)) + l

def get(d, key, default):
	try:
		return d[key]
	except KeyError:
		out = d[key] = default
		return out 

class ImapHelper(object):
	def __init__(self):
		## storage
		self._dirs = {}
		self._messages = {}

		## status
		self._last_list = 0
		self._selected = None

		## seconds before data is considered old
		self.list_ctime = 5 * 60
		self.select_ctime = 60 # is this really needed? msg_count is never used.
		self.search_ctime = 60
		self.meta_ctime = 24 * 60 * 60
		self.data_ctime = 7 * 24 * 60 * 60

	def connect(self, server, auth):
		self.imap = imaplib.IMAP4_SSL(server)
		self.imap.login(*auth)

	def _list_dirs(self, forced=False):
		if forced or time.time() - self.list_ctime > self._last_list:
			print 'LISTING DIRECTORIES'
			status,dirs = self.imap.list()
			self._last_list = time.time()
			dirs = [parse(dir) for dir in dirs]
			d = {}
			for (opts, sep, name) in dirs:
				parts = name.rsplit(sep, 1)
				path = '/'.join(parts)
				tmp = self._dirs.get(path, {})
				tmp.update(dict(zip(('opts', 'sep', 'name'), (opts, sep, parts[-1]))))
				d[path] = tmp
			self._dirs = d
		return True

	def _create_dir(self, path):
		status,r = self.imap.create(path)
		if status != 'OK':
			return False

		self._last_list = 0
		return True
	
	def _delete_dir(self, path):
		status, r = self.imap.delete(path)
		if status != 'OK':
			return False

		self._last_list = 0
		return True

	def _fetch_messages(self, uid, macro, forced=False):
		macros = {'META': ('(FLAGS INTERNALDATE RFC822.SIZE RFC822.HEADER)', self.meta_ctime),
			'DATA' : ('(RFC822)', self.data_ctime)}

		req,cache_time = macros[macro]

		if isinstance(uid, int):
			uid = (uid,)
		
		# bool predicate deciding if the message should be refetched.
		def refetch(msg):
			last_fetch = get(msg, 'last_fetch', dict([(k,0) for k in macros.keys()]))
			return time.time() - cache_time > last_fetch[macro]

		# res is sequence of mixed tuples/strings, adds tuple-end-like strings to
		# the previous item under the assumption that the previous was a tuple.
		def fixup(res):
			out = []
			for r in res:
				if isinstance(r, basestring) and r.endswith(')') and not r.startswith('('):
					out[-1] += (r,)
				else:
					out.append(r)
			return out
				
		def turkparse(x):
			if isinstance(x, tuple):
				tmp = parse(''.join(x[::2]))
				return tmp, x[1::2]
			return parse(x), []

		msgs = map(lambda u: get(self._messages, u, {}), uid)
		if not forced:
			msgs = filter(refetch, msgs)

		if len(msgs):
			uid = ','.join([str(x['UID']) for x in msgs])

			print "FETCH MESSAGE %s %s" % (uid, macro)
			status,result = self.imap.uid('fetch', uid, req)
			fetch_time = time.time()

			result = fixup(result)
			for r,extra in map(turkparse, result):
				for seq,data in zip(r[::2],r[1::2]):
					d = dict(zip(data[::2],data[1::2]))
					for k,v in d.items():
						if isinstance(v, basestring) and v.startswith('{') and v.endswith('}'):
							c,extra = extra[0],extra[1:]
							c = c.replace('\r', '') #wut
							d[k] = c
					msg = self._messages[int(d['UID'])]
					msg.update(d)
					msg['last_fetch'][macro] = fetch_time

	def _list_messages(self, path, forced=False, fetch_meta=True):
		dir = self._dirs[path]
		if forced or time.time() - self.search_ctime > dir.get('last_search', 0):
			print 'LISTING MESSAGES IN %s' % path

			# Make sure the correct mail directory is selected
			if self._selected != path:
				if not self._select_dir(path, True):
					return False
			assert self._selected == path

			# issue search command and fill cache without discarding fetched data
			status,(msg_uids,) = self.imap.uid('search', 'ALL')
			if status != 'OK':
				return False
			dir['last_search'] = time.time()

			if msg_uids == '':
				dir['msg_uids'] = []
			else:
				try:
					dir['msg_uids'] = map(int, msg_uids.split(' '))
				except ValueError as e:
					print "exception when converting (%s) (%s)" % (str(msg_uids), str(e))
					return False

			for uid in dir['msg_uids']:
				tmp = self._messages.get(uid, {})
				tmp['UID'] = uid
				self._messages[uid] = tmp

			# fetch meta data of all messages now to avoid doing it once for each message later on
			if fetch_meta:
				self._fetch_messages(dir['msg_uids'], 'META')
		return True

	def _select_dir(self, path, forced=False):
		# TODO, consider blindly issuing select commands if not in _dirs
		self._list_dirs()
		try:
			dir = self._dirs[path]
		except KeyError:
			return False

		if forced or time.time() - self.select_ctime > dir.get('last_select', 0):
			print 'SELECT %s' % path
			status,(msgc,) = self.imap.select(path)
			if status != 'OK':
				return False
			dir['last_select'] = time.time()
			dir['msg_count'] = int(msgc)
			self._selected = path
		return True

class ImapFS(Fuse,ImapHelper):
	def __init__(self, **kwargs):
		ImapHelper.__init__(self)
		Fuse.__init__(self, **kwargs)
		self.parser.add_option(mountopt='server', metavar='SERVER')
		self.parser.add_option(mountopt='username', metavar='USERNAME')
		self.parser.add_option(mountopt='password', metavar='PASSWORD')

	def main(self):
		self.connect(self.server, (self.username, self.password))
		Fuse.main(self)

	def getattr(self, path):
		st = Stat0()
		if path == '/':
			st.st_mode = stat.S_IFDIR | 0755
			st.st_nlink = 2
			return st

		self._list_dirs()
		if path[1:] in self._dirs:
				st.st_mode = stat.S_IFDIR | 0755
				st.st_nlink = 2
				return st

		base,name = padl(path[1:].rsplit('/', 1), 2)
		try:
			name = int(name)
		except ValueError:
			name = None

		# it would be possible to not give a damn about the directory and just blindly fetch the message by uid
		# 	still need to SELECT the maildir.
		if base and name:
			self._list_messages(base)
			try:
				dir = self._dirs[base]
				if name in dir['msg_uids']:
					self._fetch_messages(name, 'META')
					msg = self._messages[name]
					st.st_mode = stat.S_IFREG | 0444
					st.st_nlink = 1
					# FIXME, this doesnt match the length of the actual data
					st.st_size = int(msg['RFC822.SIZE']) + len(msg['RFC822.HEADER'])
					# TODO, fill some more fields
					return st
			except KeyError as e:
				print 'KEYERROR', e, self._messages.keys()

		return -errno.ENOENT

	def mkdir(self, path, mode):
		path = path[1:]
		# TODO: append / to path?
		if not self._create_dir(path):
			return -errno.ENOENT

	def rmdir(self, path):
		path = path[1:]

		# make sure the directory is empty.
		self._list_dirs()
		if not self._list_messages(path, fetch_meta=False):
			print "Failed to list messages in %s" % (path, )
			return -errno.ENOENT
		dir = self._dirs[path]
		if len(dir['msg_uids']):
			return -errno.ENOENT

		if not self._delete_dir(path):
			return -errno.ENOENT

	def readdir(self, path, offset):
		self._list_dirs()
		path = path[1:]

		# there should not be any messages in /, I think.
		if path:
			self._list_messages(path)

		dirs = [data for dpath,data in self._dirs.items() if padl(dpath.rsplit('/',1), 2, '')[0] == path]
		if len(dirs) == 0 and path not in self._dirs:
			return -errno.ENOENT
		dir = self._dirs.get(path, {})
		return [fuse.Direntry(d) for d in ['.', '..'] + [d['name'] for d in dirs] + map(str, dir.get('msg_uids',[]))]

	def open(self, path, flags):
		try:
			#TODO, run a search here in case some clairvoyant is guessing their uids?
			name = int(path.rsplit('/', 2)[-1])
			msg = self._messages[name]
		except ValueError,KeyError:
			return -errno.ENOENT

		accmode = os.O_RDONLY | os.O_WRONLY | os.O_RDWR
		if (flags & accmode) != os.O_RDONLY:
			return -errno.EACCES

	def read(self, path, size, offset):
		try:
			name = int(path.rsplit('/', 2)[-1])
		except ValueError:
			return -errno.ENOENT

		self._fetch_messages(name, 'DATA')
		msg = self._messages[name]

		slen = len(msg['RFC822'])
		if offset < slen:
			buf = str(msg['RFC822'])[offset:offset+size]
		else:
			buf = ''
		return buf

if __name__ == '__main__':
	server = ImapFS(version="%prog ")
	server.parse(values=server, errex=1)
	server.main()
