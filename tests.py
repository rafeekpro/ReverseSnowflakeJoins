"""
Reverse Snowflake Joins
Copyright (c) 2008, Alexandru Toth
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY Alexandru Toth ''AS IS'' AND ANY
EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL <copyright holder> BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE."""

"""
Do not run this file!
Unintuitive as it is, this is "included" verbatim into revj's namespace

Run the tests with:

python revj.py
"""
import unittest

#common for many tests
basicTests = ['select ((((a)))) from table;',
	'select (((a))),(b+(c+(d+3))) from table;',
	"select b || '((1))2))34' from table;",
	"""select a,'"',b,'"' from table; """,
	"""select "x", '"', "y" ,'"' from table; """,
	"""select "table"."a", "table"."b" from table;"""]

simplifyTests = """select sin(cos(a+b+sin(table."c"))) from table;
	select 2*a*2 + b.b-c/3, 2*x ||'xx' from table ttt where (m *n = 0) or (ww|| qq='mmmmmmmm');
	select a, (2*a.a) + ((b+c)+3) from table;
	select a*(-1) from table;
	select instr(a,'d') || 'dd' || nvl(b.b,'c') from table;
	select nvl(a,'d'||'cc') || 'dd' || nvl(b,2+1+3) from table;
	select nvl(a.a,b) from table;
	select nvl(a.a, (2*a.a) + ((b+c.c)+3)) from table;
	select substr('aa', 3, b) from table;
	select nvl (a), nvl ( a ), nvl (a ) from table;
	select x+y+substr(a+b+upper(d)) from table;
	select sin(cos(q + upper(lower(u)))) from table;
	select sin(cos(tan(x) + y)*2) + substr(a||b.b||upper(lower("d"))) from table;
	select 2*a + b.b+c+3, x ||'xx' from table;
	select * from table ttt where (m +cos(n) = 0) or (ww || concat(qq,'xx')='mmmmmm')
	select "table2"."column name" from "schema"."table";
	select table.a_a01+99, table.a_a02*99,'*', table.b_b02+'not defined', table.c_c02+'cc' + 'dd' from table where '*'='*' or table.aa04='d';
	select 2*q*4, table.*, 2*xx from table;
	select count(*)*3 from table;
	select a + 2 + pi()*3 from table where b=random();
	select * from table order by 10*sin(cos(c33)+d44)+3;
	select * from table where a=1 or sin(b)=0.5;
	select * from table where a<>'lala'; """\
	.splitlines()

outerJoinTests = ['select * from t1, t2 where t1.id1 (+) = t2.id1;',
	'select * from t1, t2 where t1.id2 (+)= t2.id2;',
	'select * from t1, t2 where t1.id3 = (+) t2.id3;',
	'select * from t1, t2 where t1.id4 =(+) t2.id4;',
	'select * from t1, t2 where t1.id5 = ( + ) t2.id5;']

class SanityCheckerTestCase(unittest.TestCase):
	def setUp(self):
		self.sc = SanityChecker()

	def testCheckParens(self):
		self.assertRaises(BadParensException, self.sc.checkParens,
			'select ((((a))))) from table')

		self.assertTrue( self.sc.checkParens('select (((a))),(b+(c+(d+3))) from t;'))

class QuoteRemoverTestCase(unittest.TestCase):
	def setUp(self):
		self.qr = QuoteRemover()
		
	def testRemoveUnknownMS(self):
		s = 'select * from table as t1 WITH( nolock) where x=1'
		res = self.qr.process(s)
		self.assertIn( 'with' ,res)
		self.assertIn( 'nolock' ,res)

		
	def testRemoveUnknownMS(self):
		s = 'select * from table as t1 WITH( nolock) where x=1'
		res = self.qr.process(s)
		self.assertIn( 'with' ,res)
		self.assertIn( 'nolock' ,res)
		
	def testRemoveUnknownPG(self):
		s = 'select t.aa::varchar( 20), bb[3], cc[4:5], dd[6][7] as [ee] from table t'
		res = self.qr.process(s)
		self.assertNotIn( '::' ,res)
		self.assertNotIn( 'varchar' ,res)
		self.assertNotIn( '20' ,res)
		self.assertIn( 'aa' , res)
		self.assertIn( 'bb' , res)
		self.assertIn( 'cc' , res)
		self.assertIn( 'dd as ee' , res)
		self.assertNotIn( ':' ,res)
		self.assertNotIn( '[3]' ,res)
		self.assertNotIn( '[4:5]' ,res)
		self.assertNotIn( '[6]' ,res)
		
	def testRemoveCast(self):
		s = """select  cast(T2.a as integer) 
			from T2
			group by  cast(T2.a as integer)"""
			
		res = self.qr.process(s)
		
		self.assertNotIn( 'integer' ,res)
		self.assertNotIn( ' as ' ,res)

	def testKeepQuotedUnknownPG(self):
		s = "select * from table where x='t.aa::varchar( 20)';"
		res = self.qr.process(s)
		self.assertNotIn( '::' ,res)
		self.assertNotIn( 'varchar' ,res)
		self.assertNotIn( '20' ,res)
		self.assertNotIn( 'aa' ,res)

	def testQuoteEscapes(self):
		res = [""" "my table"."my column\" is "" long" """,
			""" 'a''b\'c\'d' """]
		res = map(self.qr.removeQuoteEscapes, res)
		for r in res:
			for q in ESCAPEDQUOTES:
				self.assertNotIn( q ,res)

	def testRemoveDoubleQuotes(self):
		res = [""" "my table"."my column\" is "" long '' """,
			""" 'a''b\'c\'d' """]
		res = list(map(self.qr.removeQuoteEscapes, res))
		res = list(map(self.qr.removeQuotedIdent, res))
		for r in res:
			for q in ESCAPEDQUOTES:
				self.assertNotIn( q, res + [' '])

	def removeSpaces(self, x):
		return re.sub('\s+', '', x)

	def testRemoveConst(self):
		res = ["select a * 2.3E4, b * 2e4, c * .3e4 + 3.14 from table;",
			"select a || 'sin(cos(x))', '*', '1234' from table;",
			"select a || '(((' || 'dd'||'select ' from table where b='))';"]
		bads = ['sin', 'cos', '2.3', 'e4', 'E4', '2.3', '3.14',
			'((', '))', 's4']
		for r in res:
			self.qr.reset()
			x = self.qr.removeConst(r)
			for b in bads:
				try:
					self.assertNotIn( b,  x)
				except AssertionError:
					print(">>", sys.stderr, '[%s] in [%s]' %(b, x))
					self.assertNotIn( b,  x)

				self.assertTrue( x.islower())

			self.assertTrue( self.removeSpaces(self.qr.restoreConst(r)) == \
				self.removeSpaces(r))

	def testResetInProcess(self):
		self.qr.quotedConsts = {1:1, 2:2}
		x = self.qr.process('select a from table')
		self.assertTrue( self.qr.quotedConsts == {})

	def testSanityInProcess(self):
		bads = basicTests

		for r in bads:
			bads = self.qr.process(r)

		#bad SQL
		bads = ['select ((((a))))) from table;',
			'(select a+(b+(c+(d+3) from table;',
			"select a || '((1))2))34' from table;)",
			"select [a from table;",
			"select a] from table;",
			"select {a from table;",
			"select a} from table;",
			"select a' from table;",
			'select a" from table;']

		for r in bads:
			self.assertRaises(BadParensException,
				self.qr.process, r)

		#bad sql
		bads = ["""select "table"."a'", "table"."b'" from table; """,
			"""select "table"."a)" from table; """ ,
			"""select "table"."a+" from table; """]
		for r in bads:
			self.assertRaises(BadIdentException,
				self.qr.process, r)

	def testSanityOk(self):
		goods = ["select * from t where x='((((('",
			"select * from t where x=']]]]]]'",
			"select * from t where x='}}}}'",
			"""select * from t where x='"' """]
		for r in goods:
			bads = self.qr.process(r)

	def testReplacedConsts(self):
		s = "select m99 from t where a11 = 'aa' and t.b22 = 2e34 and 3.14 = c33;"

		res = self.qr.process(s)

		self.assertIn( "'aa'",self.qr.quotedConsts)
		self.assertIn( "2e34" , self.qr.quotedConsts)
		self.assertIn( "3.14" , self.qr.quotedConsts)

	def testInClause(self):
		s = ("select * from table t where t.a in (1,2,3) "
			"and t.b in ('x', 'y') and t.c='a'")
		res = self.qr.process(s)

		self.assertNotIn( '(' ,res)
		self.assertNotIn( ')' ,res)
		self.assertIn( 'in (1,2,3)' , self.qr.quotedConsts)
		self.assertIn( "in ('x','y')" , self.qr.quotedConsts)

	def testNotInClause(self):
		s = ("select * from table t where t.a not in (1,2,3) "
			"and t.b not in ('x', 'y') and t.c='a'")
		res = self.qr.process(s)

		ok = False
		for i in compar.searchString(res):
			if i[0] != 'not_in_equal':
				ok = True

		if not ok:
			raise Exception('NOT BETWEEN is not an operator')

		self.assertNotIn( '(' ,res)
		self.assertNotIn( ')' ,res)
		self.assertIn( 'not in (1,2,3)', self.qr.quotedConsts)
		self.assertIn( "not in ('x','y')", self.qr.quotedConsts)

	def testEmptyStringConst(self):
		s = "SELECT IF (cu.active, 'active','') from cu"
		res = self.qr.process(s)

		self.assertIn( "'active'", self.qr.quotedConsts)
		self.assertIn( "''", self.qr.quotedConsts)

	def testBetween(self):
		s = ("select table.* from table where table.a11 between 3 and 4 and table.b22 =5 " )
		res = self.qr.process(s)

		self.assertIn( "table.b22" , res)
		self.assertNotIn( "5" ,res)
		self.assertIn( "table.b22 =  '" , res)
		self.assertIn( "table.a11" , res)
		self.assertIn( "between_equal" , res)

	def testNotBetween(self):
		s = ("select table.* from table where table.a11 not between 3 and 4 and table.b22 =5 " )
		res = self.qr.process(s)

		ok = False
		for i in compar.searchString(res):
			if i[0] != 'not_between_equal':
				ok = True

		if not ok:
			raise Exception('NOT BETWEEN is not an operator')

		self.assertIn( "table.b22" , res)
		self.assertNotIn( "5" ,res)
		self.assertIn( "table.b22 =  '" , res)
		self.assertIn( "table.a11" , res)
		self.assertIn( "not_between_equal" , res)

	def testSelectDistinct(self):
		s = ("Select distinct a.id from a " )
		res = self.qr.process(s)

		self.assertNotIn( "distinct" ,res.lower())

class SimplifierTestCase(unittest.TestCase):
	def setUp(self):
		self.si = Simplifier()
		self.qr = QuoteRemover()

	def process(self, s):
		self.qr.reset()
		self.si.reset()
		return self.si.process(self.qr.process(s))

	def testSelectStar(self):
		s = "select table.*, *, a * 2 * 3 from table"
		res = self.process(s)

		try:
			self.assertTrue(len(res.split('*')) == 3) #that's 2 occurences of *
		except AssertionError:
			print(">>",sys.stderr, res)
			raise Exception("* is not a binop in [%s]" % s)

	def testCountStar(self):
		s = "select count(*)*3 from table;"
		res = self.process(s)

		self.assertIn( "_agg" , res)
		self.assertIn( "(*)" , res)
		self.assertNotIn( "3" ,res)


	def testNumbersInIdent(self):
		s = 'select table.a000 * 2 + b_12-3 + "c23" + d34 from table'
		res = self.process(s)

		goods = ['a000','b_12', 'c23', 'd34']
		for g in goods:
			self.assertIn( g , res)

	def testProcess(self):
		s = basicTests + simplifyTests
		for x in s:
			res = self.si.process(
				self.qr.process(x))
			self.assertIn( "select" , res.lower())
			self.assertTrue( res.islower())


	def testReduceLongOperands(self):
		s = "select a||b || c || '||||||||' from table"
		res = self.process(s)

		self.assertNotIn( '|' ,res)

	def testReduceConstBinop(self):
		s = "select 2*a, 0.2/b, (222-c), 'ss'||d from table"
		res = self.process(s)

		self.assertNotIn( '2', res)	#remove constant binop
		self.assertNotIn( "'s", res)	#remove constant binop

	def testFuncsWithComma(self):
		s = "select nvl(a111 + 2222, 3333, 4444) from table;"
		res = self.process(s)

		self.assertIn( 'a111' , res)
		self.assertNotIn( '2222' ,res)
		self.assertNotIn( '3333' ,res)
		self.assertNotIn( '4444' ,res)
		self.assertNotIn( 'nvl' ,res)

	def testFuncsWithCommaJoins(self):
		s = "select * from t1, t2 where t1.a=t2.a and nvl(t1.x,0) =(+) nvl(t2.y, 0) and t1.b = t2.b;"
		res = self.process(s)

		self.assertNotIn( 'nvl' ,res)

	def testFuncsNoParam(self):
		s = "select a + 2 + pi()*3 from table where b=random();"
		res = self.process(s)

		self.assertNotIn( 'pi', res)	#pi is redundant
		self.assertIn( "random", res)	#don' remove == constant func()

	def testFuncsAndAggregs(self):
		s = "select x, sum(t.y) from t group by x having nvl(sum(z)) > 0;"
		res = self.process(s)

		self.assertIn( '_agg(z)' , res)
		self.assertNotIn( 'nvl' ,res)

	def testDistinctAggregs(self):
		s = "select count(a), count(distinct b), count(DISTINCT c) from t "
		res = self.process(s)

		cntPos = aggregatesAsList.index('count')

		self.assertIn( '_%s_agg(a)' % cntPos , res)

		#one space left after removing "(DISTINCT b)"
		self.assertIn( '_%s_agg( b)' % (cntPos + AGG_DISTINCT) , res)
		self.assertIn( '_%s_agg( b)' % (cntPos + AGG_DISTINCT) , res)
		self.assertNotIn( 'distinct' ,res)


	def testKeepSpacing(self):
		s = "select a, (2*m) from table;"
		res = self.process(s)

		self.assertTrue(self.assertIn('a,', res) or self.assertIn('a ,', res))	#keep spacing

	def testWhere(self):
		s = "select a from table where (sin(a)+pi()/2)=3+random()+4;"
		res = self.process(s)

		self.assertTrue(self.assertNotIn('pi', res) or self.assertNotIn('2' , res))
		self.assertNotIn( '/' ,res)
		self.assertNotIn( '*' ,res)
		self.assertTrue(self.assertNotIn('3',res) or self.assertNotIn('4',res) or self.assertNotIn('random',res))


	def testAnsiJoin(self):
		s = """select a from t1 inner join t2 on (sin(t1.a)+pi()/3)
		=4+random()+5+cos(t2.b);"""
		res = self.process(s)

		self.assertIn( 't1.a' , res)
		self.assertIn( 't2.b' , res)
		self.assertTrue(self.assertNotIn('pi', res) or self.assertNotIn('3', res))
		self.assertNotIn( '/' ,res)
		self.assertNotIn( '*' ,res)
		self.assertTrue(self.assertNotIn('4', res) or self.assertNotIn('5', res) or self.assertIn('random',res))

	def testOuterJoin(self):
		s = outerJoinTests
		for x in s:
			res = self.process(x)
			try:
				self.assertIn( "#", res.lower())
			except AssertionError:
				raise Exception("# not in [%s]" % res)

	def testMultipleSelects(self):
		s = "select * from table where table.a in (select id from country)"
		res = self.process(s)

		self.assertIn( "(" , res)
		self.assertTrue( len(s.split('select')) == 3) # that's 2 selects

	def testColAliases(self):
		s = "select a as aa, 3 as bb, 2*3*sin(c)*4 as cc from table;"
		res = self.process(s)

		self.assertIn( "aa" , res)
		self.assertIn( "bb" , res)
		self.assertIn( "cc" , res)

	def testSquareBrackets(self):
		s = "select aa as [bb] from table;"
		res = self.process(s)

		self.assertIn( "aa" , res)

	def testDollarCurlyBraces(self):
		s = "select a,b,c from ${table};"
		res = self.process(s)

		self.assertIn( "$table" , res)

	def testTableAliases(self):
		s = "select * from table1 aa, table 2 bb;"
		res = self.process(s)

		self.assertIn( "aa" , res)
		self.assertIn( "bb" , res)

	def testAnsiJoinAliases(self):
		s = "select * from table1 aa inner join table2 bb on x=y;"
		res = self.process(s)

		self.assertIn( "aa" , res)
		self.assertIn( "bb" , res)

	def testGroupBy(self):
		s = 'select * from table t group by "a11", t.b22, ' + \
			'10*sin(cos(c33)+d44)+3'
		res = self.process(s)

		self.assertIn( "a11" , res)
		self.assertIn( "t.b22" , res)
		self.assertIn( "c33" , res)
		self.assertIn( "d44" , res)

		self.assertIn( 'group_by' , res)

	def testOrderBy(self):
		s = 'select * from table t order by "a11", t.b22, ' + \
			'10*sin(cos(c33)+d44)+3'
		res = self.process(s)

		self.assertIn( "a11" , res)
		self.assertIn( "t.b22" , res)
		self.assertIn( "c33" , res)
		self.assertIn( "d44" , res)

		self.assertIn( 'order_by' , res)

	def testStarComparisons(self):
		#some reportign tools generate 'select ALL' syntax
		s = "select * from t where a=1 and ('*'='*' or nvl(b,'n')='n')" + \
			" and c=0;"
		res = self.process(s)

		self.assertTrue( len(res.split('=')) == 4)	# 3 equals, one removed :-)

	def testGROUP_CONCAT(self):
		#this query is on the frontpage; embarrasing
		s = """SELECT GROUP_CONCAT(film.name, SEPARATOR ', ') AS actors
			FROM film"""

		res = self.process(s)

		self.assertNotIn( "separator", res.lower())


class SingleSelectTestCase(unittest.TestCase):
	def setUp(self):
		self.si = Simplifier()
		self.qr = QuoteRemover()
		#param is for quoted consts; those change after each process()
		self.ss = SingleSelect(self.qr)

	def process(self, s):
		self.qr.reset()
		self.si.reset()
		self.ss.reset()
		return self.ss.process(
				self.si.process(
					self.qr.process(s)))

	def testMultipleSelects(self):
		s = "select * from table where table.a in (select id from country)"
		self.assertRaises(MultipleSelectsException, self.ss.process, s)

	def testColAliases(self):
		s = "select a as aa, 2*t.b+4 as bb, sin(c) as cc, d, 3 as e from t;"
		res = self.process(s)

		self.assertIn( 'aa', self.ss.colAliases['t.a'])
		self.assertNotIn( 'aa',  self.ss.colAliases['t.b'])
		self.assertIn( 'bb', self.ss.colAliases['t.b'])
		self.assertIn( 'cc',self.ss.colAliases['t.c'])

		self.assertNotIn( 't.d',self.ss.colAliases)
		self.assertNotIn( 'd' , self.ss.colAliases)
		self.assertNotIn( 'as', self.ss.colAliases)
		self.assertNotIn( 't.e', self.ss.colAliases)
		self.assertNotIn( 'e', self.ss.colAliases)

		self.assertIn( 't.a', self.ss.columns)
		self.assertIn( 't.b', self.ss.columns)
		self.assertIn( 't.c', self.ss.columns)
		self.assertIn( 't.d', self.ss.columns)

		self.assertIn( 't.e', self.ss.columns)

	def testColAliasExpressions(self):
		s = 'select a, 2*(m+"n"+3) as oo ,b as bb from table;'
		res = self.process(s)

		self.assertIn( 'bb' , self.ss.colAliases['table.b'])
		self.assertNotIn( 'm' , self.ss.colAliases)
		self.assertNotIn( 'n' , self.ss.colAliases)
		self.assertIn( 'table.a' , self.ss.columns)
		self.assertIn( 'table.b' , self.ss.columns)
		self.assertIn( 'table.m' , self.ss.columns)
		self.assertIn( 'table.n' , self.ss.columns)

		self.assertNotIn( '2' ,self.ss.columns)
		self.assertNotIn( '3' , self.ss.columns)
		self.assertNotIn( 'table.2' , self.ss.columns)
		self.assertNotIn( 'table.3' , self.ss.columns)

		#oo is no longer expression alias; it is a column
		self.assertIn( 'table.oo' , self.ss.columns)

	def testColAliasExpressionsWithJoins(self):
		s = 'select 2*(t1.m+"t2.n"+3) as oo ,t1.b as bb from t1, t2;'
		res = self.process(s)

		self.assertIn( 'bb' , self.ss.colAliases['t1.b'])
		self.assertIn( 'oo' , self.ss.exprAliases)

	def testStarColAlias(self):
		s = "select a, table.* from table;"
		res = self.process(s)

		self.assertIn( 'table.*' , self.ss.columns)
		self.assertNotIn( '*' , self.ss.columns)
		self.assertIn( 'table.a' , self.ss.columns)
		self.assertTrue( self.ss.selectStar == False)

	def testSelectStar_X(self):
		s = "select t2.a, * from table1, table2 t2, table2 t2again;"
		res = self.process(s)

		self.assertNotIn( '*' , self.ss.columns)
		self.assertTrue( self.ss.selectStar == True)
		self.assertIn( 'table1.*' , self.ss.columns)
		self.assertIn( 't2.*' , self.ss.columns)
		self.assertIn( 't2again.*' , self.ss.columns)

	def testNaturalJoin(self):
		s = "select student_name, class_name from student natural join class"

		self.assertRaises(NaturalJoinException, self.ss.process, s)

	def testOldJoin(self):
		s = "select t1.m99 from table1 t1, table2, table3 t3 where t1.a11=t3.b22"
		res = self.process(s)

		self.assertNotIn( 'a11' , self.ss.tableAliases)
		self.assertNotIn( 't1.a11', self.ss.tableAliases)
		self.assertNotIn( 'b22' ,self.ss.tableAliases)
		self.assertNotIn( 't3.b22' , self.ss.tableAliases)
		self.assertNotIn( 'm99' , self.ss.tableAliases)
		self.assertNotIn( 't1.m99' , self.ss.tableAliases)
		self.assertIn( 'table1' , self.ss.tableAliases)
		self.assertIn( 'table2' , self.ss.tableAliases)
		self.assertIn( 'table3' , self.ss.tableAliases)

		self.assertIn( 't1' , self.ss.tableAliases['table1'])
		self.assertNotIn( 't2' , self.ss.tableAliases['table1'])
		self.assertNotIn( 't3' , self.ss.tableAliases['table1'])

		self.assertIn( 't3' , self.ss.tableAliases['table3'])

		self.assertIn( 't1.a11' , self.ss.joins)
		self.assertIn( 't3.b22' , self.ss.joins['t1.a11'])

	def testAnsiJoin(self):
		s = """select * from (table1 t1 inner join table2 t2 on
			"t1"."a" = t2.m and sin(t1.b) = t2.n+2)
			inner join table3 t3 on t1.a = 10*t3.x and t1.b || 't1.xx' = t3.y
			where t1.xxx=1;"""
		res = self.process(s)
		
		self.assertIn( 't1.a' , self.ss.columns)
		self.assertIn( 't1.b' , self.ss.columns)
		self.assertIn( 't2.m' , self.ss.columns)
		self.assertIn( 't2.n' , self.ss.columns)
		self.assertIn( 't3.x' , self.ss.columns)
		self.assertIn( 't3.y' , self.ss.columns)
		self.assertIn( 't1.xxx' , self.ss.columns)

		self.assertIn( 't1.a' ,  self.ss.joins)
		self.assertIn( 't2.m' , self.ss.joins['t1.a'])
		self.assertIn( 't3.x' , self.ss.joins['t1.a'])
		self.assertIn( 't1.b' ,  self.ss.joins)
		self.assertIn( 't2.n' ,  self.ss.joins['t1.b'])
		self.assertIn( 't3.y' ,  self.ss.joins['t1.b'])

		self.assertIn( 'table1' ,  self.ss.tableAliases)
		self.assertIn( 'table2' ,  self.ss.tableAliases)
		self.assertIn( 'table3' ,  self.ss.tableAliases)

	def testLeftJoinAnsi(self):
		s = """SELECT * FROM t1 LEFT JOIN t2 on t1.a=t2.a"""
		res = self.process(s)

		self.assertIn( 'T1.A' ,  self.ss.joins)
		self.assertIn( 't2.a' ,  self.ss.joins)

	def testRightJoinAnsi(self):
		s = """SELECT * FROM t1 right JOIN t2 on t1.a=t2.a"""
		res = self.process(s)

		self.assertIn( 't1.a' ,  self.ss.joins)
		self.assertIn( 'T2.A' ,  self.ss.joins)

	def testFullJoinAnsi(self):
		s = """SELECT * FROM t1 FULL JOIN t2 on t1.a=t2.a"""
		res = self.process(s)

		self.assertIn( 'T1.A' ,  self.ss.joins)
		self.assertIn( 'T2.A' ,  self.ss.joins)

	#def testCrossAnsi(self):
	#	s = """SELECT * FROM t1 CROSS JOIN t2 where t1.a=3 and t2.b=5"""
	#	res = self.process(s)

	#	self.assertIn( 't1.a' , self.ss.columns)
	#	self.assertIn( 't2.b' , self.ss.columns)
	#	self.assertIn( len(self.ss.joins) == 0
	#	self.assertIn( '--' ,res)

	def testCrossJoinAlias(self):
		s = """SELECT * FROM t1 a1 CROSS JOIN t2 a2;"""
		res = self.process(s)

		self.assertIn( 't1' ,  self.ss.tableAliases)
		self.assertIn( 't2' ,  self.ss.tableAliases)
		#this needs to be fixed properly
		#self.assertIn( 'a1' not ,  self.ss.tableAliases)
		self.assertNotIn( 'a2'  ,  self.ss.tableAliases)
		self.assertIn( 'a1' ,  self.ss.tableAliases)['t1']
		self.assertIn( 'a2' ,  self.ss.tableAliases)['t2']

		self.assertTrue( len(self.ss.joins) == 0)
		self.assertNotIn( '--' ,res)

	def testJoinUsing(self):
		s = """SELECT * FROM t1 JOIN t2 USING (a,b,c,d)"""
		res = self.process(s)

		self.assertIn( 't1.a' ,  self.ss.joins)
		self.assertIn( 't1.b' ,  self.ss.joins)
		self.assertIn( 't1.c' ,  self.ss.joins)
		self.assertIn( 't1.d' ,  self.ss.joins)

		self.assertIn( 't2.a' ,  self.ss.joins)
		self.assertIn( 't2.b' ,  self.ss.joins)
		self.assertIn( 't2.c' ,  self.ss.joins)
		self.assertIn( 't2.d' ,  self.ss.joins)

		self.assertIn( 't2.a' , self.ss.joins['t1.a'])
		self.assertIn( 't2.b' ,  self.ss.joins['t1.b'])
		self.assertIn( 't2.c' ,  self.ss.joins['t1.c'])
		self.assertIn( 't2.d' ,  self.ss.joins['t1.d'])

		self.assertTrue( len(self.ss.tableAliases) == 2)
		self.assertIn( 't1' ,  self.ss.tableAliases)
		self.assertIn( 't2' ,  self.ss.tableAliases)

	def testLeftJoinUsing(self):
		s = """SELECT * FROM t1 LEFT JOIN t2 USING (a,b,c,d)"""
		res = self.process(s)

		self.assertIn( 'T1.A' ,  self.ss.joins)
		self.assertIn( 't2.a' ,  self.ss.joins)

	def testRightJoinUsing(self):
		s = """SELECT * FROM t1 right JOIN t2 USING (a,b,c,d)"""
		res = self.process(s)

		self.assertIn( 't1.a' ,  self.ss.joins)
		self.assertIn( 'T2.A' ,  self.ss.joins)

	def testFullJoinUsing(self):
		s = """SELECT * FROM t1 FULL JOIN t2 USING (a,b,c,d)"""
		res = self.process(s)

		self.assertIn( 'T1.A' ,  self.ss.joins)
		self.assertIn( 'T2.A' ,  self.ss.joins)

	def testMax2TablesUsing(self):

		s = """SELECT * FROM t1 JOIN t2 USING (a,b) join t3 using (a,b)"""
		self.assertRaises(AmbiguousColumnException, self.process, s)


	def testOldJoinWithWhere(self):
		s = """select * from t1, t2 where  t1.id1 = t2.id2;"""
		res = self.process(s)

		self.assertNotIn( 't2.id2', self.ss.filters)

	def testAnsiMixedJoin(self):
		s = """select * from table1 t1, table2 t2 inner join table3 t3 on
			t2.id2 = t3.id3
			where t1.id1 = t2.id2"""

		res = self.process(s)

		self.assertNotIn( 't2.id2'  ,  self.ss.tableAliases)
		self.assertNotIn( 't3.id3'  ,  self.ss.tableAliases)
		self.assertNotIn( 'T2.ID2'  ,  self.ss.tableAliases)
		self.assertNotIn( 'T3.ID3'  ,  self.ss.tableAliases)

		self.assertIn( 't1.id1' , self.ss.columns)
		self.assertIn( 't2.id2' , self.ss.columns)
		self.assertIn( 't3.id3' , self.ss.columns)

		self.assertIn( 't1.id1' ,  self.ss.joins)
		self.assertIn( 't2.id2' ,  self.ss.joins)
		self.assertIn( 't3.id3' ,  self.ss.joins)

		self.assertIn( 'table1' ,  self.ss.tableAliases)
		self.assertIn( 't1' ,  self.ss.tableAliases['table1'])
		self.assertIn( 'table2' ,  self.ss.tableAliases)
		self.assertIn( 't2' ,  self.ss.tableAliases['table2'])
		self.assertIn( 'table3' ,  self.ss.tableAliases)
		self.assertIn( 't3' ,  self.ss.tableAliases['table3'])

	def testAnsiMixedJoinNotOrdered(self):
		s = """select * from table1 t1, table2 t2 inner join table3 t3 on
			t2.id2 = t3.id3, table4, table5 t5, table6
			where t1.id1 = t2.id2"""

		res = self.process(s)

		#same as previously
		self.assertNotIn( 't2.id2'  ,  self.ss.tableAliases)
		self.assertNotIn( 't3.id3'  ,  self.ss.tableAliases)
		self.assertNotIn( 'T2.ID2'  ,  self.ss.tableAliases)
		self.assertNotIn( 'T3.ID3'  ,  self.ss.tableAliases)

		self.assertIn( 't1.id1' , self.ss.columns)
		self.assertIn( 't2.id2' , self.ss.columns)
		self.assertIn( 't3.id3' , self.ss.columns)

		self.assertIn( 't1.id1' ,  self.ss.joins)
		self.assertIn( 't2.id2' ,  self.ss.joins)
		self.assertIn( 't3.id3' ,  self.ss.joins)

		self.assertIn( 'table1' ,  self.ss.tableAliases)
		self.assertIn( 't1' ,  self.ss.tableAliases['table1'])
		self.assertIn( 'table2' ,  self.ss.tableAliases)
		self.assertIn( 't2' ,  self.ss.tableAliases['table2'])
		self.assertIn( 'table3' ,  self.ss.tableAliases)
		self.assertIn( 't3' ,  self.ss.tableAliases['table3'])
		#new tests
		self.assertIn( 'table4' ,  self.ss.tableAliases)
		self.assertIn( 'table5' ,  self.ss.tableAliases)
		self.assertIn( 't5' ,  self.ss.tableAliases['table5'])
		self.assertIn( 'table4' ,  self.ss.tableAliases)

	def testAnsiJoinWithWhere(self):
		# from SQLite test suite
		s = """select * from t1 left join t2 on t1.b=t2.x and t1.c=1
                     left join t3 on t1.b=t3.p where t1.c=2"""
		res = self.process(s)

		self.assertIn( 't1' ,  self.ss.tableAliases)
		self.assertIn( 't2' ,  self.ss.tableAliases)
		self.assertIn( 't3' ,  self.ss.tableAliases)

	def testTableAsAlias(self):
		#MySQL speciffic ?
		s = """select * from table1 as t1 inner join table2 as t2 on t1.x=t2.y;"""
		res = self.process(s)

		self.assertIn( 'table1' ,  self.ss.tableAliases)
		self.assertIn( 't1' ,  self.ss.tableAliases['table1'])
		self.assertIn( 'table2' ,  self.ss.tableAliases)
		self.assertIn( 't2' ,  self.ss.tableAliases['table2'])

	def testAnsiJoinsNoAlias(self):
		s = """select * from category left join film_category on category.category_id = film_category.category_id;"""
		res = self.process(s)
		self.assertIn( 'category' ,  self.ss.tableAliases)
		self.assertIn( 'film_category' ,  self.ss.tableAliases)

	def testGroupBy(self):
		s = 'select m99 from table t group by "a11", t.b22, ' + \
			'10* sin(cos(c33)+d44)+3'
		res = self.process(s)

		self.assertIn( 't.a11' , self.ss.columns)
		self.assertIn( 't.b22' , self.ss.columns)
		self.assertIn( 't.c33' , self.ss.columns)
		self.assertIn( 't.d44' , self.ss.columns)

		self.assertIn( 't.a11' , self.ss.groups)
		self.assertIn( 't.b22' , self.ss.groups)
		self.assertIn( 't.c33' , self.ss.groups)
		self.assertIn( 't.d44' , self.ss.groups)

		self.assertNotIn( 'm99'  , self.ss.groups)
		self.assertNotIn( 't.m99'  , self.ss.groups)

	def testOrderBy(self):
		s = 'select m99 from t order by "a11", t.b22, 10*sin(cos(c33)+d44)+3'
		res = self.process(s)

		self.assertIn( 't.a11' , self.ss.columns)
		self.assertIn( 't.b22' , self.ss.columns)
		self.assertIn( 't.c33' , self.ss.columns)
		self.assertIn( 't.d44' , self.ss.columns)

		self.assertIn( 't.a11' , self.ss.orders)
		self.assertIn( 't.b22' , self.ss.orders)

		self.assertNotIn( 'm99'  , self.ss.orders)
		self.assertNotIn( 't.m99'  , self.ss.orders)

	def testOrderByAlias(self):
		s = 'select t1.m99 as zz from t1, t2 order by zz'
		res = self.process(s)

		self.assertIn( 't1.m99' , self.ss.columns)

		self.assertNotIn( 'm99' , self.ss.orders)
		self.assertNotIn( 't1.m99' , self.ss.orders)
		self.assertIn( 'zz' , self.ss.orders)


	def testSumGroupHaving(self):
		s = 'select c33, d44, sum(a11), min(t.b22) from t ' + \
			'group by c33, d44 where m99=0 having 0<sum(e55)'
		res = self.process(s)

		self.assertIn( 't.a11' , self.ss.columns)
		self.assertIn( 't.a11', self.ss.aggregs)
		self.assertIn( 'sum',  self.ss.aggregs['t.a11'])
		self.assertNotIn( 't.a11'  , self.ss.groups)

		self.assertIn( 't.b22' , self.ss.columns)
		self.assertIn( 't.b22' ,  self.ss.aggregs)
		self.assertIn( 'min' ,  self.ss.aggregs['t.b22'])
		self.assertNotIn( 't.b22' , self.ss.groups)

		self.assertIn( 't.c33' , self.ss.columns)
		self.assertNotIn( 't.c33' ,  self.ss.aggregs)
		self.assertIn( 't.c33' , self.ss.groups)

		self.assertIn( 't.d44' , self.ss.columns)
		self.assertNotIn( 't.d44' ,  self.ss.aggregs)
		self.assertIn( 't.d44' , self.ss.groups)

		self.assertIn( 't.e55' , self.ss.columns)
		self.assertIn( 't.e55' , self.ss.havings)
		#need to reverse comparison operator
		self.assertIn( "sum(e55)> 0" , self.ss.havings['t.e55'])

		self.assertNotIn( 't.e55'  ,  self.ss.aggregs)
		self.assertNotIn( 't.e55'  , self.ss.groups)

		self.assertIn( 't.m99' , self.ss.columns)
		self.assertNotIn( 't.m99'  ,  self.ss.aggregs)
		self.assertNotIn( 't.m99'  , self.ss.groups)

	def testSumGroupHaving(self):
		s = 'select sum(a11) from t;'
		res = self.process(s)

		self.assertIn( 't.a11' , self.ss.columns)
		self.assertIn( 't.a11' ,  self.ss.aggregs)
		self.assertIn( 'SUM(a11)' ,  self.ss.aggregs['t.a11'])

	def testOrderBy(self):
		s = 'select m99 from t order by "a11", t.b22, 10*sin(cos(c33)+d44)+3'
		res = self.process(s)

		self.assertIn( 't.a11' , self.ss.columns)
		self.assertIn( 't.a11' , self.ss.orders)
		self.assertIn( 't.b22' , self.ss.columns)
		self.assertIn( 't.b22' , self.ss.orders)
		self.assertIn( 't.c33' , self.ss.columns)
		self.assertIn( 't.c33' , self.ss.orders)
		self.assertIn( 't.d44' , self.ss.columns)
		self.assertIn( 't.d44' , self.ss.orders)

	def testOrderBy(self):
		s = 'select m99 from t order by "a11", t.b22 DESC, 10*sin(cos(c33)+d44)+3 DESC'
		res = self.process(s)

		self.assertIn( 't.a11' , self.ss.columns)
		self.assertIn( 't.a11' , self.ss.orders)
		self.assertIn( 't.b22' , self.ss.columns)
		self.assertIn( 't.b22' + '_'*10 , self.ss.orders)
		self.assertIn( 't.c33' , self.ss.columns)
		self.assertIn( 't.c33' , self.ss.orders)
		self.assertIn( 't.d44' , self.ss.columns)
		self.assertIn( 't.d44' , self.ss.orders)

	def testStarOrderBy(self):
		s = 'SELECT * from t order by y'
		res = self.process(s)

		self.assertIn( 't.y' , self.ss.columns)
		self.assertIn( 't.y' , self.ss.orders)

	def testFilter(self):
		s = "select m99 from t where (a11 = 0 or a11 = 1 ) and " + \
			"  'qqq' =t.b22 and c33=pi();"
		res = self.process(s)

		self.assertIn( 't.a11' , self.ss.filters)
		self.assertIn( '= 0' , self.ss.filters['t.a11'])
		self.assertIn( '= 1' , self.ss.filters['t.a11'])

		self.assertIn( 't.b22' , self.ss.filters)
		self.assertIn( "= 'qqq'" , self.ss.filters['t.b22'])

		self.assertIn( 't.c33' , self.ss.filters)
		self.assertIn( '= pi()', repr(self.ss.filters['t.c33']))

		self.assertNotIn( 'm99'  , self.ss.filters)
		self.assertNotIn( 't.m99'  , self.ss.filters)

	def testReverseOrderFilter(self):
		s = "select * from t where 0 < x;"
		res = self.process(s)

		self.assertIn( 't.x' , self.ss.filters)
		#need to reverse comparison operator
		self.assertIn( '> 0' , self.ss.filters['t.x'])

	def testFuncOverSum(self):
		#func is not reduced
		s = "select t.x, sum(t.y) from t group by x having nvl(sum(t.z),0) > 0;"
		res = self.process(s)
		self.assertIn( 't.z' , self.ss.havings)
		self.assertIn( "SUM(t.z)> 0" , self.ss.havings['t.z'])

	def testSanityCheckTables(self):
		s = "select m99 from unknown_table where (a11 = 0 or a11 = 1 ) and " + \
			"  'qqq' = b.b22 and c33=pi();"

		self.assertRaises(Exception, self.ss.process,
			self.si.process(self.qr.process(s)) )

	def testEOLComments(self):
		s = """select aa from --bla bla
			table --bla"""
		res = self.process(s)
		self.assertIn( 'table.aa' , self.ss.columns)

	def testMultilineComments(self):
		s = """select aa /*, bb, cc,
			dd */ from 
			table a"""
		res = self.process(s)
		self.assertIn( 'a.aa' , self.ss.columns)
		self.assertNotIn( 'a.bb'  , self.ss.columns)
		self.assertNotIn( 'cc'  , self.ss.columns)
		self.assertNotIn( 'dd'  , self.ss.columns)
		self.assertNotIn( 'a.cc'  , self.ss.columns)
		self.assertNotIn( 'a.dd'  , self.ss.columns)

	def testMultipleComments(self):
		s = """select a.aa /*, bb, cc,
			dd */ from 
			table_a a /* nonono */, b /*hahaha*/, c"""
		res = self.process(s)
		self.assertIn( 'a.aa' , self.ss.columns)
		self.assertNotIn( 'a.bb'  , self.ss.columns)
		self.assertNotIn( 'cc'  , self.ss.columns)
		self.assertNotIn( 'dd'  , self.ss.columns)
		self.assertNotIn( 'a.cc'  , self.ss.columns)
		self.assertNotIn( 'a.dd'  , self.ss.columns)

		self.assertIn( 'table_a' ,  self.ss.tableAliases)
		self.assertIn( 'a' ,  self.ss.tableAliases['table_a'])
		self.assertIn( 'b' ,  self.ss.tableAliases)
		self.assertIn( 'c' ,  self.ss.tableAliases)
		self.assertNotIn( 'nonono'  ,  self.ss.tableAliases)
		self.assertNotIn( 'a.nonono'  , self.ss.columns)
		self.assertNotIn( 'b.nonono'  , self.ss.columns)
		self.assertNotIn( 'c.nonono'  , self.ss.columns)
		self.assertNotIn( 'hahaha'  ,  self.ss.tableAliases)
		self.assertNotIn( 'a.hahaha'  , self.ss.columns)
		self.assertNotIn( 'b.hahaha'  , self.ss.columns)
		self.assertNotIn( 'c.hahaha'  , self.ss.columns)

	def testOraOuterJoins_l(self):
		s = "select * from t1, t2 where  t1.id (+) = t2.id ;"
		res = self.process(s)

		self.assertIn( "T1.ID" ,  self.ss.joins)
		self.assertIn( "t2.id" ,  self.ss.joins)

	def testOraOuterJoins_r(self):
		s = "select * from t1, t2 where t1.id = t2.id (+);"
		res = self.process(s)

		self.assertIn( "t1.id" ,  self.ss.joins)
		self.assertIn( "T2.ID" ,  self.ss.joins)

	def testFullOuterJoins_r(self):
		s = "select * from t1 full outer join t2 on t1.id = t2.id"
		res = self.process(s)

		self.assertIn( "T1.ID" ,  self.ss.joins)
		self.assertIn( "T2.ID" ,  self.ss.joins)

	def testLeftOuterJoins(self):
		s = "select * from t1 left outer join t2 on t1.id = t2.id ;"
		res = self.process(s)

		self.assertIn( "T1.ID" ,  self.ss.joins)
		self.assertIn( "t2.id" ,  self.ss.joins)

	def testRightOuterJoins(self):
		s = "select * from t1 right outer join t2 on t1.id = t2.id ;"
		res = self.process(s)

		self.assertIn( "t1.id" ,  self.ss.joins)
		self.assertIn( "T2.ID" ,  self.ss.joins)

	def testInClause(self):
		s = ("select * from table t where t.a in (1,2,3) "
			"and t.b in ('x', 'y') and t.c='a';" )
		res = self.process(s)

		self.assertIn( "t.a" , self.ss.filters)
		self.assertIn( "t.b" , self.ss.filters)
		self.assertIn( "t.c" , self.ss.filters)

	def testNotInClause(self):
		s = ("select * from table t where t.a not in (111,222,333) "
			"and t.b not in ('xxx', 'yyy') and t.c='a';" )
		res = self.process(s)

		self.assertIn( "t.a" , self.ss.filters)
		self.assertIn( "t.b" , self.ss.filters)
		self.assertIn( "t.c" , self.ss.filters)

	def testLIKE(self):
		s = ("select table.* from table where table.a LIKE '%qqq%' " )
		res = self.process(s)

		self.assertIn( "table.a" , self.ss.filters)
		self.assertIn( "like '%qqq%'" , self.ss.filters)['table.a']
		self.assertIn( "table.*" , self.ss.columns)

	def testNotLIKE(self):
		s = ("select table.* from table where table.a NoT liKE '%qqq%' " )
		res = self.process(s)

		self.assertIn( "table.a" , self.ss.filters)
		self.assertIn( "not like '%qqq%'" , self.ss.filters)['table.a']

	def testISNULL(self):
		s = ("select table.* from table where table.a IS NulL " )
		res = self.process(s)

		self.assertIn( 'table.a' , self.ss.filters)
		self.assertIn( 'is null' , self.ss.filters['table.a'])

	def testISNOTNULL(self):
		s = ("select table.* from table where table.a is not null " )
		res = self.process(s)

		self.assertIn( 'table.a' , self.ss.filters)
		self.assertIn( 'is not null' , self.ss.filters['table.a'])

	def testBetween(self):
		s = ("select table.* from table where table.a between 1 and 2 and table.b =3 " )
		res = self.process(s)

		self.assertIn( "table.b" , self.ss.filters)
		self.assertIn( "= 3" , self.ss.filters['table.b'])
		self.assertIn( "table.a" , self.ss.filters)
		self.assertIn( "between_equal" , list(self.ss.filters['table.a'])[0])
		self.assertIn( "1 and 2" , list(self.ss.filters['table.a'])[0])
		#bugfix for double quoting
		self.assertNotIn( "1 and 2'" , list(self.ss.filters['table.a'])[0])

		self.assertNotIn( 'between_equal' ,res)
		self.assertNotIn( 'table.between_equal'  , self.ss.columns)
		self.assertNotIn( 'between_equal'  , self.ss.columns)

	def _testExprBetween(self):
		s = ("select table.* from table where a between 1 and f(3)" )
		res = self.process(s)

		self.assertIn( 'table.a' , self.ss.filters)

		self.assertNotIn( 'between'  , self.ss.columns)
		self.assertNotIn( 'table.between'  , self.ss.columns)
		self.assertNotIn( 'between_equal' ,res)
		self.assertNotIn( 'table.between_equal'  , self.ss.columns)
		self.assertNotIn( 'between_equal'  , self.ss.columns)

		self.assertIn( '' , self.ss.filters['table.a'])
		self.assertIn( "between_equal", list(self.ss.filters['table.a'])[0])
		self.assertIn( "1 and " , list(self.ss.filters['table.a'])[0])

	def testNestedCaseWhenThen(self):
		s = """SELECT
		  aa, bb,
		  sin(cos((CASE cc
			WHEN 'M' THEN 'Male'
			WHEN 'F' THEN 'Female'
		  END)))
		FROM t """
		res = self.process(s)

		for bad in 'sin cos case when then end Male Female'.split():
			self.assertNotIn( bad ,res)
			self.assertNotIn( bad  , self.ss.columns)
			self.assertNotIn( bad.upper() , res)
			self.assertNotIn( bad.upper()  , self.ss.columns)

		self.assertIn( 't.aa' , self.ss.columns)
		self.assertIn( 't.bb' , self.ss.columns)
		self.assertIn( 't.cc' , self.ss.columns)

	def testCaseWhenFallTrhu(self):
		s = "select case when a+b+c=0 then 'x' when a+b='0' then '0' end from t"
		res = self.process(s)

		self.assertIn( 't.a' , self.ss.columns)
		self.assertIn( 't.b' , self.ss.columns)
		self.assertIn( 't.c' , self.ss.columns)

	def testaliasSingleSelect(self):
		s = ("select * from table where a=1" )
		res = self.process(s)

		self.assertIn( "table.a" , self.ss.filters)
		self.assertIn( "= 1" , self.ss.filters['table.a'])

	def testS0Columns(self):
		s = "select a.s5 from a where a.s6='aaa' and a.s7='fff'"
		res = self.process(s)

		self.assertNotIn( "s0"  , self.ss.columns)
		self.assertNotIn( "s1"  , self.ss.columns)
		self.assertNotIn( "a.s0"  , self.ss.columns)
		self.assertNotIn( "a.s1"  , self.ss.columns)

	def testAmbiguousColumnException(self):
		s = "select a from t1, t2"
		self.assertRaises(AmbiguousColumnException, self.process, s)


	def testEmptyStringConst(self):
		s = "SELECT IF (cu.active, 'active','') from cu"
		res = self.process(s)

		self.assertNotIn( 'if'  , self.ss.columns)
		self.assertNotIn( 'IF'  , self.ss.columns)

	def testDerivedTables(self):
		#SimpleSelect handles only one SELECT, simulate the rest
		s = "select w.a, t.b from [ 1 ] as w, t"
		res = self.process(s)

		self.assertIn( 'w' ,  self.ss.tableAliases)
		self.assertIn( 'w' , self.ss.derivedTables)
		self.assertTrue( self.ss.derivedTables['w'] == 1)

	def testSplitByCommasWihoutParens(self):
		s = "a, b as bb, nvl(a,'b') as c, 1+f(1,1,1, g(2,2,2,d))+3 as d"
		self.assertTrue( len(splitByCommasWithoutParens(s)) == 4)
		self.assertNotIn( ',' , splitByCommasWithoutParens(s)[1])

		s = "b as bb"
		self.assertTrue(splitByCommasWithoutParens(s) == ["b as bb"])

	def testAliasesBelongInTables(self):
		s = "select a as aa, 2*t.b+4 as bb, sin(c) as cc, d, 3 as e from t;"
		res = self.process(s)

		self.assertIn( 't.aa' ,  self.ss.projectionCols)
		self.assertIn( 't.bb' ,  self.ss.projectionCols)
		self.assertIn( 't.cc' ,  self.ss.projectionCols)
		self.assertIn( 't.d' ,  self.ss.projectionCols)
		self.assertIn( 't.e' ,  self.ss.projectionCols)

		#aa, bb, cc are displayed "a as aa"
		self.assertIn( 't.e' , self.ss.columns)

	def testAliasesForMoreTables(self):
		pass

	def testMultipleFrom(self):
		s = "select a, b from t1 from t2;"
		self.assertRaises(MallformedSQLException, self.process, s)

	def testReservedWordsJoin(self):
		s = "select * from t1 join t2 on t1.inner_1 = t2.left_2 join " + \
			"t3 on t2.left_2 = t3.full_3"
		res = self.process(s)

		self.assertIn( 't1.inner_1' ,  self.ss.joins)
		self.assertIn( 't2.left_2' ,  self.ss.joins)
		self.assertIn( 't3.full_3' ,  self.ss.joins)
		self.assertTrue( len(self.ss.joins) == 3)

	def testJoinWithExtraParens(self):
		s = "select * from t1 join (t2 a2) on t1.x = a2.x join " + \
			"(t3 a3) on a2.y = a3.y"
		res = self.process(s)

		self.assertIn( 't1.x' ,  self.ss.joins)
		self.assertIn( 'a2.x' ,  self.ss.joins)
		self.assertIn( 'a2.y' ,  self.ss.joins)
		self.assertIn( 'a3.y' ,  self.ss.joins)

	def testCountStar(self):
		s = "select count(*) from t1, t2;"

		# * is now a valid column, previously only t.* was ok
		res = self.process(s)

	def testCountDistinct(self):
		s = """SELECT count(distinct x) from t"""
		res = self.process(s)

		self.assertNotIn( 't.x)'  , self.ss.columns)

	def testSelectDistinct(self):
		s = """Select distinct a.id from a """
		res = self.process(s)

		self.assertIn( 'a.id' , self.ss.columns)

	def testSubselectEdges(self):
		s = """select fact.*, region.name, product.name
			from fact,
					[ 1 ] region,
					[ 2 ] product
			where 
				fact.region_id = region.id and
				fact.product_id = product.id;"""

		res = self.process(s)
		self.assertIn( 'region' ,   self.ss.subselects)
		self.assertIn( 'product' ,   self.ss.subselects)

	def testGROUP_CONCAT2(self):
		#this query is on the frontpage; embarrasing
		s = """SELECT CONCAT(a, 2 + b) FROM t"""

		res = self.process(s)

		self.assertNotIn( 'concat'  , self.ss.columns)

class DotOutputTestCase(unittest.TestCase):
	def setUp(self):
		self.si = Simplifier()
		self.qr = QuoteRemover()
		#param is for quoted consts; those change after each process()
		self.ss = SingleSelect(self.qr)
		self.dot = DotOutput(self.ss)

	def process(self, s, verbose = True):
		dummy = self.ss.process(
					self.si.process(
						self.qr.process(s)))
		if verbose:
			print('*'*5, self.si.process(self.qr.process(s)))
		return self.dot.process()[0]

	def testSimple(self):
		s = "select t.m99 from table t where t.a11=1"
		res = self.process(s)

		self.assertIn( 'a11 = 1' , res)
		self.assertIn( 'm99' , res)
		self.assertIn( 'T | (TABLE)' , res)
		
	def testColumnAlias(self):
		s = "select t.m99 as mm from t"
		res = self.process(s)

		self.assertIn( 'm99' , res)
		self.assertIn( '<m99>' , res)
		self.assertIn( 'AS mm' , res)
		self.assertIn( '<mm>' , res)

	def testSum(self):
		s = "select sum(t.m99) from table t where t.a11=1 group by t.b22"
		res = self.process(s)

		self.assertIn( 'SUM(m99)' , res)
		self.assertIn( 'a11 = 1' , res)
		self.assertIn( 'GROUP BY b22' , res)

	def testJoins(self):
		s = "select t1.a11, sum(t2.b22) from table1 t1 inner join table2 t2 on t1.id = t2.id group by t1.a11"
		res = self.process(s)

		self.assertIn( 'a11' , res)
		self.assertIn( 'SUM(b22)' , res)
		self.assertIn( 'GROUP BY a' , res)
		self.assertIn( '<id> id' , res)
		self.assertIn( 'T1:id' , res)
		self.assertIn( 'T2:id' , res)

	def testSakila_1(self):
		s = """SELECT
			cu.customer_id AS ID,
			CONCAT(cu.first_name, _utf8' ', cu.last_name) AS name,
			a.address AS address,
			a.postal_code AS `zip code`,
    		a.phone AS phone,
			city.city AS city,
			country.country AS country,
			IF (cu.active, _utf8'active',_utf8'') AS notes,
			cu.store_id AS SID
		FROM
			customer AS cu JOIN address AS a ON
				cu.address_id = a.address_id
			JOIN city ON a.city_id = city.city_id
    		JOIN country ON city.country_id = country.country_id;"""
		res = self.process(s)

		self.assertIn( 'A:address_id -- CU:address_id [color = black arrowtail="none"' , res)
		self.assertIn( 'CITY:country_id -- COUNTRY:country_id [color = black arrowtail="none"' , res)
		self.assertIn( 'A:city_id -- CITY:city_id [color = black arrowtail="none"' , res)
		self.assertNotIn( 'concat' , res.lower() )

	def testPagila_1(self):
		s = """SELECT
				cu.customer_id AS id,
				(((cu.first_name)::text || ' '::text) || (cu.last_name)::text) AS name,
				a.address,
				a.postal_code AS "zip code",
				a.phone,
				city.city,
				country.country,
				CASE WHEN cu.activebool THEN 'active'::text ELSE ''::text END AS notes,
				cu.store_id AS sid
			FROM
				(((customer cu JOIN address a ON
					((cu.address_id = a.address_id)))
				JOIN city ON ((a.city_id = city.city_id)))
				JOIN country ON ((city.country_id = country.country_id)));"""
		res = self.process(s)

		self.assertIn( 'A:address_id -- CU:address_id [color = black arrowtail="none"' , res)
		self.assertIn( 'CITY:country_id -- COUNTRY:country_id [color = black arrowtail="none"' , res)
		self.assertIn( 'A:city_id -- CITY:city_id [color = black arrowtail="none"' , res)
		self.assertIn( 'zip' , res)

	def testPagila_2(self):
		#Pagila, Postgres syntax
		s="""SELECT
			film.film_id AS fid,
			film.title,
			film.description,
			category.name AS category,
			film.rental_rate AS price,
			film.length,
			film.rating,
			group_concat((((actor.first_name)::text || ' '::text) ||
				(actor.last_name)::text)) AS actors
			FROM
				((((category LEFT JOIN film_category ON
					((category.category_id = film_category.category_id)))
				LEFT JOIN film ON ((film_category.film_id = film.film_id)))
				JOIN film_actor ON ((film.film_id = film_actor.film_id)))
				JOIN actor ON ((film_actor.actor_id = actor.actor_id)))
			GROUP BY
				film.film_id, film.title, film.description, category.name, 
				film.rental_rate, film.length, film.rating;"""

		res = self.process(s)
		self.assertIn( 'ACTOR:actor_id -- FILM_ACTOR:actor_id [color = black arrowtail="none"' , res)
		self.assertIn( ('CATEGORY:category_id -- FILM_CATEGORY:category_id [color = %s arrowtail="%s" arrowhead="none"];' % \
			(OUTERJOINCOLOR, OUTERJOINARROW) ) , res)
		self.assertIn( ('FILM:film_id -- FILM_CATEGORY:film_id [color = %s arrowtail="none" arrowhead="%s"];' % \
			(OUTERJOINCOLOR, OUTERJOINARROW) ) , res)
		self.assertIn( 'FILM:film_id -- FILM_ACTOR:film_id [color = black arrowtail="none"' , res)
		self.assertIn( 'actors' , res)

	def testSakila_3(self):
		s = """SELECT
			c.name AS category,
			sum(p.amount) AS total_sales
			FROM
				(((((payment p JOIN rental r ON ((p.rental_id = r.rental_id)))
				JOIN inventory i ON ((r.inventory_id = i.inventory_id)))
				JOIN film f ON ((i.film_id = f.film_id)))
				JOIN film_category fc ON ((f.film_id = fc.film_id)))
				JOIN category c ON ((fc.category_id = c.category_id)))
			GROUP BY c.name ORDER BY sum(p.amount) DESC;"""

		res = self.process(s)
		self.assertIn( 'P:rental_id -- R:rental_id [color = black arrowtail="none"' , res)
		self.assertIn( 'C:category_id -- FC:category_id [color = black arrowtail="none"' , res)
		self.assertIn( 'I:inventory_id -- R:inventory_id [color = black arrowtail="none"' , res)
		self.assertIn( 'F:film_id -- I:film_id [color = black arrowtail="none"' , res)
		self.assertIn( 'F:film_id -- FC:film_id [color = black arrowtail="none"' , res)

	def testMondrian(self):
		s = """SELECT
            `DC`.`gender`,
            `DC`.`marital_status`,
            `DPC`.`product_family`,
            `DPC`.`product_department`,
            `DPC`.`product_category`,
            `DT`.`month_of_year`,
            `DT`.`quarter`,
            `DT`.`the_year`,
            `DB`.`customer_id`
        FROM
            `sales_fact_1997` `DB`,
            `time_by_day` `DT`,
            `product` `DP`,
            `product_class` `DPC`,
            `customer` `DC`
        WHERE
            `DB`.`time_id` = `DT`.`time_id`
        AND `DB`.`customer_id` = `DC`.`customer_id`
        AND `DB`.`product_id` = `DP`.`product_id`
        AND `DP`.`product_class_id` = `DPC`.`product_class_id`"""
		res = self.process(s)
		self.assertIn( 'DB:customer_id -- DC:customer_id [color = black arrowtail="none"' , res)
		self.assertIn( 'DB:time_id -- DT:time_id [color = black arrowtail="none"' , res)
		self.assertIn( 'DP:product_class_id -- DPC:product_class_id [color = black arrowtail="none"' , res)
		self.assertIn( 'DB:product_id -- DP:product_id [color = black arrowtail="none"' , res)


	def testDiffer(self):
		s = """select * from t where t.xxxx <> 1 """
		res = self.process(s)
		self.assertIn('\<\> 1', res)

	def testOuterJoins(self):
		#not sure it's ok to mix INNER and (+)= ...
		s = "select * from t1 inner join t2 on t1.id1 (+)= t2.id2;"
		res = self.process(s)

		self.assertIn( "<id1> id1" , res)
		self.assertIn( "<id2> id2" , res)
		self.assertIn( "T1:id1 -- T2:id2" , res)
		self.assertNotIn( "T2:id2 -- T1:id1" ,res)
		self.assertIn( ('[color = %s arrowtail="%s" arrowhead="none"];' % (OUTERJOINCOLOR, OUTERJOINARROW) )\
				, res)

	def testFullJoins(self):
		s = "select * from t1 full outer join t2 on t1.id1 = t2.id2"
		res = self.process(s)

		self.assertIn( "<id1> id1" , res)
		self.assertIn( "<id2> id2" , res)
		self.assertIn( "T1:id1 -- T2:id2" , res)
		self.assertIn( ('arrowtail="%s" arrowhead="%s"' % (OUTERJOINARROW, OUTERJOINARROW) ) , res)

	def testInClause(self):
		s = ("select * from table t where t.a11 in (1,2,3) "
			"and t.b22 in ('x', 'y')" )
		res = self.process(s)

		self.assertIn( "a11", res)
		self.assertIn( "b22", res)
		self.assertIn( "in (1,2,3)", res)
		asserNotIn( "IN IN", res.upper())
		
	def testInClause(self):
		s = ("select * from table t where t.a11 not in (1,2,3) "
			"and t.b22 not in ('x', 'y')" )
		res = self.process(s)

		self.assertIn("a11", res)
		self.assertIn("b22", res)
		self.assertIn("not in (1,2,3)", res)
		self.assertIn("not in ('x'", res)
		self.assertNotIn("IN IN", res.upper())

	def testLIKE(self):
		s = ("select table.* from table where table.a11 LIKE '%qqq%' " )
		res = self.process(s)

		self.assertIn("a11",res)
		self.assertIn("like '%qqq%'", res)
		self.assertIn("*", res)
		

	def testBetween(self):
		s = ("select table.* from table where table.a11 between 1 and 2 and table.b22 =3 " )
		res = self.process(s)

		self.assertIn("b22", res)
		self.assertIn("= 3", res)
		self.assertIn("a11", res)
		self.assertIn("between 1 and 2", res)
		#bugfix
		self.assertNotIn("'between 1 and 2'", res)
		
	def testDollar(self):
		s = ("select * from ${table}" )
		res = self.process(s)

		self.assertIn( "TABLE | ($TABLE)" , res)
		
	def testPipeCompar(self):
		s = ("select * from t where x |= 3" )
		res = self.process(s)

		self.assertIn("x \|= 3", res)
		
	def testNegativeNumber(self):
		s = ("select * from t where x = -3" )
		res = self.process(s)

		self.assertIn("x = -3", res)
		
	def testFloatNumber(self):
		s = ("select * from t where x = 3.3" )
		res = self.process(s)

		self.assertIn("x = 3.3", res)
		
	def testLessVerboseOutput(self):
		s = ("SELECT sum(t1.x) from t1, t2 where t1.x=t2.y and t2.y=3")
		res = self.process(s)

		self.assertIn( "<x> SUM(x)",res)
		self.assertIn( "<y> y = 3", res)
		
	def testSingleQuotedDoubleQuote(self):
		s = ("""SELECT * from t where x='quote="'; """)
		res = self.process(s)

		self.assertIn("quote=\\", res)
		
	def testSpaceInAlias(self):
		s = ("""SELECT x as "x alias" from t""")
		res = self.process(s)
		
		self.assertIn(r'\"x alias\"',res)
		
	def testOrderBy(self):
		s = ("""SELECT * from t order by y, z desc""")
		res = self.process(s)
		
		self.assertIn('ORDER BY y|', res)
		self.assertIn('ORDER BY z DESC', res)
		
	def testCountDistinct(self):
		s = ("""SELECT count(distinct x) from t""")
		res = self.process(s)
		
		self.assertIn('COUNT(DISTINCT x)',res)
		self.assertIn('COUNT',res)
		
	def testHavingHasNoTableName(self):
		s = "select t.x, sum(t.y) from t group by x having nvl(sum(t.z),0) > 0;"
		res = self.process(s)

		self.assertIn('SUM(z)', res)
		self.assertNotIn('_agg',res)
		
	def testDerivedTables(self):
		s = 'select w.a, t.b from [ 1 ] as w, t where t.a=w.a'
		res = self.process(s)
		
		self.assertNotIn('label="W |', res)
		self.assertTrue(self.assertIn('T:a --', res) or self.assertIn('-- T:a', res))
		
	def testColAliasExpressionsWithJoins(self):
		s = 'select 2*(t1.m+"t2.n"+3) as oo ,t1.b as bb from t1, t2;'
		res = self.process(s)

		self.assertNotIn( '... AS bb',res)
		self.assertIn( '... AS oo',res)
		
	def testShorterAliasExpressions(self):
		s = 'select t1.b+t1.m as bb from t1, t2;'
		res = self.process(s)

		self.assertIn('... AS bb', res)
		#_ is a special field meaning expression alias
		self.assertNotIn( '_|' ,res)
		
	def testNotShorterAliasExpressions(self):
		s = 'select t1.b+t1.m as bb, t1.xx as xx from t1, t2 order by t1.bb;'
		res = self.process(s)

		self.assertIn('... AS bb', res)
		self.assertIn('ORDER BY bb', res)
		
		self.assertIn('xx AS xx', res)
		
	def testSelectStarAlone(self):
		s = 'select * from t;'
		res = self.process(s)

		self.assertIn( '*' , res)
		
	def testColAliasesWithoutAS(self):
		s = 'select t1.a aa, t2.b bb, t1.a+t1.b c from t1, t2;'
		res = self.process(s)

		self.assertIn( 'a AS aa',res)
		self.assertIn('b AS bb',res)
		self.assertIn('... AS c',res)
		
	def testTrueFalse(self):
		s = "select * from t where a = true and b=false;"
		res = self.process(s)
		
		self.assertIn('a = 1', res)
		self.assertIn('b = 0', res)
		
	def testLeftJoinWithFilter(self):
		s = "select * from t1 left join t2 on t1.a=t2.a and t2.b=4;"
		res = self.process(s)
		
		# left join represented internally as #=
		self.assertIn('b = 4',res)
		self.assertNotIn('#', res)
		
	def testHaving(self):	
		s = "select * from t group by x having nvl(sum(z)) > 0;"
		res = self.process(s)
		
		self.assertIn(r'HAVING SUM(z)\> 0', res)
		
	def testIncorrectConstantAlias(self):
		s = "select case when a='x' then 1 end from t"
		res = self.process(s)
		
		#previously alias was set to the constant
		self.assertNotIn( QUOTESYMBOL, res)
		
		
	def testFanChasmTrap(self):
		s = """select p.product, sum(o.sales), sum(sp.sales) from 
		products p inner join orders o on p.pid = o.pid inner join 
		salesplans sp on p.pid = sp.pid group by p.pid"""
		res = self.process(s)
		
		self.assertIn("Risk of Fan and/or Chasm trap", res)
		
	def testSchemaField(self):
		s = "Select a.x, a.y, schema.B.z From a, schema.b where a.id=schema.b.id"
		res = self.process(s)
		
		self.assertIn('SCHEMA__B |', res)
		self.assertIn('(SCHEMA.B)', res)
		self.assertTrue(self.assertIn('A:id -- SCHEMA__B:id', res) or self.assertIn('SCHEMA__B:id -- A:id', res))
		
	def testSchemaFieldAlias(self):
		s = "Select a.x, a.y, B.z From a, schema.b b where a.id=b.id"
		res = self.process(s)

		self.assertIn( 'B |', res)
		self.assertIn('(SCHEMA.B)', res)
		self.assertTrue(self.assertIn('A:id -- B:id', res) or self.assertIn('B:id -- A:id', res))
		
	def testBindVariables(self):
		s = "select * from a where a = :999 and b = :xxx"
		res = self.process(s)

		self.assertIn( 'a = :999', res)
		self.assertIn( 'b = :xxx', res)
		
class SelectAndSubselectsTest(unittest.TestCase):
	def setUp(self):
		self.sas = SelectAndSubselects()
		
	def _testIn(self):
		s = "select ((a)) .. where x in (select 1...) and y in (select 2...)"
		(start, end) = self.sas.getMostNested(s)
		self.assertIn( '(select 1...)', s[start:end])
		
	def testNA(self):
		s = "select ((a)) .. where x =0"
		(start, end) = self.sas.getMostNested(s)
		self.assertTrue( s == s[start:end])
		
	def testNesting1(self):
		s = "select ((a)) from (select 1... where (select 2...))"
		(start, end) = self.sas.getMostNested(s)
		self.assertIn( '(select 2...)' , s[start:end])
		
	def testNesting2(self):
		s = ("select ((a)) from (select 1... where (select 2...)) and "
			"((x in (select 3...)))")
		(start, end) = self.sas.getMostNested(s)
		self.assertIn( '(select 3...)' , s[start:end])
		
	def testNesting3(self):
		s = "select ((a)) from (select 1...() where (select 2...()))"
		(start, end) = self.sas.getMostNested(s)
		self.assertIn( '(select 2...())' , s[start:end])

	def testUnionAndSubselects(self):
		s = ("select ((a)) from (select 1... where (select 2...)) "
			"union select 3...")
		(start, end) = self.sas.getMostNested(s)
		self.assertIn( '(select 2...)' , s[start:end])
		self.assertNotIn( 'union' , s[start:end])
		
	def testUnionOnly(self):
		s = "select 1... union select 2... union select 3..."
		(start, end) = self.sas.getMostNested(s)
		self.assertIn( 'select 2...' , s[start:end])
		self.assertIn( 'union' , s[start:end])
		
	def testNestedUnion(self):
		s = "select 1 .. (select 2... union select 3...)"
		(start, end) = self.sas.getMostNested(s)
		self.assertIn( 'select 3...' , s[start:end])
		self.assertNotIn( 'union', s[start:end])
		self.assertNotIn( ')', s[start:end])

	def testAliasedSubselect(self):
		s = "select 1... from table, (select 2...) as a where ..."
		(start, end) = self.sas.getMostNested(s)
		self.assertIn( 'select 2...' , s[start:end])

		
	def testProcessIn(self):
		s = ("select ((a)) from (select 1... where (select 2...)) and "
			"((x in (select 3...)))")
		stack = self.sas.getSqlStack(s)
		self.assertIn( '(select 3...)' , stack[0][1])
		self.assertIn( '(select 2...' , stack[1][1])
		self.assertIn( '(select 1... where  [ 2 ] )' , stack[2][1])
		self.assertIn( 'select ((a)) from  [ 3 ]  and ((x in  [ 1 ] ))' , stack[3][1])
		
		#self.sas.process(s)
		
	def testProcessCorrelated(self):
		s = """SELECT Album.song_name FROM Album
		WHERE Album.band_name = 'Metallica'
		AND EXISTS
		(SELECT Cover.song_name FROM Cover
		WHERE Cover.band_name = 'Damage, Inc.'
		AND Cover.song_name = Album.song_name);"""
				
		res = query2Dot(s)
		self.assertIn( 'ALBUM:song_name -> ___SUBSELECT____1_COVER:song_name ' , res)
		
	def testCorrelatedAlias(self):
		s = "select a.a as xxx from a, (select * from b where b.b = a.xxx) "
		res = self.sas.process(s, 'neato')
		
		self.assertIn( 'A:xxx -> ___SUBSELECT____1_B:b ' , res)

	def testSubselect(self):
		s = "SELECT a.id from a where a.b in (select c from d) "
		res = query2Dot(s)
		self.assertNotIn( 'workaround_' ,res)
		self.assertIn( '[label="IN"]' , res)
		self.assertIn( '<b> b' , res)
		self.assertIn( '<c> c' , res)
		
	def testDerivedTables(self):
		s = """select dt_1.a, dt_1.b, dt_2.c
		from (select a, b from t1) 
         as dt_1,
        (select b, c from t2) 
         as dt_2
		where dt_1.b = dt_2.b"""
		res = query2Dot(s)
		
		self.assertIn( "DERIVED TABLE | (DT_1)" , res)
		self.assertIn( "DERIVED TABLE | (DT_2)" , res)
		self.assertIn( "T1 | (T1)" , res)
		self.assertIn( "T2 | (T2)" , res)
		self.assertIn( "DT_1:b:e -> DT_2:b:w" , res)
		
	def testSubselectEdges(self):
		s = """select fact.*, region.name, product.name
			from fact,
					(select name, id from translation where lang='EN' and type='region') region,
					(select name, id from translation where lang='EN' and type='product') product
			where 
				fact.region_id = region.id and
				fact.product_id = product.id;"""
				
		res = query2Dot(s)
		
		self.assertIn( 'REGION -> ___SUBSELECT____1__dummy' , res)
		self.assertIn( 'PRODUCT -> ___SUBSELECT____2__dummy' , res)
		
	def testSubselectPreferLocalTable(self):
		s = """select *
			from
			(select * from A,B where A.x=B.y) D1,
			(select * from A,B where A.x=B.y) D2"""
			
		res = query2Dot(s)
		
		#disregard tables A,B from the other subselect; use local ones
		self.assertIn( '___SUBSELECT____2_A:x:e -> ___SUBSELECT____2_B:y:w' , res)
		self.assertIn( '___SUBSELECT____1_A:x:e -> ___SUBSELECT____1_B:y:w' , res)
		
	def testSubselectPreferLocalAlias(self):
		s = """select *
		from
		(select * from ta A,tb B where A.x=B.y) D1,
		(select * from ta A,tb B where A.x=B.y) D2"""
		
		res = query2Dot(s)
		
		#disregard tables A,B from the other subselect; use local ones
		self.assertIn( '___SUBSELECT____2_B:y:e -> ___SUBSELECT____2_A:x:w' , res)
		self.assertIn( '___SUBSELECT____1_B:y:e -> ___SUBSELECT____1_A:x:w' , res)
		
	def testSubselectParentTables(self):
		s = """select parentP.y,q.z
			from
			 parentP,
			 parentQ q,
			 (select * from A,B where A.x=B.x and A.y=parentP.y and A.z = q.z) D1"""
			 
		res = query2Dot(s)
		
		#finds tables and aliases from parent
		self.assertIn( '___SUBSELECT____1_A:y -> PARENTP:y' , res)
		self.assertIn( '___SUBSELECT____1_A:z -> Q:z' , res)
		
	def testInSubselect(self):
		s = """select * from (select A.id from A 
		where A.id IN (select B.id from B where v=5)) D1 """
			 
		res = query2Dot(s)
		
		#finds tables and aliases from parent
		self.assertIn( '___SUBSELECT____2_A:id:e -> ___SUBSELECT____1_B:id:w' , res)
		
	def testLongIn(self):
		s = """SELECT * from t where x in (1,2,3,4,5,6,7,8,9 , 100 )"""
		res = query2Dot(s)
		self.assertIn( '...' , res)
		self.assertNotIn( '100' ,res)
		
	def testAnonSubselects(self, s):
		s = """select * from (select A.id from A)"""
		res = query2Dot(s)
		self.assertIn( 'ANON_SUBSELECT_1 -> ___SUBSELECT____1' , res)
		
	def testInSubSelectPopFromEmptySet(self):
		s = "select o.x from out o where o.y in ( select * from t1 at1)"
		res = query2Dot(s)
		self.assertIn( 'O:y:e -> ___SUBSELECT____1__dummy [label="IN"]' , res)
		
	def testUnion(self):
		s = """select o.x from out o where o.y in	(
				 select t1.a from t1 at1
				 uniON all
				 select t2.b from t2
				 eXcePT All
				 select t3.c from t3)"""
		res = query2Dot(s)
		self.assertIn( '___SUBSELECT____3__dummy -> ___SUBSELECT____2__dummy' , res)
		self.assertIn( 'label="union all"' , res)
		self.assertIn( '___SUBSELECT____2__dummy -> ___SUBSELECT____1__dummy' , res)
		self.assertIn( 'label="except all"' , res)
		
	def testUnionTopLevel(self):
		s = "SELECT a from A union select b from B"
		res = query2Dot(s)
		#add a cluster also for the main =top SQL frame
		self.assertIn( 'subgraph cluster_main' , res)
		self.assertIn( '_dummy -> ___SUBSELECT____1__dummy' , res)
		
	def testInSubselectWithoutTableCol(self):
		s = "SELECT * FROM A WHERE x IN  (SELECT y FROM B )"
		res = query2Dot(s)
		self.assertIn( 'A:x:e -> ___SUBSELECT____1_B:y:w [label="IN"]' , res)
		
	def testJoinIsNotInSubselect(self):
		s = "select * from D1 full outer join (select b from B) D2"
		res = query2Dot(s)
		#in does not match last 2 letters of join
		self.assertNotIn( 'jo:' ,res)
		self.assertNotIn( '[label="IN"]' ,res)
		
	def testCaselessUnion(self):
		s = "SELECT a FROM A UNION seLECT b FROM B"
		#usd to throw exception because select was not all lowercase
		res = query2Dot(s)
		self.assertIn( 'label="union"' , res)
		
	def testMoreUnions(self):
		s = """select t1.a from T1
			union all select t2.b from T2
			union all select t3.c from T3"""

		res = query2Dot(s)
		#2 UNION edges
		self.assertIn( '_dummy -> ___SUBSELECT____1__dummy' , res)
		self.assertIn( '_dummy -> ___SUBSELECT____2__dummy' , res)
		self.assertIn( 'label="union all"' , res)
		
		#no table alias for ALL
		self.assertNotIn( 'ALL | (ALL)' ,res)

	def testFuncsNoParens(self):
		s = "Select  t1.x, t2.y from t1, t2 where t1.z=sysdate;"
		res = query2Dot(s)
		
		self.assertIn("sysdate" , res)	#don' remove == constant func()