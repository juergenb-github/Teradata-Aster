-- Simple check with direct includes
SELECT count(*)
FROM XMLFastFilter(
	ON(	select	data
		from	table_from_afs(
					on dummy
					path('/home/beehive/xml/*.zip')
					input_format('com.asterdata.ncluster.sqlmr.dfsinput.WholeFileAsBinaryInputFormat')
					outputs('data bytea')
				)
    )
	xml('data')
	unzip('UTF-8')
	include('Diagnostic_Tree', 'XDSystemError'));

-- Simple check with regexp includes
SELECT count(*)
FROM XMLFastFilter(
	ON(	select	data
		from	table_from_afs(
					on dummy
					path('/home/beehive/xml/*.zip')
					input_format('com.asterdata.ncluster.sqlmr.dfsinput.WholeFileAsBinaryInputFormat')
					outputs('data bytea')
				)
    )
	xml('data')
	unzip('UTF-8')
	include('/.*/Diagnostic_Tree/instance_URL/', '/.*/XDSystemError/Timestamp/'));

-- Simple check all included
SELECT *
FROM XMLFastFilter(
	ON(	select	data
		from	table_from_afs(
					on dummy
					path('/home/beehive/xml/*.zip')
					input_format('com.asterdata.ncluster.sqlmr.dfsinput.WholeFileAsBinaryInputFormat')
					outputs('data bytea')
				)
    )
	xml('data')
	unzip('UTF-8'))
LIMIT 5;

-- Simple check direct excludes
SELECT *
FROM XMLFastFilter(
	ON(	select	data
		from	table_from_afs(
					on dummy
					path('/home/beehive/xml/*.zip')
					input_format('com.asterdata.ncluster.sqlmr.dfsinput.WholeFileAsBinaryInputFormat')
					outputs('data bytea')
				)
    )
	xml('data')
	unzip('UTF-8')
	exclude('Diagnostic_Tree'))
LIMIT 5;

-- Simple skip after attribute
SELECT count(*)
FROM XMLFastFilter(
	ON(	select	data
		from	table_from_afs(
					on dummy
					path('/home/beehive/xml/*.zip')
					input_format('com.asterdata.ncluster.sqlmr.dfsinput.WholeFileAsBinaryInputFormat')
					outputs('data bytea')
				)
    )
	xml('data')
	unzip('UTF-8')
	include('language')
	skipAfter('language'));


-- unzip and skip after attribute
SELECT count(*)
FROM XMLFastFilter(
	ON(	select	data
		from	table_from_afs(
					on dummy
					path('/home/beehive/xml/*.zip')
					input_format('com.asterdata.ncluster.sqlmr.dfsinput.WholeFileAsBinaryInputFormat')
					outputs('data bytea')
				)
    )
	xml('data')
	unzip('UTF-8')
	include('language')
	skipAfter('language'));
