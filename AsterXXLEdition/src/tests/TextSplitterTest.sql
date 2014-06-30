--delete	from "text_lens";
insert	into "text_lens"
select	*
from	nPathViz(
			on(	select	"author", "sentence", 1 as "frequency"
				from	nPath(
							on(	select	"author", "type", "value"
								from	TextSplitter(
											on(	select	row_number() over( order by "text") as "author", "text"
												from	table_from_afs(
															on dummy
															path('/home/beehive/text/*')
															input_format('com.asterdata.ncluster.sqlmr.dfsinput.WholeFileAsTextInputFormat')
															outputs('text character varying')
														)
											)
											"text"('text')
											accumulate('author')
										)
								where	"type" = 'LETTER_DIGIT'
									or	("type" = 'OTHER' and "value" in ('.', '!'))
							)
							partition by "author"
							MODE(NONOVERLAPPING)
							SYMBOLS(
								"type" = 'LETTER_DIGIT' as W,
								"type" = 'OTHER' as P
							)
							PATTERN('W+.P')
							RESULT(
								first("author" of P) as "author",
								accumulate("value" of W) as "sentence"
							)
						)
				where "author" = 2
				limit 100
			)
			partition by "author"
			frequency_col('frequency')
			path_col('sentence')
			GRAPH_TYPE('chord')
			DIRECTED('true')
			TITLE('Shakespeare or Willcox?')
			SUBTITLE('This is Willcox')
			accumulate('author') 
		) 

