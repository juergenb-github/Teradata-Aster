grant execute on function public.unzip to b_schulmeister;

-- Small data example
select	archive, name as trace, "time", "size", "compressedSize", "isDirectory", "method", "crc", "comment", "chunk", octet_length("content")
from	unzip(	
			on(
				select	"file" as archive, chunk, content as zipped
				from	a_input.t_binary_load
				where	"file" = '/data/daimlerdata/2012/03/14/INDIA_archive_20120314-0230.zip'
			)
			partition by (archive)
			order by chunk
			zip('zipped')
			accumulate('archive')
		);

-- Large data example
select	archive, name as trace, "time", "size", "compressedSize", "isDirectory", "method", "crc", "comment", "chunk", octet_length("content")
from	unzip(	
			on(
				select	"file" as archive, chunk, content as zipped
				from	a_input.t_binary_load
				where	"file" = '/data/daimlerdata/2012/05/09/INDIA_archive_20120509-1530.zip'
			)
			partition by (archive)
			order by chunk
			zip('zipped')
			accumulate('archive')
		);
		
-- Encoding sample. continues at the error
select	archive, trace, name, chunk, octet_length(content), "time", "size", "compressedSize", "isDirectory", "method", "crc", "comment"
from	unzip(
			on(	select	archive, name as trace, chunk, content as zipped
				from	unzip(
							on(
								select	"file" as archive, chunk, content as zipped
								from	a_input.t_binary_load
								where	"file" = '/data/daimlerdata/2012/10/04/INDIA_archive_20121004-1625.zip'
							)
							partition by archive
							order by chunk
							zip('zipped')
							accumulate('archive')
						)
			)
			partition by archive, trace
			order by chunk
			zip('zipped')
			accumulate('archive', 'trace')
			encode('UTF-8')
		);

-- Stops at the error, print partition definition
select	archive, trace, name, chunk, octet_length(content), "time", "size", "compressedSize", "isDirectory", "method", "crc", "comment"
from	unzip(
			on(	select	archive, name as trace, chunk, content as zipped
				from	unzip(
							on(
								select	"file" as archive, chunk, content as zipped
								from	a_input.t_binary_load
								where	"file" = '/data/daimlerdata/2012/10/04/INDIA_archive_20121004-1625.zip'
							)
							partition by archive
							order by chunk
							zip('zipped')
							accumulate('archive')
						)
			)
			partition by archive, trace
			order by chunk
			zip('zipped')
			accumulate('archive', 'trace')
			encode('UTF-8')
			stopOnError('true')
		);

		