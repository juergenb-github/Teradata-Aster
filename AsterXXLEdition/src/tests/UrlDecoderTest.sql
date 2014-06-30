select	*
from	UrlDecode(
			on(	select 'a' as "myTextColumn", 1 as "id" from dummy
				union
				select 'Mozilla%2F5.0%20%28Windows%20NT%206.1%3B%20WOW64%3B%20rv%3A29.0%29%20Gecko%2F20100101%20Firefox%2F29.0' as "myTextColumn", 2 as "id" from dummy
			)
			value('myTextColumn')
		)