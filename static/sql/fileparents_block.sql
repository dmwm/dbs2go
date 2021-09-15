SELECT DISTINCT file_id from {{.Owner}}.FILES f
	   INNER JOIN {{.Owner}}.BLOCKS b on f.block_id=b.block_id
	   WHERE b.block_name = :block_name
