-- author: @jotegui

unload ('SELECT {0} FROM {1} WHERE {5} IS NOT NULL AND {5} !=\'\'')
to '{2}/slim' credentials
'aws_access_key_id={3};aws_secret_access_key={4}'
delimiter as '\t';
