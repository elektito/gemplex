alter table urls
      add column hostname text;

-- set the hostnames for the seed urls, before we make it "not null"
update urls set hostname = 'elektito.com' where url = 'gemini://elektito.com/';
update urls set hostname = 'medusae.space' where url = 'gemini://medusae.space/index.gmi';
update urls set hostname = 'gemini.circumlunar.space' where url = 'gemini://gemini.circumlunar.space/';
update urls set hostname = 'gemini.conman.org' where url = 'gemini://gemini.conman.org/';

alter table urls
      alter column hostname set not null;

create index urls_hostname on urls (hostname);
