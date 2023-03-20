alter table urls
      add column hostname text not null;

create index urls_hostname on urls (hostname);
