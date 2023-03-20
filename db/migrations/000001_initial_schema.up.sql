create table contents (
       id bigserial primary key,
       hash text unique not null,
       content text not null,
       fetch_time timestamp not null,
       content_type text not null,
       content_type_args text,
       title text
);

create table urls (
       id bigserial primary key,
       url text unique not null,
       last_visit_time timestamp,
       content_id bigint references contents(id),
       error text,
       status_code int,
       redirect_target text,

       -- this has several meanings, depending on other fields:
       --  - if successfully visited before, it's how long we wait until
       --    we visit it again. the value will be increased linearly (up
       --    to a cap), each time we get the same content.
       --  - if last visit resulted in a temporary redirect (4x code),
       --    it's how long we wait until we try this url again to see
       --    if the redirect is gone. the value will be increased
       --    exponentially each time we get the same redirect.
       --  - if last visit resulted in an error (connection/dns/tls/
       --    protocol), it's the next time we'll try again. the value
       --    will be increased exponentially on each failure.
       retry_time interval
);

create table links (
       src_url_id bigint not null references urls(id),
       dst_url_id bigint not null references urls(id),

       primary key (src_url_id, dst_url_id)
);

-- insert seed urls
insert into urls (url) values
       ('gemini://elektito.com'),
       ('gemini://medusae.space/index.gmi'),
       ('gemini://gemini.circumlunar.space'),
       ('gemini://gemini.conman.org/');
