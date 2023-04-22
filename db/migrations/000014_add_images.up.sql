-- this table is intentionally de-normalized. already, deleting urls takes quite
-- a long time because of FK checks.
create table images (
       id bigserial primary key,
       image_hash text unique not null,
       content_hash text not null,
       image text not null,
       alt text not null,
       fetch_time timestamp not null,
       url text not null
);
