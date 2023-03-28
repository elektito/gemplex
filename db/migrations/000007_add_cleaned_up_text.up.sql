alter table contents
      rename column content to content_text;
alter table contents
      add column content bytea not null;
