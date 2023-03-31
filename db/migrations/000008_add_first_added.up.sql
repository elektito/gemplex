alter table urls
      add column first_added timestamp;
alter table urls
      rename column last_visit_time to last_visited;
