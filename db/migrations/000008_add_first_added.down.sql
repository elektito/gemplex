alter table urls
      rename column last_visited to last_visit_time;
alter table urls
      drop column first_added;
