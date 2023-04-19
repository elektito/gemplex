alter table hosts
      drop column robots_prefixes,
      drop column robots_valid_until,
      drop column robots_last_visited,
      drop column robots_retry_time;
