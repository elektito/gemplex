alter table hosts
      add column robots_prefixes text,
      add column robots_valid_until timestamp,
      add column robots_last_visited timestamp,
      add column robots_retry_time interval;
