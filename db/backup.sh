#!/usr/bin/env sh

set -e

DB_NAME=${DB_NAME:-gemplex}
MAX_BACKUPS=${MAX_BACKUPS:-7}
BACKUP_PREFIX=${BACKUP_PREFIX:-gemplex-}
BACKUP_SUFFIX=${BACKUP_SUFFIX:-.dump}
DEST_DIR=${DEST_DIR:-/var/lib/gemplex/backups}

mkdir -p "${DEST_DIR}"

nbackups=$(ls "${DEST_DIR}" | wc -l)
new_backup=${DEST_DIR}/${BACKUP_PREFIX}$(date '+%Y-%m-%d')${BACKUP_SUFFIX}

oldest_backup=${DEST_DIR}/$(ls "${DEST_DIR}" --sort=time | tail -n1)

pg_dump -Fc "${DB_NAME}" >"${new_backup}"
echo "Created backup: ${new_backup}"

if [ "${nbackups}" -ge "${MAX_BACKUPS}" ]; then
   if [ -n "${oldest_backup}" ]; then
       rm "${oldest_backup}"
       echo "Deleted old backup: ${oldest_backup}"
   fi
fi
