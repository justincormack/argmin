#!/bin/sh

set -eu

tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT INT TERM

disable_legal_hold() {
  bucket=$1
  key=$2
  version_id=$3

  [ -n "$version_id" ] || return 0
  [ "$version_id" != "null" ] || return 0

  aws s3api put-object-legal-hold \
    --bucket "$bucket" \
    --key "$key" \
    --version-id "$version_id" \
    --legal-hold Status=OFF \
    >/dev/null 2>&1 || true
}

delete_version() {
  bucket=$1
  key=$2
  version_id=$3

  if [ -z "$version_id" ]; then
    return 1
  fi

  aws s3api delete-object \
    --bucket "$bucket" \
    --key "$key" \
    --version-id "$version_id" \
    --bypass-governance-retention \
    >/dev/null 2>&1 && return 0

  aws s3api delete-object \
    --bucket "$bucket" \
    --key "$key" \
    --version-id "$version_id" \
    >/dev/null 2>&1 && return 0

  if [ "$version_id" = "null" ]; then
    aws s3api delete-object \
      --bucket "$bucket" \
      --key "$key" \
      --bypass-governance-retention \
      >/dev/null 2>&1 && return 0

    aws s3api delete-object \
      --bucket "$bucket" \
      --key "$key" \
      >/dev/null 2>&1 && return 0
  fi

  return 1
}

delete_marker() {
  bucket=$1
  key=$2
  version_id=$3

  aws s3api delete-object \
    --bucket "$bucket" \
    --key "$key" \
    --version-id "$version_id" \
    >/dev/null 2>&1
}

delete_current_object() {
  bucket=$1
  key=$2

  aws s3api delete-object \
    --bucket "$bucket" \
    --key "$key" \
    --bypass-governance-retention \
    >/dev/null 2>&1 && return 0

  aws s3api delete-object \
    --bucket "$bucket" \
    --key "$key" \
    >/dev/null 2>&1
}

bucketlist="$tmpdir/bucketlist"
aws s3api list-buckets --query 'Buckets[].Name' --output text \
  | tr '\t' '\n' \
  | grep '^claude-s3-' > "$bucketlist" || true

echo "$(grep -c . "$bucketlist" 2>/dev/null || true) buckets to go..."

failed=0

for bucket in $(head -100 "$bucketlist")
do
  versions_file="$tmpdir/$bucket-versions.json"
  version_rows="$tmpdir/$bucket-version-rows.txt"
  marker_rows="$tmpdir/$bucket-marker-rows.txt"
  objects_file="$tmpdir/$bucket-objects.json"
  object_rows="$tmpdir/$bucket-object-rows.txt"
  bucket_failed=0

  aws s3api list-object-versions \
    --bucket "$bucket" \
    --output json \
    > "$versions_file"

  jq -r '.Versions[]? | @base64' "$versions_file" > "$version_rows"
  jq -r '.DeleteMarkers[]? | @base64' "$versions_file" > "$marker_rows"

  versions_count=$(jq '(.Versions // []) | length' "$versions_file")
  markers_count=$(jq '(.DeleteMarkers // []) | length' "$versions_file")

  if [ "$versions_count" -gt 0 ]; then
    while IFS= read -r row
    do
      [ -n "$row" ] || continue

      entry=$(printf '%s' "$row" | base64 -d)
      key=$(printf '%s' "$entry" | jq -r '.Key')
      version_id=$(printf '%s' "$entry" | jq -r '.VersionId')

      disable_legal_hold "$bucket" "$key" "$version_id"

      if ! delete_version "$bucket" "$key" "$version_id"; then
        retention_mode=$(
          aws s3api get-object-retention \
            --bucket "$bucket" \
            --key "$key" \
            --version-id "$version_id" \
            --output json \
            2>/dev/null \
            | jq -r '.Retention.Mode // empty' 2>/dev/null || true
        )

        if [ "$retention_mode" = "COMPLIANCE" ]; then
          echo "Failed to delete $bucket/$key@$version_id: compliance retention is still active" >&2
        else
          echo "Failed to delete $bucket/$key@$version_id" >&2
        fi

        bucket_failed=1
      fi
    done < "$version_rows"
  fi

  if [ "$markers_count" -gt 0 ]; then
    while IFS= read -r row
    do
      [ -n "$row" ] || continue

      entry=$(printf '%s' "$row" | base64 -d)
      key=$(printf '%s' "$entry" | jq -r '.Key')
      version_id=$(printf '%s' "$entry" | jq -r '.VersionId')

      if ! delete_marker "$bucket" "$key" "$version_id"; then
        echo "Failed to delete delete-marker $bucket/$key@$version_id" >&2
        bucket_failed=1
      fi
    done < "$marker_rows"
  fi

  aws s3api list-objects-v2 \
    --bucket "$bucket" \
    --output json \
    > "$objects_file"

  jq -r '.Contents[]? | @base64' "$objects_file" > "$object_rows"

  while IFS= read -r row
  do
    [ -n "$row" ] || continue

    entry=$(printf '%s' "$row" | base64 -d)
    key=$(printf '%s' "$entry" | jq -r '.Key')

    if ! delete_current_object "$bucket" "$key"; then
      echo "Failed to delete current object $bucket/$key" >&2
      bucket_failed=1
    fi
  done < "$object_rows"

  if [ "$bucket_failed" -eq 0 ] && aws s3api delete-bucket --bucket "$bucket" >/dev/null 2>&1; then
    echo "Deleted $bucket"
  else
    echo "Skipped deleting $bucket" >&2
    failed=1
  fi
done

exit "$failed"
