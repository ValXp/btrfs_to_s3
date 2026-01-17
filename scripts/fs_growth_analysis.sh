#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Estimate data growth from file timestamps and sizes.

This is intended as a quick way to approximate how much data is added/changed per
week/month so you can reason about btrfs-send incremental sizes and S3 storage
tiers. For "append-only" datasets where files are created once and rarely
modified, bucketing by ctime tends to work better than mtime (since mtime is
often preserved by copy tools like rsync -a).

Usage:
  fs_growth_analysis.sh [options] <path> [<path> ...]

Options:
  --days N                 Lookback window in days (default: 180)
  --time {ctime|mtime}     Timestamp to bucket by (default: ctime)
  --chunk-gib N            Also estimate number of ~N GiB chunks per bucket
  --exclude-dir NAME       Exclude directories with this basename (repeatable)
  --no-default-excludes    Don't exclude common snapshot directories
  --cross-filesystems      Allow crossing filesystem boundaries (default: off)
  --no-progress            Don't print periodic progress to stderr
  -h, --help               Show this help

Output:
  For each <path>, prints totals plus per-ISO-week and per-month bucket sizes.
  Also prints a "what-if" estimate for incremental strategy:
    - chained: each weekly incremental is just that week's changes
    - cumulative: each weekly incremental is "since last full" (additive-only model)

Notes / caveats:
  - This is an estimate; btrfs send streams include metadata, and may be smaller
    or larger than summed file sizes depending on workload (reflinks, renames,
    deletes, compression, etc.).
  - If you have snapshots mounted inside the path being scanned, you will
    double-count data. Point this at your live subvolume mountpoints and/or use
    --exclude-dir to prune snapshot directories.

Example:
  sudo ./scripts/fs_growth_analysis.sh --days 180 --time ctime --chunk-gib 200 /mnt/pool/data
EOF
}

days=180
time_basis="ctime"
chunk_gib=""
cross_filesystems="false"
show_progress="true"
default_excludes="true"
declare -a exclude_dirs=()

die() {
  echo "error: $*" >&2
  exit 2
}

is_uint() {
  [[ "${1:-}" =~ ^[0-9]+$ ]]
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --days)
      [[ $# -ge 2 ]] || die "--days requires an argument"
      is_uint "$2" || die "--days must be an integer"
      days="$2"
      shift 2
      ;;
    --time)
      [[ $# -ge 2 ]] || die "--time requires an argument"
      case "$2" in
        ctime|mtime) time_basis="$2" ;;
        *) die "--time must be 'ctime' or 'mtime'" ;;
      esac
      shift 2
      ;;
    --chunk-gib)
      [[ $# -ge 2 ]] || die "--chunk-gib requires an argument"
      [[ "${2:-}" =~ ^[0-9]+([.][0-9]+)?$ ]] || die "--chunk-gib must be a number"
      chunk_gib="$2"
      shift 2
      ;;
    --exclude-dir)
      [[ $# -ge 2 ]] || die "--exclude-dir requires an argument"
      exclude_dirs+=("$2")
      shift 2
      ;;
    --no-default-excludes)
      default_excludes="false"
      shift
      ;;
    --cross-filesystems)
      cross_filesystems="true"
      shift
      ;;
    --no-progress)
      show_progress="false"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    -*)
      die "unknown option: $1"
      ;;
    *)
      break
      ;;
  esac
done

[[ $# -ge 1 ]] || { usage; exit 2; }

command -v find >/dev/null || die "missing dependency: find"
command -v awk >/dev/null || die "missing dependency: awk"
command -v sort >/dev/null || die "missing dependency: sort"
command -v date >/dev/null || die "missing dependency: date"

if ! find . -maxdepth 0 -printf '' >/dev/null 2>&1; then
  die "find does not support -printf (need GNU findutils; run this on the Proxmox host)"
fi

if ! date -d "@0" +%s >/dev/null 2>&1; then
  die "date does not support -d '@<epoch>' (need GNU coreutils; run this on the Proxmox host)"
fi

now_epoch="$(date +%s)"
cutoff_epoch="$(( now_epoch - (days * 86400) ))"
cutoff_human="$(date -d "@${cutoff_epoch}" +%F 2>/dev/null || true)"

if [[ -z "$cutoff_human" ]]; then
  cutoff_human="(epoch ${cutoff_epoch})"
fi

if [[ "$default_excludes" == "true" ]]; then
  exclude_dirs+=(".snapshots" "@snapshots" ".btrfs" ".btrfs_snapshots")
fi

printf_bytes_gib() {
  awk -v b="$1" 'BEGIN { printf "%.2f", b / (1024*1024*1024) }'
}

run_for_path() {
  local root="$1"
  [[ -d "$root" ]] || die "not a directory: $root"

  local time_fmt
  case "$time_basis" in
    ctime) time_fmt='%C@' ;;
    mtime) time_fmt='%T@' ;;
    *) die "internal error: unexpected time basis: $time_basis" ;;
  esac

  local -a find_cmd
  find_cmd=(find "$root")
  if [[ "$cross_filesystems" != "true" ]]; then
    find_cmd+=(-xdev)
  fi

  if [[ ${#exclude_dirs[@]} -gt 0 ]]; then
    find_cmd+=( \( -type d \( )
    local first="true"
    local d
    for d in "${exclude_dirs[@]}"; do
      if [[ "$first" == "true" ]]; then
        first="false"
      else
        find_cmd+=(-o)
      fi
      find_cmd+=(-name "$d")
    done
    find_cmd+=( \) -prune \) -o )
  fi

  find_cmd+=(-type f -printf "${time_fmt} %s\n")

  local tmp_dir
  tmp_dir="$(mktemp -d)"
  (
    trap 'rm -rf "$tmp_dir"' EXIT

    local raw_tsv="${tmp_dir}/raw.tsv"
    local err_log="${tmp_dir}/find_errors.log"

    # Aggregate into a small TSV: summary + weekly + monthly buckets.
    "${find_cmd[@]}" 2>"$err_log" | awk \
      -v cutoff="$cutoff_epoch" \
      -v now="$now_epoch" \
      -v show_progress="$show_progress" \
      '
        BEGIN {
          scanned_files = 0
          scanned_bytes = 0
          recent_files = 0
          recent_bytes = 0
        }
        {
          ts = $1 + 0
          sz = $2 + 0
          scanned_files++
          scanned_bytes += sz

          if (ts >= cutoff) {
            recent_files++
            recent_bytes += sz

            week_key = strftime("%G-W%V", ts)
            month_key = strftime("%Y-%m", ts)

            week_files[week_key]++
            week_bytes[week_key] += sz
            month_files[month_key]++
            month_bytes[month_key] += sz
          }

          if (show_progress == "true" && (scanned_files % 1000000) == 0) {
            printf("... processed %d files\n", scanned_files) > "/dev/stderr"
          }
        }
        END {
          printf("S\t%d\t%.0f\t%d\t%.0f\n", scanned_files, scanned_bytes, recent_files, recent_bytes)
          for (k in week_bytes) {
            printf("W\t%s\t%d\t%.0f\n", k, week_files[k], week_bytes[k])
          }
          for (k in month_bytes) {
            printf("M\t%s\t%d\t%.0f\n", k, month_files[k], month_bytes[k])
          }
        }
      ' >"$raw_tsv"

    echo
    echo "Path: $root"
    echo "Lookback: ${days} days (since ${cutoff_human})"
    echo "Time basis: ${time_basis}"
    if [[ ${#exclude_dirs[@]} -gt 0 ]]; then
      echo "Excluded dirs (by basename): ${exclude_dirs[*]}"
    fi
    if [[ -s "$err_log" ]]; then
      echo "Find warnings/errors (first 20 lines):"
      sed -n '1,20p' "$err_log"
    fi

    local summary_line
    summary_line="$(awk -F '\t' '$1=="S"{print; exit}' "$raw_tsv")"
    [[ -n "$summary_line" ]] || die "failed to parse summary output"

    local scanned_files scanned_bytes recent_files recent_bytes
    scanned_files="$(awk -F '\t' '{print $2}' <<<"$summary_line")"
    scanned_bytes="$(awk -F '\t' '{print $3}' <<<"$summary_line")"
    recent_files="$(awk -F '\t' '{print $4}' <<<"$summary_line")"
    recent_bytes="$(awk -F '\t' '{print $5}' <<<"$summary_line")"

    echo "Total files scanned: ${scanned_files}"
    echo "Total logical size: $(printf_bytes_gib "$scanned_bytes") GiB"
    echo "Recent (lookback) files: ${recent_files}"
    echo "Recent (lookback) logical size: $(printf_bytes_gib "$recent_bytes") GiB"

    local chunk_bytes=""
    if [[ -n "$chunk_gib" ]]; then
      chunk_bytes="$(awk -v g="$chunk_gib" 'BEGIN { printf "%.0f", g * 1024*1024*1024 }')"
      echo "Chunk size: ${chunk_gib} GiB (~${chunk_bytes} bytes)"
    fi

    echo
    echo "Weekly (ISO) breakdown:"
    if [[ -n "$chunk_bytes" ]]; then
      printf "%-10s %12s %12s %12s\n" "Week" "Files" "GiB" "Chunks"
    else
      printf "%-10s %12s %12s\n" "Week" "Files" "GiB"
    fi

    awk -F '\t' '$1=="W"{print $2 "\t" $3 "\t" $4}' "$raw_tsv" \
      | sort -k1,1 \
      | awk -F '\t' -v chunk_bytes="$chunk_bytes" '
          function gib(b) { return b / (1024*1024*1024) }
          function ceil_div(a, b) { return int((a + b - 1) / b) }
          {
            week = $1
            files = $2 + 0
            bytes = $3 + 0
            if (chunk_bytes != "") {
              chunks = ceil_div(bytes, chunk_bytes + 0)
              printf "%-10s %12d %12.2f %12d\n", week, files, gib(bytes), chunks
            } else {
              printf "%-10s %12d %12.2f\n", week, files, gib(bytes)
            }
          }
        '

    # What-if totals based on the weekly buckets (sorted chronologically).
    local whatif
    whatif="$(awk -F '\t' '$1=="W"{print $2 "\t" $4}' "$raw_tsv" \
      | sort -k1,1 \
      | awk -F '\t' '
          BEGIN { chained = 0; cumulative = 0; running = 0; weeks = 0 }
          {
            weeks++
            bytes = $2 + 0
            chained += bytes
            running += bytes
            cumulative += running
          }
          END {
            printf "%d\t%.0f\t%.0f\n", weeks, chained, cumulative
          }
        ')"

    local weeks_n chained_bytes cumulative_bytes
    weeks_n="$(awk -F '\t' '{print $1}' <<<"$whatif")"
    chained_bytes="$(awk -F '\t' '{print $2}' <<<"$whatif")"
    cumulative_bytes="$(awk -F '\t' '{print $3}' <<<"$whatif")"

    echo
    echo "Incremental strategy what-if (over ${weeks_n} weeks within lookback):"
    echo "  chained (weekly vs previous weekly): $(printf_bytes_gib "$chained_bytes") GiB uploaded"
    echo "  cumulative (weekly vs last full, additive-only model): $(printf_bytes_gib "$cumulative_bytes") GiB uploaded"

    echo
    echo "Monthly breakdown:"
    if [[ -n "$chunk_bytes" ]]; then
      printf "%-7s %12s %12s %12s\n" "Month" "Files" "GiB" "Chunks"
    else
      printf "%-7s %12s %12s\n" "Month" "Files" "GiB"
    fi

    awk -F '\t' '$1=="M"{print $2 "\t" $3 "\t" $4}' "$raw_tsv" \
      | sort -k1,1 \
      | awk -F '\t' -v chunk_bytes="$chunk_bytes" '
          function gib(b) { return b / (1024*1024*1024) }
          function ceil_div(a, b) { return int((a + b - 1) / b) }
          {
            month = $1
            files = $2 + 0
            bytes = $3 + 0
            if (chunk_bytes != "") {
              chunks = ceil_div(bytes, chunk_bytes + 0)
              printf "%-7s %12d %12.2f %12d\n", month, files, gib(bytes), chunks
            } else {
              printf "%-7s %12d %12.2f\n", month, files, gib(bytes)
            }
          }
        '
  )
}

for path in "$@"; do
  run_for_path "$path"
done
