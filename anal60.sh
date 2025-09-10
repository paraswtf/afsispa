#!/usr/bin/env bash
set -euo pipefail

# ---------------- CONFIG ----------------
SONG_DIR="../60clip"         # directory with your test clips
GO_PROG="accurate.go"       # Go program file
INDEX_FILE="index2.gob.gz"   # index to query against
NUM_TEST=100                # number of random songs to test
SPECIFIC_SONG=""            # set path to test one song, leave empty for random batch
# ----------------------------------------

FILES=()

# Helper: normalize string (lowercase, remove punctuation, collapse spaces)
normalize() {
    local s="$1"
    echo "$s" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9 ]+/ /g' | tr -s ' ' | sed -E 's/^ //;s/ $//'
}

# Portable token overlap matcher (no associative arrays)
# returns 1 if overlap ratio >= 0.60 else 0
token_overlap_match() {
    local a_norm="$1"
    local b_norm="$2"

    # split into arrays (bash supports read -a)
    IFS=' ' read -r -a a_tokens <<< "$a_norm"
    IFS=' ' read -r -a b_tokens <<< "$b_norm"

    if (( ${#a_tokens[@]} == 0 )); then
        echo 0
        return
    fi

    local match_count=0
    local at bt
    for at in "${a_tokens[@]}"; do
        for bt in "${b_tokens[@]}"; do
            if [[ "$at" == "$bt" ]]; then
                ((match_count++))
                break
            fi
        done
    done

    local ratio
    ratio=$(awk -v n="$match_count" -v m="${#a_tokens[@]}" 'BEGIN { if (m==0) print 0; else printf "%.3f", n/m }')
    awk -v r="$ratio" 'BEGIN { if (r+0 >= 0.60) print 1; else print 0 }'
}

# Pick files
if [[ -n "${SPECIFIC_SONG:-}" ]]; then
    FILES=("$SPECIFIC_SONG")
    echo "======================================"
    echo "Running batch test for SPECIFIC song: $SPECIFIC_SONG"
    echo "======================================"
else
    while IFS= read -r file; do
        FILES+=("$file")
    done < <(find "$SONG_DIR" -type f -name "*.flac" | sort -R | head -n "$NUM_TEST" || true)
    echo "======================================"
    echo "Running batch test for ${#FILES[@]} random songs from $SONG_DIR (requested $NUM_TEST)"
    echo "======================================"
fi

# Guard: no files found
if (( ${#FILES[@]} == 0 )); then
    echo "No input files found. Check SONG_DIR and SPECIFIC_SONG."
    exit 1
fi

# Guard: index file must exist
if [[ ! -f "$INDEX_FILE" ]]; then
    echo "Index file '$INDEX_FILE' not found."
    echo "Create it first with: go run $GO_PROG index <folder> $INDEX_FILE"
    exit 1
fi

# Initialize counters
top_counts=(0 0 0 0 0 0 0 0 0 0)  # ranks 1–10
mrr_sum=0
total_songs=${#FILES[@]}

# Start batch timer
batch_start=$(date +%s)

current=1
for f in "${FILES[@]}"; do
    progressPercentage=$(awk "BEGIN {printf \"%.0f\", $current/$total_songs*100}")
    echo "======================================"
    echo "SONG: $current/$total_songs     |     $progressPercentage%"
    echo "======================================"
    echo "Testing: $f"

    # Run the Go program with query
    if ! output=$(go run "$GO_PROG" query "$INDEX_FILE" "$f" 2>&1); then
        echo "go run returned non-zero exit code. Output:"
        echo "$output"
        ((current++))
        continue
    fi

    echo "$output"

    # parse top 10 matches: first try numbered lines, else try header fallback
    top_lines=()
    while IFS= read -r line; do
        top_lines+=("$line")
    done < <(echo "$output" | sed -n '/^[[:space:]]*[0-9]\{1,2\}\./p' | sed -n '1,10p' || true)

    if (( ${#top_lines[@]} == 0 )); then
        while IFS= read -r line; do
            top_lines+=("$line")
        done < <(echo "$output" | grep -A10 -i "Top 10 matches:" | tail -n 10 || true)
    fi

    top_files=()
    for line in "${top_lines[@]}"; do
        candidate=$(echo "$line" | sed -E 's/^[[:space:]]*[0-9]+\)\s*//; s/^[[:space:]]*[0-9]+\.\s*//')
        candidate=$(echo "$candidate" | sed -E 's/score=[^ ]+//; s/conf=[^ ]+//; s/delta=[^ ]+//')
        candidate=$(echo "$candidate" | sed -E 's/^[[:space:]]*-?[[:space:]]*//; s/^\s+//;s/\s+$//')
        top_files+=("$candidate")
    done

    # expected fingerprint from filename
    song_base=$(basename "$f")
    song_base=${song_base%.*}
    song_base=${song_base/_clip10/}
    expected_fp="${song_base}.fp"
    expected_norm=$(normalize "$song_base")

    # find rank using exact, substring and fuzzy token overlap
    rank=0
    for i in "${!top_files[@]}"; do
        cand="${top_files[i]}"
        cand_trim=$(echo "$cand" | sed -E 's/^[[:space:]]*//;s/[[:space:]]*$//')
        # exact file/basename match
        if [[ "$cand_trim" == "$expected_fp" ]] || [[ "$cand_trim" == "$song_base" ]]; then
            rank=$((i+1)); break
        fi
        # substring match (case-insensitive)
        if echo "$cand_trim" | tr '[:upper:]' '[:lower:]' | grep -qF "$(echo "$song_base" | tr '[:upper:]' '[:lower:]')"; then
            rank=$((i+1)); break
        fi
        # fuzzy token overlap
        cand_norm=$(normalize "$cand_trim")
        if [[ $(token_overlap_match "$expected_norm" "$cand_norm") -eq 1 ]]; then
            rank=$((i+1)); break
        fi
    done

    if (( rank >= 1 && rank <= 10 )); then ((top_counts[rank-1]++)); fi
    if (( rank > 0 )); then mrr_sum=$(awk -v s="$mrr_sum" -v r="$rank" 'BEGIN {printf "%.12f", s + 1.0/r}'); fi

    if (( rank > 0 )); then
        echo "=> Expected (${expected_fp}) FOUND at rank ${rank}."
    else
        echo "=> Expected (${expected_fp}) NOT found in Top 10."
    fi

    ((current++))
done

# results
mrr=$(awk -v s="$mrr_sum" -v t="$total_songs" 'BEGIN { if (t==0) print "0"; else printf "%.6f", s/t }')
avg_rank=$(awk -v s="$mrr_sum" -v t="$total_songs" 'BEGIN { if (s>0) printf "%.3f", t/s; else print "N/A" }')

batch_end=$(date +%s)
batch_duration=$((batch_end - batch_start))

echo "======================================"
echo "Results over $total_songs song(s):"
for i in {0..9}; do
    echo "Top $((i+1)) matches: ${top_counts[i]}/$total_songs"
done
echo "MRR: $mrr"
echo "Avg rank (based on MRR): $avg_rank"
printf "Total batch time: %02d:%02d:%02d\n" $((batch_duration/3600)) $(((batch_duration%3600)/60)) $((batch_duration%60))
echo "======================================"