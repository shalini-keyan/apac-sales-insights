#!/bin/bash
# ANZ Sales Insights — Generate data and deploy to apacinsights.quick.shopify.io
# Run manually each week or schedule via launchd (Monday 8am)

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="/tmp/anz-sales-insights-deploy"
PYTHON=/Users/shalini.keyan/.local/bin/python3.12

echo "📊 Generating rep data..."
$PYTHON "$SCRIPT_DIR/generate-rep-data.py"

echo ""
echo "📁 Preparing deploy folder..."
rm -rf "$DEPLOY_DIR"
mkdir -p "$DEPLOY_DIR/data"
cp "$SCRIPT_DIR/index.html"               "$DEPLOY_DIR/index.html"
cp "$SCRIPT_DIR/data/reps.json"           "$DEPLOY_DIR/data/reps.json"
cp "$(dirname "$SCRIPT_DIR")/hot-this-week.json" "$DEPLOY_DIR/hot-this-week.json"

echo ""
echo "🚀 Deploying to apacinsights.quick.shopify.io..."
quick deploy "$DEPLOY_DIR" apacinsights

echo ""
echo "✅ Done! Visit https://apacinsights.quick.shopify.io"

echo ""
echo "📬 Sending weekly Slack DMs to ANZ reps..."
$PYTHON "$SCRIPT_DIR/send-weekly-dms.py"
