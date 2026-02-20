#!/bin/bash
set -e

echo "========================================"
echo "  RushTI Website Build Script"
echo "========================================"
echo ""

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if we're in the right directory
if [ ! -f "mkdocs.yml" ]; then
    echo "Error: mkdocs.yml not found. Please run this script from the project root."
    exit 1
fi

# Build homepage
echo -e "${BLUE}[1/3] Building homepage...${NC}"
cd site/homepage
if [ ! -d "node_modules" ]; then
    echo -e "${YELLOW}      Installing dependencies...${NC}"
    npm install
fi
npm run build
cd ../..
echo -e "${GREEN}      Done!${NC}"
echo ""

# Build documentation
echo -e "${BLUE}[2/3] Building documentation...${NC}"
mkdocs build --site-dir mkdocs_site
echo -e "${GREEN}      Done!${NC}"
echo ""

# Merge builds
# Note: We put everything under /rushti/ to match the GitHub Pages URL structure
echo -e "${BLUE}[3/3] Merging builds...${NC}"
rm -rf _site
mkdir -p _site/rushti/docs
cp -r site/homepage/dist/* _site/rushti/
cp -r mkdocs_site/* _site/rushti/docs/
echo -e "${GREEN}      Done!${NC}"
echo ""

echo "========================================"
echo -e "${GREEN}  Build Complete!${NC}"
echo "========================================"
echo ""
echo -e "${BLUE}Output directory:${NC} _site/"
echo ""
echo "Structure:"
echo "  _site/"
echo "  └── rushti/"
echo "      ├── index.html      (Homepage)"
echo "      ├── assets/         (Homepage assets)"
echo "      └── docs/           (MkDocs documentation)"
echo ""
echo -e "${BLUE}To preview locally:${NC}"
echo "  cd _site && python3 -m http.server 8000"
echo "  Open http://localhost:8000/rushti/"
echo ""
