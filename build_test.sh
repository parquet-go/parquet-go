#!/bin/bash

echo "Testing ARM64 assembly fix..."
echo "Working directory: $(pwd)"
echo ""

echo "1. Testing Go build..."
if go build -v .; then
    echo "✅ Build succeeded!"
    echo ""
    
    echo "2. Testing specific dictionary test..."
    if go test -run "TestDictionary/INT32/N=1" -v .; then
        echo "✅ Dictionary test passed!"
        echo ""
        echo "✅ ARM64 assembly fix verified successfully!"
    else
        echo "❌ Dictionary test failed"
        exit 1
    fi
else
    echo "❌ Build failed"
    exit 1
fi