import base64, sys
b = sys.stdin.read()
d = base64.b64decode(b).decode()
with open("e2e-playwright-test.js", "w") as out:
    out.write(d)
print("Written", len(d), "bytes")
